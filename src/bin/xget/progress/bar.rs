//! The live segmented bar: an aggregate header over a per-chunk line, two-tone (verified vs
//! received-ahead), scaled to the terminal.

use core::cell::RefCell;
use core::time::Duration;

use xget::Progress;
use xprogress::{AnsiColor, Bar, DrawTarget, Style};

use super::{DIM, Meter};
use crate::DisplayFormat;

/// The bar takes at most this fraction of the terminal width, so it scales with the screen instead of
/// stretching across it. Capped by [`BAR_MAX`] and floored so it never vanishes.
const BAR_MAX_PCT: u16 = 40;
/// The bar never grows past this many cells, however wide the terminal, keeping the stats in focus.
const BAR_MAX: u16 = 48;
/// How many retry notices stay parked above the bar. Past this the oldest is dropped: the bar is the
/// primary readout and must stay on screen, and the run summary at the end carries the full count.
const MAX_NOTICES: usize = 5;

// Styles for the live readout text. anstyle renders each with its own reset, and the codes have zero
// display width, so they never affect the bar's own width accounting.
const GREEN: anstyle::Style = anstyle::Style::new()
    .fg_color(Some(anstyle::Color::Ansi(AnsiColor::Green)))
    .bold();
const BLUE: anstyle::Style = anstyle::Style::new()
    .fg_color(Some(anstyle::Color::Ansi(AnsiColor::BrightBlue)))
    .bold();
const YELLOW: anstyle::Style = anstyle::Style::new()
    .fg_color(Some(anstyle::Color::Ansi(AnsiColor::Yellow)))
    .bold();

/// The live bar. With a handful of chunks it draws an aggregate header line over a per-chunk line, both
/// two-tone (done vs received-ahead); with one chunk or many it collapses to a single aggregate bar.
/// The lead comes from bytes received off the network, the done from bytes written and hashed, so the
/// shaded gap between them is the buffered-ahead window. Drawn in place and throttled.
pub(crate) struct BarProgress {
    inner: RefCell<Option<Live>>,
    format: DisplayFormat,
}

struct Live {
    /// One segment per chunk: each segment's lead is bytes received, its done is bytes verified. The
    /// aggregate header is derived from these same segments (summed lead and done), so there is one
    /// source of truth, not a second bar to keep in step.
    bar: Bar,
    target: DrawTarget,
    width: u16,
    multi: bool,
    prev_lines: usize,
    meter: Meter,
    format: DisplayFormat,
    /// The chunk count, for a retry line's `chunk N/total`.
    chunks: usize,
    /// Whether the terminal renders unicode, so a retry line's marker and separators match the bar.
    unicode: bool,
    /// Retry notices parked above the bar, oldest first. The interposition is a layout policy, so it lives
    /// here rather than in `xprogress`: the crate owns one block-drawing primitive, the bin decides what
    /// goes in the block. Bounded so a mass-retry cannot push the bar off the top of the screen.
    notices: Vec<String>,
}

impl BarProgress {
    pub(super) fn new(format: DisplayFormat) -> Self {
        Self {
            inner: RefCell::new(None),
            format,
        }
    }
}

impl Progress for BarProgress {
    fn start(&self, chunks: &[u64]) {
        // A two-line view is worth it for a few chunks; one chunk has nothing to aggregate and twenty
        // would not fit, so both collapse to a single aggregate bar.
        let multi = (2..20).contains(&chunks.len());
        // On a UTF-8 terminal, a pip/rich color-only two-tone: one solid heavy line throughout, the
        // regions told apart by hue (bright cyan done, dimmer cyan buffered-ahead, gray track), with a
        // half-cell head and a light chunk separator. On a terminal that cannot render those glyphs,
        // fall back to a plain-ascii bar (told apart by glyph too). The cyan colors are kept either way,
        // since ANSI color is far more widely supported than the box-drawing glyphs.
        let unicode = terminal_supports_unicode();
        let base = if unicode {
            Style::default()
                .with_filler('━')
                .with_leader('━')
                .with_blank('━')
                .with_header('╸')
                .with_separator('┆')
        } else {
            Style::ascii().with_separator('|')
        };
        let style = base
            .with_color(AnsiColor::BrightCyan)
            .with_lead_color(AnsiColor::Cyan)
            .with_blank_color(AnsiColor::BrightBlack);
        let total: u64 = chunks.iter().sum();
        let bar = Bar::new(chunks.iter().copied()).with_style(style);
        let target = DrawTarget::from_env();
        // Fill the space beside the stats, but never past a fraction of the screen, so the bar scales
        // with the terminal up to a cap and only shrinks past that when the stats would clip.
        let columns = target.columns().unwrap_or(80);
        let reserve = if multi { 34 } else { 52 };
        let cap = (columns.saturating_mul(BAR_MAX_PCT) / 100).min(BAR_MAX);
        let width = cap.min(columns.saturating_sub(reserve)).max(8);
        let mut slot = self.inner.borrow_mut();
        *slot = Some(Live {
            bar,
            target,
            width,
            multi,
            prev_lines: 0,
            meter: Meter::new(total),
            format: self.format,
            chunks: chunks.len(),
            unicode,
            notices: Vec::new(),
        });
        if let Some(live) = slot.as_mut() {
            redraw(live);
        }
    }

    fn restore(&self, present: &[u64]) {
        if let Some(live) = self.inner.borrow_mut().as_mut() {
            // Open where the last run stopped: each already-present chunk fills its lead (downloaded,
            // not yet verified). The verify pass then sweeps done up through them. These bytes skip the
            // speedometer, so a resume does not open with a phantom instant-speed spike.
            for (index, &bytes) in present.iter().enumerate() {
                if bytes > 0 {
                    live.bar.advance_lead(index, bytes);
                    live.meter.restore(bytes);
                }
            }
            redraw(live);
        }
    }

    fn received(&self, index: usize, bytes: u64) {
        if let Some(live) = self.inner.borrow_mut().as_mut() {
            // Downloaded but not yet verified: buffered-ahead (lead) on this chunk's segment. The
            // readout tracks download progress, so the meter counts received bytes.
            live.bar.advance_lead(index, bytes);
            live.meter.add(bytes);
            if live.meter.ready(Duration::from_millis(60)) {
                redraw(live);
            }
        }
    }

    fn wrote(&self, index: usize, bytes: u64) {
        if let Some(live) = self.inner.borrow_mut().as_mut() {
            // Verified in order: confirmed (done) on this chunk's segment; the aggregate's contiguous
            // prefix is the sum of the segments' done, so it follows for free.
            live.bar.advance(index, bytes);
            if live.meter.ready(Duration::from_millis(60)) {
                redraw(live);
            }
        }
    }

    fn retry(&self, index: usize, retry: u32, max: u32, resume_from: u64, error: &str) {
        if let Some(live) = self.inner.borrow_mut().as_mut() {
            // Park the retry as a notice above the bar (only real retries reach here), so a stalled
            // download explains itself instead of a silently frozen bar. The notice is held here and
            // re-emitted by every frame, which is what keeps it on screen while the bar redraws under it.
            let line = retry_line(index, live, retry, max, resume_from, error);
            live.notices.push(line);
            if live.notices.len() > MAX_NOTICES {
                live.notices.remove(0);
            }
            redraw(live);
        }
    }

    fn finish(&self) {
        if let Some(live) = self.inner.borrow_mut().as_mut() {
            let block = frame(live);
            let _ = live.target.finish_block(&block, live.prev_lines);
        }
    }
}

/// The retry line drawn above the bar, e.g. `▸ chunk 3/5 · retry 2/10 · connection reset · from 3.4 MiB`.
/// The marker and separators track the bar's unicode/ascii choice; the marker and counts are dim-yellow,
/// the cause plain, the resume point dim.
fn retry_line(
    index: usize,
    live: &Live,
    retry: u32,
    max: u32,
    resume_from: u64,
    error: &str,
) -> String {
    let marker = if live.unicode { '▸' } else { '>' };
    let sep = if live.unicode { '·' } else { '-' };
    let size = live.format.size(resume_from);
    let chunks = live.chunks;
    let chunk = index + 1;
    format!(
        "  {}{marker} chunk {chunk}/{chunks} {sep} retry {retry}/{max}{} {sep} {error} {sep} {}from {size}{}",
        YELLOW.render(),
        YELLOW.render_reset(),
        DIM.render(),
        DIM.render_reset(),
    )
}

/// Redraw the bar block in place, remembering how many lines it drew for the next redraw.
fn redraw(live: &mut Live) {
    let block = frame(live);
    if let Ok(lines) = live.target.draw_block(&block, live.prev_lines) {
        live.prev_lines = lines;
    }
}

/// Compose the full block: retry notices parked above the bar, then the bar itself. The notices are
/// re-emitted on every frame, which is what keeps them on screen while the bar redraws underneath.
fn frame(live: &mut Live) -> String {
    let bar = bar_frame(live);
    if live.notices.is_empty() {
        return bar;
    }
    let mut block = String::with_capacity(bar.len() + 64 * live.notices.len());
    for notice in &live.notices {
        block.push_str(notice);
        block.push('\n');
    }
    block.push_str(&bar);
    block
}

/// The bar block itself: an aggregate header line over the per-chunk line when multi, otherwise a
/// single aggregate bar. The readout (percent, speed, eta, sizes) is written here; the `xprogress`
/// crate only renders the bars.
fn bar_frame(live: &mut Live) -> String {
    let format = live.format;
    // Read the meter behind a scoped `&mut` borrow: `rate`/`eta` now age the window to now, so they
    // mutate. The borrow is confined to this block so the bar fields below can be read afterward.
    let (rate, size) = {
        let meter = &mut live.meter;
        let pct = meter.percent();
        let speed = format.speed(meter.rate());
        let eta = format_eta(meter.eta());
        let done = format.size(meter.done);
        let total = format.size(meter.total);
        let rate = format!(
            "{}{pct:>3}%{}  {}{speed}{}  {}{eta}{}",
            GREEN.render(),
            GREEN.render_reset(),
            BLUE.render(),
            BLUE.render_reset(),
            YELLOW.render(),
            YELLOW.render_reset(),
        );
        let size = format!("{}{done}/{total}{}", GREEN.render(), GREEN.render_reset());
        (rate, size)
    };
    if live.multi {
        format!(
            "  {}┏{} {} {}┓{}  {rate}\n  {}┗{} {} {}┛{}  {size}",
            DIM.render(),
            DIM.render_reset(),
            live.bar.render_aggregate(live.width),
            DIM.render(),
            DIM.render_reset(),
            DIM.render(),
            DIM.render_reset(),
            live.bar.render(live.width),
            DIM.render(),
            DIM.render_reset(),
        )
    } else {
        format!(
            "  {}[{}{}{}]{}  {rate}  {size}",
            DIM.render(),
            DIM.render_reset(),
            live.bar.render_aggregate(live.width),
            DIM.render(),
            DIM.render_reset(),
        )
    }
}

/// Whether the terminal can render the heavy block and box-drawing glyphs the bar prefers. A UTF-8
/// locale is the usual signal; without one, fall back to a plain-ascii bar so the output stays legible.
fn terminal_supports_unicode() -> bool {
    ["LC_ALL", "LC_CTYPE", "LANG"]
        .into_iter()
        .find_map(|var| std::env::var(var).ok())
        .as_deref()
        .is_some_and(locale_is_utf8)
}

/// Whether a locale string names a UTF-8 encoding (e.g. `en_US.UTF-8`, `C.utf8`).
fn locale_is_utf8(locale: &str) -> bool {
    let locale = locale.to_ascii_uppercase();
    locale.contains("UTF-8") || locale.contains("UTF8")
}

/// A compact `1h2m` / `3m4s` / `5s` duration for the ETA readout.
fn format_eta(seconds: u64) -> String {
    if seconds >= 3600 {
        format!("{}h{}m", seconds / 3600, (seconds % 3600) / 60)
    } else if seconds >= 60 {
        format!("{}m{}s", seconds / 60, seconds % 60)
    } else {
        format!("{seconds}s")
    }
}

#[cfg(test)]
#[path = "bar_tests.rs"]
mod bar_tests;
