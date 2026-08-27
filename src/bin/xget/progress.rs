use core::cell::RefCell;
use core::time::Duration;
use std::time::Instant;

use libxget::Progress;
use xprogress::{Bar, Color, DrawTarget, Style};

use crate::speedometer::Speedometer;
use crate::{ProgressMode, fmt_size};

/// The bar takes at most this fraction of the terminal width, so it scales with the screen instead of
/// stretching across it. Capped by [`BAR_MAX`] and floored so it never vanishes.
const BAR_MAX_PCT: u16 = 40;
/// The bar never grows past this many cells, however wide the terminal, keeping the stats in focus.
const BAR_MAX: u16 = 48;

// ANSI colors for the live readout. Each code has zero display width, so it never affects the bar's
// own width accounting; it only tints the text that follows the bar.
const GREEN: &str = "\x1b[1;32m";
const BLUE: &str = "\x1b[1;94m";
const YELLOW: &str = "\x1b[1;33m";
pub(crate) const DIM: &str = "\x1b[2m";
pub(crate) const RESET: &str = "\x1b[0m";

/// The chosen progress reporter, dispatching to whichever mode was selected. Every variant reports the
/// same download; they differ only in how they render it.
pub(crate) enum Reporter {
    /// A live segmented bar with a size, speed, and ETA readout.
    Bar(BarProgress),
    /// A single updating text line.
    Plain(PlainProgress),
    /// One JSON event per update.
    Json(JsonProgress),
    /// No progress output.
    Silent,
}

impl Reporter {
    pub(crate) fn new(mode: ProgressMode, raw: bool) -> Self {
        match mode {
            ProgressMode::Bar | ProgressMode::Auto => Self::Bar(BarProgress::new(raw)),
            ProgressMode::Plain => Self::Plain(PlainProgress::new(raw)),
            ProgressMode::Json => Self::Json(JsonProgress::new()),
            ProgressMode::None => Self::Silent,
        }
    }
}

impl Progress for Reporter {
    fn start(&self, chunks: &[u64]) {
        match self {
            Self::Bar(bar) => bar.start(chunks),
            Self::Plain(plain) => plain.start(chunks),
            Self::Json(json) => json.start(chunks),
            Self::Silent => {}
        }
    }

    fn restore(&self, present: &[u64]) {
        match self {
            Self::Bar(bar) => bar.restore(present),
            Self::Plain(plain) => plain.restore(present),
            Self::Json(json) => json.restore(present),
            Self::Silent => {}
        }
    }

    fn received(&self, index: usize, bytes: u64) {
        match self {
            Self::Bar(bar) => bar.received(index, bytes),
            Self::Plain(plain) => plain.received(index, bytes),
            Self::Json(json) => json.received(index, bytes),
            Self::Silent => {}
        }
    }

    fn wrote(&self, index: usize, bytes: u64) {
        match self {
            Self::Bar(bar) => bar.wrote(index, bytes),
            Self::Plain(plain) => plain.wrote(index, bytes),
            Self::Json(json) => json.wrote(index, bytes),
            Self::Silent => {}
        }
    }

    fn finish(&self) {
        match self {
            Self::Bar(bar) => bar.finish(),
            Self::Plain(plain) => plain.finish(),
            Self::Json(json) => json.finish(),
            Self::Silent => {}
        }
    }
}

/// Running totals shared by the text-based reporters: how much of how many bytes have arrived, a
/// windowed speedometer over those bytes, and a throttle so a fast download does not spend its time
/// printing.
struct Meter {
    total: u64,
    done: u64,
    started: Instant,
    last: Instant,
    speed: Speedometer,
}

impl Meter {
    fn new(total: u64) -> Self {
        let now = Instant::now();
        Self {
            total,
            done: 0,
            started: now,
            last: now,
            speed: Speedometer::new(),
        }
    }

    /// Record `bytes` more of confirmed progress, feeding the speedometer too.
    fn add(&mut self, bytes: u64) {
        self.done += bytes;
        self.speed.add(bytes);
    }

    /// Account for `bytes` already present at the start of a resumed download. They count toward the
    /// readout total but not the speedometer, since they did not arrive over the network this run.
    fn restore(&mut self, bytes: u64) {
        self.done += bytes;
    }

    /// Whether enough time has passed since the last redraw to draw again.
    fn ready(&mut self, every: Duration) -> bool {
        let now = Instant::now();
        if now.duration_since(self.last) < every {
            return false;
        }
        self.last = now;
        true
    }

    /// Record `bytes` more and report whether enough time has passed to redraw.
    fn advance(&mut self, bytes: u64, every: Duration) -> bool {
        self.add(bytes);
        self.ready(every)
    }

    /// Bytes per second across the recent window.
    fn rate(&self) -> u64 {
        self.speed.rate()
    }

    /// Seconds until completion at the current rate, or `0` if unknown.
    fn eta(&self) -> u64 {
        let rate = self.rate();
        if rate > 0 && self.total > self.done {
            (self.total - self.done) / rate
        } else {
            0
        }
    }

    /// Percent complete, `0` when the total is unknown.
    fn percent(&self) -> u64 {
        self.done
            .saturating_mul(100)
            .checked_div(self.total)
            .unwrap_or(0)
    }

    /// Percent complete as a fraction, `0.0` when the total is unknown.
    fn percent_f64(&self) -> f64 {
        if self.total > 0 {
            self.done as f64 * 100.0 / self.total as f64
        } else {
            0.0
        }
    }

    /// The ETA as a `MM:SS` clock (minutes may exceed 59 for a long transfer).
    fn eta_clock(&self) -> String {
        let eta = self.eta();
        format!("{:02}:{:02}", eta / 60, eta % 60)
    }

    /// Milliseconds since the transfer started.
    fn elapsed_ms(&self) -> u128 {
        self.started.elapsed().as_millis()
    }
}

/// A single updating text line: `done / total (pct%)  speed  eta`, drawn in place on stderr and
/// throttled, with a final newline so the shell prompt lands cleanly.
pub(crate) struct PlainProgress {
    meter: RefCell<Option<Meter>>,
    raw: bool,
}

impl PlainProgress {
    fn new(raw: bool) -> Self {
        Self {
            meter: RefCell::new(None),
            raw,
        }
    }

    fn line(&self, meter: &Meter) -> String {
        format!(
            "\r\x1b[K{:>5.1}%  {}/{}  {}/s  ETA {}",
            meter.percent_f64(),
            fmt_size(meter.done, self.raw),
            fmt_size(meter.total, self.raw),
            fmt_size(meter.rate(), self.raw),
            meter.eta_clock(),
        )
    }
}

impl Progress for PlainProgress {
    fn start(&self, chunks: &[u64]) {
        let meter = Meter::new(chunks.iter().sum());
        eprint!("{}", self.line(&meter));
        let _ = std::io::Write::flush(&mut std::io::stderr());
        *self.meter.borrow_mut() = Some(meter);
    }

    fn restore(&self, present: &[u64]) {
        if let Some(meter) = self.meter.borrow_mut().as_mut() {
            meter.restore(present.iter().sum());
            eprint!("{}", self.line(meter));
            let _ = std::io::Write::flush(&mut std::io::stderr());
        }
    }

    fn received(&self, _index: usize, bytes: u64) {
        if let Some(meter) = self.meter.borrow_mut().as_mut() {
            if meter.advance(bytes, Duration::from_millis(200)) {
                eprint!("{}", self.line(meter));
                let _ = std::io::Write::flush(&mut std::io::stderr());
            }
        }
    }

    fn finish(&self) {
        if let Some(meter) = self.meter.borrow().as_ref() {
            eprintln!("{}", self.line(meter));
        }
    }
}

/// One JSON event per update, to stderr: a `start`, throttled `progress` events, and a `done`. The
/// numbers are always raw bytes so a consumer can format them itself.
pub(crate) struct JsonProgress {
    meter: RefCell<Option<Meter>>,
}

impl JsonProgress {
    fn new() -> Self {
        Self {
            meter: RefCell::new(None),
        }
    }
}

impl Progress for JsonProgress {
    fn start(&self, chunks: &[u64]) {
        *self.meter.borrow_mut() = Some(Meter::new(chunks.iter().sum()));
    }

    fn restore(&self, present: &[u64]) {
        if let Some(meter) = self.meter.borrow_mut().as_mut() {
            meter.restore(present.iter().sum());
        }
    }

    fn received(&self, _index: usize, bytes: u64) {
        if let Some(meter) = self.meter.borrow_mut().as_mut() {
            if meter.advance(bytes, Duration::from_millis(200)) {
                eprintln!(
                    r#"{{"event":"progress","bytes":{},"total":{},"percent":{:.1},"speed":{},"eta":{}}}"#,
                    meter.done,
                    meter.total,
                    meter.percent_f64(),
                    meter.rate(),
                    meter.eta()
                );
            }
        }
    }

    fn finish(&self) {
        if let Some(meter) = self.meter.borrow().as_ref() {
            eprintln!(
                r#"{{"event":"end","bytes":{},"total":{},"elapsed":{}}}"#,
                meter.done,
                meter.total,
                meter.elapsed_ms()
            );
        }
    }
}

/// The live bar. With a handful of chunks it draws an aggregate header line over a per-chunk line, both
/// two-tone (done vs received-ahead); with one chunk or many it collapses to a single aggregate bar.
/// The lead comes from bytes received off the network, the done from bytes written and hashed, so the
/// shaded gap between them is the buffered-ahead window. Drawn in place and throttled.
pub(crate) struct BarProgress {
    inner: RefCell<Option<Live>>,
    raw: bool,
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
    raw: bool,
}

impl BarProgress {
    fn new(raw: bool) -> Self {
        Self {
            inner: RefCell::new(None),
            raw,
        }
    }
}

impl Progress for BarProgress {
    fn start(&self, chunks: &[u64]) {
        // A two-line view is worth it for a few chunks; one chunk has nothing to aggregate and twenty
        // would not fit, so both collapse to a single aggregate bar.
        let multi = (2..20).contains(&chunks.len());
        // A pip/rich color-only two-tone: one solid heavy line throughout, the regions told apart by
        // hue, not glyph. Done is bright cyan, buffered-ahead a dimmer cyan, and the empty track gray,
        // with a half-cell head on the frontier and a light separator between chunks.
        let style = Style::default()
            .with_filler('━')
            .with_leader('━')
            .with_blank('━')
            .with_header('╸')
            .with_separator('┆')
            .with_color(Color::BrightCyan)
            .with_lead_color(Color::Cyan)
            .with_blank_color(Color::BrightBlack);
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
            raw: self.raw,
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

    fn finish(&self) {
        if let Some(live) = self.inner.borrow_mut().as_mut() {
            let block = frame(live);
            let _ = live.target.finish_block(&block, live.prev_lines);
        }
    }
}

/// Redraw the bar block in place, remembering how many lines it drew for the next redraw.
fn redraw(live: &mut Live) {
    let block = frame(live);
    if let Ok(lines) = live.target.draw_block(&block, live.prev_lines) {
        live.prev_lines = lines;
    }
}

/// Compose the bar block: an aggregate header line over the per-chunk line when multi, otherwise a
/// single aggregate bar. The readout (percent, speed, eta, sizes) is xget's to write; xprogress only
/// renders the bars.
fn frame(live: &Live) -> String {
    let meter = &live.meter;
    let pct = meter.percent();
    let speed = fmt_size(meter.rate(), live.raw);
    let eta = format_eta(meter.eta());
    let done = fmt_size(meter.done, live.raw);
    let total = fmt_size(meter.total, live.raw);
    let rate = format!("{GREEN}{pct:>3}%{RESET}  {BLUE}{speed}/s{RESET}  {YELLOW}{eta}{RESET}");
    let size = format!("{GREEN}{done}/{total}{RESET}");
    if live.multi {
        format!(
            "  {DIM}┏{RESET} {} {DIM}┓{RESET}  {rate}\n  {DIM}┗{RESET} {} {DIM}┛{RESET}  {size}",
            live.bar.render_aggregate(live.width),
            live.bar.render(live.width),
        )
    } else {
        format!(
            "  {DIM}[{RESET}{}{DIM}]{RESET}  {rate}  {size}",
            live.bar.render_aggregate(live.width),
        )
    }
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
