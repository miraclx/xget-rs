//! Explicit stderr-only xprogress drawing, with geometry independent of changing numbers.

use std::io::{self, Write as _};

use xget::transfer::{ByteCount, Length};
use xprogress::{AnsiColor, Bar, DrawTarget, Style};

use crate::failure::Failure;

pub(super) struct TerminalBar {
    target: DrawTarget,
    decoration: Decoration,
    theme: Theme,
    bar: Option<Bar>,
    length: Option<u64>,
    drawn: ByteCount,
    heading: Option<String>,
    restored: Option<String>,
    previous: Footprint,
}

#[derive(Clone, Copy)]
pub(super) enum Decoration {
    Bar,
    Plain,
}

/// Presets change ornament and palette only, never the meaning of a received frontier.
#[derive(Clone, Copy, Default, clap::ValueEnum)]
pub(super) enum RibbonStyle {
    #[default]
    Classic,
    Fine,
    Solid,
}

/// These row components contain only ASCII labels and xbytes numbers/units, never remote names.
pub(super) struct Row {
    pub(super) count: String,
    pub(super) detailed: String,
    pub(super) rate: Option<String>,
    pub(super) eta: Option<String>,
    pub(super) percent: Option<String>,
}

struct Layout {
    text: String,
    bar_cells: u16,
    updating: bool,
}

/// Owned rows use ASCII text or known single-cell ribbon glyphs, with only our own SGR decoration.
/// Retaining cell widths lets a shrinking, reflowing terminal clear the entire previous block.
#[derive(Default)]
struct Footprint(Vec<usize>);

/// The track changes only at a width-profile boundary, not when counts, rate or ETA grow.
#[derive(Clone, Copy)]
enum Profile {
    Wide,
    Regular,
    Narrow,
    Tiny,
}

#[derive(Clone, Copy)]
enum Glyphs {
    Unicode,
    Ascii,
}

#[derive(Clone, Copy)]
enum Color {
    Enabled,
    Disabled,
}

struct Theme {
    preset: RibbonStyle,
    glyphs: Glyphs,
    color: Color,
}

impl TerminalBar {
    pub(super) fn new(decoration: Decoration, preset: RibbonStyle) -> Self {
        Self {
            // Neither this target nor the Bar's own target may inherit the demo Dump override.
            target: DrawTarget::stderr(),
            decoration,
            theme: Theme::from_env(preset),
            bar: None,
            length: None,
            drawn: ByteCount(0),
            heading: None,
            restored: None,
            previous: Footprint::default(),
        }
    }

    pub(super) fn start(&mut self, heading: String) -> Result<(), Failure> {
        self.clear()?;
        self.heading = Some(heading);
        let columns = self.target.columns().unwrap_or(80);
        self.commit_long_heading(columns)?;
        if let Some(heading) = &self.heading {
            self.draw(&self.theme.heading(heading))?;
        }
        Ok(())
    }

    pub(super) fn restored(&mut self, text: String) {
        self.restored = Some(text);
    }

    pub(super) fn render(
        &mut self,
        length: Length,
        received: ByteCount,
        row: &Row,
    ) -> Result<(), Failure> {
        let length = match length {
            Length::Known(total) if total.0 > 0 => Some(total.0),
            _ => None,
        };
        let drawn = ByteCount(length.map_or(0, |total| received.0.min(total)));
        if length != self.length || drawn.0 < self.drawn.0 {
            self.bar = match self.decoration {
                Decoration::Bar => length.map(|total| {
                    Bar::new([total])
                        .with_target(DrawTarget::stderr())
                        .with_style(self.theme.style())
                }),
                Decoration::Plain => None,
            };
            self.length = length;
            self.drawn = ByteCount(0);
        }
        if let Some(bar) = &mut self.bar {
            let delta = drawn.0.saturating_sub(self.drawn.0);
            // One received frontier, not an unlabeled received/verified two-tone picture.
            bar.advance_lead(0, delta);
            bar.advance(0, delta);
        }
        self.drawn = drawn;
        let columns = self.target.columns().unwrap_or(80);
        self.commit_long_heading(columns)?;
        let layout = row.layout(columns, self.decoration);
        if !layout.updating {
            // A count too wide for a safe redraw remains complete, even on a tiny terminal.
            self.commit_heading()?;
            self.clear_region()?;
            if let Some(restored) = self.restored.take() {
                self.snapshot(&format!("  {restored}"))?;
            }
            return self.snapshot(&format!("  {}", row.plain()));
        }
        let mut note = String::new();
        if let Some(restored) = &self.restored
            && !Row::append_wrapped(&mut note, restored, columns)
        {
            self.commit_heading()?;
            self.clear_region()?;
            if let Some(restored) = self.restored.take() {
                self.snapshot(&format!("  {restored}"))?;
            }
            note.clear();
        }
        let mut block = String::new();
        if let Some(heading) = &self.heading {
            block.push_str(&self.theme.heading(heading));
            block.push('\n');
        }
        if !note.is_empty() {
            block.push_str(&note);
            block.push('\n');
        }
        if let Some(bar) = &self.bar
            && layout.bar_cells != 0
        {
            block.push_str(&format!(
                "  {}  {:>6}\n",
                bar.render_aggregate(layout.bar_cells),
                row.percent.as_deref().unwrap_or("")
            ));
        }
        block.push_str(&layout.text);
        self.draw(&block)
    }

    /// Only an acknowledged FileCompleted replaces the instrument with a permanent receipt.
    pub(super) fn receipt(&mut self, text: &str) -> Result<(), Failure> {
        let receipt = self.theme.receipt(text);
        let previous_lines = self
            .previous
            .take_rows_at(self.target.columns().unwrap_or(80));
        self.target
            .finish_block(&receipt, previous_lines)
            .map_err(|cause| {
                Failure::io(
                    cause,
                    "cannot write stderr receipt; published files are not rolled back",
                )
            })?;
        self.reset();
        Ok(())
    }

    pub(super) fn failed(&mut self) -> Result<(), Failure> {
        self.commit_heading()?;
        self.clear()
    }

    pub(super) fn clear(&mut self) -> Result<(), Failure> {
        self.clear_region()?;
        self.reset();
        Ok(())
    }

    fn reset(&mut self) {
        self.bar = None;
        self.length = None;
        self.drawn = ByteCount(0);
        self.heading = None;
        self.restored = None;
    }

    fn draw(&mut self, block: &str) -> Result<(), Failure> {
        let previous_lines = self
            .previous
            .take_rows_at(self.target.columns().unwrap_or(80));
        self.target.draw_block(block, previous_lines).map_err(|cause| {
            Failure::io(cause, "cannot draw stderr progress; published files or delivered prefixes are not rolled back")
        })?;
        self.previous.record(block);
        Ok(())
    }

    fn clear_region(&mut self) -> Result<(), Failure> {
        // A failed clear must not be retried by Drop on the same broken output stream.
        let previous_lines = self
            .previous
            .take_rows_at(self.target.columns().unwrap_or(80));
        if previous_lines != 0 {
            self.target
                .draw_block("", previous_lines)
                .map_err(|cause| {
                    Failure::io(
                        cause,
                        "cannot clear stderr progress; inspect the retained run before continuing",
                    )
                })?;
        }
        Ok(())
    }

    fn commit_long_heading(&mut self, columns: u16) -> Result<(), Failure> {
        // Names of unknown cell width and long identities become permanent once, and may wrap.
        // This keeps full Unicode/native evidence without guessing its width, truncating it or
        // allowing a later resize to move the cursor above the region we actually own.
        if self
            .heading
            .as_ref()
            .is_some_and(|heading| !heading.is_ascii() || heading.len() >= usize::from(columns))
        {
            self.commit_heading()?;
        }
        Ok(())
    }

    fn commit_heading(&mut self) -> Result<(), Failure> {
        if let Some(heading) = self.heading.take() {
            self.clear_region()?;
            self.snapshot(&self.theme.heading(&heading))?;
        }
        Ok(())
    }

    fn snapshot(&self, text: &str) -> Result<(), Failure> {
        writeln!(io::stderr().lock(), "{text}").map_err(|cause| {
            Failure::io(cause, "cannot write stderr progress; published files or delivered prefixes are not rolled back")
        })
    }
}

impl Row {
    pub(super) fn plain(&self) -> String {
        let mut text = String::clone(&self.detailed);
        if let Some(rate) = &self.rate {
            text.push_str("  ");
            text.push_str(rate);
        }
        if let Some(eta) = &self.eta {
            text.push_str("  ");
            text.push_str(eta);
        }
        text
    }

    fn layout(&self, columns: u16, decoration: Decoration) -> Layout {
        let profile = Profile::at(columns);
        let bar_cells = match (decoration, self.percent.as_ref()) {
            (Decoration::Bar, Some(_)) => profile.bar_cells(),
            _ => 0,
        };
        // Leave the last terminal column unused so autowrap cannot add an unowned row.
        let available = usize::from(columns.saturating_sub(3));
        let mut text = String::new();
        if matches!(profile, Profile::Wide)
            && self.count.len() <= 34
            && self.rate.as_ref().is_none_or(|rate| rate.len() <= 14)
            && self.eta.as_ref().is_none_or(|eta| eta.len() <= 12)
        {
            // Reserve all three fields even before a rate/ETA exists. Digit growth cannot move them.
            text = format!(
                "  {:<34}  {:>14}  {:>12}",
                self.count,
                self.rate.as_deref().unwrap_or(""),
                self.eta.as_deref().unwrap_or("")
            )
            .trim_end()
            .to_owned();
        } else {
            if !Self::append_wrapped(&mut text, &self.count, columns) {
                return Layout {
                    text: self.plain(),
                    bar_cells: 0,
                    updating: false,
                };
            }
            let rate = self.rate.as_deref().filter(|rate| rate.len() <= available);
            let eta = self.eta.as_deref().filter(|eta| {
                !matches!(profile, Profile::Narrow | Profile::Tiny) && eta.len() <= 12
            });
            if let Some(rate) = rate {
                text.push_str(&format!("\n  {rate}"));
                if let Some(eta) = eta
                    && available >= 28
                    && rate.len() <= 14
                {
                    text.push_str(&format!("{:>width$}{eta}", "", width = 16 - rate.len()));
                }
            } else if let Some(eta) = eta {
                text.push_str(&format!("\n  {eta}"));
            }
        }
        Layout {
            text,
            bar_cells,
            updating: true,
        }
    }

    /// Only internal ASCII labels/numbers reach here. An indivisible oversized count stays a snapshot.
    fn append_wrapped(block: &mut String, text: &str, columns: u16) -> bool {
        let available = usize::from(columns.saturating_sub(3));
        let mut used = 0;
        block.push_str("  ");
        for word in text.split_whitespace() {
            if word.len() > available {
                return false;
            }
            if used != 0 {
                if used + 1 + word.len() > available {
                    block.push_str("\n  ");
                    used = 0;
                } else {
                    block.push(' ');
                    used += 1;
                }
            }
            block.push_str(word);
            used += word.len();
        }
        true
    }
}

impl Footprint {
    fn record(&mut self, block: &str) {
        let Self(widths) = self;
        widths.clear();
        widths.extend(block.lines().map(|line| {
            let mut cells = 0;
            let mut decoration = false;
            for glyph in line.chars() {
                match glyph {
                    '\x1b' => decoration = true,
                    'm' if decoration => decoration = false,
                    _ if decoration => {}
                    _ => cells += 1,
                }
            }
            cells
        }));
    }

    fn take_rows_at(&mut self, columns: u16) -> usize {
        let rows = self.rows_at(columns);
        let Self(widths) = self;
        widths.clear();
        rows
    }

    fn rows_at(&self, columns: u16) -> usize {
        let Self(widths) = self;
        let columns = usize::from(columns.max(1));
        widths
            .iter()
            .map(|width| (*width).max(1).div_ceil(columns))
            .sum()
    }
}

impl Profile {
    fn at(columns: u16) -> Self {
        match columns {
            72.. => Self::Wide,
            40.. => Self::Regular,
            20.. => Self::Narrow,
            _ => Self::Tiny,
        }
    }

    fn bar_cells(self) -> u16 {
        match self {
            Self::Wide => 48,
            Self::Regular => 28,
            Self::Narrow => 8,
            Self::Tiny => 0,
        }
    }
}

impl Theme {
    fn from_env(preset: RibbonStyle) -> Self {
        let locale = ["LC_ALL", "LC_CTYPE", "LANG"]
            .into_iter()
            .filter_map(|name| std::env::var(name).ok())
            .find(|value| !value.is_empty());
        Self {
            preset,
            glyphs: if locale.as_deref().is_some_and(Self::utf8) {
                Glyphs::Unicode
            } else {
                Glyphs::Ascii
            },
            color: if std::env::var_os("NO_COLOR").is_some_and(|value| !value.is_empty())
                || std::env::var("TERM").is_ok_and(|term| term == "dumb")
            {
                Color::Disabled
            } else {
                Color::Enabled
            },
        }
    }

    fn utf8(locale: &str) -> bool {
        let locale = locale.to_ascii_uppercase();
        locale.contains("UTF-8") || locale.contains("UTF8")
    }

    fn style(&self) -> Style {
        let style = match (self.glyphs, self.preset) {
            (Glyphs::Ascii, _) => Style::ascii(),
            (Glyphs::Unicode, RibbonStyle::Classic) => Style::default()
                .with_filler('━')
                .with_leader('━')
                .with_blank('─')
                .with_header('╸'),
            (Glyphs::Unicode, RibbonStyle::Fine) => Style::default()
                .with_filler('─')
                .with_leader('─')
                .with_blank('┄')
                .with_header('╴'),
            (Glyphs::Unicode, RibbonStyle::Solid) => Style::default()
                .with_filler('█')
                .with_leader('█')
                .with_blank('░')
                .with_header('▌'),
        };
        match self.color {
            Color::Disabled => style,
            Color::Enabled => {
                let active = match self.preset {
                    RibbonStyle::Classic => AnsiColor::BrightCyan,
                    RibbonStyle::Fine => AnsiColor::BrightBlue,
                    RibbonStyle::Solid => AnsiColor::Cyan,
                };
                style
                    .with_color(active)
                    .with_lead_color(active)
                    .with_blank_color(AnsiColor::BrightBlack)
            }
        }
    }

    fn heading(&self, text: &str) -> String {
        match self.color {
            Color::Disabled => text.to_owned(),
            Color::Enabled => {
                let style = anstyle::Style::new().bold();
                format!("{}{text}{}", style.render(), style.render_reset())
            }
        }
    }

    fn receipt(&self, text: &str) -> String {
        match self.color {
            Color::Disabled => text.to_owned(),
            Color::Enabled => {
                let (heading, details) = text.split_once('\n').unwrap_or((text, ""));
                let style = anstyle::Style::new()
                    .fg_color(Some(anstyle::Color::Ansi(AnsiColor::Green)))
                    .bold();
                let mut receipt = format!("{}{heading}{}", style.render(), style.render_reset());
                if !details.is_empty() {
                    receipt.push('\n');
                    receipt.push_str(details);
                }
                receipt
            }
        }
    }
}

#[cfg(test)]
#[path = "bar_tests.rs"]
mod tests;
