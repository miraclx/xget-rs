//! Explicit stderr-only xprogress drawing, budgeted against the whole active row.

use std::io::{self, Write as _};

use xget::transfer::{ByteCount, Length};
use xprogress::{AnsiColor, Bar, DrawTarget, Style};

use crate::failure::Failure;

pub(super) struct TerminalBar {
    target: DrawTarget,
    decoration: Decoration,
    bar: Option<Bar>,
    length: Option<u64>,
    drawn: ByteCount,
    previous_lines: usize,
}

#[derive(Clone, Copy)]
pub(super) enum Decoration {
    Bar,
    Plain,
}

/// These row components contain only ASCII labels and xbytes numbers/units, never remote names.
pub(super) struct Row {
    pub(super) count: String,
    pub(super) detailed: String,
    pub(super) rate: Option<String>,
    pub(super) eta: Option<String>,
    pub(super) determinate: bool,
}

struct Layout {
    text: String,
    bar_cells: u16,
    updating: bool,
}

impl TerminalBar {
    pub(super) fn new(decoration: Decoration) -> Self {
        Self {
            target: DrawTarget::stderr(),
            decoration,
            bar: None,
            length: None,
            drawn: ByteCount(0),
            previous_lines: 0,
        }
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
                Decoration::Bar => length.map(|total| Bar::new([total]).with_style(Self::style())),
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
        let layout = row.layout(self.target.columns().unwrap_or(80), self.decoration);
        if !layout.updating {
            self.clear_region()?;
            return writeln!(io::stderr().lock(), "  {}", layout.text).map_err(|cause| Failure::io(cause,
                "cannot write stderr progress; published files or delivered prefixes are not rolled back"));
        }
        let block = match (&self.bar, layout.bar_cells) {
            (Some(bar), width) if width != 0 => {
                format!("  [{}] {}", bar.render_aggregate(width), layout.text)
            }
            _ => format!("  {}", layout.text),
        };
        self.previous_lines = self.target.draw_block(&block, self.previous_lines).map_err(|cause| {
            self.previous_lines = 0;
            Failure::io(cause, "cannot draw stderr progress; published files or delivered prefixes are not rolled back")
        })?;
        Ok(())
    }

    pub(super) fn clear(&mut self) -> Result<(), Failure> {
        self.clear_region()?;
        self.bar = None;
        self.length = None;
        self.drawn = ByteCount(0);
        Ok(())
    }

    fn clear_region(&mut self) -> Result<(), Failure> {
        // A failed clear must not be retried by Drop on the same broken output stream.
        let previous_lines = core::mem::take(&mut self.previous_lines);
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

    fn style() -> Style {
        let unicode = ["LC_ALL", "LC_CTYPE", "LANG"]
            .into_iter()
            .find_map(|name| std::env::var(name).ok())
            .is_some_and(|locale| {
                let locale = locale.to_ascii_uppercase();
                locale.contains("UTF-8") || locale.contains("UTF8")
            });
        let style = if unicode {
            Style::default()
                .with_filler('━')
                .with_leader('━')
                .with_blank('━')
                .with_header('╸')
        } else {
            Style::ascii()
        };
        style
            .with_color(AnsiColor::BrightCyan)
            .with_lead_color(AnsiColor::BrightCyan)
            .with_blank_color(AnsiColor::BrightBlack)
    }
}

impl Row {
    pub(super) fn plain(&self) -> String {
        self.text(&self.detailed, self.rate.as_deref(), self.eta.as_deref())
    }

    fn text(&self, count: &str, rate: Option<&str>, eta: Option<&str>) -> String {
        let mut text = count.to_owned();
        if let Some(rate) = rate {
            text.push_str("  ");
            text.push_str(rate);
        }
        if let Some(eta) = eta {
            text.push_str("  ");
            text.push_str(eta);
        }
        text
    }

    fn layout(&self, columns: u16, decoration: Decoration) -> Layout {
        let count = if columns < 40 {
            &self.count
        } else {
            &self.detailed
        };
        let mut rate = self.rate.as_deref();
        let mut eta = self.eta.as_deref();
        let cap = if columns >= 40 && self.determinate && matches!(decoration, Decoration::Bar) {
            (columns.saturating_mul(40) / 100).min(48)
        } else {
            0
        };
        let required = |rate: Option<&str>, eta: Option<&str>| {
            // Only ASCII is admitted to this active row. Names remain permanent, escaped blocks.
            // The cap is a maximum, not reserved space. Shrink the bar before discarding useful
            // numbers; if even one cell cannot fit, drop ETA, then rate, then the bar.
            count.len()
                + rate.map_or(0, |text| text.len() + 2)
                + eta.map_or(0, |text| text.len() + 2)
                + 2
                + if cap == 0 { 0 } else { 4 }
        };
        if required(rate, eta) > usize::from(columns) {
            eta = None;
        }
        if required(rate, eta) > usize::from(columns) {
            rate = None;
        }
        let text = self.text(count, rate, eta);
        let remaining = usize::from(columns).saturating_sub(text.len() + 2);
        let bar_cells = cap.min(remaining.saturating_sub(3).min(usize::from(u16::MAX)) as u16);
        Layout {
            updating: text.len() + 2 <= usize::from(columns),
            text,
            bar_cells,
        }
    }
}

#[cfg(test)]
#[path = "bar_tests.rs"]
mod tests;
