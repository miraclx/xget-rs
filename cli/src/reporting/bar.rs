//! Explicit stderr-only xprogress drawing. No environment-selected Dump target is consulted.

use xget::transfer::{ByteCount, Length};
use xprogress::{Bar, DrawTarget, Style};

use crate::failure::Failure;

pub(super) struct TerminalBar {
    target: DrawTarget,
    bar: Option<Bar>,
    length: Option<u64>,
    received: ByteCount,
    settled: ByteCount,
    previous_lines: usize,
}

impl TerminalBar {
    pub(super) fn new() -> Self {
        Self {
            target: DrawTarget::stderr(),
            bar: None,
            length: None,
            received: ByteCount(0),
            settled: ByteCount(0),
            previous_lines: 0,
        }
    }

    pub(super) fn render(
        &mut self,
        length: Length,
        received: ByteCount,
        settled: ByteCount,
        text: &str,
    ) -> Result<(), Failure> {
        let length = match length {
            Length::Known(total) => Some(total.0),
            Length::Unknown => None,
        };
        if length != self.length || received.0 < self.received.0 || settled.0 < self.settled.0 {
            self.bar = length.map(|total| Bar::new([total]).with_style(Style::ascii()));
            self.length = length;
            self.received = ByteCount(0);
            self.settled = ByteCount(0);
        }
        // These are received/written (or live accepted), NOT an expectation-verified frontier.
        if let Some(bar) = &mut self.bar {
            bar.advance_lead(0, received.0.saturating_sub(self.received.0));
            bar.advance(0, settled.0.saturating_sub(self.settled.0));
        }
        self.received = received;
        self.settled = settled;
        let width = self
            .target
            .columns()
            .unwrap_or(80)
            .saturating_sub(20)
            .clamp(8, 40);
        let block = match &self.bar {
            Some(bar) => format!("[{}]\n{text}", bar.render_aggregate(width)),
            None => text.to_owned(),
        };
        self.previous_lines = self.target.draw_block(&block, self.previous_lines).map_err(|cause| Failure::io(cause,
            "cannot draw stderr progress; published files or delivered prefixes are not rolled back"
        ))?;
        Ok(())
    }

    pub(super) fn clear(&mut self) -> Result<(), Failure> {
        if self.previous_lines != 0 {
            self.target
                .draw_block("", self.previous_lines)
                .map_err(|cause| {
                    Failure::io(
                        cause,
                        "cannot clear stderr progress; inspect the retained run before continuing",
                    )
                })?;
        }
        self.previous_lines = 0;
        self.bar = None;
        self.length = None;
        self.received = ByteCount(0);
        self.settled = ByteCount(0);
        Ok(())
    }
}
