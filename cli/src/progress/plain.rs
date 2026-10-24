//! Throttled text progress: updating on a terminal, newline-delimited when captured.

use core::cell::RefCell;
use core::time::Duration;
use std::io::IsTerminal as _;

use xget::Progress;

use super::Meter;
use crate::DisplayFormat;

#[cfg(test)]
#[path = "plain_tests.rs"]
mod tests;

/// Human progress on stderr, without terminal cursor controls in captured output.
pub(crate) struct PlainProgress {
    meter: RefCell<Option<Meter>>,
    format: DisplayFormat,
    rendering: Rendering,
}

#[derive(Clone, Copy)]
enum Rendering {
    Updating,
    Lines,
}

impl PlainProgress {
    pub(super) fn new(format: DisplayFormat) -> Self {
        Self {
            meter: RefCell::new(None),
            format,
            rendering: if std::io::stderr().is_terminal() {
                Rendering::Updating
            } else {
                Rendering::Lines
            },
        }
    }

    fn line(&self, meter: &mut Meter) -> String {
        // Pull the aging mutable measurements before borrowing the remaining fields for formatting.
        let speed = self.format.speed(meter.rate());
        let eta = meter.eta_clock();
        format!(
            "{:>5.1}%  {}/{}  {speed}  ETA {eta}",
            meter.percent_f64(),
            self.format.size(meter.done),
            self.format.size(meter.total),
        )
    }

    fn prefix(&self) -> &'static str {
        match self.rendering {
            Rendering::Updating => "\r\x1b[K",
            Rendering::Lines => "",
        }
    }

    fn render(&self, meter: &mut Meter) {
        let line = self.line(meter);
        match self.rendering {
            Rendering::Updating => {
                eprint!("{}{line}", self.prefix());
                let _ = std::io::Write::flush(&mut std::io::stderr());
            }
            Rendering::Lines => eprintln!("{line}"),
        }
    }
}

impl Progress for PlainProgress {
    fn start(&self, chunks: &[u64]) {
        let mut meter = Meter::new(chunks.iter().sum());
        self.render(&mut meter);
        *self.meter.borrow_mut() = Some(meter);
    }

    fn restore(&self, present: &[u64]) {
        if let Some(meter) = self.meter.borrow_mut().as_mut() {
            meter.restore(present.iter().sum());
            self.render(meter);
        }
    }

    fn received(&self, _index: usize, bytes: u64) {
        if let Some(meter) = self.meter.borrow_mut().as_mut()
            && meter.advance(bytes, Duration::from_millis(200))
        {
            self.render(meter);
        }
    }

    fn retry(&self, index: usize, retry: u32, max: u32, resume_from: u64, error: &str) {
        // A remote cause must not inject cursor controls or extra log lines.
        let error = error.escape_debug();
        eprintln!(
            "{}chunk {} retry {retry}/{max}: {error} (from {})",
            self.prefix(),
            index + 1,
            self.format.size(resume_from)
        );
    }

    fn finish(&self) {
        if let Some(meter) = self.meter.borrow_mut().as_mut() {
            eprintln!("{}{}", self.prefix(), self.line(meter));
        }
    }
}
