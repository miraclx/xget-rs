//! A single updating text line, for when there is no terminal to draw a live bar on.

use core::cell::RefCell;
use core::time::Duration;

use xget::Progress;

use super::Meter;
use crate::fmt_size;

/// A single updating text line: `done / total (pct%)  speed  eta`, drawn in place on stderr and
/// throttled, with a final newline so the shell prompt lands cleanly.
pub(crate) struct PlainProgress {
    meter: RefCell<Option<Meter>>,
    raw: bool,
}

impl PlainProgress {
    pub(super) fn new(raw: bool) -> Self {
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
        if let Some(meter) = self.meter.borrow_mut().as_mut()
            && meter.advance(bytes, Duration::from_millis(200))
        {
            eprint!("{}", self.line(meter));
            let _ = std::io::Write::flush(&mut std::io::stderr());
        }
    }

    fn retry(&self, index: usize, retry: u32, max: u32, resume_from: u64, error: &str) {
        // Clear the in-place progress line and drop the retry on its own line; the next update redraws.
        eprintln!(
            "\r\x1b[Kchunk {} retry {retry}/{max}: {error} (from {})",
            index + 1,
            fmt_size(resume_from, self.raw)
        );
    }

    fn finish(&self) {
        if let Some(meter) = self.meter.borrow().as_ref() {
            eprintln!("{}", self.line(meter));
        }
    }
}
