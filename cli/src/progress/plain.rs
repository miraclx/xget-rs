//! A single updating text line, for when there is no terminal to draw a live bar on.

use core::cell::RefCell;
use core::time::Duration;

use xget::Progress;

use super::Meter;
use crate::DisplayFormat;

/// A single updating text line: `done / total (pct%)  speed  eta`, drawn in place on stderr and
/// throttled, with a final newline so the shell prompt lands cleanly.
pub(crate) struct PlainProgress {
    meter: RefCell<Option<Meter>>,
    format: DisplayFormat,
}

impl PlainProgress {
    pub(super) fn new(format: DisplayFormat) -> Self {
        Self {
            meter: RefCell::new(None),
            format,
        }
    }

    fn line(&self, meter: &mut Meter) -> String {
        // `rate`/`eta_clock` age the window to now, so they take `&mut`; pull them into locals first so
        // the one format call does not mix a mutable read with the immutable field reads beside it.
        let speed = self.format.speed(meter.rate());
        let eta = meter.eta_clock();
        format!(
            "\r\x1b[K{:>5.1}%  {}/{}  {speed}  ETA {eta}",
            meter.percent_f64(),
            self.format.size(meter.done),
            self.format.size(meter.total),
        )
    }
}

impl Progress for PlainProgress {
    fn start(&self, chunks: &[u64]) {
        let mut meter = Meter::new(chunks.iter().sum());
        eprint!("{}", self.line(&mut meter));
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
            self.format.size(resume_from)
        );
    }

    fn finish(&self) {
        if let Some(meter) = self.meter.borrow_mut().as_mut() {
            eprintln!("{}", self.line(meter));
        }
    }
}
