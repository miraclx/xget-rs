//! One JSON event per update, for machine consumers.

use core::cell::RefCell;
use core::time::Duration;

use xget::Progress;

use super::Meter;

/// One JSON event per update, to stderr: throttled `progress` events, a `retry` on a dropped chunk, and
/// a final `end`; `main` adds a `verified` event with the checksum. Byte counts are raw so a consumer can
/// format them itself; `done` is the cumulative confirmed bytes, `total` the resource length.
pub(crate) struct JsonProgress {
    meter: RefCell<Option<Meter>>,
}

impl JsonProgress {
    pub(super) fn new() -> Self {
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
        if let Some(meter) = self.meter.borrow_mut().as_mut()
            && meter.advance(bytes, Duration::from_millis(200))
        {
            // `rate`/`eta` age the window to now, so they take `&mut`; pull them into locals first so the
            // one format call does not mix a mutable read with the immutable field reads beside it.
            let speed = meter.rate();
            let eta = meter.eta();
            eprintln!(
                r#"{{"event":"progress","done":{},"total":{},"percent":{:.1},"speed":{speed},"eta":{eta}}}"#,
                meter.done,
                meter.total,
                meter.percent_f64(),
            );
        }
    }

    fn retry(&self, index: usize, retry: u32, max: u32, resume_from: u64, error: &str) {
        // Escape the cause so the line stays valid JSON.
        let error = error.replace('\\', "\\\\").replace('"', "\\\"");
        eprintln!(
            r#"{{"event":"retry","chunk":{index},"retry":{retry},"max":{max},"resume_from":{resume_from},"error":"{error}"}}"#
        );
    }

    fn finish(&self) {
        if let Some(meter) = self.meter.borrow().as_ref() {
            eprintln!(
                r#"{{"event":"end","done":{},"total":{},"elapsed":{}}}"#,
                meter.done,
                meter.total,
                meter.elapsed_ms()
            );
        }
    }
}
