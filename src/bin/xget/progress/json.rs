//! One JSON event per update, for machine consumers.

use core::cell::RefCell;
use core::time::Duration;

use xget::Progress;

use super::Meter;

/// One JSON event per update, to stderr: a `start`, throttled `progress` events, and a `done`. The
/// numbers are always raw bytes so a consumer can format them itself.
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
