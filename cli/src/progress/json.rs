//! One JSON event per update, for machine consumers.

use core::cell::RefCell;
use core::fmt;
use core::fmt::Write as _;
use core::time::Duration;

use xget::Progress;

use super::Meter;

#[cfg(test)]
#[path = "json_tests.rs"]
mod tests;

/// One JSON event per update, to stderr: throttled `progress` events, a `retry` on a dropped chunk, and
/// a final `end`; `main` adds a `verified` event with the checksum. Byte counts are raw so a consumer can
/// format them itself; `done` is the cumulative downloaded bytes, `total` the resource length.
/// `speed` is bytes per second, `eta` is seconds, and `end.elapsed` is milliseconds. Human display
/// flags (`--bits`, `--raw-sizes`) never change these machine-readable units.
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
        let error = JsonString(error);
        eprintln!(
            r#"{{"event":"retry","chunk":{index},"retry":{retry},"max":{max},"resume_from":{resume_from},"error":{error}}}"#
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

// A JSON string is not a Rust debug string: all U+0000..U+001F characters need JSON escapes.
struct JsonString<'a>(&'a str);

impl fmt::Display for JsonString<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_char('"')?;
        for character in self.0.chars() {
            match character {
                '"' => formatter.write_str("\\\"")?,
                '\\' => formatter.write_str("\\\\")?,
                '\u{0000}'..='\u{001f}' => write!(formatter, "\\u{:04x}", u32::from(character))?,
                character => formatter.write_char(character)?,
            }
        }
        formatter.write_char('"')
    }
}
