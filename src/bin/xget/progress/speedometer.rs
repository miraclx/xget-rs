//! A sliding-window speedometer: the download rate over the last few seconds, so the readout tracks the
//! current speed instead of a lifetime average that lags a stall or a burst.

use core::time::Duration;
use std::time::Instant;

/// The width of one speedometer bucket, and how many are kept: 30 buckets of 100ms cover a 3s sliding
/// window, responsive enough to track a changing rate without jittering on every packet.
const BUCKET: Duration = Duration::from_millis(100);
const BUCKETS: usize = 30;

/// A sliding-window speedometer. Bytes land in fixed sub-second buckets indexed by elapsed time; the
/// rate is the bytes still inside the window divided by the real span they cover. Two things a lifetime
/// average gets wrong and this does not: a sub-second transfer reads its true rate (not its bytes over a
/// one-second floor), and once the window fills the reading reflects the recent rate rather than being
/// dragged down by a slow start. The clock is injected through [`Speedometer::record`], so the windowing
/// is testable without sleeping.
pub(crate) struct Speedometer {
    started: Instant,
    buckets: [u64; BUCKETS],
    /// Absolute index (`elapsed / BUCKET`) of the newest live bucket, so older ones can be aged out.
    head: u64,
    /// The rate from the last record, cached so a read needs no `&mut` and no re-summing.
    current: u64,
}

impl Speedometer {
    pub(crate) fn new() -> Self {
        Self {
            started: Instant::now(),
            buckets: [0; BUCKETS],
            head: 0,
            current: 0,
        }
    }

    /// Add `bytes` at the current time.
    pub(crate) fn add(&mut self, bytes: u64) {
        self.record(self.started.elapsed(), bytes);
    }

    /// Add `bytes` at `elapsed` since the start: drop them in their bucket, age out anything older than
    /// the window, and recompute the windowed rate. Pure in its inputs, so tests drive it with a chosen
    /// clock.
    fn record(&mut self, elapsed: Duration, bytes: u64) {
        let index = (elapsed.as_nanos() / BUCKET.as_nanos()) as u64;
        self.roll_to(index);
        self.buckets[(index % BUCKETS as u64) as usize] += bytes;

        let sum: u64 = self.buckets.iter().sum();
        let window = BUCKET.as_secs_f64() * BUCKETS as f64;
        // Divide by the real span covered so far, floored at one bucket so the first slice does not
        // divide by a near-zero span and spike, and capped at the window once it has filled.
        let span = elapsed.as_secs_f64().min(window).max(BUCKET.as_secs_f64());
        self.current = (sum as f64 / span) as u64;
    }

    /// Zero every bucket from just past the old head through `index`, so bytes older than the window age
    /// out of the sum. A no-op when no bucket boundary was crossed.
    fn roll_to(&mut self, index: u64) {
        if index <= self.head {
            return;
        }
        let steps = (index - self.head).min(BUCKETS as u64);
        for step in 1..=steps {
            self.buckets[((self.head + step) % BUCKETS as u64) as usize] = 0;
        }
        self.head = index;
    }

    /// Bytes per second across the window, as of the last record.
    pub(crate) fn rate(&self) -> u64 {
        self.current
    }
}

#[cfg(test)]
#[path = "speedometer_tests.rs"]
mod speedometer_tests;
