//! A sliding-window speedometer: the download rate over the last few seconds, sampled at READ time so a
//! stall decays to zero instead of freezing at the last reading. Bytes land in fixed sub-second buckets
//! indexed by elapsed time; the rate is the bytes still inside the window divided by the real span they
//! cover, aged forward to now on every read.

use core::time::Duration;
use std::time::Instant;

/// The width of one speedometer bucket, and how many are kept: 30 buckets of 100ms cover a 3s sliding
/// window, responsive enough to track a changing rate without jittering on every packet.
const BUCKET: Duration = Duration::from_millis(100);
const BUCKETS: usize = 30;
/// Milliseconds one bucket spans and the whole window spans, precomputed so the hot path stays in the
/// integer domain (no `Duration` math, no float).
const BUCKET_MS: u64 = BUCKET.as_millis() as u64;
const WINDOW_MS: u64 = BUCKET_MS * BUCKETS as u64;

/// The clock the speedometer reads, injected at construction so the window and its decay are testable
/// without sleeping. Production passes a real-[`Instant`] clock; tests pass a fake they advance by hand.
pub(crate) trait Clock {
    fn now(&self) -> Instant;
}

/// The production clock: a real monotonic [`Instant`]. Monotonic on every supported platform and
/// saturating across suspend, so the elapsed span never runs backwards.
pub(crate) struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// A sliding-window speedometer. Two things a lifetime average gets wrong and this does not: a
/// sub-second transfer reads its true rate (not its bytes over a one-second floor), and a stall decays
/// the reading toward zero instead of freezing it at the last value, because the window is aged to the
/// read clock, not the write clock.
pub(crate) struct Speedometer<C: Clock = SystemClock> {
    clock: C,
    started: Instant,
    buckets: [u64; BUCKETS],
    /// Absolute index (`elapsed / BUCKET`) of the newest bucket that has been aged or written, so reads
    /// and writes share one aging path and older buckets can be zeroed out.
    head: u64,
}

impl Speedometer<SystemClock> {
    pub(crate) fn new() -> Self {
        Self::with_clock(SystemClock)
    }
}

impl<C: Clock> Speedometer<C> {
    pub(crate) fn with_clock(clock: C) -> Self {
        let started = clock.now();
        Self {
            clock,
            started,
            buckets: [0; BUCKETS],
            head: 0,
        }
    }

    /// The absolute bucket index for an elapsed span. `u64` milliseconds hold roughly 584 million years,
    /// so there is no realistic overflow of the elapsed clock.
    fn index_at(&self, elapsed: Duration) -> u64 {
        (elapsed.as_millis() as u64) / BUCKET_MS
    }

    /// Zero every bucket from just past the old head through `index`, so bytes older than the window age
    /// out of the sum. Idempotent, a no-op when no bucket boundary was crossed, and capped at a full
    /// wipe so a gap wider than the window clears the whole ring exactly once.
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

    /// Add `bytes` at the current time: age the window forward to now, then drop them in their bucket.
    pub(crate) fn add(&mut self, bytes: u64) {
        let elapsed = self.clock.now().saturating_duration_since(self.started);
        let index = self.index_at(elapsed);
        self.roll_to(index);
        let slot = (index % BUCKETS as u64) as usize;
        self.buckets[slot] = self.buckets[slot].saturating_add(bytes);
    }

    /// Bytes per second across the window, aged to NOW. A stall slides empty buckets in and shrinks the
    /// sum, so the rate decays to zero over one window; there is no cached value that can go stale.
    pub(crate) fn rate(&mut self) -> u64 {
        let elapsed = self.clock.now().saturating_duration_since(self.started);
        // Age the window forward to read time before summing, so an idle period is reflected even when
        // no `add` has happened since. This is what makes a stall decay by construction.
        self.roll_to(self.index_at(elapsed));
        let sum: u64 = self.buckets.iter().sum();
        // Integer domain: bytes-per-second = sum * 1000 / span_ms. The span is floored at one bucket so
        // the first slice does not divide by a near-zero span and spike, and capped at the window once it
        // has filled. `BUCKET_MS < WINDOW_MS` is a compile-time constant, so the clamp is well-ordered;
        // the floor is above zero, so integer division never divides by zero: no NaN, no inf path.
        let span_ms = (elapsed.as_millis() as u64).clamp(BUCKET_MS, WINDOW_MS);
        sum.saturating_mul(1000) / span_ms
    }
}

#[cfg(test)]
#[path = "speedometer_tests.rs"]
mod speedometer_tests;
