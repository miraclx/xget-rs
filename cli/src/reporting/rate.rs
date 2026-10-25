//! A bounded recent source rate for humans. Machine rates retain their existing lifetime sampling.

use core::time::Duration;

use xget::transfer::ByteCount;

use super::BytesPerSecond;

const BUCKET_MS: u64 = 100;
const BUCKETS: usize = 30;
const WINDOW_MS: u64 = BUCKET_MS * BUCKETS as u64;

pub(super) struct RecentRate {
    started: Duration,
    previous: ByteCount,
    buckets: [u64; BUCKETS],
    head: u64,
}

impl RecentRate {
    pub(super) fn at(started: Duration) -> Self {
        Self {
            started,
            previous: ByteCount(0),
            buckets: [0; BUCKETS],
            head: 0,
        }
    }

    pub(super) fn sample(&mut self, current: ByteCount, elapsed: Duration) {
        if current.0 < self.previous.0 {
            *self = Self::at(elapsed);
        }
        let index = self.millis(elapsed) / BUCKET_MS;
        self.roll(index);
        let delta = current.0.saturating_sub(self.previous.0);
        self.previous = current;
        let slot = (index % BUCKETS as u64) as usize;
        self.buckets[slot] = self.buckets[slot].saturating_add(delta);
    }

    /// Reads age the window too. No redraw task or wall-clock stall notification is implied.
    pub(super) fn rate(&mut self, elapsed: Duration) -> Option<BytesPerSecond> {
        let millis = self.millis(elapsed);
        self.roll(millis / BUCKET_MS);
        if millis < BUCKET_MS {
            return None;
        }
        let sum: u128 = self.buckets.iter().map(|bytes| u128::from(*bytes)).sum();
        let rate = sum * 1000 / u128::from(millis.min(WINDOW_MS));
        (rate != 0).then_some(BytesPerSecond(rate.min(u128::from(u64::MAX)) as u64))
    }

    fn millis(&self, elapsed: Duration) -> u64 {
        elapsed
            .saturating_sub(self.started)
            .as_millis()
            .min(u128::from(u64::MAX)) as u64
    }

    fn roll(&mut self, index: u64) {
        if index <= self.head {
            return;
        }
        for step in 1..=(index - self.head).min(BUCKETS as u64) {
            self.buckets[((self.head + step) % BUCKETS as u64) as usize] = 0;
        }
        self.head = index;
    }
}

#[cfg(test)]
#[path = "rate_tests.rs"]
mod tests;
