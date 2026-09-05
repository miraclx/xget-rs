use core::cell::Cell;
use core::time::Duration;
use std::time::Instant;

use super::{BUCKET, BUCKETS, Clock, Speedometer};

/// A hand-driven clock: tests set "now" to a fixed offset from a fixed origin, so the window and its
/// decay are exercised without sleeping. Every read of the speedometer sees exactly the time the test
/// chose, which is the whole point of injecting the clock.
struct FakeClock {
    origin: Instant,
    offset: Cell<Duration>,
}

impl FakeClock {
    fn new() -> Self {
        Self {
            origin: Instant::now(),
            offset: Cell::new(Duration::ZERO),
        }
    }

    /// Move the clock to `elapsed` since the speedometer started. Absolute, not relative, so a test
    /// reads as "at t = elapsed, ...".
    fn set(&self, elapsed: Duration) {
        self.offset.set(elapsed);
    }
}

impl Clock for &FakeClock {
    fn now(&self) -> Instant {
        self.origin + self.offset.get()
    }
}

/// The whole reason for the window: bytes delivered in under a second read their true rate, not the
/// bytes spread over a one-second floor. 3000 bytes by 0.3s is 10_000 B/s, not 3000.
#[test]
fn a_sub_second_burst_reads_its_true_rate() {
    let clock = FakeClock::new();
    let mut speed = Speedometer::with_clock(&clock);
    clock.set(Duration::from_millis(300));
    speed.add(3000);
    assert_eq!(speed.rate(), 10_000);
}

/// Inside the first window the reading is the running total over the elapsed span, so a steady feed
/// reads its steady rate: 2000 bytes across 1.0s is 2000 B/s.
#[test]
fn a_steady_feed_reads_its_steady_rate() {
    let clock = FakeClock::new();
    let mut speed = Speedometer::with_clock(&clock);
    clock.set(Duration::from_millis(0));
    speed.add(1000);
    clock.set(Duration::from_millis(1000));
    speed.add(1000);
    assert_eq!(speed.rate(), 2000);
}

/// Bytes older than the window age out of the sum: a byte at t=0 is gone by the time the window has
/// slid past it, so a later read finds zero rather than counting it forever.
#[test]
fn bytes_older_than_the_window_age_out() {
    let clock = FakeClock::new();
    let mut speed = Speedometer::with_clock(&clock);
    clock.set(Duration::from_millis(0));
    speed.add(1_000_000);
    // The window is BUCKETS * BUCKET = 3s; read well past it.
    clock.set(BUCKET * 40);
    assert_eq!(speed.rate(), 0);
}

/// The span is floored at one bucket, so bytes in the first instant read a smooth ramp rather than
/// dividing by a near-zero span and spiking: 1000 bytes at t=1ms is 10_000 B/s (1000 / 0.1s), not a
/// million.
#[test]
fn the_first_instant_does_not_spike() {
    let clock = FakeClock::new();
    let mut speed = Speedometer::with_clock(&clock);
    clock.set(Duration::from_millis(1));
    speed.add(1000);
    assert_eq!(speed.rate(), 10_000);
}

/// A recent burst still counts once older buckets have aged out, so the meter tracks the current rate
/// rather than being anchored to the start.
#[test]
fn the_window_tracks_the_recent_rate() {
    let clock = FakeClock::new();
    let mut speed = Speedometer::with_clock(&clock);
    clock.set(Duration::from_millis(0));
    speed.add(5_000_000); // long ago, will age out
    clock.set(BUCKET * 40);
    speed.add(6000); // 6000 bytes in this bucket, window now past the first
    // Only the recent 6000 remain, over a full 3s window: 2000 B/s.
    assert_eq!(speed.rate(), 2000);
}

/// The bug this fix exists for (BLOCKER-1): when bytes stop arriving, the reported speed must decay
/// toward zero, not freeze at the last reading. No `add` happens during the stall; the read alone ages
/// the window forward, empty buckets slide in, and the rate falls to zero over one window.
#[test]
fn a_stall_decays_to_zero() {
    let clock = FakeClock::new();
    let mut speed = Speedometer::with_clock(&clock);
    // A brisk feed: 1 MiB in the first bucket.
    clock.set(Duration::from_millis(0));
    speed.add(1_000_000);
    clock.set(Duration::from_millis(100));
    assert!(speed.rate() > 0, "the feed should read a live rate");

    // Now the bytes stop. Read partway through the window: the rate has begun to decay but is not yet
    // zero, because live buckets remain inside the window.
    clock.set(BUCKET * 15);
    let midway = speed.rate();
    assert!(
        midway > 0,
        "mid-window the rate has decayed but is not yet zero"
    );

    // Read one full window past the last byte with no further adds: every live bucket has aged out and
    // the reported speed is zero. This is the exact frozen-readout bug, now decayed by construction.
    clock.set(BUCKET * 40);
    assert_eq!(
        speed.rate(),
        0,
        "after one idle window the stall decays to zero"
    );
}

/// A gap then a burst: bytes arrive, stop for longer than the window, then resume. The pre-gap bytes
/// must have aged out entirely, so the post-gap reading reflects only the new burst, not a sum that
/// double-counts across the idle stretch.
#[test]
fn a_gap_then_burst_reads_only_the_burst() {
    let clock = FakeClock::new();
    let mut speed = Speedometer::with_clock(&clock);
    clock.set(Duration::from_millis(0));
    speed.add(9_000_000); // pre-gap traffic

    // Idle far longer than the 3s window, then resume with a fresh burst.
    clock.set(BUCKET * 100);
    speed.add(6000);
    // The pre-gap 9 MiB is long gone; only the 6000 remain, over a full window: 2000 B/s.
    assert_eq!(speed.rate(), 2000);
}

/// Two samples land in the same bucket (same tick): they sum into one bucket, and the read divides by
/// the floored one-bucket span rather than a zero span. Zero-interval is safe by construction, no
/// div-by-zero.
#[test]
fn two_samples_in_one_tick_sum_without_dividing_by_zero() {
    let clock = FakeClock::new();
    let mut speed = Speedometer::with_clock(&clock);
    clock.set(Duration::from_millis(0));
    speed.add(400);
    speed.add(600); // same tick, same bucket
    // 1000 bytes over the floored one-bucket span (0.1s): 10_000 B/s, and crucially not a panic.
    assert_eq!(speed.rate(), 10_000);
}

/// Warmup: a read before any bytes arrive is zero, not a divide-by-zero or a spike. The window is empty
/// and the span is floored, so the sum-over-span is a clean zero.
#[test]
fn a_read_before_any_bytes_is_zero() {
    let clock = FakeClock::new();
    let mut speed = Speedometer::with_clock(&clock);
    assert_eq!(speed.rate(), 0);
    clock.set(Duration::from_millis(250));
    assert_eq!(speed.rate(), 0);
}

/// An index jump of exactly a `BUCKETS` multiple lands in the same slot `roll_to` just zeroed in the
/// same read (MINOR-6): the aging runs before the sum, so the collision is handled and only the new
/// bytes count. Guards the subtle ordering with a test the reviewer flagged as missing.
#[test]
fn an_exact_window_multiple_jump_does_not_double_count() {
    let clock = FakeClock::new();
    let mut speed = Speedometer::with_clock(&clock);
    clock.set(Duration::from_millis(0));
    speed.add(7000); // bucket 0
    // Jump exactly BUCKETS buckets forward: index BUCKETS wraps to slot 0, the same slot the old bytes
    // sit in. roll_to must zero it before the new bytes land.
    clock.set(BUCKET * BUCKETS as u32);
    speed.add(3000);
    // Only the 3000 remain, over a full window: 1000 B/s. The stale 7000 was aged out, not summed in.
    assert_eq!(speed.rate(), 1000);
}
