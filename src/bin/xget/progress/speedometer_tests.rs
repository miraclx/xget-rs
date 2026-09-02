use core::time::Duration;

use super::{BUCKET, Speedometer};

/// The whole reason for the window: bytes delivered in under a second read their true rate, not the
/// bytes spread over a one-second floor. 3000 bytes by 0.3s is 10_000 B/s, not 3000.
#[test]
fn a_sub_second_burst_reads_its_true_rate() {
    let mut speed = Speedometer::new();
    speed.record(Duration::from_millis(300), 3000);
    assert_eq!(speed.rate(), 10_000);
}

/// Inside the first window the reading is the running total over the elapsed span, so a steady feed
/// reads its steady rate: 2000 bytes across 1.0s is 2000 B/s.
#[test]
fn a_steady_feed_reads_its_steady_rate() {
    let mut speed = Speedometer::new();
    speed.record(Duration::from_millis(0), 1000);
    speed.record(Duration::from_millis(1000), 1000);
    assert_eq!(speed.rate(), 2000);
}

/// Bytes older than the window age out of the sum: a byte at t=0 is gone by the time the window has
/// slid past it, so a later empty tick reads zero rather than counting it forever.
#[test]
fn bytes_older_than_the_window_age_out() {
    let mut speed = Speedometer::new();
    speed.record(Duration::from_millis(0), 1_000_000);
    // The window is BUCKETS * BUCKET = 3s; record a zero-byte tick well past it.
    speed.record(BUCKET * 40, 0);
    assert_eq!(speed.rate(), 0);
}

/// The span is floored at one bucket, so bytes in the first instant read a smooth ramp rather than
/// dividing by a near-zero span and spiking: 1000 bytes at t=1ms is 10_000 B/s (1000 / 0.1s), not a
/// million.
#[test]
fn the_first_instant_does_not_spike() {
    let mut speed = Speedometer::new();
    speed.record(Duration::from_millis(1), 1000);
    assert_eq!(speed.rate(), 10_000);
}

/// A recent burst still counts once older buckets have aged out, so the meter tracks the current
/// rate rather than being anchored to the start.
#[test]
fn the_window_tracks_the_recent_rate() {
    let mut speed = Speedometer::new();
    speed.record(Duration::from_millis(0), 5_000_000); // long ago, will age out
    speed.record(BUCKET * 40, 6000); // 6000 bytes in this bucket, window now past the first
    // Only the recent 6000 remain, over a full 3s window: 2000 B/s.
    assert_eq!(speed.rate(), 2000);
}
