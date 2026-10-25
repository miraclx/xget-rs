use core::time::Duration;

use xget::transfer::ByteCount;

use super::RecentRate;
use crate::reporting::BytesPerSecond;

#[test]
fn human_rate_needs_a_useful_sample_and_ages_out_at_read_time() {
    let mut rate = RecentRate::at(Duration::ZERO);
    rate.sample(ByteCount(30), Duration::ZERO);
    assert_eq!(rate.rate(Duration::from_millis(99)), None);
    assert_eq!(
        rate.rate(Duration::from_millis(100)),
        Some(BytesPerSecond(300))
    );
    assert_eq!(rate.rate(Duration::from_secs(3)), None);
}

#[test]
fn only_source_differences_enter_the_window_and_counter_reset_starts_a_new_sample() {
    let mut rate = RecentRate::at(Duration::from_secs(4));
    rate.sample(ByteCount(0), Duration::from_secs(4));
    assert_eq!(rate.rate(Duration::from_secs(5)), None);
    rate.sample(ByteCount(100), Duration::from_secs(5));
    assert_eq!(rate.rate(Duration::from_secs(5)), Some(BytesPerSecond(100)));
    rate.sample(ByteCount(3), Duration::from_secs(6));
    assert_eq!(rate.rate(Duration::from_secs(6)), None);
    rate.sample(ByteCount(8), Duration::from_millis(6100));
    assert_eq!(
        rate.rate(Duration::from_millis(6100)),
        Some(BytesPerSecond(80))
    );
}

#[test]
fn large_exact_counts_saturate_only_the_reported_rate() {
    let mut rate = RecentRate::at(Duration::ZERO);
    rate.sample(ByteCount(u64::MAX), Duration::from_millis(100));
    assert_eq!(
        rate.rate(Duration::from_millis(100)),
        Some(BytesPerSecond(u64::MAX))
    );
    assert_eq!(rate.rate(Duration::from_secs(u64::MAX)), None);
}
