use crate::ByteRange;
use crate::plan::{plan, plan_range, plan_resume};

fn covers_exactly(ranges: &[ByteRange], length: u64) {
    let mut prev_end = 0;
    for range in ranges {
        assert_eq!(range.start, prev_end, "contiguous, no gap or overlap");
        assert!(range.end > range.start, "every chunk is non-empty");
        prev_end = range.end;
    }
    assert_eq!(prev_end, length, "the chunks cover the whole length");
}

/// Like [`covers_exactly`] but for a range that need not begin at zero, as when resuming.
fn tiles(ranges: &[ByteRange], start: u64, end: u64) {
    let mut prev_end = start;
    for range in ranges {
        assert_eq!(range.start, prev_end, "contiguous, no gap or overlap");
        assert!(range.end > range.start, "every chunk is non-empty");
        prev_end = range.end;
    }
    assert_eq!(prev_end, end, "the chunks reach the end");
}

#[test]
fn a_zero_length_resource_plans_to_nothing() {
    assert!(plan(0, 4).is_empty());
}

#[test]
fn an_even_split() {
    let ranges = plan(100, 4);
    assert_eq!(ranges.len(), 4);
    assert!(ranges.iter().all(|r| r.len() == 25));
    covers_exactly(&ranges, 100);
}

#[test]
fn an_uneven_split_gives_the_remainder_to_the_earliest_chunks() {
    let ranges = plan(10, 3);
    assert_eq!(
        ranges.iter().map(ByteRange::len).collect::<Vec<_>>(),
        [4, 3, 3]
    );
    covers_exactly(&ranges, 10);
}

#[test]
fn more_parts_than_bytes_caps_at_one_byte_each() {
    let ranges = plan(3, 10);
    assert_eq!(ranges.len(), 3);
    assert!(ranges.iter().all(|r| r.len() == 1));
    covers_exactly(&ranges, 3);
}

#[test]
fn zero_parts_is_treated_as_one() {
    assert_eq!(plan(50, 0), [ByteRange { start: 0, end: 50 }]);
}

#[test]
fn a_range_starts_where_told_and_tiles_the_remainder() {
    let ranges = plan_range(300, 1000, 4);
    assert_eq!(ranges.len(), 4);
    assert_eq!(
        ranges[0].start, 300,
        "the first chunk resumes at the offset"
    );
    tiles(&ranges, 300, 1000);
}

#[test]
fn an_empty_or_inverted_range_plans_to_nothing() {
    assert!(plan_range(1000, 1000, 4).is_empty(), "already complete");
    assert!(plan_range(1000, 500, 4).is_empty(), "inverted");
}

#[test]
fn planning_a_whole_resource_matches_planning_its_full_range() {
    assert_eq!(plan(1000, 5), plan_range(0, 1000, 5));
}

#[test]
fn a_resume_uses_the_same_chunks_as_a_fresh_run() {
    // Nothing on disk: the same chunks a fresh run would use, all with zero received.
    let (ranges, received) = plan_resume(1000, 5, &[]);
    assert_eq!(ranges, plan_range(0, 1000, 5));
    assert_eq!(received, [0, 0, 0, 0, 0]);
}

#[test]
fn a_partial_chunk_resumes_from_its_prefix() {
    // 100 bytes of the first 200-byte chunk are on disk: the plan stays five chunks, and only the first
    // carries a received prefix, so the download fetches just that chunk's remaining 100 bytes.
    let (ranges, received) = plan_resume(1000, 5, &[ByteRange { start: 0, end: 100 }]);
    assert_eq!(ranges, plan_range(0, 1000, 5), "same five chunks as fresh");
    assert_eq!(received, [100, 0, 0, 0, 0]);
}

#[test]
fn a_hole_before_a_chunks_prefix_is_not_credited() {
    // A recorded range that does not start at the chunk's start (a hole precedes it) credits nothing:
    // only a clean leading prefix counts, so the covered bytes past the hole are refetched.
    let (_, received) = plan_resume(
        1000,
        5,
        &[ByteRange {
            start: 50,
            end: 150,
        }],
    );
    assert_eq!(
        received[0], 0,
        "the [50,150) range is not a prefix of [0,200)"
    );
}

#[test]
fn a_resume_credits_only_the_contiguous_prefix() {
    // The first chunk [0,200) has [0,120) then a hole then [160,200): only the leading 120 count.
    let done = [
        ByteRange {
            start: 160,
            end: 200,
        },
        ByteRange { start: 0, end: 120 },
    ];
    let (_, received) = plan_resume(1000, 5, &done);
    assert_eq!(received[0], 120, "sorted and merged, then the prefix taken");
}

#[test]
fn a_different_parts_count_re_splits_and_credits_each_chunk() {
    // Recorded as one 250-byte prefix, resumed as ten 100-byte chunks: the first two are complete, the
    // third is half done, the rest empty. Same as the user's "ten chunks, some done" model.
    let (ranges, received) = plan_resume(1000, 10, &[ByteRange { start: 0, end: 250 }]);
    assert_eq!(ranges, plan_range(0, 1000, 10), "the requested ten chunks");
    assert_eq!(received, [100, 100, 50, 0, 0, 0, 0, 0, 0, 0]);
}

#[test]
fn a_fully_downloaded_resume_has_every_chunk_complete() {
    let (ranges, received) = plan_resume(
        1000,
        4,
        &[ByteRange {
            start: 0,
            end: 1000,
        }],
    );
    assert_eq!(
        received,
        ranges.iter().map(ByteRange::len).collect::<Vec<_>>(),
        "each chunk's received equals its full length"
    );
}
