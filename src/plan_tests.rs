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
fn a_resume_with_nothing_done_matches_a_fresh_plan() {
    let (ranges, completed) = plan_resume(1000, 5, &[]);
    assert_eq!(ranges, plan_range(0, 1000, 5));
    assert!(completed.is_empty());
}

#[test]
fn a_resume_tiles_the_whole_resource_and_marks_the_done_range() {
    // 500 bytes already on disk at the front; the remainder is re-split into fresh chunks.
    let (ranges, completed) = plan_resume(1000, 4, &[ByteRange { start: 0, end: 500 }]);
    covers_exactly(&ranges, 1000);
    assert_eq!(completed.len(), 1);
    assert_eq!(ranges[completed[0]], ByteRange { start: 0, end: 500 });
}

#[test]
fn a_resume_keeps_a_hole_in_the_middle_as_its_own_chunk() {
    let (ranges, completed) = plan_resume(
        1000,
        4,
        &[ByteRange {
            start: 400,
            end: 600,
        }],
    );
    covers_exactly(&ranges, 1000);
    assert_eq!(completed.len(), 1);
    assert_eq!(
        ranges[completed[0]],
        ByteRange {
            start: 400,
            end: 600
        }
    );
    // No chunk straddles either boundary of the on-disk range.
    assert!(ranges.iter().all(|r| r.end <= 400 || r.start >= 400));
    assert!(ranges.iter().all(|r| r.end <= 600 || r.start >= 600));
}

#[test]
fn a_resume_merges_overlapping_and_unsorted_done_ranges() {
    let done = [
        ByteRange {
            start: 600,
            end: 800,
        },
        ByteRange { start: 0, end: 300 },
        ByteRange {
            start: 250,
            end: 400,
        }, // overlaps the [0, 300) range
    ];
    let (ranges, completed) = plan_resume(1000, 4, &done);
    covers_exactly(&ranges, 1000);
    let done_ranges: Vec<_> = completed.iter().map(|&index| ranges[index]).collect();
    assert_eq!(
        done_ranges,
        [
            ByteRange { start: 0, end: 400 },
            ByteRange {
                start: 600,
                end: 800
            },
        ],
        "overlapping and out-of-order ranges are clamped and merged"
    );
}

#[test]
fn a_fully_downloaded_resume_is_all_done_and_still_tiles() {
    let (ranges, completed) = plan_resume(
        1000,
        4,
        &[ByteRange {
            start: 0,
            end: 1000,
        }],
    );
    assert_eq!(
        ranges,
        [ByteRange {
            start: 0,
            end: 1000
        }]
    );
    assert_eq!(completed, [0]);
}
