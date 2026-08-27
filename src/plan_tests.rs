use crate::ByteRange;
use crate::plan::plan;

fn covers_exactly(ranges: &[ByteRange], length: u64) {
    let mut prev_end = 0;
    for range in ranges {
        assert_eq!(range.start, prev_end, "contiguous, no gap or overlap");
        assert!(range.end > range.start, "every chunk is non-empty");
        prev_end = range.end;
    }
    assert_eq!(prev_end, length, "the chunks cover the whole length");
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
