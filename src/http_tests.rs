use crate::http::{content_range_start, content_range_total};

#[test]
fn total_is_the_number_after_the_slash() {
    assert_eq!(content_range_total("bytes 0-0/1234"), Some(1234));
    assert_eq!(content_range_total("bytes 100-199/58000"), Some(58000));
    assert_eq!(content_range_total("bytes */4096"), Some(4096));
    assert_eq!(content_range_total("nonsense"), None);
}

#[test]
fn start_is_the_offset_after_the_unit() {
    assert_eq!(content_range_start("bytes 0-0/1234"), Some(0));
    assert_eq!(content_range_start("bytes 100-199/1234"), Some(100));
    assert_eq!(
        content_range_start("100-199/1234"),
        None,
        "the bytes unit is required"
    );
    assert_eq!(content_range_start("bytes x-y/z"), None);
}
