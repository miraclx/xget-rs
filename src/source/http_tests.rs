use super::http::{
    content_disposition_name, content_range, content_range_start, content_range_total,
};
use crate::ByteRange;

#[test]
fn a_content_range_is_a_nonempty_window_within_its_known_total() {
    assert_eq!(
        content_range("bytes 0-0/1"),
        Some(ByteRange { start: 0, end: 1 })
    );
    assert_eq!(
        content_range("bytes 100-199/1000"),
        Some(ByteRange {
            start: 100,
            end: 200
        })
    );
    assert_eq!(
        content_range("bytes 100-199/*"),
        Some(ByteRange {
            start: 100,
            end: 200
        })
    );
    for value in [
        "bytes 0-0/0",
        "bytes 100-199/199",
        "bytes 200-100/1000",
        "bytes */1000",
        "bytes 0-18446744073709551615/*",
        "bytes 0-1/2/3",
        "bytes 0-1/garbage",
        "items 0-1/2",
    ] {
        assert_eq!(content_range(value), None, "invalid Content-Range: {value}");
    }
}

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

#[test]
fn a_plain_disposition_filename_is_taken_and_unquoted() {
    assert_eq!(
        content_disposition_name(r#"attachment; filename="report.pdf""#),
        Some("report.pdf".to_owned())
    );
    assert_eq!(
        content_disposition_name("attachment; filename=data.csv"),
        Some("data.csv".to_owned())
    );
    assert_eq!(content_disposition_name("inline"), None);
}

#[test]
fn an_extended_filename_is_percent_decoded_and_preferred() {
    assert_eq!(
        content_disposition_name(
            "attachment; filename=\"fallback.bin\"; filename*=UTF-8''my%20file.zip"
        ),
        Some("my file.zip".to_owned()),
        "the extended form wins over the plain one"
    );
}

#[test]
fn a_disposition_filename_is_stripped_to_its_basename() {
    assert_eq!(
        content_disposition_name(r#"attachment; filename="../../etc/passwd""#),
        Some("passwd".to_owned()),
        "path components cannot escape the output directory"
    );
}
