#![allow(clippy::expect_used)]

use std::sync::Arc;

use super::{
    HeaderError, HeaderSet, MAX_HEADERS, MAX_NAME_BYTES, MAX_SET_BYTES, MAX_VALUE_BYTES,
    RequestHeader,
};

fn header(name: &str, value: &str) -> RequestHeader {
    RequestHeader::new(name, value).expect("valid request header")
}

fn exactly_full_set() -> HeaderSet {
    let value = "x".repeat(MAX_VALUE_BYTES - 1);
    let mut set = HeaderSet::default();
    for name in ["a", "b", "c", "d"] {
        set = set
            .set(RequestHeader::new(name, &value).expect("bounded header"))
            .expect("header within text limit");
    }
    set
}

#[test]
fn token_names_are_canonicalized_to_lowercase() {
    let parsed = RequestHeader::new("X-Request-ID_1.~", "value").expect("token name");
    assert_eq!(parsed.name(), "x-request-id_1.~");

    for name in ["a", "Z9", "x-y", "x_y", "x.y", "x!#$%&'*+-.^_`|~"] {
        assert!(
            RequestHeader::new(name, "value").is_ok(),
            "expected valid token name {name}"
        );
    }
}

#[test]
fn empty_values_are_allowed_and_the_constructor_preserves_exact_spacing() {
    let parsed: RequestHeader = "X-Empty:".parse().expect("empty value");
    assert_eq!(parsed.name(), "x-empty");
    assert_eq!(parsed.value(), "");

    let padded: RequestHeader = "X-Empty:   ".parse().expect("padded empty value");
    assert_eq!(padded.value(), "");

    let constructed = header("X-Empty", "   ");
    assert_eq!(constructed.value(), "   ");
}

#[test]
fn from_str_splits_on_the_first_colon_and_omits_surrounding_ascii_spaces() {
    let parsed: RequestHeader = "  X-Origin  :  https://example.test/a:b  "
        .parse()
        .expect("header spelling");
    assert_eq!(parsed.name(), "x-origin");
    assert_eq!(parsed.value(), "https://example.test/a:b");
}

#[test]
fn from_str_requires_the_colon_separator() {
    let error = "x-test"
        .parse::<RequestHeader>()
        .expect_err("missing colon");
    assert!(matches!(error, HeaderError::MissingColon));
}

#[test]
fn every_transport_managed_name_is_rejected_in_any_case() {
    const MANAGED: [&str; 20] = [
        "host",
        "range",
        "if-range",
        "content-range",
        "if-match",
        "if-none-match",
        "if-modified-since",
        "if-unmodified-since",
        "accept-encoding",
        "content-encoding",
        "content-length",
        "transfer-encoding",
        "content-type",
        "connection",
        "proxy-connection",
        "proxy-authorization",
        "upgrade",
        "te",
        "trailer",
        "depth",
    ];
    assert_eq!(super::MANAGED_NAMES.len(), MANAGED.len());
    for name in MANAGED {
        assert!(
            super::MANAGED_NAMES.contains(&name),
            "implementation must reject {name}"
        );
        let upper = name.to_ascii_uppercase();
        let mixed = name
            .chars()
            .enumerate()
            .map(|(index, character)| {
                if index % 2 == 0 {
                    character.to_ascii_uppercase()
                } else {
                    character
                }
            })
            .collect::<String>();
        for spelling in [name, upper.as_str(), mixed.as_str()] {
            assert!(
                matches!(
                    RequestHeader::new(spelling, "value"),
                    Err(HeaderError::ManagedName)
                ),
                "expected managed rejection for {spelling}"
            );
        }
    }
}

#[test]
fn values_reject_controls_del_newlines_and_non_ascii() {
    for value in ["\r", "\n", "\t", "\0", "\u{7f}", "caf\u{e9}"] {
        assert!(
            matches!(
                RequestHeader::new("x-test", value),
                Err(HeaderError::InvalidValue)
            ),
            "expected invalid value for {value:?}"
        );
    }
    assert!(matches!(
        "x-test: ok\r\nx-injected: yes".parse::<RequestHeader>(),
        Err(HeaderError::InvalidValue)
    ));
}

#[test]
fn names_reject_spaces_controls_non_ascii_and_separators() {
    for name in [
        "x test", "x\ttest", "x\rtest", "x\ntest", "x-\u{e9}", "x:test",
    ] {
        assert!(
            matches!(
                RequestHeader::new(name, "value"),
                Err(HeaderError::InvalidName)
            ),
            "expected invalid name for {name:?}"
        );
    }
    assert!(matches!(
        RequestHeader::new("", "value"),
        Err(HeaderError::EmptyName)
    ));
    assert!(matches!(
        ": value".parse::<RequestHeader>(),
        Err(HeaderError::EmptyName)
    ));
}

#[test]
fn name_and_value_limits_are_exact() {
    let name = "a".repeat(MAX_NAME_BYTES);
    assert!(RequestHeader::new(&name, "").is_ok());
    let name = "a".repeat(MAX_NAME_BYTES + 1);
    assert!(matches!(
        RequestHeader::new(&name, ""),
        Err(HeaderError::NameTooLong)
    ));

    let value = "v".repeat(MAX_VALUE_BYTES);
    assert!(RequestHeader::new("x", &value).is_ok());
    let spelling = format!("x:{value}");
    assert!(spelling.parse::<RequestHeader>().is_ok());

    let value = "v".repeat(MAX_VALUE_BYTES + 1);
    assert!(matches!(
        RequestHeader::new("x", &value),
        Err(HeaderError::ValueTooLong)
    ));
}

#[test]
fn distinct_header_count_is_bounded_at_thirty_two() {
    let mut set = HeaderSet::default();
    for index in 0..MAX_HEADERS {
        let name = format!("x-{index}");
        set = set
            .set(RequestHeader::new(&name, "v").expect("bounded header"))
            .expect("header within count limit");
    }
    assert_eq!(set.len(), MAX_HEADERS);

    set = set
        .set(header("X-0", "replacement"))
        .expect("same-name replacement at the count limit");
    assert_eq!(set.len(), MAX_HEADERS);

    let error = set
        .set(header("x-overflow", "v"))
        .expect_err("new name beyond count limit");
    assert!(matches!(error, HeaderError::TooManyHeaders));
}

#[test]
fn replacement_subtracts_previous_text_and_keeps_the_insertion_position() {
    let set = HeaderSet::default()
        .set(header("x-a", "12345"))
        .expect("first header")
        .set(header("x-b", "abc"))
        .expect("second header");
    let previous = set.text_bytes();
    assert_eq!(previous, ("x-a".len() + 5) + ("x-b".len() + 3));

    let replaced = set.set(header("X-A", "z")).expect("canonical replacement");
    assert_eq!(replaced.len(), 2);
    assert_eq!(
        replaced.text_bytes(),
        previous - ("x-a".len() + 5) + ("x-a".len() + 1)
    );
    assert_eq!(
        replaced
            .iter()
            .map(|entry| entry.name())
            .collect::<Vec<_>>(),
        ["x-a", "x-b"]
    );
    let retained = replaced
        .iter()
        .find(|entry| entry.name() == "x-a")
        .expect("replacement retained");
    assert_eq!(retained.value(), "z");
}

#[test]
fn the_combined_text_bound_is_exact_and_an_addition_does_not_overflow_it() {
    let set = exactly_full_set();
    assert_eq!(set.len(), 4);
    assert_eq!(set.text_bytes(), MAX_SET_BYTES);

    let error = set
        .set(RequestHeader::new("e", "").expect("valid empty value"))
        .expect_err("new header beyond text limit");
    assert!(matches!(error, HeaderError::TextLimit));
}

#[test]
fn a_replacement_that_grows_past_the_text_bound_is_rejected() {
    let set = exactly_full_set();
    let value = "x".repeat(MAX_VALUE_BYTES);
    let error = set
        .set(RequestHeader::new("a", &value).expect("valid replacement value"))
        .expect_err("replacement beyond text limit");
    assert!(matches!(error, HeaderError::TextLimit));
}

#[test]
fn an_empty_set_is_default_and_accounts_for_nothing() {
    let set = HeaderSet::default();
    assert!(set.is_empty());
    assert_eq!(set.len(), 0);
    assert_eq!(set.text_bytes(), 0);
    assert_eq!(set.iter().count(), 0);
}

#[test]
fn iteration_is_borrowed_and_insertion_ordered() {
    let set = HeaderSet::default()
        .set(header("x-b", "2"))
        .expect("first")
        .set(header("x-a", "1"))
        .expect("second")
        .set(header("X-B", "3"))
        .expect("replacement");
    let observed = set
        .iter()
        .map(|entry| (entry.name(), entry.value()))
        .collect::<Vec<_>>();
    assert_eq!(observed, [("x-b", "3"), ("x-a", "1")]);
}

#[test]
fn clone_shares_the_zeroizing_value_and_debug_redacts_it() {
    let secret = "token-value-that-must-not-leak";
    let original = header("X-Api-Key", secret);
    let clone = RequestHeader::clone(&original);
    assert_eq!(clone.name(), "x-api-key");
    assert_eq!(clone.value(), secret);
    assert!(Arc::ptr_eq(&original.value, &clone.value));
    assert_eq!(original.text_bytes(), clone.text_bytes());

    let rendered = format!("{original:?}");
    assert!(rendered.contains("x-api-key"));
    assert!(rendered.contains("[redacted]"));
    assert!(!rendered.contains(secret));

    let set = HeaderSet::default().set(original).expect("bounded set");
    let rendered = format!("{set:?}");
    assert!(rendered.contains("[redacted]"));
    assert!(!rendered.contains(secret));
}

#[test]
fn typed_errors_retain_no_rejected_text() {
    let name_error = RequestHeader::new("secret name", "value").expect_err("invalid name");
    let rendered = format!("{name_error} {name_error:?}");
    assert!(!rendered.contains("secret"));

    let value_error = RequestHeader::new("x-test", "secret-value\r").expect_err("control value");
    let rendered = format!("{value_error} {value_error:?}");
    assert!(!rendered.contains("secret-value"));
    assert!(!rendered.contains("x-test"));
}
