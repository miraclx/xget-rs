//! Tests for formatting and pre-network expectation validation.

use clap::Parser as _;

use super::{Cli, fmt_speed};

#[test]
fn speed_defaults_to_bytes_matching_the_size_readout() {
    // 10 MiB/s: bytes by default, IEC, the same spelling as the size readout, with a `/s` suffix.
    assert_eq!(fmt_speed(10 * 1024 * 1024, false, false), "10 MiB/s");
}

#[test]
fn bits_flag_renders_decimal_bits_per_second() {
    // 10 MiB/s in bits is ~83.89 Mbps (10 * 1024 * 1024 * 8 / 1_000_000), contracted `Mbps`.
    assert_eq!(fmt_speed(10 * 1024 * 1024, false, true), "83.89 Mbps");
}

#[test]
fn raw_takes_precedence_over_bits() {
    // A raw count is a byte count, so --bits does not apply: a plain number with a `/s` suffix,
    // even with bits requested.
    assert_eq!(fmt_speed(1_048_576, true, true), "1048576/s");
}

#[test]
fn an_expectation_without_an_algorithm_is_rejected_before_network_setup() {
    for expectation in ["abcd", "https://example.com/checksum", "none:abcd"] {
        let cli = Cli::try_parse_from([
            "xget",
            "not-a-url",
            "--checksum",
            "none",
            "--expect",
            expectation,
        ])
        .expect("syntactically valid flags");
        let error = cli.validate().expect_err("no effective algorithm");
        assert_eq!(
            error.to_string(),
            "--expect needs an algorithm: pass --checksum or use algo:hex"
        );
    }
}

#[test]
fn a_pinned_expectation_selects_its_algorithm_even_when_checksum_is_none() {
    for expectation in ["sha256:abcd", "https://example.com/file.sha256"] {
        let cli = Cli::try_parse_from([
            "xget",
            "not-a-url",
            "--checksum",
            "none",
            "--expect",
            expectation,
        ])
        .expect("valid pinned algorithm");
        cli.validate()
            .expect("the expectation supplies its own algorithm");
    }
}

#[test]
fn a_download_without_an_expectation_can_disable_hashing() {
    let cli =
        Cli::try_parse_from(["xget", "not-a-url", "--checksum", "none"]).expect("valid flags");
    cli.validate().expect("nothing needs a hash");
}
