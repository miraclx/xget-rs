//! Tests for the binary's display policy and pre-network argument checks.

use clap::Parser as _;

use super::{Cli, DisplayFormat};

#[test]
fn speed_defaults_to_bytes_matching_the_size_readout() {
    assert_eq!(
        DisplayFormat::HumanBytes.speed(10 * 1024 * 1024),
        "10 MiB/s"
    );
}

#[test]
fn bits_flag_renders_decimal_bits_per_second() {
    assert_eq!(
        DisplayFormat::HumanBits.speed(10 * 1024 * 1024),
        "83.89 Mbps"
    );
    assert_eq!(DisplayFormat::HumanBits.size(1024 * 1024), "1 MiB");
}

#[test]
fn raw_takes_precedence_over_bits() {
    let cli = Cli::try_parse_from(["xget", "https://example.com/file", "--raw-sizes", "--bits"])
        .expect("valid display flags");
    assert_eq!(DisplayFormat::from(&cli).speed(1_048_576), "1048576/s");
    assert_eq!(DisplayFormat::from(&cli).size(1_048_576), "1048576");
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
