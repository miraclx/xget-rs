//! Tests for the binary's pure formatting helpers.

use super::fmt_speed;

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
