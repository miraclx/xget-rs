use xget::transfer::{Algorithm, Digest};

use super::Parsed;

const HEX: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

#[test]
fn an_unlabelled_digest_binds_the_explicit_single_file_only() {
    let parsed = Parsed::parse(HEX, Algorithm::Sha256).unwrap();
    assert_eq!(
        parsed.bind("nested/original.bin").unwrap(),
        vec![Digest::parse(Algorithm::Sha256, HEX).unwrap()]
    );
    assert!(Parsed::parse("", Algorithm::Sha256).is_err());
}

#[test]
fn labelled_gnu_and_typed_records_match_exact_original_paths_not_local_renames() {
    for line in [
        format!("{HEX}  nested/original.bin"),
        format!("{HEX} *nested/original.bin"),
        format!("sha256:{HEX}  nested/original.bin"),
    ] {
        assert_eq!(
            Parsed::parse(&line, Algorithm::Sha256)
                .unwrap()
                .bind("nested/original.bin")
                .unwrap()
                .len(),
            1
        );
        for other in [
            "renamed.bin",
            "original.bin",
            "Nested/original.bin",
            "nested/./original.bin",
        ] {
            assert!(
                Parsed::parse(&line, Algorithm::Sha256)
                    .unwrap()
                    .bind(other)
                    .is_err()
            );
        }
    }
}

#[test]
fn multiple_paths_are_valid_but_duplicates_conflicts_and_unlabelled_mixtures_are_not() {
    let other = "00".repeat(32);
    let text = format!("{HEX}  first\n{other}  second\n");
    assert_eq!(
        Parsed::parse(&text, Algorithm::Sha256)
            .unwrap()
            .bind("first")
            .unwrap(),
        vec![Digest::parse(Algorithm::Sha256, HEX).unwrap()]
    );
    for text in [
        format!("{HEX}\n{other}\n"),
        format!("{HEX}\n{other}  second\n"),
        format!("{HEX}  first\n{HEX}  first\n"),
        format!("{HEX}  first\n{other}  first\n"),
    ] {
        assert!(Parsed::parse(&text, Algorithm::Sha256).is_err());
    }
}

#[test]
fn different_supported_algorithms_for_one_path_remain_independent_requirements() {
    let sha512 = "00".repeat(64);
    let text = format!("sha256:{HEX}  original\nsha512:{sha512}  original\n");
    let digests = Parsed::parse(&text, Algorithm::Md5)
        .unwrap()
        .bind("original")
        .unwrap();
    assert_eq!(digests.len(), 2);
    assert_eq!(digests[0].algorithm(), Algorithm::Sha256);
    assert_eq!(digests[1].algorithm(), Algorithm::Sha512);
}

#[test]
fn original_unicode_spaces_and_percent_spelling_are_not_normalized_or_decoded() {
    for path in ["  a  b ", "nested/caf\u{e9}", "nested/a%20b"] {
        let text = format!("{HEX} *{path}");
        assert!(
            Parsed::parse(&text, Algorithm::Sha256)
                .unwrap()
                .bind(path)
                .is_ok()
        );
    }
    let text = format!("{HEX}  nested/a%20b");
    assert!(
        Parsed::parse(&text, Algorithm::Sha256)
            .unwrap()
            .bind("nested/a b")
            .is_err()
    );
}

#[test]
fn malformed_unmatched_records_fail_instead_of_hiding_behind_a_valid_match() {
    for bad in [
        "not-hex  unrelated",
        "sha256:abcd  unrelated",
        "SHA256 (unrelated) = abcd",
        "none:abcd  unrelated",
        "\\escaped  unrelated",
        "abcd unrelated",
        "# arbitrary comment",
    ] {
        let text = format!("{HEX}  original\n{bad}\n");
        assert!(Parsed::parse(&text, Algorithm::Sha256).is_err());
    }
}

#[test]
fn record_text_path_and_entry_budgets_are_actual_parse_bounds() {
    assert!(Parsed::parse(&"x".repeat(65_537), Algorithm::Sha256).is_err());
    let long_path = format!("{HEX}  {}", "a".repeat(4097));
    assert!(Parsed::parse(&long_path, Algorithm::Sha256).is_err());
    let bounded = (0..256)
        .map(|index| format!("{HEX}  path-{index}\n"))
        .collect::<String>();
    assert!(Parsed::parse(&bounded, Algorithm::Sha256).is_ok());
    assert!(
        Parsed::parse(
            &(bounded + &format!("{HEX}  one-too-many\n")),
            Algorithm::Sha256
        )
        .is_err()
    );
}

#[test]
fn control_and_secret_sidecar_text_never_appears_in_parse_diagnostics() {
    let text =
        format!("{HEX}  original\nhttps://user:password@example.test/checks?signature=secret\n");
    let failure = Parsed::parse(&text, Algorithm::Sha256).err().unwrap();
    for secret in ["password", "signature", "secret", "example.test"] {
        assert!(!failure.message().contains(secret));
    }
    assert!(Parsed::parse(&format!("{HEX}  bad\u{1b}path"), Algorithm::Sha256).is_err());
}
