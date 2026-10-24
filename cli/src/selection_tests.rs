use xget::transfer::{Digest, Provenance};

use super::Checks;

const SHA256: &str = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

#[test]
fn agreeing_origins_survive_without_duplicate_proposals() {
    let digest: Digest = SHA256.parse().unwrap();
    let mut checks = Checks::default();
    for origin in [
        Provenance::Source,
        Provenance::Caller,
        Provenance::Sidecar,
        Provenance::Sidecar,
    ] {
        checks.add(digest, origin).unwrap();
    }
    assert_eq!(checks.iter().count(), 3);
    assert!(
        checks
            .iter()
            .any(|(_, origin)| origin == Provenance::Sidecar)
    );
}

#[test]
fn conflicting_literal_source_or_sidecar_values_fail_before_opening_payload() {
    let good: Digest = SHA256.parse().unwrap();
    let bad: Digest = format!("sha256:{}", "00".repeat(32)).parse().unwrap();
    for first in [Provenance::Source, Provenance::Caller, Provenance::Sidecar] {
        let mut checks = Checks::default();
        checks.add(good, first).unwrap();
        let failure = checks.add(bad, Provenance::Sidecar).err().unwrap();
        assert_eq!(failure.code(), 2);
        assert_eq!(checks.iter().count(), 1);
    }
}

#[test]
fn zero_one_and_different_algorithm_proposals_remain_independent() {
    let mut checks = Checks::default();
    assert_eq!(checks.iter().count(), 0);
    checks
        .add(SHA256.parse().unwrap(), Provenance::Sidecar)
        .unwrap();
    assert_eq!(checks.iter().count(), 1);
    checks
        .add(
            format!("sha512:{}", "00".repeat(64)).parse().unwrap(),
            Provenance::Sidecar,
        )
        .unwrap();
    assert_eq!(checks.iter().count(), 2);
}
