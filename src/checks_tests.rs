#![allow(clippy::unwrap_used)]

use core::str::FromStr;

use super::{
    Algorithm, Checks, Digest, Measurement, Origins, ParseDigestError, Provenance, Requirements,
};

// RFC 1321 (MD5), FIPS 180-4's standard empty/abc examples (SHA),
// and the BLAKE3 reference implementation's empty/abc hashes.
const VECTORS: [(Algorithm, &str, &str); 5] = [
    (
        Algorithm::Md5,
        "d41d8cd98f00b204e9800998ecf8427e",
        "900150983cd24fb0d6963f7d28e17f72",
    ),
    (
        Algorithm::Sha1,
        "da39a3ee5e6b4b0d3255bfef95601890afd80709",
        "a9993e364706816aba3e25717850c26c9cd0d89d",
    ),
    (
        Algorithm::Sha256,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    ),
    (
        Algorithm::Sha512,
        concat!(
            "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce",
            "47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"
        ),
        concat!(
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a",
            "2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
        ),
    ),
    (
        Algorithm::Blake3,
        "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
        "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85",
    ),
];

struct Fixture {
    requirements: Requirements,
}

impl Fixture {
    fn abc() -> Self {
        let mut requirements = Requirements::default();
        for (algorithm, _, abc) in VECTORS {
            requirements
                .insert(Digest::parse(algorithm, abc).unwrap(), Provenance::Caller)
                .unwrap();
        }
        Self { requirements }
    }

    fn empty() -> Self {
        let mut requirements = Requirements::default();
        for (algorithm, empty, _) in VECTORS {
            requirements
                .insert(Digest::parse(algorithm, empty).unwrap(), Provenance::Caller)
                .unwrap();
        }
        Self { requirements }
    }

    fn start(self) -> Checks {
        self.requirements.start(Measurement::None)
    }
}

#[test]
fn prefix_anchors_finalize_requested_strong_state_copies_without_consuming_whole_checks() {
    for (algorithm, empty, abc) in VECTORS {
        let mut checks = Requirements::default().start(Measurement::Algorithm(algorithm));
        let strong = matches!(
            algorithm,
            Algorithm::Sha256 | Algorithm::Sha512 | Algorithm::Blake3
        );
        assert_eq!(
            checks.prefix_anchor(),
            strong.then(|| Digest::parse(algorithm, empty).unwrap())
        );
        checks.update(b"abc");
        assert_eq!(
            checks.prefix_anchor(),
            strong.then(|| Digest::parse(algorithm, abc).unwrap())
        );
        assert_eq!(
            checks.finish().unwrap().measured(),
            Some(Digest::parse(algorithm, abc).unwrap())
        );
    }
    assert!(
        Requirements::default()
            .start(Measurement::None)
            .prefix_anchor()
            .is_none()
    );
    let mut checks = Fixture::abc().start();
    checks.update(b"a");
    assert!(checks.prefix_anchor().is_some());
    checks.update(b"bc");
    assert_eq!(checks.finish().unwrap().matched_requirements().count(), 5);
}

#[test]
fn all_supported_algorithms_match_real_empty_vectors_with_no_chunks_or_empty_chunks() {
    for count in [0, 1, 8] {
        let mut checks = Fixture::empty().start();
        for _ in 0..count {
            checks.update(b"");
        }
        let outcome = checks.finish().unwrap();
        assert_eq!(outcome.matched_requirements().count(), VECTORS.len());
        assert_eq!(outcome.measured(), None);
    }
}

#[test]
fn all_required_algorithms_share_one_input_traversal_regardless_of_chunking() {
    let partitions: [&[&[u8]]; 3] = [
        &[b"abc"],
        &[b"a", b"bc"],
        &[b"", b"a", b"", b"b", b"c", b""],
    ];
    for chunks in partitions {
        let mut checks = Fixture::abc().start();
        for chunk in chunks {
            checks.update(chunk);
        }
        assert_eq!(checks.finish().unwrap().matched_requirements().count(), 5);
    }
}

#[test]
fn parsing_builds_only_supported_exact_length_digests_and_normalizes_hex() {
    for (algorithm, _, abc) in VECTORS {
        let parsed = Digest::parse(algorithm, abc).unwrap();
        assert_eq!(parsed.algorithm(), algorithm);
        assert_eq!(parsed.as_bytes().len(), algorithm.byte_len());
        assert_eq!(parsed.to_string().parse::<Digest>().unwrap(), parsed);
        assert_eq!(
            Digest::parse(algorithm, &abc.to_uppercase()).unwrap(),
            parsed
        );
        assert!(matches!(
            Digest::parse(algorithm, &abc[2..]),
            Err(ParseDigestError::Length { algorithm: got }) if got == algorithm
        ));
        assert!(matches!(
            Digest::parse(algorithm, &format!("{abc}00")),
            Err(ParseDigestError::Length { algorithm: got }) if got == algorithm
        ));
        assert!(matches!(
            Digest::parse(algorithm, &"z".repeat(abc.len())),
            Err(ParseDigestError::Hex { algorithm: got }) if got == algorithm
        ));
    }
    assert!(matches!(
        Digest::from_str(""),
        Err(ParseDigestError::Syntax)
    ));
    for value in ["none:00", "sha384:00", "https://secret.invalid/token"] {
        let error = value.parse::<Digest>().unwrap_err();
        assert!(matches!(error, ParseDigestError::Algorithm));
        assert!(!format!("{error:?} {error}").contains(value));
    }
    assert!(matches!(
        Digest::parse(Algorithm::Md5, "éééééééééééééééé"),
        Err(ParseDigestError::Hex {
            algorithm: Algorithm::Md5
        })
    ));
}

#[test]
fn agreeing_requirements_merge_all_origins_without_growing_the_set() {
    let digest = Digest::parse(Algorithm::Sha256, VECTORS[2].2).unwrap();
    let mut requirements = Requirements::default();
    for _ in 0..100 {
        for provenance in [Provenance::Caller, Provenance::Source, Provenance::Sidecar] {
            requirements.insert(digest, provenance).unwrap();
        }
    }
    let mut checks = requirements.start(Measurement::None);
    assert_eq!(checks.hashers.iter().flatten().count(), 1);
    checks.update(b"abc");
    let outcome = checks.finish().unwrap();
    let matched: Vec<_> = outcome.matched_requirements().collect();
    assert_eq!(matched.len(), 1);
    assert_eq!(
        matched[0].origins.iter().collect::<Vec<_>>(),
        [Provenance::Caller, Provenance::Source, Provenance::Sidecar]
    );
}

#[test]
fn conflicts_are_typed_and_do_not_replace_the_agreed_requirement_or_origins() {
    for (algorithm, empty, abc) in VECTORS {
        let expected = Digest::parse(algorithm, abc).unwrap();
        let proposed = Digest::parse(algorithm, empty).unwrap();
        let mut requirements = Requirements::default();
        requirements.insert(expected, Provenance::Caller).unwrap();
        let conflict = requirements
            .insert(proposed, Provenance::Sidecar)
            .unwrap_err();
        assert_eq!(conflict.existing.digest, expected);
        assert_eq!(conflict.existing.origins, Origins(Provenance::Caller.bit()));
        assert_eq!(conflict.proposed, proposed);
        assert_eq!(conflict.provenance, Provenance::Sidecar);
        // A Result-returning insertion is rejected, not a sticky builder operation.
        // Integration must propagate this error before accepting payload bytes.
        requirements.insert(expected, Provenance::Source).unwrap();
        let mut checks = requirements.start(Measurement::None);
        checks.update(b"abc");
        let outcome = checks.finish().unwrap();
        let matched = outcome.matched_requirements().next().unwrap();
        assert_eq!(matched.digest, expected);
        assert_eq!(
            matched.origins.iter().collect::<Vec<_>>(),
            [Provenance::Caller, Provenance::Source]
        );
    }
}

#[test]
fn measurement_none_cannot_cancel_required_checks_or_create_hidden_work() {
    let mut checks = Fixture::abc().start();
    assert_eq!(checks.hashers.iter().flatten().count(), 5);
    checks.update(b"wrong");
    let mismatch = checks.finish().unwrap_err();
    assert_eq!(mismatch.requirement.digest.algorithm(), Algorithm::Md5);
    assert_eq!(mismatch.actual.algorithm(), Algorithm::Md5);
    assert_ne!(mismatch.actual, mismatch.requirement.digest);

    let mut checks = Requirements::default().start(Measurement::None);
    assert_eq!(checks.hashers.iter().flatten().count(), 0);
    checks.update(b"abc");
    let outcome = checks.finish().unwrap();
    assert_eq!(outcome.matched_requirements().count(), 0);
    assert_eq!(outcome.measured(), None);
    assert_eq!(outcome.strong_anchor(), None);
}

#[test]
fn every_algorithm_can_be_measured_without_manufacturing_requirements() {
    for (algorithm, _, abc) in VECTORS {
        let mut checks = Requirements::default().start(Measurement::Algorithm(algorithm));
        assert_eq!(checks.hashers.iter().flatten().count(), 1);
        checks.update(b"abc");
        let outcome = checks.finish().unwrap();
        assert_eq!(outcome.matched_requirements().count(), 0);
        assert_eq!(
            outcome.measured(),
            Some(Digest::parse(algorithm, abc).unwrap())
        );
    }
    assert_eq!(
        Measurement::default(),
        Measurement::Algorithm(Algorithm::Sha256)
    );
}

#[test]
fn measurement_and_requirement_of_the_same_algorithm_compute_once() {
    for (algorithm, _, abc) in VECTORS {
        let digest = Digest::parse(algorithm, abc).unwrap();
        let mut requirements = Requirements::default();
        requirements.insert(digest, Provenance::Source).unwrap();
        let mut checks = requirements.start(Measurement::Algorithm(algorithm));
        assert_eq!(checks.hashers.iter().flatten().count(), 1);
        checks.update(b"abc");
        let outcome = checks.finish().unwrap();
        assert_eq!(outcome.measured(), Some(digest));
        assert_eq!(
            outcome.matched_requirements().next().unwrap().digest,
            digest
        );
    }
}

#[test]
fn a_different_optional_algorithm_does_not_replace_a_requirement() {
    let mut requirements = Requirements::default();
    let required = Digest::parse(Algorithm::Md5, VECTORS[0].2).unwrap();
    requirements.insert(required, Provenance::Caller).unwrap();
    let mut checks = requirements.start(Measurement::default());
    assert_eq!(checks.hashers.iter().flatten().count(), 2);
    checks.update(b"abc");
    let outcome = checks.finish().unwrap();
    assert_eq!(
        outcome.matched_requirements().next().unwrap().digest,
        required
    );
    assert_eq!(outcome.measured().unwrap().algorithm(), Algorithm::Sha256);
    assert_eq!(outcome.strong_anchor(), outcome.measured());
}

#[test]
fn a_later_algorithm_mismatch_is_not_hidden_by_earlier_matches_or_measurement() {
    let mut requirements = Requirements::default();
    for (algorithm, empty, abc) in VECTORS {
        let value = if algorithm == Algorithm::Blake3 {
            empty
        } else {
            abc
        };
        requirements
            .insert(Digest::parse(algorithm, value).unwrap(), Provenance::Caller)
            .unwrap();
    }
    let mut checks = requirements.start(Measurement::default());
    checks.update(b"abc");
    let mismatch = checks.finish().unwrap_err();
    assert_eq!(
        mismatch.actual,
        Digest::parse(Algorithm::Blake3, VECTORS[4].2).unwrap()
    );
    assert_eq!(
        mismatch.requirement.digest,
        Digest::parse(Algorithm::Blake3, VECTORS[4].1).unwrap()
    );
}

#[test]
fn only_already_computed_strong_digests_can_anchor_content() {
    for (algorithm, _, abc) in VECTORS {
        let digest = Digest::parse(algorithm, abc).unwrap();
        let mut requirements = Requirements::default();
        requirements.insert(digest, Provenance::Sidecar).unwrap();
        let mut required = requirements.start(Measurement::None);
        required.update(b"abc");
        let mut measured = Requirements::default().start(Measurement::Algorithm(algorithm));
        measured.update(b"abc");
        let expected = match algorithm {
            Algorithm::Md5 | Algorithm::Sha1 => None,
            Algorithm::Sha256 | Algorithm::Sha512 | Algorithm::Blake3 => Some(digest),
        };
        assert_eq!(required.finish().unwrap().strong_anchor(), expected);
        assert_eq!(measured.finish().unwrap().strong_anchor(), expected);
    }
    let mut checks = Fixture::abc().start();
    checks.update(b"abc");
    assert_eq!(
        checks
            .finish()
            .unwrap()
            .strong_anchor()
            .unwrap()
            .algorithm(),
        Algorithm::Sha256
    );
}

#[test]
fn owned_check_state_and_results_preserve_send() {
    fn assert_send<T: Send>() {}
    assert_send::<Checks>();
    assert_send::<Requirements>();
    assert_send::<super::Outcome>();
    assert_send::<super::Mismatch>();
}

#[test]
fn a_required_failure_keeps_the_separate_finished_optional_measurement() {
    let mut requirements = Requirements::default();
    requirements
        .insert(
            Digest::parse(Algorithm::Md5, VECTORS[0].1).unwrap(),
            Provenance::Caller,
        )
        .unwrap();
    let mut checks = requirements.start(Measurement::Algorithm(Algorithm::Sha512));
    checks.update(b"abc");
    let mismatch = checks.finish().unwrap_err();
    assert_eq!(
        mismatch.actual,
        Digest::parse(Algorithm::Md5, VECTORS[0].2).unwrap()
    );
    assert_eq!(
        mismatch.results.measured(),
        Some(Digest::parse(Algorithm::Sha512, VECTORS[3].2).unwrap())
    );
    assert_eq!(mismatch.results.matched_requirements().count(), 0);
}

#[test]
fn frozen_policy_rejects_contradictory_measured_and_matched_values() {
    let expected = Digest::parse(Algorithm::Sha256, VECTORS[2].2).unwrap();
    let different = Digest::parse(Algorithm::Sha256, VECTORS[2].1).unwrap();
    let requirements = Requirements::single(expected, Provenance::Caller);
    let policy = super::CheckPolicy {
        requirements,
        measurement: Measurement::default(),
    };
    let fabricated = super::Outcome::from_parts(requirements, Some(different));
    assert!(!policy.describes(fabricated));
    assert!(!policy.satisfied_by(fabricated));
}
