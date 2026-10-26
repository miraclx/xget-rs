//! Real native-file fixtures only. Every fixture owns a newly created empty directory.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use core::cell::Cell;
use core::convert::Infallible;
use core::error::Error as _;
use core::fmt;
use core::sync::atomic::{AtomicU64, Ordering};
use std::fs;
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, symlink};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::stream;

use super::{
    Algorithm, ByteCount, Digest, FailureKind, Length, Measurement, OwnedSha256, Partial,
    Provenance, Reconciliation, RequestError, RequiredSha256, Sha256Digest, StateRef, Transfer,
};

struct LocalObservation<'a> {
    latest: &'a Rc<Cell<Option<super::observation::ProgressFacts>>>,
    calls: &'a Cell<u32>,
}

impl super::observation::Observer for LocalObservation<'_> {
    fn observe(&mut self, facts: super::observation::ProgressFacts) {
        self.latest.set(Some(facts));
        self.calls.set(self.calls.get() + 1);
    }
}

#[test]
fn a_send_but_not_sync_borrowed_observer_preserves_send_body_execution() {
    struct SendObservation(Cell<ByteCount>);
    impl super::observation::Observer for SendObservation {
        fn observe(&mut self, facts: super::observation::ProgressFacts) {
            self.0.set(facts.written());
        }
    }
    fn needs_send(_: impl Send) {}
    let fixture = Fixture::new();
    let mut observer = SendObservation(Cell::new(ByteCount(0)));
    let owner: super::OperationOwner = std::sync::Arc::new(());
    let pending = Transfer::from_body(
        stream::empty::<Result<Bytes, Infallible>>(),
        Length::Unknown,
    )
    .to_file(fixture.output())
    .unwrap();
    needs_send(pending.run_registered_observed(owner, &mut observer));
    assert_eq!(fs::read_dir(fixture.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn body_observation_is_borrowed_exact_and_does_not_invent_hash_work() {
    for measurement in [Measurement::None, Measurement::default()] {
        let fixture = Fixture::new();
        let latest = Rc::new(Cell::new(None));
        let calls = Cell::new(0);
        let mut observer = LocalObservation {
            latest: &latest,
            calls: &calls,
        };
        let owner: super::OperationOwner = std::sync::Arc::new(());
        let receipt = Transfer::from_body(
            stream::iter([Ok::<_, Infallible>(Bytes::from(vec![1; 70_000]))]),
            Length::Known(ByteCount(70_000)),
        )
        .measurement(measurement)
        .to_file(fixture.output())
        .unwrap()
        .run_registered_observed(owner, &mut observer)
        .await
        .unwrap();
        let facts = latest.get().unwrap();
        assert_eq!(facts.received(), ByteCount(70_000));
        assert_eq!(facts.written(), ByteCount(70_000));
        assert_eq!(facts.restored(), ByteCount(0));
        assert_eq!(facts.attempt_received(), facts.received());
        assert_eq!(facts.attempt_written(), facts.written());
        assert_eq!(facts.hashed(), receipt.partial().hashed);
        assert_eq!(facts.available(), ByteCount(70_000));
        assert_eq!(facts.reception(), super::observation::Reception::CleanEof);
        assert_eq!(facts.windows().count(), 0);
        assert_eq!(
            facts.hashed(),
            if measurement == Measurement::None {
                ByteCount(0)
            } else {
                ByteCount(70_000)
            },
        );
        // Initial, admitted chunk, two bounded writes and terminal return.
        assert!(calls.get() >= 5);
    }
}

#[tokio::test]
async fn stopped_and_overlong_body_observation_does_not_claim_publication() {
    for stop_now in [false, true] {
        let fixture = Fixture::new();
        let latest = Rc::new(Cell::new(None));
        let calls = Cell::new(0);
        let mut observer = LocalObservation {
            latest: &latest,
            calls: &calls,
        };
        let owner: super::OperationOwner = std::sync::Arc::new(());
        let failure = Transfer::from_body(
            stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"abc"))]),
            Length::Known(ByteCount(2)),
        )
        .to_file(fixture.output())
        .unwrap()
        .run_registered_until_observed(
            owner,
            async {
                if !stop_now {
                    core::future::pending::<()>().await;
                }
            },
            &mut observer,
        )
        .await
        .unwrap_err();
        let facts = latest.get().unwrap();
        assert_eq!(facts.received(), failure.partial.received);
        assert_eq!(facts.written(), ByteCount(0));
        assert_eq!(facts.hashed(), ByteCount(0));
        assert_eq!(facts.available(), ByteCount(0));
        assert_eq!(facts.windows().count(), 0);
        assert_eq!(
            facts.reception(),
            if stop_now {
                super::observation::Reception::Stopped
            } else {
                super::observation::Reception::Failed
            }
        );
        assert_eq!(
            facts.received(),
            if stop_now { ByteCount(0) } else { ByteCount(3) },
        );
        assert!(!fixture.output().exists());
        assert!(matches!(
            failure.kind,
            FailureKind::Stopped | FailureKind::LengthMismatch
        ));
    }
}

#[test]
fn requirements_are_parsed_not_silently_dropped() {
    assert!(matches!(
        "sha1:abc".parse::<RequiredSha256>(),
        Err(RequestError::UnsupportedRequirement)
    ));
    for malformed in ["", "sha256", "sha256:", "sha256:00", "sha256:zz"] {
        assert!(matches!(
            malformed.parse::<RequiredSha256>(),
            Err(RequestError::InvalidRequirement)
        ));
    }
    assert_eq!(
        requirement(b"abc").digest().to_string(),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

#[test]
fn unrequired_planning_and_unsupported_compatibility_parsing_have_no_filesystem_effects() {
    let fixture = Fixture::new();
    let missing = Transfer::from_body(
        stream::empty::<Result<Bytes, Infallible>>(),
        Length::Unknown,
    )
    .to_file(fixture.output());
    assert!(missing.is_ok());
    assert!(matches!(
        "blake3:00".parse::<RequiredSha256>(),
        Err(RequestError::UnsupportedRequirement)
    ));
    assert_eq!(fs::read_dir(fixture.path()).unwrap().count(), 0);
}

#[test]
fn reserved_file_roles_fail_before_any_filesystem_effect() {
    let fixture = Fixture::new();
    for name in [".xget-native.lock", ".xget-native-foreign"] {
        let pending = Transfer::from_body(
            stream::empty::<Result<Bytes, Infallible>>(),
            Length::Unknown,
        )
        .expect(requirement(b""))
        .to_file(fixture.path().join(name));
        assert!(matches!(pending, Err(RequestError::InvalidDestination)));
    }
    assert_eq!(fs::read_dir(fixture.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn conflicting_requirements_do_not_disappear_when_another_is_added() {
    let fixture = Fixture::new();
    let pending = Transfer::from_body(
        stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"b"))]),
        Length::Unknown,
    )
    .expect(requirement(b"a"))
    .expect(requirement(b"b"))
    .to_file(fixture.output());
    assert!(matches!(
        pending,
        Err(RequestError::ConflictingRequirements(_))
    ));
    assert_eq!(fs::read_dir(fixture.path()).unwrap().count(), 0);
}

#[test]
fn conflicts_remain_rejected_when_an_earlier_requirement_is_repeated() {
    let fixture = Fixture::new();
    let first = requirement(b"a");
    let pending = Transfer::from_body(
        stream::empty::<Result<Bytes, Infallible>>(),
        Length::Unknown,
    )
    .expect(first)
    .expect(requirement(b"b"))
    .expect(first)
    .to_file(fixture.output());
    assert!(matches!(
        pending,
        Err(RequestError::ConflictingRequirements(_))
    ));
    assert_eq!(fs::read_dir(fixture.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn repeated_matching_requirements_are_idempotent() {
    let fixture = Fixture::new();
    let required = requirement(b"a");
    Transfer::from_body(
        stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"a"))]),
        Length::Unknown,
    )
    .expect(required)
    .expect(required)
    .to_file(fixture.output())
    .unwrap()
    .run()
    .await
    .unwrap();
    assert_eq!(fs::read(fixture.output()).unwrap(), b"a");
}

#[tokio::test]
async fn directory_shaped_destinations_are_rejected_before_filesystem_effects() {
    for suffix in ["/", "/."] {
        let fixture = Fixture::new();
        let destination = PathBuf::from(format!("{}/output{suffix}", fixture.path().display()));
        let pending = Transfer::from_body(
            stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"a"))]),
            Length::Unknown,
        )
        .expect(requirement(b"a"))
        .to_file(&destination);
        assert!(matches!(pending, Err(RequestError::InvalidDestination)));
        assert_eq!(fs::read_dir(fixture.path()).unwrap().count(), 0);
    }
}

#[tokio::test]
async fn real_sha256_covers_zero_one_and_many_chunks() {
    let cases: &[(&[&[u8]], &str)] = &[
        (
            &[],
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        ),
        (
            &[b"a"],
            "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
        ),
        (
            &[b"a", b"", b"b", b"c"],
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        ),
    ];
    for (chunks, expected) in cases {
        let fixture = Fixture::new();
        let required: RequiredSha256 = format!("sha256:{expected}").parse().unwrap();
        let contents: Vec<u8> = chunks
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect();
        let body = stream::iter(
            chunks
                .iter()
                .map(|bytes| Ok::<_, Infallible>(Bytes::copy_from_slice(bytes))),
        );
        let pending = Transfer::from_body(body, Length::Known(ByteCount(contents.len() as u64)))
            .expect(required)
            .to_file(fixture.output())
            .unwrap();
        let state = StateRef::clone(pending.state());
        assert!(!state.path().exists());
        let receipt = pending.run().await.unwrap();
        assert_eq!(receipt.partial.actual.unwrap().to_string(), *expected);
        assert_eq!(receipt.partial.received, ByteCount(contents.len() as u64));
        assert_eq!(receipt.partial.written, receipt.partial.received);
        assert_eq!(receipt.partial.hashed, receipt.partial.received);
        assert_eq!(fs::read(fixture.output()).unwrap(), contents);
        assert_eq!(
            fs::metadata(fixture.output()).unwrap().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(fixture.output()).unwrap().ino(),
            fs::metadata(state.path().join("data")).unwrap().ino()
        );
        assert!(matches!(
            state.reconcile().await.unwrap(),
            Reconciliation::Published(_)
        ));
    }
}

#[tokio::test]
async fn a_large_chunk_is_written_in_bounded_physical_pieces() {
    let fixture = Fixture::new();
    let contents = vec![37; 3 * 64 * 1024 + 11];
    let required = requirement(&contents);
    let body = stream::iter([Ok::<_, Infallible>(Bytes::from(Vec::clone(&contents)))]);
    let receipt = Transfer::from_body(body, Length::Unknown)
        .expect(required)
        .to_file(fixture.output())
        .unwrap()
        .run()
        .await
        .unwrap();
    assert_eq!(receipt.partial.written, ByteCount(contents.len() as u64));
    assert_eq!(fs::read(fixture.output()).unwrap(), contents);
}

#[tokio::test]
async fn short_body_retains_a_needs_body_checkpoint_and_no_final() {
    let fixture = Fixture::new();
    let failure = Transfer::from_body(
        stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"a"))]),
        Length::Known(ByteCount(2)),
    )
    .expect(requirement(b"ab"))
    .to_file(fixture.output())
    .unwrap()
    .run()
    .await
    .unwrap_err();
    assert!(matches!(failure.kind, FailureKind::LengthMismatch));
    assert_eq!(failure.partial.written, ByteCount(1));
    assert_eq!(failure.partial.hashed, ByteCount(1));
    assert_eq!(failure.partial.actual, None);
    assert!(!fixture.output().exists());
    assert!(matches!(
        failure.state.unwrap().reconcile().await.unwrap(),
        Reconciliation::NeedsBody { .. }
    ));
}

#[tokio::test]
async fn overlong_body_reports_admitted_bytes_without_writing_the_rejected_chunk() {
    let fixture = Fixture::new();
    let body = stream::iter([
        Ok::<_, Infallible>(Bytes::from_static(b"a")),
        Ok(Bytes::from_static(b"bc")),
    ]);
    let failure = Transfer::from_body(body, Length::Known(ByteCount(2)))
        .expect(requirement(b"ab"))
        .to_file(fixture.output())
        .unwrap()
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.kind, FailureKind::LengthMismatch));
    assert_eq!(failure.partial.received, ByteCount(3));
    assert_eq!(failure.partial.written, ByteCount(1));
    assert_eq!(
        fs::read(failure.state.unwrap().path().join("data")).unwrap(),
        b"a"
    );
    assert!(!fixture.output().exists());
}

#[tokio::test]
async fn all_declared_bytes_do_not_hide_a_late_body_failure() {
    let fixture = Fixture::new();
    let body = stream::iter([Ok(Bytes::from_static(b"abc")), Err(Borrowed("late"))]);
    let failure = Transfer::from_body(body, Length::Known(ByteCount(3)))
        .expect(requirement(b"abc"))
        .to_file(fixture.output())
        .unwrap()
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.kind, FailureKind::Body(Borrowed("late"))));
    assert_eq!(failure.partial.written, ByteCount(3));
    assert_eq!(failure.partial.actual, None);
    assert!(!fixture.output().exists());
}

#[tokio::test]
async fn digest_mismatch_really_hashes_and_preserves_the_incumbent_bytes_and_inode() {
    let fixture = Fixture::new();
    fs::write(fixture.output(), b"sentinel").unwrap();
    let before = fs::metadata(fixture.output()).unwrap();
    let body = stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"abc"))]);
    let failure = Transfer::from_body(body, Length::Unknown)
        .expect(requirement(b"different"))
        .to_file(fixture.output())
        .unwrap()
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.kind, FailureKind::DigestMismatch(_)));
    assert_eq!(failure.partial.actual, Some(requirement(b"abc").digest()));
    assert_eq!(failure.partial.hashed, ByteCount(3));
    assert_eq!(fs::read(fixture.output()).unwrap(), b"sentinel");
    assert_eq!(fs::metadata(fixture.output()).unwrap().ino(), before.ino());
    assert_eq!(
        fs::read(failure.state.unwrap().path().join("data")).unwrap(),
        b"abc"
    );
}

#[tokio::test]
async fn a_matching_incumbent_still_returns_destination_exists_after_hashing() {
    let fixture = Fixture::new();
    fs::write(fixture.output(), b"abc").unwrap();
    let before = fs::metadata(fixture.output()).unwrap().ino();
    let body = stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"abc"))]);
    let failure = Transfer::from_body(body, Length::Unknown)
        .expect(requirement(b"abc"))
        .to_file(fixture.output())
        .unwrap()
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.kind, FailureKind::DestinationExists));
    assert_eq!(failure.partial.actual, Some(requirement(b"abc").digest()));
    assert_eq!(before, fs::metadata(fixture.output()).unwrap().ino());
    assert_eq!(fs::read(fixture.output()).unwrap(), b"abc");
}

#[tokio::test]
async fn symlink_incumbents_are_never_followed_or_replaced() {
    let fixture = Fixture::new();
    let target = fixture.path().join("sentinel");
    fs::write(&target, b"sentinel").unwrap();
    symlink(&target, fixture.output()).unwrap();
    let before = fs::symlink_metadata(fixture.output()).unwrap().ino();
    let body = stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"abc"))]);
    let failure = Transfer::from_body(body, Length::Unknown)
        .expect(requirement(b"abc"))
        .to_file(fixture.output())
        .unwrap()
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.kind, FailureKind::DestinationExists));
    assert_eq!(
        fs::symlink_metadata(fixture.output()).unwrap().ino(),
        before
    );
    assert_eq!(fs::read(&target).unwrap(), b"sentinel");
}

#[tokio::test]
async fn a_foreign_stable_lock_is_not_truncated_or_adopted() {
    let fixture = Fixture::new();
    let lock = fixture.path().join(".xget-native.lock");
    fs::write(&lock, b"foreign").unwrap();
    let before = fs::metadata(&lock).unwrap().ino();
    let body = stream::empty::<Result<Bytes, Infallible>>();
    let failure = Transfer::from_body(body, Length::Unknown)
        .expect(requirement(b""))
        .to_file(fixture.output())
        .unwrap()
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.kind, FailureKind::InvalidState));
    assert_eq!(fs::read(&lock).unwrap(), b"foreign");
    assert_eq!(fs::metadata(&lock).unwrap().ino(), before);
    assert_eq!(fs::read_dir(fixture.path()).unwrap().count(), 1);
}

#[tokio::test]
async fn an_owned_send_body_has_a_real_native_send_future() {
    let fixture = Fixture::new();
    let body = stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"abc"))]);
    let pending = Transfer::from_body(body, Length::Unknown)
        .expect(requirement(b"abc"))
        .to_file(fixture.output())
        .unwrap();
    let receipt = require_send(pending.run()).await.unwrap();
    assert_eq!(receipt.partial.written, ByteCount(3));
}

#[tokio::test]
async fn borrowed_body_causes_are_preserved_by_value_without_static_bounds() {
    let fixture = Fixture::new();
    let message = String::from("borrowed cause");
    let body = stream::iter([Err::<Bytes, _>(Borrowed(&message))]);
    let failure = Transfer::from_body(body, Length::Unknown)
        .expect(requirement(b""))
        .to_file(fixture.output())
        .unwrap()
        .run()
        .await
        .unwrap_err();
    match failure.kind {
        FailureKind::Body(Borrowed(value)) => {
            assert!(core::ptr::eq(value.as_ptr(), message.as_ptr()))
        }
        other => panic!("expected borrowed cause, got {other:?}"),
    }
}

#[tokio::test]
async fn a_static_local_rc_cause_keeps_its_real_error_source() {
    let fixture = Fixture::new();
    let value = Rc::new(Cell::new(7));
    let body = stream::iter([Err::<Bytes, _>(LocalCause(Rc::clone(&value)))]);
    let failure = Transfer::from_body(body, Length::Unknown)
        .expect(requirement(b""))
        .to_file(fixture.output())
        .unwrap()
        .run()
        .await
        .unwrap_err();
    let source = failure
        .source()
        .unwrap()
        .downcast_ref::<LocalCause>()
        .unwrap();
    assert!(Rc::ptr_eq(&source.0, &value));
}

#[test]
fn no_tokio_runtime_is_a_typed_failure_before_native_effects() {
    let fixture = Fixture::new();
    let pending = Transfer::from_body(
        stream::empty::<Result<Bytes, Infallible>>(),
        Length::Unknown,
    )
    .expect(requirement(b""))
    .to_file(fixture.output())
    .unwrap();
    let failure = futures::executor::block_on(pending.run()).unwrap_err();
    assert!(matches!(failure.kind, FailureKind::RuntimeUnavailable));
    assert_eq!(fs::read_dir(fixture.path()).unwrap().count(), 0);
}

const ABC_DIGESTS: [&str; 5] = [
    "md5:900150983cd24fb0d6963f7d28e17f72",
    "sha1:a9993e364706816aba3e25717850c26c9cd0d89d",
    "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    concat!(
        "sha512:ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a",
        "2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
    ),
    "blake3:6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85",
];

const EMPTY_DIGESTS: [&str; 5] = [
    "md5:d41d8cd98f00b204e9800998ecf8427e",
    "sha1:da39a3ee5e6b4b0d3255bfef95601890afd80709",
    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    concat!(
        "sha512:cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce",
        "47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"
    ),
    "blake3:af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
];

#[tokio::test]
async fn every_required_algorithm_publishes_real_empty_and_abc_vectors() {
    for (payload, vectors) in [
        (b"".as_slice(), EMPTY_DIGESTS),
        (b"abc".as_slice(), ABC_DIGESTS),
    ] {
        for value in vectors {
            let fixture = Fixture::new();
            let required: Digest = value.parse().unwrap();
            let receipt = Transfer::from_body(
                stream::iter([Ok::<_, Infallible>(Bytes::copy_from_slice(payload))]),
                Length::Known(ByteCount(payload.len() as u64)),
            )
            .expect(required)
            .measurement(Measurement::None)
            .to_file(fixture.output())
            .unwrap()
            .run()
            .await
            .unwrap();
            assert_eq!(receipt.checks().measured(), None);
            assert_eq!(
                receipt
                    .checks()
                    .matched_requirements()
                    .next()
                    .unwrap()
                    .digest,
                required
            );
            assert_eq!(receipt.partial.hashed, ByteCount(payload.len() as u64));
            assert_eq!(fs::read(fixture.output()).unwrap(), payload);
        }
    }
}

#[tokio::test]
async fn all_required_algorithms_share_the_real_body_traversal_with_measurement_disabled() {
    let fixture = Fixture::new();
    let chunks = Rc::new(Cell::new(0));
    let seen = Rc::clone(&chunks);
    let body = stream::iter([b"a".as_slice(), b"", b"b", b"c"].into_iter().map(|bytes| {
        seen.set(seen.get() + 1);
        Ok::<_, Infallible>(Bytes::copy_from_slice(bytes))
    }));
    let mut transfer =
        Transfer::from_body(body, Length::Known(ByteCount(3))).measurement(Measurement::None);
    for value in ABC_DIGESTS {
        transfer = transfer.expect(value.parse::<Digest>().unwrap());
    }
    assert_eq!(chunks.get(), 0);
    let receipt = transfer
        .to_file(fixture.output())
        .unwrap()
        .run()
        .await
        .unwrap();
    assert_eq!(chunks.get(), 4);
    assert_eq!(receipt.partial.received, ByteCount(3));
    assert_eq!(receipt.partial.written, ByteCount(3));
    assert_eq!(receipt.partial.hashed, ByteCount(3));
    assert_eq!(receipt.checks().matched_requirements().count(), 5);
    assert_eq!(receipt.checks().measured(), None);
    assert!(matches!(
        receipt.state.reconcile().await.unwrap(),
        Reconciliation::Published(_)
    ));
}

#[tokio::test]
async fn agreeing_provenance_merges_and_never_manufactures_optional_measurement() {
    let fixture = Fixture::new();
    let required: Digest = ABC_DIGESTS[0].parse().unwrap();
    let receipt = Transfer::from_body(
        stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"abc"))]),
        Length::Unknown,
    )
    .require(required, Provenance::Caller)
    .require(required, Provenance::Source)
    .require(required, Provenance::Sidecar)
    .expect(required)
    .measurement(Measurement::None)
    .to_file(fixture.output())
    .unwrap()
    .run()
    .await
    .unwrap();
    let matched = receipt.checks().matched_requirements().collect::<Vec<_>>();
    assert_eq!(matched.len(), 1);
    assert_eq!(
        matched[0].origins.iter().collect::<Vec<_>>(),
        [Provenance::Caller, Provenance::Source, Provenance::Sidecar]
    );
    assert_eq!(receipt.checks().measured(), None);
    assert_eq!(receipt.required, None);
}

#[test]
fn general_conflicts_stay_typed_and_sticky_before_native_effects() {
    let fixture = Fixture::new();
    let first: Digest = ABC_DIGESTS[0].parse().unwrap();
    let proposed: Digest = EMPTY_DIGESTS[0].parse().unwrap();
    let result = Transfer::from_body(
        stream::empty::<Result<Bytes, Infallible>>(),
        Length::Unknown,
    )
    .expect(first)
    .require(proposed, Provenance::Sidecar)
    .expect(ABC_DIGESTS[3].parse::<Digest>().unwrap())
    .expect(first)
    .measurement(Measurement::None)
    .to_file(fixture.output());
    match result {
        Err(RequestError::ConflictingRequirements(conflict)) => {
            assert_eq!(conflict.existing.digest, first);
            assert_eq!(conflict.proposed, proposed);
            assert_eq!(conflict.provenance, Provenance::Sidecar);
        }
        _ => panic!("expected a sticky typed conflict before native effects"),
    }
    assert_eq!(fs::read_dir(fixture.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn a_late_required_algorithm_mismatch_cannot_publish_or_be_disabled_by_none() {
    let fixture = Fixture::new();
    fs::write(fixture.output(), b"sentinel").unwrap();
    let inode = fs::metadata(fixture.output()).unwrap().ino();
    let mut transfer = Transfer::from_body(
        stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"abc"))]),
        Length::Unknown,
    )
    .measurement(Measurement::None);
    for value in &ABC_DIGESTS[..4] {
        transfer = transfer.expect(value.parse::<Digest>().unwrap());
    }
    transfer = transfer.expect(EMPTY_DIGESTS[4].parse::<Digest>().unwrap());
    let failure = transfer
        .to_file(fixture.output())
        .unwrap()
        .run()
        .await
        .unwrap_err();
    match &failure.kind {
        FailureKind::DigestMismatch(mismatch) => {
            assert_eq!(mismatch.actual, ABC_DIGESTS[4].parse::<Digest>().unwrap());
            assert_eq!(
                mismatch.requirement.digest,
                EMPTY_DIGESTS[4].parse::<Digest>().unwrap()
            );
            assert_eq!(mismatch.results.matched_requirements().count(), 4);
            assert_eq!(mismatch.results.measured(), None);
        }
        other => panic!("expected the required blake3 mismatch, got {other:?}"),
    }
    assert_eq!(failure.partial.hashed, ByteCount(3));
    assert_eq!(fs::read(fixture.output()).unwrap(), b"sentinel");
    assert_eq!(fs::metadata(fixture.output()).unwrap().ino(), inode);
    assert!(matches!(
        failure.state.unwrap().reconcile().await.unwrap(),
        Reconciliation::NeedsBody { .. }
    ));
}

#[tokio::test]
async fn an_unrequired_file_has_only_the_default_optional_sha256_measurement() {
    let fixture = Fixture::new();
    let receipt = Transfer::from_body(
        stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"abc"))]),
        Length::Unknown,
    )
    .to_file(fixture.output())
    .unwrap()
    .run()
    .await
    .unwrap();
    assert_eq!(receipt.required, None);
    assert_eq!(receipt.checks().matched_requirements().count(), 0);
    assert_eq!(
        receipt.checks().measured(),
        Some(ABC_DIGESTS[2].parse().unwrap())
    );
    assert_eq!(receipt.partial.hashed, ByteCount(3));
    assert!(matches!(
        receipt.state.reconcile().await.unwrap(),
        Reconciliation::Published(_)
    ));
}

#[tokio::test]
async fn a_different_optional_algorithm_is_reported_separately_and_can_anchor_recovery() {
    let fixture = Fixture::new();
    let required: Digest = ABC_DIGESTS[0].parse().unwrap();
    let measured: Digest = ABC_DIGESTS[3].parse().unwrap();
    let receipt = Transfer::from_body(
        stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"abc"))]),
        Length::Unknown,
    )
    .expect(required)
    .measurement(Measurement::Algorithm(Algorithm::Sha512))
    .to_file(fixture.output())
    .unwrap()
    .run()
    .await
    .unwrap();
    assert_eq!(
        receipt
            .checks()
            .matched_requirements()
            .next()
            .unwrap()
            .digest,
        required
    );
    assert_eq!(receipt.checks().measured(), Some(measured));
    assert_eq!(receipt.checks().strong_anchor(), Some(measured));
    assert_eq!(receipt.partial.actual, None);
    assert_eq!(receipt.partial.hashed, ByteCount(3));
    assert!(matches!(
        receipt.state.reconcile().await.unwrap(),
        Reconciliation::Published(_)
    ));
}

#[tokio::test]
async fn required_strong_algorithms_anchor_recovery_without_optional_or_hidden_hashes() {
    for value in &ABC_DIGESTS[2..] {
        let fixture = Fixture::new();
        let required: Digest = value.parse().unwrap();
        let receipt = Transfer::from_body(
            stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"abc"))]),
            Length::Unknown,
        )
        .expect(required)
        .measurement(Measurement::None)
        .to_file(fixture.output())
        .unwrap()
        .run()
        .await
        .unwrap();
        assert_eq!(receipt.checks().measured(), None);
        assert_eq!(receipt.checks().strong_anchor(), Some(required));
        assert!(matches!(
            receipt.state.reconcile().await.unwrap(),
            Reconciliation::Published(_)
        ));
        fs::write(fixture.output(), b"xyz").unwrap();
        assert!(matches!(
            receipt.state.reconcile().await.unwrap(),
            Reconciliation::Conflict(_)
        ));
    }
}

#[tokio::test]
async fn no_anchor_means_not_revalidatable_not_a_hidden_recovery_fingerprint() {
    for required in [None, Some(ABC_DIGESTS[0]), Some(ABC_DIGESTS[1])] {
        let fixture = Fixture::new();
        let mut transfer = Transfer::from_body(
            stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"abc"))]),
            Length::Unknown,
        )
        .measurement(Measurement::None);
        if let Some(required) = required {
            transfer = transfer.expect(required.parse::<Digest>().unwrap());
        }
        let receipt = transfer
            .to_file(fixture.output())
            .unwrap()
            .run()
            .await
            .unwrap();
        assert_eq!(receipt.checks().measured(), None);
        assert_eq!(receipt.checks().strong_anchor(), None);
        assert_eq!(receipt.partial.actual, None);
        assert_eq!(
            receipt.partial.hashed,
            ByteCount(if required.is_some() { 3 } else { 0 })
        );
        assert!(matches!(
            receipt.state.reconcile().await.unwrap(),
            Reconciliation::NotRevalidatable(_)
        ));
        fs::write(fixture.output(), b"xyz").unwrap();
        assert!(matches!(
            receipt.state.reconcile().await.unwrap(),
            Reconciliation::NotRevalidatable(_)
        ));
        assert_eq!(fs::read(fixture.output()).unwrap(), b"xyz");
    }
}

#[tokio::test]
async fn no_hash_policy_still_rejects_a_foreign_inode_with_equal_bytes_and_does_not_republish_missing_output()
 {
    let fixture = Fixture::new();
    let receipt = Transfer::from_body(
        stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"abc"))]),
        Length::Unknown,
    )
    .measurement(Measurement::None)
    .to_file(fixture.output())
    .unwrap()
    .run()
    .await
    .unwrap();
    fs::remove_file(fixture.output()).unwrap();
    assert!(matches!(
        receipt.state.reconcile().await.unwrap(),
        Reconciliation::PublicationUnknown(_)
    ));
    fs::write(fixture.output(), b"abc").unwrap();
    let inode = fs::metadata(fixture.output()).unwrap().ino();
    assert!(matches!(
        receipt.state.reconcile().await.unwrap(),
        Reconciliation::Conflict(_)
    ));
    assert_eq!(fs::metadata(fixture.output()).unwrap().ino(), inode);
}

#[tokio::test]
async fn no_hash_policy_still_requires_clean_actual_eof() {
    let fixture = Fixture::new();
    let failure = Transfer::from_body(
        stream::iter([Ok(Bytes::from_static(b"abc")), Err(Borrowed("late"))]),
        Length::Known(ByteCount(3)),
    )
    .measurement(Measurement::None)
    .to_file(fixture.output())
    .unwrap()
    .run()
    .await
    .unwrap_err();
    assert!(matches!(failure.kind, FailureKind::Body(Borrowed("late"))));
    assert_eq!(failure.partial.written, ByteCount(3));
    assert_eq!(failure.partial.hashed, ByteCount(0));
    assert_eq!(failure.partial.checks, None);
    assert!(!fixture.output().exists());
}

pub(super) struct Fixture {
    path: PathBuf,
}

impl Fixture {
    pub(super) fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "xget-native-test-{}-{time:x}-{sequence:x}",
            std::process::id()
        ));
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self { path }
    }
    pub(super) fn path(&self) -> &Path {
        &self.path
    }
    pub(super) fn output(&self) -> PathBuf {
        self.path.join("output")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).unwrap();
    }
}

pub(super) fn requirement(bytes: &[u8]) -> RequiredSha256 {
    let mut hasher = OwnedSha256::default();
    hasher.update(bytes);
    RequiredSha256(Sha256Digest(hasher.finish()))
}

pub(super) fn complete(bytes: &[u8]) -> Partial {
    let mut checks = super::CheckPolicy::from(requirement(bytes)).start();
    checks.update(bytes);
    let mut partial = Partial {
        received: ByteCount(bytes.len() as u64),
        written: ByteCount(bytes.len() as u64),
        hashed: ByteCount(bytes.len() as u64),
        ..Partial::default()
    };
    partial.set_checks(checks.finish().unwrap());
    partial
}

fn require_send<T: Send>(value: T) -> T {
    value
}

#[derive(Debug, thiserror::Error)]
#[error("borrowed body cause")]
struct Borrowed<'a>(&'a str);

#[derive(Debug)]
struct LocalCause(Rc<Cell<u32>>);

impl fmt::Display for LocalCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("local body cause")
    }
}

impl core::error::Error for LocalCause {}
