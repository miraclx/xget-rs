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
    ByteCount, FailureKind, Length, OwnedSha256, Partial, Reconciliation, RequestError,
    RequiredSha256, Sha256Digest, StateRef, Transfer,
};

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
fn missing_and_unsupported_requirements_have_no_filesystem_effects() {
    let fixture = Fixture::new();
    let missing = Transfer::from_body(
        stream::empty::<Result<Bytes, Infallible>>(),
        Length::Unknown,
    )
    .to_file(fixture.output());
    assert!(matches!(missing, Err(RequestError::MissingRequirement)));
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
        Err(RequestError::ConflictingRequirements)
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
        Err(RequestError::ConflictingRequirements)
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
    assert!(matches!(failure.kind, FailureKind::DigestMismatch));
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
    Partial {
        received: ByteCount(bytes.len() as u64),
        written: ByteCount(bytes.len() as u64),
        hashed: ByteCount(bytes.len() as u64),
        actual: Some(requirement(bytes).digest()),
        ..Partial::default()
    }
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
