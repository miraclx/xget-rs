//! Public lifetime-specific source/helper shapes, using the real native range path.
#![allow(clippy::unwrap_used)]

use core::future::{Future, Ready, ready};
use std::path::PathBuf;

use bytes::Bytes;
use futures::Stream;
use futures::stream::{self, Iter};
use xget::Transfer;
use xget::transfer::reader::{
    Evidence, RangeReader, ReadResponse, ReaderError, ReaderReceipt, Window,
};
use xget::transfer::{ByteCount, Failure};

#[derive(Debug, PartialEq, Eq)]
struct BorrowedError<'data>(&'data str);

impl core::fmt::Display for BorrowedError<'_> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl core::error::Error for BorrowedError<'_> {}

struct Borrowed<'data> {
    bytes: &'data [u8],
    label: &'data str,
}

impl<'read, 'data: 'read> RangeReader<'read> for Borrowed<'data> {
    type Error = BorrowedError<'read>;
    type Body = Iter<core::array::IntoIter<Result<Bytes, Self::Error>, 1>>;
    type Read = Ready<Result<ReadResponse<'read, Self::Body>, Self::Error>>;
    fn size(&self) -> ByteCount {
        ByteCount(self.bytes.len() as u64)
    }
    fn evidence(&'read self) -> Evidence<'read> {
        Evidence::Enforced {
            representation: self.label,
            version: b"immutable borrow",
        }
    }
    fn read(&'read self, window: Window) -> Self::Read {
        let bytes =
            Bytes::copy_from_slice(&self.bytes[window.start().0 as usize..window.end().0 as usize]);
        ready(Ok(ReadResponse {
            window,
            evidence: self.evidence(),
            body: stream::iter([Ok(bytes)]),
        }))
    }
}

// Name one borrow's families, avoiding projection generalization and a for<'a> static requirement.
fn send_helper<'read, R, F, B, E>(
    reader: &'read R,
    destination: PathBuf,
) -> impl Future<Output = Result<ReaderReceipt, Box<Failure<ReaderError<E>>>>> + Send + 'read
where
    R: RangeReader<'read, Read = F, Body = B, Error = E> + Sync,
    F: Future<Output = Result<ReadResponse<'read, B>, E>> + Send + 'read,
    B: Stream<Item = Result<Bytes, E>> + Send + 'read,
    E: Send + 'read,
{
    send_resume_helper(reader, destination, None)
}

fn send_resume_helper<'read, R, F, B, E>(
    reader: &'read R,
    destination: PathBuf,
    state: Option<xget::transfer::StateRef>,
) -> impl Future<Output = Result<ReaderReceipt, Box<Failure<ReaderError<E>>>>> + Send + 'read
where
    R: RangeReader<'read, Read = F, Body = B, Error = E> + Sync,
    F: Future<Output = Result<ReadResponse<'read, B>, E>> + Send + 'read,
    B: Stream<Item = Result<Bytes, E>> + Send + 'read,
    E: Send + 'read,
{
    let request = Transfer::from_reader(reader);
    let request = match state {
        Some(state) => request.resume_from(state),
        None => request,
    };
    request.to_file(destination).unwrap().run()
}

fn assert_send<T: Send>(value: T) -> T {
    value
}

#[cfg(unix)]
#[tokio::test]
async fn actual_public_generic_send_helper_accepts_nonstatic_source_and_error_families() {
    use std::fs;
    use std::os::unix::fs::DirBuilderExt as _;
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root =
        std::env::temp_dir().join(format!("xget-reader-public-{}-{now:x}", std::process::id()));
    fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
    let label = String::from("application-owned bytes");
    let data = vec![b'a', b'b', b'c'];
    let reader = Borrowed {
        bytes: &data,
        label: &label,
    };
    let receipt = assert_send(send_helper(&reader, root.join("borrowed")))
        .await
        .unwrap();
    assert_eq!(receipt.native().partial().written, ByteCount(3));
    assert_eq!(fs::read(root.join("borrowed")).unwrap(), data);
    // Owned Send values moved into a caller-spawnable future remain Send as well.
    let destination = root.join("owned");
    let owned = assert_send(async move {
        let data = vec![b'a', b'b', b'c'];
        let label = String::from("owned bytes");
        let reader = Borrowed {
            bytes: &data,
            label: &label,
        };
        Transfer::from_reader(&reader)
            .to_file(destination)
            .unwrap()
            .run()
            .await
            .unwrap()
    });
    owned.await;
    fs::remove_dir_all(root).unwrap();
}

// Runnable public API proof: cargo test --test transfer_reader public_resume -- --nocapture
#[cfg(unix)]
#[tokio::test]
async fn public_resume_rehashes_saved_bytes_and_exposes_negative_diagnostics() {
    use std::fs;
    use std::os::unix::fs::DirBuilderExt as _;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    use xget::transfer::reader::ReaderStats;
    use xget::transfer::{FailureKind, Measurement};

    struct Pausable<'data> {
        inner: Borrowed<'data>,
        pause: bool,
        paused: tokio::sync::Notify,
        calls: Mutex<Vec<Window>>,
    }
    impl<'read, 'data: 'read> RangeReader<'read> for Pausable<'data> {
        type Error = BorrowedError<'read>;
        type Body =
            core::pin::Pin<Box<dyn Stream<Item = Result<Bytes, Self::Error>> + Send + 'read>>;
        type Read = Ready<Result<ReadResponse<'read, Self::Body>, Self::Error>>;
        fn size(&self) -> ByteCount {
            self.inner.size()
        }
        fn evidence(&'read self) -> Evidence<'read> {
            self.inner.evidence()
        }
        fn read(&'read self, window: Window) -> Self::Read {
            // The public fixture is Sync so its concrete borrowed future can also prove Send.
            self.calls.lock().unwrap().push(window);
            let start = window.start().0 as usize;
            let end = window.end().0 as usize;
            let body: Self::Body = if self.pause && start == 0 {
                let bytes = Bytes::copy_from_slice(&self.inner.bytes[..4]);
                Box::pin(stream::unfold(Some(bytes), move |next| async move {
                    if let Some(bytes) = next {
                        return Some((Ok(bytes), None));
                    }
                    self.paused.notify_one();
                    core::future::pending().await
                }))
            } else {
                Box::pin(stream::iter([Ok(Bytes::copy_from_slice(
                    &self.inner.bytes[start..end],
                ))]))
            };
            ready(Ok(ReadResponse {
                window,
                evidence: self.evidence(),
                body,
            }))
        }
    }
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "xget-public-resume-{}-{stamp:x}",
        std::process::id()
    ));
    fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
    let data: Vec<u8> = (0..32).collect();
    let label = String::from("application-owned bytes");
    let source = Pausable {
        inner: Borrowed {
            bytes: &data,
            label: &label,
        },
        pause: true,
        paused: tokio::sync::Notify::new(),
        calls: Mutex::new(Vec::new()),
    };
    let output = root.join("output");
    let pending = Transfer::from_reader(&source).to_file(&output).unwrap();
    let state = xget::transfer::StateRef::clone(pending.state());
    let stopped = pending
        .run_until(source.paused.notified())
        .await
        .unwrap_err();
    assert!(matches!(stopped.kind, FailureKind::Stopped));
    drop(stopped);
    let no_hash = Transfer::from_reader(&source)
        .measurement(Measurement::None)
        .resume_from(xget::transfer::StateRef::clone(&state))
        .to_file(&output);
    let Err(error) = no_hash else {
        panic!("no-hash continuation must be refused");
    };
    eprintln!("intended no-hash refusal: {error}");
    let changed = Borrowed {
        bytes: &data,
        label: "different representation",
    };
    let failure = send_resume_helper(
        &changed,
        PathBuf::clone(&output),
        Some(xget::transfer::StateRef::clone(&state)),
    )
    .await
    .unwrap_err();
    assert!(matches!(failure.kind, FailureKind::InvalidState));
    eprintln!("intended changed-session refusal: {failure}");
    let resumed = Pausable {
        pause: false,
        ..source
    };
    resumed.calls.lock().unwrap().clear();
    let receipt = assert_send(send_resume_helper(
        &resumed,
        PathBuf::clone(&output),
        Some(state),
    ))
    .await
    .unwrap();
    assert_eq!(
        receipt.stats(),
        ReaderStats {
            restored: ByteCount(4),
            source: ByteCount(28),
            written: ByteCount(28)
        }
    );
    assert!(
        resumed
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|window| window.start() >= ByteCount(4))
    );
    assert_eq!(fs::read(&output).unwrap(), data);
    eprintln!("public continuation counts: {:?}", receipt.stats());
    fs::remove_dir_all(root).unwrap();
}

#[cfg(not(unix))]
#[tokio::test]
async fn generic_borrowed_send_path_is_typed_unsupported_without_source_polling() {
    let label = String::from("application-owned bytes");
    let data = vec![b'a'];
    let reader = Borrowed {
        bytes: &data,
        label: &label,
    };
    let failure = assert_send(send_helper(&reader, PathBuf::from("output")))
        .await
        .unwrap_err();
    assert!(matches!(
        failure.kind,
        xget::transfer::FailureKind::Unsupported
    ));
}
