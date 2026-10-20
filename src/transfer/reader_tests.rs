#![allow(clippy::unwrap_used)]

use core::cell::Cell;
use core::future::{Ready, ready};
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;
#[cfg(unix)]
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::stream::{self, Iter};

#[cfg(unix)]
use super::ReaderError;
use super::{Evidence, RangeReader, ReadResponse, ReaderRequestError, ReaderTransfer, Window};
use crate::transfer::{ByteCount, Digest, FailureKind, Measurement};

struct Directory(PathBuf);

impl Directory {
    fn create() -> Self {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("xget-reader-{}-{time:x}", std::process::id()));
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt as _;
            fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        }
        #[cfg(not(unix))]
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

// Native protocol scenarios cannot execute on unsupported hosts.
#[cfg_attr(not(unix), allow(dead_code))]
#[derive(Clone, Copy)]
enum Behavior {
    Exact,
    EmptyChunks,
    Short,
    Overlong,
    Late,
    WrongWindow,
    Changed,
    Candidate,
}

struct Memory<'data> {
    bytes: &'data [u8],
    behavior: Behavior,
    calls: Rc<Cell<u32>>,
}

#[derive(Debug, PartialEq, Eq)]
struct BorrowedCause<'data>(&'data [u8]);

impl core::fmt::Display for BorrowedCause<'_> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            formatter,
            "fixture failed for {} borrowed bytes",
            self.0.len()
        )
    }
}

impl core::error::Error for BorrowedCause<'_> {}

impl<'read, 'data: 'read> RangeReader<'read> for Memory<'data> {
    type Error = BorrowedCause<'read>;
    type Body = Iter<std::vec::IntoIter<Result<Bytes, Self::Error>>>;
    type Read = Ready<Result<ReadResponse<'read, Self::Body>, Self::Error>>;

    fn size(&self) -> ByteCount {
        ByteCount(self.bytes.len() as u64)
    }
    fn evidence(&'read self) -> Evidence<'read> {
        match self.behavior {
            Behavior::Candidate => Evidence::Candidate {
                representation: "bytes",
            },
            _ => Evidence::Enforced {
                representation: "bytes",
                version: b"one",
            },
        }
    }
    fn read(&'read self, window: Window) -> Self::Read {
        self.calls.set(self.calls.get() + 1);
        let mut bytes = self.bytes[window.start().0 as usize..window.end().0 as usize].to_vec();
        match self.behavior {
            Behavior::Short => {
                bytes.pop();
            }
            Behavior::Overlong => bytes.push(0),
            _ => {}
        }
        let mut chunks = vec![Ok(Bytes::from(bytes))];
        if matches!(self.behavior, Behavior::EmptyChunks) {
            chunks.insert(0, Ok(Bytes::new()));
            chunks.push(Ok(Bytes::new()));
        }
        if matches!(self.behavior, Behavior::Late) {
            chunks.push(Err(BorrowedCause(self.bytes)));
        }
        let response_window = if matches!(self.behavior, Behavior::WrongWindow) {
            Window::new(ByteCount(1), ByteCount(2), self.size()).unwrap()
        } else {
            window
        };
        let evidence = if matches!(self.behavior, Behavior::Changed) {
            Evidence::Enforced {
                representation: "bytes",
                version: b"two",
            }
        } else {
            self.evidence()
        };
        ready(Ok(ReadResponse {
            window: response_window,
            evidence,
            body: stream::iter(chunks),
        }))
    }
}

fn required() -> Digest {
    "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        .parse()
        .unwrap()
}

#[cfg(unix)]
#[tokio::test]
async fn large_source_chunks_cross_write_and_readback_bounds_with_optional_empty_chunks() {
    use digest::Digest as _;

    for behavior in [Behavior::Exact, Behavior::EmptyChunks] {
        let directory = Directory::create();
        let data: Vec<_> = (0..4 * (64 * 1024 + 1))
            .map(|index| (index % 251) as u8)
            .collect();
        let expected = Digest::parse(
            crate::transfer::Algorithm::Sha256,
            &hex::encode(sha2::Sha256::digest(&data)),
        )
        .unwrap();
        let calls = Rc::new(Cell::new(0));
        let reader = Memory {
            bytes: &data,
            behavior,
            calls: Rc::clone(&calls),
        };
        let receipt = ReaderTransfer::from_reader(&reader)
            .expect(expected)
            .measurement(Measurement::None)
            .to_file(directory.0.join("output"))
            .unwrap()
            .run()
            .await
            .unwrap();
        assert_eq!(calls.get(), 4);
        assert_eq!(
            receipt.native().partial().received,
            ByteCount(data.len() as u64)
        );
        assert_eq!(
            receipt.native().partial().written,
            ByteCount(data.len() as u64)
        );
        assert_eq!(
            receipt.native().partial().hashed,
            ByteCount(data.len() as u64)
        );
        assert!(receipt.native().checks().measured().is_none());
        assert_eq!(fs::read(directory.0.join("output")).unwrap(), data);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn first_opening_failure_prevents_invoking_queued_openings() {
    struct FailFirst(Cell<usize>);
    impl<'read> RangeReader<'read> for FailFirst {
        type Error = &'static str;
        type Body = stream::Empty<Result<Bytes, Self::Error>>;
        type Read = Ready<Result<ReadResponse<'read, Self::Body>, Self::Error>>;
        fn size(&self) -> ByteCount {
            ByteCount(4)
        }
        fn evidence(&'read self) -> Evidence<'read> {
            Evidence::Enforced {
                representation: "bytes",
                version: b"one",
            }
        }
        fn read(&'read self, _: Window) -> Self::Read {
            self.0.set(self.0.get() + 1);
            ready(Err("opening failed"))
        }
    }
    let directory = Directory::create();
    let reader = FailFirst(Cell::new(0));
    let failure = ReaderTransfer::from_reader(&reader)
        .to_file(directory.0.join("output"))
        .unwrap()
        .run()
        .await
        .unwrap_err();
    assert!(matches!(
        failure.kind,
        FailureKind::Body(ReaderError::Source("opening failed"))
    ));
    assert_eq!(
        reader.0.get(),
        1,
        "queued slots must not invoke read after the first failure"
    );
    assert!(!directory.0.join("output").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn ready_empty_chunks_return_control_with_a_bounded_source_poll_count() {
    use core::future::Future as _;
    use core::pin::Pin;
    use core::task::{Context, Poll};

    use futures::Stream;
    use futures::task::noop_waker_ref;

    struct EmptyBody(Rc<Cell<usize>>);
    impl Stream for EmptyBody {
        type Item = Result<Bytes, core::convert::Infallible>;
        fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.0.set(self.0.get() + 1);
            // Bound the regression itself, even if the library does not yield. A same-runtime
            // timeout cannot protect against an unbounded ready-poll loop.
            if self.0.get() <= 256 {
                Poll::Ready(Some(Ok(Bytes::new())))
            } else {
                Poll::Pending
            }
        }
    }
    struct EmptyReader(Rc<Cell<usize>>);
    impl<'read> RangeReader<'read> for EmptyReader {
        type Error = core::convert::Infallible;
        type Body = EmptyBody;
        type Read = Ready<Result<ReadResponse<'read, Self::Body>, Self::Error>>;
        fn size(&self) -> ByteCount {
            ByteCount(4)
        }
        fn evidence(&'read self) -> Evidence<'read> {
            Evidence::Enforced {
                representation: "bytes",
                version: b"one",
            }
        }
        fn read(&'read self, window: Window) -> Self::Read {
            ready(Ok(ReadResponse {
                window,
                evidence: self.evidence(),
                body: EmptyBody(Rc::clone(&self.0)),
            }))
        }
    }
    let directory = Directory::create();
    let polls = Rc::new(Cell::new(0));
    let reader = EmptyReader(Rc::clone(&polls));
    let pending = ReaderTransfer::from_reader(&reader)
        .to_file(directory.0.join("output"))
        .unwrap();
    let native = super::Native::acquire(pending.plan).await.unwrap();
    let mut partial = crate::transfer::Partial::default();
    let mut future = Box::pin(super::receive(
        &reader,
        ByteCount(4),
        reader.evidence(),
        &native,
        &mut partial,
    ));
    let mut cx = Context::from_waker(noop_waker_ref());
    assert!(future.as_mut().poll(&mut cx).is_pending());
    assert!(
        polls.get() > 0 && polls.get() <= 4,
        "source polled {} times before yielding",
        polls.get()
    );
    drop(future);
    assert!(!directory.0.join("output").exists());
}

#[test]
fn windows_reject_empty_inverted_and_out_of_object_requests() {
    for (start, end) in [(0, 0), (2, 1), (0, 4)] {
        assert!(Window::new(ByteCount(start), ByteCount(end), ByteCount(3)).is_none());
    }
    assert!(Window::new(ByteCount(0), ByteCount(3), ByteCount(3)).is_some());
}

#[test]
fn optional_measurement_and_weak_requirements_never_pin_a_candidate() {
    let directory = Directory::create();
    let calls = Rc::new(Cell::new(0));
    let data = vec![1, 2, 3];
    let reader = Memory {
        bytes: &data,
        behavior: Behavior::Candidate,
        calls: Rc::clone(&calls),
    };
    for measurement in [Measurement::None, Measurement::default()] {
        let error = ReaderTransfer::from_reader(&reader)
            .measurement(measurement)
            .to_file(directory.0.join("output"))
            .err()
            .unwrap();
        assert!(matches!(error, ReaderRequestError::Unpinned));
    }
    for weak in [
        "md5:900150983cd24fb0d6963f7d28e17f72",
        "sha1:a9993e364706816aba3e25717850c26c9cd0d89d",
    ] {
        assert!(matches!(
            ReaderTransfer::from_reader(&reader)
                .expect(weak.parse::<Digest>().unwrap())
                .to_file(directory.0.join("output")),
            Err(ReaderRequestError::Unpinned)
        ));
    }
    for strong in [required(), "sha512:ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f".parse().unwrap(),
        "blake3:6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85".parse().unwrap()] {
        assert!(ReaderTransfer::from_reader(&reader).expect(strong).to_file(directory.0.join("output")).is_ok());
    }
    assert_eq!(calls.get(), 0);
    assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn real_vectors_include_zero_one_many_and_unversioned_checked_candidates() {
    for data in [b"".as_slice(), b"a".as_slice(), b"abc".as_slice()] {
        let directory = Directory::create();
        let reader = Memory {
            bytes: data,
            behavior: Behavior::Exact,
            calls: Rc::new(Cell::new(0)),
        };
        let receipt = ReaderTransfer::from_reader(&reader)
            .to_file(directory.0.join("output"))
            .unwrap()
            .run()
            .await
            .unwrap();
        assert_eq!(
            receipt.native().partial().hashed,
            ByteCount(data.len() as u64)
        );
        let expected = match data {
            b"" => "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            b"a" => "sha256:ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
            _ => "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        };
        assert_eq!(
            receipt.native().checks().measured(),
            Some(expected.parse().unwrap())
        );
        assert_eq!(fs::read(directory.0.join("output")).unwrap(), data);
        assert_eq!(reader.calls.get(), (data.len() as u32).min(4));
    }
    let directory = Directory::create();
    let data = vec![b'a', b'b', b'c'];
    let reader = Memory {
        bytes: &data,
        behavior: Behavior::Candidate,
        calls: Rc::new(Cell::new(0)),
    };
    let receipt = ReaderTransfer::from_reader(&reader)
        .expect(required())
        .measurement(Measurement::None)
        .to_file(directory.0.join("output"))
        .unwrap()
        .run()
        .await
        .unwrap();
    assert_eq!(receipt.consistency(), super::Consistency::ExpectedContent);
    assert_eq!(receipt.native().checks().measured(), None);
    assert_eq!(receipt.native().checks().matched_requirements().count(), 1);
    assert_eq!(receipt.native().partial().hashed, ByteCount(3));
}

#[cfg(unix)]
#[tokio::test]
async fn an_enforced_session_with_no_checks_performs_no_hash_work() {
    let directory = Directory::create();
    let data = vec![b'a', b'b', b'c'];
    let reader = Memory {
        bytes: &data,
        behavior: Behavior::Exact,
        calls: Rc::new(Cell::new(0)),
    };
    let receipt = ReaderTransfer::from_reader(&reader)
        .measurement(Measurement::None)
        .to_file(directory.0.join("output"))
        .unwrap()
        .run()
        .await
        .unwrap();
    assert_eq!(receipt.consistency(), super::Consistency::EnforcedVersion);
    assert_eq!(receipt.native().partial().written, ByteCount(3));
    assert_eq!(receipt.native().partial().hashed, ByteCount(0));
    assert_eq!(receipt.native().checks().measured(), None);
    assert_eq!(receipt.native().checks().matched_requirements().count(), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn candidate_checks_share_one_frontier_and_a_mismatch_preserves_an_incumbent_inode() {
    use std::os::unix::fs::MetadataExt as _;

    let directory = Directory::create();
    let data = vec![b'a', b'b', b'c'];
    let reader = Memory {
        bytes: &data,
        behavior: Behavior::Candidate,
        calls: Rc::new(Cell::new(0)),
    };
    let sha512: Digest = "sha512:ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f".parse().unwrap();
    let blake3: Digest = "blake3:6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"
        .parse()
        .unwrap();
    let receipt = ReaderTransfer::from_reader(&reader)
        .expect(required())
        .expect(sha512)
        .expect(blake3)
        .measurement(Measurement::None)
        .to_file(directory.0.join("checked"))
        .unwrap()
        .run()
        .await
        .unwrap();
    assert_eq!(receipt.native().checks().matched_requirements().count(), 3);
    assert_eq!(receipt.native().partial().hashed, ByteCount(3));
    let output = directory.0.join("sentinel");
    fs::write(&output, b"keep me").unwrap();
    let before = fs::metadata(&output).unwrap();
    let wrong: Digest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
        .parse()
        .unwrap();
    let failure = ReaderTransfer::from_reader(&reader)
        .expect(wrong)
        .to_file(&output)
        .unwrap()
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.kind, FailureKind::DigestMismatch(_)));
    assert_eq!(fs::read(&output).unwrap(), b"keep me");
    let after = fs::metadata(&output).unwrap();
    assert_eq!((before.dev(), before.ino()), (after.dev(), after.ino()));
}

#[cfg(unix)]
#[tokio::test]
async fn wrong_windows_changed_sessions_short_overlong_and_late_errors_cannot_publish() {
    for behavior in [
        Behavior::WrongWindow,
        Behavior::Changed,
        Behavior::Short,
        Behavior::Overlong,
        Behavior::Late,
    ] {
        let directory = Directory::create();
        let data = vec![b'a', b'b', b'c'];
        let reader = Memory {
            bytes: &data,
            behavior,
            calls: Rc::new(Cell::new(0)),
        };
        let failure = ReaderTransfer::from_reader(&reader)
            .expect(required())
            .to_file(directory.0.join("output"))
            .unwrap()
            .run()
            .await
            .unwrap_err();
        assert!(failure.partial.received >= failure.partial.written);
        if matches!(behavior, Behavior::Overlong) {
            assert!(failure.partial.received > failure.partial.written);
        }
        if matches!(behavior, Behavior::WrongWindow | Behavior::Changed) {
            // Other concurrent ranges may already have settled, but the rejected response itself
            // cannot poll its body or authorize publication.
            assert!(failure.partial.written.0 <= 2);
        }
        match behavior {
            Behavior::WrongWindow => assert!(matches!(
                failure.kind,
                FailureKind::Body(ReaderError::Window)
            )),
            Behavior::Changed => assert!(matches!(
                failure.kind,
                FailureKind::Body(ReaderError::SessionChanged)
            )),
            Behavior::Late => match failure.kind {
                FailureKind::Body(ReaderError::Source(cause)) => {
                    assert_eq!(cause.0.as_ptr(), data.as_ptr())
                }
                kind => panic!("expected concrete borrowed cause, got {kind:?}"),
            },
            _ => assert!(matches!(failure.kind, FailureKind::LengthMismatch)),
        }
        assert!(!directory.0.join("output").exists());
        assert!(failure.state.is_some());
    }
}

#[cfg(unix)]
#[tokio::test]
async fn failed_physical_steps_keep_accepted_prefixes_and_mark_lost_extents_unconfirmed() {
    use core::sync::atomic::Ordering;

    use super::PendingReader;
    use crate::transfer::WriteExtent;
    use crate::transfer::native::Native;

    for lose_result in [false, true] {
        let directory = Directory::create();
        let data = vec![0; 16];
        let reader = Memory {
            bytes: &data,
            behavior: Behavior::Exact,
            calls: Rc::new(Cell::new(0)),
        };
        let PendingReader {
            reader,
            size,
            evidence,
            plan,
        } = ReaderTransfer::from_reader(&reader)
            .to_file(directory.0.join("output"))
            .unwrap();
        let native = Native::acquire(plan).await.unwrap();
        if lose_result {
            native
                .hooks()
                .lose_write_result
                .store(true, Ordering::Release);
        } else {
            native
                .hooks()
                .fail_write_prefix
                .store(true, Ordering::Release);
        }
        let mut partial = crate::transfer::Partial::default();
        let result = super::receive(reader, size, evidence, &native, &mut partial).await;
        assert!(matches!(
            result,
            Err(FailureKind::Io {
                phase: crate::transfer::Phase::Write,
                ..
            })
        ));
        if lose_result {
            assert_eq!(partial.write_extent, WriteExtent::Unconfirmed);
        } else {
            assert_eq!(partial.write_extent, WriteExtent::Confirmed);
            assert!(partial.written.0 > 0);
            assert!(partial.written.0 <= 4);
        }
        assert!(partial.received.0 >= partial.written.0);
        assert!(!directory.0.join("output").exists());
        drop(native);
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn tails_then_partial_head_are_hashed_before_the_head_can_finish() {
    use core::future::Future;
    use core::pin::Pin;
    use core::sync::atomic::{AtomicUsize, Ordering};

    use futures::Stream;
    use tokio::sync::{Barrier, Notify};

    struct Slow {
        opened: Barrier,
        tails: AtomicUsize,
        tails_done: Notify,
        frontier: Arc<Notify>,
    }
    impl<'read> RangeReader<'read> for Slow {
        type Error = core::convert::Infallible;
        type Body = Pin<Box<dyn Stream<Item = Result<Bytes, Self::Error>> + 'read>>;
        type Read = Pin<
            Box<dyn Future<Output = Result<ReadResponse<'read, Self::Body>, Self::Error>> + 'read>,
        >;
        fn size(&self) -> ByteCount {
            ByteCount(16)
        }
        fn evidence(&'read self) -> Evidence<'read> {
            Evidence::Enforced {
                representation: "zeros",
                version: b"1",
            }
        }
        fn read(&'read self, window: Window) -> Self::Read {
            Box::pin(async move {
                self.opened.wait().await;
                let head = window.start() == ByteCount(0);
                let body = stream::unfold(0, move |step| async move {
                    if !head {
                        if step == 0 {
                            return Some((Ok(Bytes::from_static(&[0; 4])), 1));
                        }
                        if self.tails.fetch_add(1, Ordering::AcqRel) == 2 {
                            self.tails_done.notify_one();
                        }
                        return None;
                    }
                    if step == 0 {
                        while self.tails.load(Ordering::Acquire) != 3 {
                            self.tails_done.notified().await;
                        }
                        return Some((Ok(Bytes::from_static(&[0; 2])), 1));
                    }
                    if step == 1 {
                        // If verification waits for complete ranges, this wait never resolves.
                        self.frontier.notified().await;
                        return Some((Ok(Bytes::from_static(&[0; 2])), 2));
                    }
                    None
                });
                Ok(ReadResponse {
                    window,
                    evidence: self.evidence(),
                    body: Box::pin(body) as Self::Body,
                })
            })
        }
    }
    let directory = Directory::create();
    let frontier = Arc::new(Notify::new());
    super::FRONTIER_TEST.with(|hook| *hook.borrow_mut() = Some(Arc::clone(&frontier)));
    let reader = Slow {
        opened: Barrier::new(4),
        tails: AtomicUsize::new(0),
        tails_done: Notify::new(),
        frontier,
    };
    let result = tokio::time::timeout(
        core::time::Duration::from_secs(5),
        ReaderTransfer::from_reader(&reader)
            .to_file(directory.0.join("output"))
            .unwrap()
            .run(),
    )
    .await;
    super::FRONTIER_TEST.with(|hook| *hook.borrow_mut() = None);
    let receipt = result.unwrap().unwrap();
    assert_eq!(receipt.native().partial().hashed, ByteCount(16));
    assert_eq!(reader.tails.load(Ordering::Acquire), 3);
    assert_eq!(fs::read(directory.0.join("output")).unwrap(), [0; 16]);
}

#[cfg(not(unix))]
#[tokio::test]
async fn unsupported_hosts_do_not_open_borrowed_local_readers() {
    let directory = Directory::create();
    let data = vec![1, 2, 3];
    let reader = Memory {
        bytes: &data,
        behavior: Behavior::Exact,
        calls: Rc::new(Cell::new(0)),
    };
    let failure = ReaderTransfer::from_reader(&reader)
        .to_file(directory.0.join("output"))
        .unwrap()
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.kind, FailureKind::Unsupported));
    assert_eq!(reader.calls.get(), 0);
    assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 0);
}
