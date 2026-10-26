#![allow(clippy::unwrap_used)]

use core::cell::Cell;
#[cfg(unix)]
use core::cell::RefCell;
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

#[cfg(unix)]
struct ReaderObservation<'a>(&'a Rc<RefCell<Vec<crate::transfer::observation::ProgressFacts>>>);

#[cfg(unix)]
impl super::Observer for ReaderObservation<'_> {
    fn observe(&mut self, facts: crate::transfer::observation::ProgressFacts) {
        self.0.borrow_mut().push(facts);
    }
}

#[cfg(unix)]
struct ReaderSignal<'a> {
    facts: &'a RefCell<Vec<crate::transfer::observation::ProgressFacts>>,
    changed: &'a tokio::sync::Notify,
}

#[cfg(unix)]
impl super::Observer for ReaderSignal<'_> {
    fn observe(&mut self, facts: crate::transfer::observation::ProgressFacts) {
        self.facts.borrow_mut().push(facts);
        self.changed.notify_one();
    }
}

struct Directory(PathBuf);

impl Directory {
    fn create() -> Self {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Self::with_stamp(time)
    }

    fn with_stamp(time: u128) -> Self {
        use core::sync::atomic::{AtomicU64, Ordering};

        // Wall-clock precision is not uniqueness. The counter distinguishes same-process calls;
        // create_dir still rejects any stale collision rather than adopting an existing directory.
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "xget-reader-{}-{time:x}-{sequence:x}",
            std::process::id()
        ));
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

#[cfg(unix)]
#[tokio::test]
async fn reader_stop_during_seal_settles_without_publication_or_writable_reentry() {
    let directory = Directory::create();
    let output = directory.0.join("output");
    let reader = Memory {
        bytes: b"abc",
        behavior: Behavior::Exact,
        calls: Rc::new(Cell::new(0)),
    };
    let pending = ReaderTransfer::from_reader(&reader)
        .to_file(&output)
        .unwrap();
    let state = crate::transfer::StateRef::clone(pending.state());
    let (installed, barrier) = tokio::sync::oneshot::channel();
    super::SEAL_TEST.with(|hook| *hook.borrow_mut() = Some(installed));
    let (stop, signal) = tokio::sync::oneshot::channel();
    let facts = Rc::new(RefCell::new(Vec::new()));
    let mut observer = ReaderObservation(&facts);
    let mut future = Box::pin(pending.run_registered_until_observed(
        None,
        async {
            let _ = signal.await;
        },
        &mut observer,
    ));
    let held = tokio::select! {
        result = &mut future => panic!("seal fixture was not installed: {result:?}"),
        result = barrier => result.unwrap(),
    };
    tokio::select! {
        result = &mut future => panic!("physical seal must be held: {result:?}"),
        () = held.entered.notified() => {}
    }
    let _ = stop.send(());
    held.release();
    let failure = future.await.unwrap_err();
    let terminal = *facts.borrow().last().unwrap();
    assert_eq!(terminal.received(), failure.partial.received);
    assert_eq!(terminal.written(), failure.partial.written);
    assert_eq!(terminal.hashed(), failure.partial.hashed);
    // Reception really reached EOF before sealing; a later stop is not a failed body or publication.
    assert_eq!(terminal.reception(), super::Reception::CleanEof);
    assert_eq!(terminal.available(), ByteCount(3));
    assert!(matches!(failure.kind, FailureKind::Stopped));
    assert!(!output.exists());
    let record =
        super::super::record::Record::decode(&fs::read(state.path().join("state")).unwrap())
            .unwrap();
    assert!(record.reader.unwrap().closed);
    drop(failure);
    let calls = reader.calls.get();
    let failure = ReaderTransfer::from_reader(&reader)
        .resume_from(state)
        .to_file(&output)
        .unwrap()
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.kind, FailureKind::InvalidState));
    assert_eq!(reader.calls.get(), calls);
    assert!(!output.exists());
}

#[test]
fn a_send_but_not_sync_observer_preserves_one_borrows_send_reader_future() {
    struct LocalBytes<'data>(&'data [u8]);
    impl<'read, 'data: 'read> RangeReader<'read> for LocalBytes<'data> {
        type Error = core::convert::Infallible;
        type Body = Iter<core::array::IntoIter<Result<Bytes, Self::Error>, 1>>;
        type Read = Ready<Result<ReadResponse<'read, Self::Body>, Self::Error>>;
        fn size(&self) -> ByteCount {
            ByteCount(self.0.len() as u64)
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
                body: stream::iter([Ok(Bytes::copy_from_slice(
                    &self.0[window.start.0 as usize..window.end.0 as usize],
                ))]),
            }))
        }
    }
    struct SendObservation(Cell<ByteCount>);
    impl super::Observer for SendObservation {
        fn observe(&mut self, facts: crate::transfer::observation::ProgressFacts) {
            self.0.set(facts.written());
        }
    }
    fn needs_send(_: impl Send) {}
    let directory = Directory::create();
    let data = vec![1, 2, 3];
    let reader = LocalBytes(&data);
    let mut observer = SendObservation(Cell::new(ByteCount(0)));
    let pending = ReaderTransfer::from_reader(&reader)
        .to_file(directory.0.join("output"))
        .unwrap();
    needs_send(pending.run_registered_observed(None, &mut observer));
    assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn reader_observation_without_checks_keeps_hash_work_zero() {
    for bytes in [b"".as_slice(), b"a", b"abc", b"unequal windows"] {
        let directory = Directory::create();
        let reader = Memory {
            bytes,
            behavior: Behavior::Exact,
            calls: Rc::new(Cell::new(0)),
        };
        let facts = Rc::new(RefCell::new(Vec::new()));
        let mut observer = ReaderObservation(&facts);
        let receipt = ReaderTransfer::from_reader(&reader)
            .measurement(Measurement::None)
            .to_file(directory.0.join("output"))
            .unwrap()
            .run_registered_observed(None, &mut observer)
            .await
            .unwrap();
        assert_eq!(receipt.native().partial().hashed, ByteCount(0));
        let facts = facts.borrow();
        assert!(facts.iter().all(|facts| facts.hashed() == ByteCount(0)));
        let terminal = *facts.last().unwrap();
        let size = ByteCount(bytes.len() as u64);
        assert_eq!(terminal.received(), size);
        assert_eq!(terminal.written(), size);
        assert_eq!(terminal.attempt_received(), size);
        assert_eq!(terminal.attempt_written(), size);
        assert_eq!(terminal.restored(), ByteCount(0));
        assert_eq!(terminal.available(), size);
        assert_eq!(
            terminal.reception(),
            if bytes.is_empty() {
                super::Reception::Pending
            } else {
                super::Reception::CleanEof
            }
        );
        assert_eq!(terminal.windows().count(), bytes.len().min(4));
        assert!(
            terminal
                .windows()
                .all(|window| window.reception() == super::Reception::CleanEof)
        );
        assert_eq!(reader.calls.get() as usize, bytes.len().min(4));
    }
}

#[test]
fn directories_created_at_the_same_clock_stamp_do_not_alias() {
    let first = Directory::with_stamp(1);
    let second = Directory::with_stamp(1);
    assert_ne!(first.0, second.0);
    assert!(first.0.is_dir() && second.0.is_dir());
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
    let facts = Rc::new(RefCell::new(Vec::new()));
    let mut observer = ReaderObservation(&facts);
    let failure = ReaderTransfer::from_reader(&reader)
        .to_file(directory.0.join("output"))
        .unwrap()
        .run_registered_observed(None, &mut observer)
        .await
        .unwrap_err();
    let terminal = *facts.borrow().last().unwrap();
    assert_eq!(terminal.available(), ByteCount(0));
    assert_eq!(terminal.reception(), super::Reception::Failed);
    assert!(terminal.windows().all(|window| matches!(
        window.reception(),
        super::Reception::Failed | super::Reception::Stopped
    )));
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
    let mut observer = ();
    let mut observation =
        super::Observation::start(&mut observer, crate::transfer::Length::Known(ByteCount(4)));
    let mut future = Box::pin(super::receive(
        &reader,
        ByteCount(4),
        reader.evidence(),
        &native,
        &mut partial,
        native.policy().start(),
        core::future::pending(),
        &mut observation,
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
fn partitioning_an_empty_or_inverted_suffix_opens_no_windows() {
    for (start, end) in [(0, 0), (7, 7), (9, 3), (u64::MAX, u64::MAX), (u64::MAX, 0)] {
        assert!(Window::partition(ByteCount(start), ByteCount(end)).is_empty());
    }
}

#[test]
fn partitioning_one_remaining_byte_keeps_its_exact_offset() {
    for start in [0, 7, u64::MAX - 1] {
        let windows = Window::partition(ByteCount(start), ByteCount(start + 1));
        assert_eq!(
            windows,
            [Window::new(ByteCount(start), ByteCount(start + 1), ByteCount(start + 1)).unwrap()]
        );
    }
}

#[test]
fn partitioning_many_bytes_tiles_the_suffix_with_at_most_four_balanced_windows() {
    for start in [0, 7, u64::MAX - 32] {
        for length in 2..=32 {
            let end = start + length;
            let windows = Window::partition(ByteCount(start), ByteCount(end));
            assert_eq!(windows.len(), length.min(4) as usize);
            let mut next = start;
            let mut previous_size = u64::MAX;
            for window in windows {
                assert_eq!(window.start(), ByteCount(next));
                assert!(Window::new(window.start(), window.end(), ByteCount(end)).is_some());
                let size = window.end().0 - window.start().0;
                assert!(size <= previous_size);
                assert!(size == length / length.min(4) || size == length / length.min(4) + 1);
                previous_size = size;
                next = window.end().0;
            }
            assert_eq!(next, end);
        }
    }
    let windows = Window::partition(ByteCount(5), ByteCount(16));
    let bounds: Vec<_> = windows
        .iter()
        .map(|window| (window.start().0, window.end().0))
        .collect();
    assert_eq!(bounds, [(5, 8), (8, 11), (11, 14), (14, 16)]);
}

#[test]
fn partitioning_u64_edge_suffixes_never_wraps_or_leaves_a_gap() {
    for start in [0, 1, 2, 3, 4, u64::MAX / 2, u64::MAX - 5] {
        let windows = Window::partition(ByteCount(start), ByteCount(u64::MAX));
        assert_eq!(windows.len(), 4);
        let mut next = start;
        let mut total = 0_u128;
        let first_size = windows[0].end().0 - windows[0].start().0;
        for window in windows {
            assert_eq!(window.start(), ByteCount(next));
            assert!(Window::new(window.start(), window.end(), ByteCount(u64::MAX)).is_some());
            let size = window.end().0 - window.start().0;
            assert!(size == first_size || size == first_size - 1);
            total += u128::from(size);
            next = window.end().0;
        }
        assert_eq!(next, u64::MAX);
        assert_eq!(total, u128::from(u64::MAX - start));
    }
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
        let facts = Rc::new(RefCell::new(Vec::new()));
        let mut observer = ReaderObservation(&facts);
        let failure = ReaderTransfer::from_reader(&reader)
            .expect(required())
            .to_file(directory.0.join("output"))
            .unwrap()
            .run_registered_observed(None, &mut observer)
            .await
            .unwrap_err();
        let terminal = *facts.borrow().last().unwrap();
        assert_eq!(terminal.reception(), super::Reception::Failed);
        assert!(
            terminal
                .windows()
                .any(|window| window.reception() == super::Reception::Failed)
        );
        assert!(terminal.available() <= terminal.written());
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
            session,
            resume,
        } = ReaderTransfer::from_reader(&reader)
            .to_file(directory.0.join("output"))
            .unwrap();
        let native = Native::acquire_reader(plan, session, resume, None)
            .await
            .unwrap();
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
        let facts = Rc::new(RefCell::new(Vec::new()));
        let mut observer = ReaderObservation(&facts);
        let mut observation =
            super::Observation::start(&mut observer, crate::transfer::Length::Known(size));
        let result = super::receive(
            reader,
            size,
            evidence,
            &native,
            &mut partial,
            native.policy().start(),
            core::future::pending(),
            &mut observation,
        )
        .await;
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
        let observed = *facts.borrow().last().unwrap();
        assert_eq!(observed.received(), partial.received);
        assert_eq!(observed.written(), partial.written);
        assert_eq!(observed.hashed(), partial.hashed);
        assert_eq!(observed.write_extent(), partial.write_extent);
        assert_eq!(observed.reception(), super::Reception::Failed);
        let head = observed.windows().next().unwrap();
        assert_eq!(observed.available(), head.written());
        if lose_result {
            assert_eq!(observed.available(), ByteCount(0));
            assert_eq!(head.write_extent(), WriteExtent::Unconfirmed);
        } else {
            assert_eq!(observed.available(), ByteCount(1));
        }
        assert!(partial.received.0 >= partial.written.0);
        assert!(!directory.0.join("output").exists());
        drop(native);
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn a_drained_write_failure_supersedes_an_earlier_source_only_refusal() {
    use core::pin::Pin;
    use core::sync::atomic::Ordering;
    use core::task::{Context, Poll, Waker};

    use futures::Stream;
    use tokio::sync::Notify;

    struct Refusal {
        ready: Cell<bool>,
        waiting: RefCell<Vec<Waker>>,
    }
    struct Body<'read> {
        reader: &'read Refusal,
        head: bool,
        yielded: bool,
    }
    impl Stream for Body<'_> {
        type Item = Result<Bytes, &'static str>;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if self.head {
                if self.yielded {
                    return Poll::Pending;
                }
                self.yielded = true;
                return Poll::Ready(Some(Ok(Bytes::from_static(b"head"))));
            }
            if !self.reader.ready.get() {
                self.reader
                    .waiting
                    .borrow_mut()
                    .push(Waker::clone(cx.waker()));
                return Poll::Pending;
            }
            Poll::Ready(Some(Err("source-only refusal")))
        }
    }
    impl<'read> RangeReader<'read> for Refusal {
        type Error = &'static str;
        type Body = Body<'read>;
        type Read = Ready<Result<ReadResponse<'read, Self::Body>, Self::Error>>;
        fn size(&self) -> ByteCount {
            ByteCount(16)
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
                body: Body {
                    reader: self,
                    head: window.start() == ByteCount(0),
                    yielded: false,
                },
            }))
        }
    }
    let directory = Directory::create();
    let reader = Refusal {
        ready: Cell::new(false),
        waiting: RefCell::new(Vec::new()),
    };
    let pending = ReaderTransfer::from_reader(&reader)
        .measurement(Measurement::None)
        .to_file(directory.0.join("output"))
        .unwrap();
    let native = super::Native::acquire_reader(pending.plan, pending.session, pending.resume, None)
        .await
        .unwrap();
    native
        .hooks()
        .fail_write_prefix
        .store(true, Ordering::Release);
    let held = native.hooks().write.install();
    let facts = RefCell::new(Vec::new());
    let changed = Notify::new();
    let mut observer = ReaderSignal {
        facts: &facts,
        changed: &changed,
    };
    let mut observation =
        super::Observation::start(&mut observer, crate::transfer::Length::Known(reader.size()));
    let mut partial = crate::transfer::Partial::default();
    let mut receiving = Box::pin(super::receive(
        &reader,
        reader.size(),
        reader.evidence(),
        &native,
        &mut partial,
        native.policy().start(),
        core::future::pending(),
        &mut observation,
    ));
    let result = tokio::time::timeout(core::time::Duration::from_secs(5), async {
        tokio::select! {
            _ = &mut receiving => panic!("submitted head write must be held"),
            () = held.entered.notified() => {}
        }
        reader.ready.set(true);
        for waker in reader.waiting.borrow_mut().drain(..) { waker.wake(); }
        loop {
            tokio::select! {
                _ = &mut receiving => panic!("source refusal must drain the held write"),
                () = changed.notified() => {
                    if facts.borrow().last().is_some_and(|facts| facts.reception() == super::Reception::Failed) { break; }
                }
            }
        }
        assert_eq!(facts.borrow().last().unwrap().written(), ByteCount(0));
        held.release();
        receiving.as_mut().await
    }).await.unwrap();
    drop(receiving);
    match result {
        Err(FailureKind::Io {
            phase: crate::transfer::Phase::Write,
            cause,
        }) => assert_eq!(cause.to_string(), "injected write failure"),
        Err(other) => {
            panic!("settled native cause must prevent source-only negotiation: {other:?}")
        }
        Ok(_) => panic!("a source refusal plus failed write cannot succeed"),
    }
    assert_eq!(partial.written, ByteCount(1));
    assert_eq!(
        partial.write_extent,
        crate::transfer::WriteExtent::Confirmed
    );
    let terminal = *facts.borrow().last().unwrap();
    assert_eq!(terminal.available(), ByteCount(1));
    assert_eq!(
        terminal.write_extent(),
        crate::transfer::WriteExtent::Confirmed
    );
    assert_eq!(terminal.windows().next().unwrap().written(), ByteCount(1));
    assert_eq!(terminal.reception(), super::Reception::Failed);
    assert!(!directory.0.join("output").exists());
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn available_prefix_precedes_hash_readback_and_does_not_depend_on_hashing() {
    use core::pin::Pin;
    use core::task::{Context, Poll, Waker};

    use futures::Stream;
    use tokio::sync::Notify;

    struct Gated {
        head_ready: Cell<bool>,
        finish_head: Cell<bool>,
        head_waker: RefCell<Option<Waker>>,
    }
    struct Body<'read> {
        reader: &'read Gated,
        window: Window,
        sent: u64,
    }
    impl Stream for Body<'_> {
        type Item = Result<Bytes, core::convert::Infallible>;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let head = self.window.start() == ByteCount(0);
            if head
                && ((!self.reader.head_ready.get() && self.sent == 0)
                    || (!self.reader.finish_head.get() && self.sent != 0))
            {
                *self.reader.head_waker.borrow_mut() = Some(Waker::clone(cx.waker()));
                return Poll::Pending;
            }
            let remaining = self.window.end().0 - self.window.start().0 - self.sent;
            if remaining == 0 {
                return Poll::Ready(None);
            }
            let count = if head && self.sent == 0 { 2 } else { remaining };
            self.sent += count;
            Poll::Ready(Some(Ok(Bytes::from(vec![0; count as usize]))))
        }
    }
    impl<'read> RangeReader<'read> for Gated {
        type Error = core::convert::Infallible;
        type Body = Body<'read>;
        type Read = Ready<Result<ReadResponse<'read, Self::Body>, Self::Error>>;
        fn size(&self) -> ByteCount {
            ByteCount(19)
        }
        fn evidence(&'read self) -> Evidence<'read> {
            Evidence::Enforced {
                representation: "zeros",
                version: b"one",
            }
        }
        fn read(&'read self, window: Window) -> Self::Read {
            ready(Ok(ReadResponse {
                window,
                evidence: self.evidence(),
                body: Body {
                    reader: self,
                    window,
                    sent: 0,
                },
            }))
        }
    }

    for measurement in [Measurement::None, Measurement::default()] {
        let directory = Directory::create();
        let reader = Gated {
            head_ready: Cell::new(false),
            finish_head: Cell::new(false),
            head_waker: RefCell::new(None),
        };
        let pending = ReaderTransfer::from_reader(&reader)
            .measurement(measurement)
            .to_file(directory.0.join("output"))
            .unwrap();
        let native =
            super::Native::acquire_reader(pending.plan, pending.session, pending.resume, None)
                .await
                .unwrap();
        let held = native.hooks().read.install();
        let facts = RefCell::new(Vec::new());
        let changed = Notify::new();
        let mut observer = ReaderSignal {
            facts: &facts,
            changed: &changed,
        };
        let mut observation =
            super::Observation::start(&mut observer, crate::transfer::Length::Known(reader.size()));
        let mut partial = crate::transfer::Partial::default();
        let mut receiving = Box::pin(super::receive(
            &reader,
            reader.size(),
            reader.evidence(),
            &native,
            &mut partial,
            native.policy().start(),
            core::future::pending(),
            &mut observation,
        ));
        tokio::time::timeout(core::time::Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    _ = &mut receiving => panic!("held head must not finish"),
                    () = changed.notified() => {
                        if facts.borrow().last().is_some_and(|facts| facts.written() == ByteCount(14)) { break; }
                    }
                }
            }
            let tails = *facts.borrow().last().unwrap();
            assert_eq!(tails.available(), ByteCount(0));
            assert_eq!(tails.hashed(), ByteCount(0));
            let windows: Vec<_> = tails.windows().collect();
            assert_eq!(windows.iter().map(|facts| (facts.window().start().0, facts.window().end().0)).collect::<Vec<_>>(), [(0, 5), (5, 10), (10, 15), (15, 19)]);
            assert_eq!(windows[0].written(), ByteCount(0));
            assert_eq!(windows[3].written(), ByteCount(4));
            reader.head_ready.set(true);
            reader.head_waker.borrow_mut().take().unwrap().wake();
            if measurement != Measurement::None {
                tokio::select! {
                    _ = &mut receiving => panic!("hash readback must be held"),
                    () = held.entered.notified() => {}
                }
            } else {
                loop {
                    tokio::select! {
                        _ = &mut receiving => panic!("partial head must not finish"),
                        () = changed.notified() => {
                            if facts.borrow().last().is_some_and(|facts| facts.available() == ByteCount(2)) { break; }
                        }
                    }
                }
            }
            let head = *facts.borrow().last().unwrap();
            assert_eq!(head.available(), ByteCount(2));
            assert_eq!(head.written(), ByteCount(16));
            assert_eq!(head.hashed(), ByteCount(0));
            assert_eq!(head.windows().next().unwrap().written(), ByteCount(2));
            assert_eq!(head.windows().next().unwrap().reception(), super::Reception::Receiving);
            held.release();
            reader.finish_head.set(true);
            if let Some(waker) = reader.head_waker.borrow_mut().take() { waker.wake(); }
            receiving.as_mut().await.unwrap();
        }).await.unwrap();
        drop(receiving);
        let terminal = *facts.borrow().last().unwrap();
        assert_eq!(terminal.available(), reader.size());
        assert_eq!(terminal.reception(), super::Reception::CleanEof);
        assert_eq!(terminal.windows().count(), 4);
        assert_eq!(
            terminal.hashed(),
            if measurement == Measurement::None {
                ByteCount(0)
            } else {
                reader.size()
            }
        );
        assert!(
            !directory.0.join("output").exists(),
            "reception alone does not publish"
        );
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
    let facts = Rc::new(RefCell::new(Vec::new()));
    let mut observer = ReaderObservation(&facts);
    let result = tokio::time::timeout(
        core::time::Duration::from_secs(5),
        ReaderTransfer::from_reader(&reader)
            .to_file(directory.0.join("output"))
            .unwrap()
            .run_registered_observed(None, &mut observer),
    )
    .await;
    super::FRONTIER_TEST.with(|hook| *hook.borrow_mut() = None);
    let receipt = result.unwrap().unwrap();
    assert_eq!(receipt.native().partial().hashed, ByteCount(16));
    let facts = facts.borrow();
    assert!(facts.iter().any(|facts| {
        facts.written() == ByteCount(12)
            && facts.hashed() == ByteCount(0)
            && facts.available() == ByteCount(0)
    }));
    assert!(facts.iter().any(|facts| {
        facts.written() == ByteCount(14)
            && facts.hashed() == ByteCount(2)
            && facts.available() == ByteCount(2)
    }));
    let terminal = *facts.last().unwrap();
    assert_eq!(terminal.attempt_received(), ByteCount(16));
    assert_eq!(terminal.attempt_written(), ByteCount(16));
    assert_eq!(terminal.hashed(), ByteCount(16));
    assert_eq!(terminal.restored(), ByteCount(0));
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

#[cfg(unix)]
struct Resumable<'data> {
    bytes: &'data [u8],
    version: &'data [u8],
    representation: &'data str,
    pause: bool,
    calls: RefCell<Vec<Window>>,
    paused: tokio::sync::Notify,
}

#[cfg(unix)]
impl<'read, 'data: 'read> RangeReader<'read> for Resumable<'data> {
    type Error = BorrowedCause<'read>;
    type Body = core::pin::Pin<Box<dyn futures::Stream<Item = Result<Bytes, Self::Error>> + 'read>>;
    type Read = Ready<Result<ReadResponse<'read, Self::Body>, Self::Error>>;

    fn size(&self) -> ByteCount {
        ByteCount(self.bytes.len() as u64)
    }
    fn evidence(&'read self) -> Evidence<'read> {
        Evidence::Enforced {
            representation: self.representation,
            version: self.version,
        }
    }
    fn read(&'read self, window: Window) -> Self::Read {
        self.calls.borrow_mut().push(window);
        let start = window.start.0 as usize;
        let end = window.end.0 as usize;
        let body: Self::Body = if self.pause && start == 0 {
            let bytes = Bytes::copy_from_slice(&self.bytes[start..end.min(4)]);
            Box::pin(stream::unfold(Some(bytes), move |next| async move {
                if let Some(bytes) = next {
                    return Some((Ok(bytes), None));
                }
                // Polling resumes only after the previous write, check and checkpoint settled.
                self.paused.notify_one();
                core::future::pending().await
            }))
        } else {
            Box::pin(stream::iter([Ok(Bytes::copy_from_slice(
                &self.bytes[start..end],
            ))]))
        };
        ready(Ok(ReadResponse {
            window,
            evidence: self.evidence(),
            body,
        }))
    }
}

#[cfg(unix)]
impl<'data> Resumable<'data> {
    fn fixture(bytes: &'data [u8]) -> Self {
        Self {
            bytes,
            version: b"one",
            representation: "bytes",
            pause: false,
            calls: RefCell::new(Vec::new()),
            paused: tokio::sync::Notify::new(),
        }
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn real_stop_and_drop_restore_only_anchored_prefix_and_fetch_only_suffix() {
    use crate::transfer::Algorithm;
    for (abrupt, algorithm) in [
        (false, Algorithm::Sha256),
        (true, Algorithm::Sha256),
        (false, Algorithm::Sha512),
        (false, Algorithm::Blake3),
    ] {
        let directory = Directory::create();
        let data: Vec<u8> = (0..32).collect();
        let reader = Resumable {
            pause: true,
            ..Resumable::fixture(&data)
        };
        let output = directory.0.join("output");
        let pending = ReaderTransfer::from_reader(&reader)
            .measurement(Measurement::Algorithm(algorithm))
            .to_file(&output)
            .unwrap();
        let state = crate::transfer::StateRef::clone(pending.state());
        let initial_facts = Rc::new(RefCell::new(Vec::new()));
        let mut initial_observer = ReaderObservation(&initial_facts);
        if abrupt {
            let mut running =
                Box::pin(pending.run_registered_observed(None, &mut initial_observer));
            tokio::select! {
                result = &mut running => panic!("paused source completed: {result:?}"),
                () = reader.paused.notified() => {}
            }
            drop(running);
            assert_eq!(
                initial_facts.borrow().last().unwrap().reception(),
                super::Reception::Receiving
            );
        } else {
            let failure = pending
                .run_registered_until_observed(
                    None,
                    reader.paused.notified(),
                    &mut initial_observer,
                )
                .await
                .unwrap_err();
            let terminal = *initial_facts.borrow().last().unwrap();
            assert_eq!(terminal.received(), failure.partial.received);
            assert_eq!(terminal.written(), failure.partial.written);
            assert_eq!(terminal.hashed(), failure.partial.hashed);
            assert_eq!(terminal.reception(), super::Reception::Stopped);
            assert_eq!(terminal.available(), ByteCount(4));
            assert_eq!(
                terminal.windows().next().unwrap().reception(),
                super::Reception::Stopped
            );
            assert!(matches!(failure.kind, FailureKind::Stopped));
        }
        let stopped_observations = initial_facts.borrow().len();
        assert!(!output.exists());
        let checkpoint =
            super::super::record::Record::decode(&fs::read(state.path().join("state")).unwrap())
                .unwrap();
        let (length, _) = checkpoint.reader.unwrap().prefix.unwrap();
        assert_eq!(length, ByteCount(4));
        assert_eq!(
            &fs::read(state.path().join("data")).unwrap()[..4],
            &data[..4]
        );
        let reader = Resumable::fixture(&data);
        let facts = Rc::new(RefCell::new(Vec::new()));
        let mut observer = ReaderObservation(&facts);
        let receipt = tokio::time::timeout(core::time::Duration::from_secs(5), async {
            loop {
                let result = ReaderTransfer::from_reader(&reader)
                    .measurement(Measurement::Algorithm(algorithm))
                    .resume_from(crate::transfer::StateRef::clone(&state))
                    .to_file(&output)
                    .unwrap()
                    .run_registered_observed(None, &mut observer)
                    .await;
                match result {
                    Err(failure) if matches!(failure.kind, FailureKind::Busy) => {
                        tokio::task::yield_now().await
                    }
                    result => break result.unwrap(),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            receipt.stats(),
            super::ReaderStats {
                restored: ByteCount(4),
                source: ByteCount(28),
                written: ByteCount(28)
            }
        );
        assert_eq!(receipt.native().partial().hashed, ByteCount(32));
        let facts = facts.borrow();
        assert!(facts.iter().any(|facts| {
            facts.restored() == ByteCount(4)
                && facts.received() == ByteCount(4)
                && facts.written() == ByteCount(4)
                && facts.hashed() == ByteCount(4)
                && facts.attempt_received() == ByteCount(0)
                && facts.attempt_written() == ByteCount(0)
                && facts.available() == ByteCount(4)
                && facts
                    .windows()
                    .map(|window| (window.window().start().0, window.window().end().0))
                    .collect::<Vec<_>>()
                    == [(4, 11), (11, 18), (18, 25), (25, 32)]
        }));
        let terminal = *facts.last().unwrap();
        assert_eq!(terminal.received(), ByteCount(32));
        assert_eq!(terminal.written(), ByteCount(32));
        assert_eq!(terminal.hashed(), ByteCount(32));
        assert_eq!(terminal.restored(), ByteCount(4));
        assert_eq!(terminal.attempt_received(), ByteCount(28));
        assert_eq!(terminal.attempt_written(), ByteCount(28));
        assert_eq!(terminal.available(), ByteCount(32));
        assert_eq!(terminal.reception(), super::Reception::CleanEof);
        assert_eq!(
            terminal
                .windows()
                .map(|window| window.received().0)
                .sum::<u64>(),
            28
        );
        assert_eq!(
            terminal
                .windows()
                .map(|window| window.written().0)
                .sum::<u64>(),
            28
        );
        assert_eq!(initial_facts.borrow().len(), stopped_observations);
        assert!(
            reader
                .calls
                .borrow()
                .iter()
                .all(|window| window.start >= ByteCount(4))
        );
        assert_eq!(fs::read(output).unwrap(), data);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn corrupt_prefix_foreign_inode_changed_pin_and_changed_checks_are_refused_before_reads() {
    use std::os::unix::fs::PermissionsExt as _;
    for mutation in 0..8 {
        let directory = Directory::create();
        let data: Vec<u8> = (0..32).collect();
        let paused = Resumable {
            pause: true,
            ..Resumable::fixture(&data)
        };
        let output = directory.0.join("output");
        let mut hash = crate::checks::Requirements::default().start(Measurement::default());
        hash.update(&data);
        let expected = hash.finish().unwrap().measured().unwrap();
        let pending = ReaderTransfer::from_reader(&paused)
            .expect(expected)
            .to_file(&output)
            .unwrap();
        let state = crate::transfer::StateRef::clone(pending.state());
        pending
            .run_until(paused.paused.notified())
            .await
            .unwrap_err();
        let before = fs::read(state.path().join("state")).unwrap();
        let path = state.path().join("data");
        let mut reader = Resumable::fixture(&data);
        match mutation {
            0 => {
                let mut bytes = fs::read(&path).unwrap();
                bytes[0] ^= 1;
                fs::write(&path, bytes).unwrap();
            }
            1 => {
                fs::rename(&path, state.path().join("foreign-original")).unwrap();
                fs::write(&path, &data).unwrap();
                fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            }
            2 => reader.version = b"two",
            3 => fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap(),
            5 => reader.bytes = &data[..31],
            6 => reader.representation = "decoded bytes",
            _ => {}
        }
        let mut request = ReaderTransfer::from_reader(&reader)
            .expect(expected)
            .resume_from(crate::transfer::StateRef::clone(&state));
        if mutation == 4 {
            request =
                request.measurement(Measurement::Algorithm(crate::transfer::Algorithm::Sha512));
        }
        let destination = if mutation == 7 {
            directory.0.join("changed-output")
        } else {
            PathBuf::clone(&output)
        };
        let failure = request
            .to_file(destination)
            .unwrap()
            .run()
            .await
            .unwrap_err();
        assert!(matches!(failure.kind, FailureKind::InvalidState));
        assert!(reader.calls.borrow().is_empty());
        assert_eq!(fs::read(state.path().join("state")).unwrap(), before);
        assert!(!output.exists());
    }
}

#[cfg(unix)]
#[tokio::test]
async fn full_received_late_errors_restore_a_proper_prefix_but_never_cached_eof() {
    struct Late<'data> {
        bytes: &'data [u8],
        bodies: tokio::sync::Barrier,
    }
    impl<'read, 'data: 'read> RangeReader<'read> for Late<'data> {
        type Error = BorrowedCause<'read>;
        type Body =
            core::pin::Pin<Box<dyn futures::Stream<Item = Result<Bytes, Self::Error>> + 'read>>;
        type Read = Ready<Result<ReadResponse<'read, Self::Body>, Self::Error>>;
        fn size(&self) -> ByteCount {
            ByteCount(self.bytes.len() as u64)
        }
        fn evidence(&'read self) -> Evidence<'read> {
            Evidence::Enforced {
                representation: "bytes",
                version: b"one",
            }
        }
        fn read(&'read self, window: Window) -> Self::Read {
            use futures::StreamExt as _;
            let bytes =
                Bytes::copy_from_slice(&self.bytes[window.start.0 as usize..window.end.0 as usize]);
            let body = stream::iter([Ok(bytes)]).chain(stream::once(async move {
                self.bodies.wait().await;
                Err(BorrowedCause(self.bytes))
            }));
            ready(Ok(ReadResponse {
                window,
                evidence: self.evidence(),
                body: Box::pin(body) as Self::Body,
            }))
        }
    }
    for length in [1, 32] {
        let directory = Directory::create();
        let data = vec![7; length];
        let late = Late {
            bytes: &data,
            bodies: tokio::sync::Barrier::new(length.min(4)),
        };
        let output = directory.0.join("output");
        let failure = ReaderTransfer::from_reader(&late)
            .to_file(&output)
            .unwrap()
            .run()
            .await
            .unwrap_err();
        assert!(matches!(
            failure.kind,
            FailureKind::Body(ReaderError::Source(_))
        ));
        assert_eq!(failure.partial.received, ByteCount(length as u64));
        assert!(!output.exists());
        let state = failure.state.unwrap();
        let reader = Resumable::fixture(&data);
        let resumed = ReaderTransfer::from_reader(&reader)
            .resume_from(state)
            .to_file(&output)
            .unwrap()
            .run()
            .await;
        if length == 1 {
            assert!(
                matches!(resumed, Err(failure) if matches!(failure.kind, FailureKind::InvalidState))
            );
            assert!(reader.calls.borrow().is_empty());
        } else {
            let receipt = resumed.unwrap();
            assert!(receipt.stats().restored > ByteCount(0));
            assert!(receipt.stats().restored < ByteCount(length as u64));
            assert!(receipt.stats().source > ByteCount(0));
            assert_eq!(fs::read(output).unwrap(), data);
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn mismatched_and_published_reader_inodes_never_reopen_writable() {
    for mismatch in [false, true] {
        let directory = Directory::create();
        let data = vec![7; 32];
        let reader = Resumable::fixture(&data);
        let output = directory.0.join("output");
        let build = || {
            let request = ReaderTransfer::from_reader(&reader);
            if mismatch {
                request.expect(
                    Digest::parse(crate::transfer::Algorithm::Sha256, &"0".repeat(64)).unwrap(),
                )
            } else {
                request
            }
        };
        let pending = build().to_file(&output).unwrap();
        let state = crate::transfer::StateRef::clone(pending.state());
        match pending.run().await {
            Ok(_) => assert!(!mismatch),
            Err(failure) => {
                assert!(mismatch && matches!(failure.kind, FailureKind::DigestMismatch(_)))
            }
        }
        reader.calls.borrow_mut().clear();
        let before = fs::read(state.path().join("data")).unwrap();
        let failure = build()
            .resume_from(state)
            .to_file(&output)
            .unwrap()
            .run()
            .await
            .unwrap_err();
        assert!(matches!(failure.kind, FailureKind::InvalidState));
        assert!(reader.calls.borrow().is_empty());
        assert_eq!(
            fs::read(failure.state.unwrap().path().join("data")).unwrap(),
            before
        );
        assert_eq!(output.exists(), !mismatch);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn no_hash_and_weak_only_continuation_refuse_without_creating_prefix_evidence() {
    for weak in [false, true] {
        let directory = Directory::create();
        let data: Vec<u8> = (0..32).collect();
        let reader = Resumable {
            pause: true,
            ..Resumable::fixture(&data)
        };
        let output = directory.0.join("output");
        let build = || {
            let request = ReaderTransfer::from_reader(&reader).measurement(Measurement::None);
            if weak {
                request.expect(
                    Digest::parse(
                        crate::transfer::Algorithm::Md5,
                        "00000000000000000000000000000000",
                    )
                    .unwrap(),
                )
            } else {
                request
            }
        };
        let pending = build().to_file(&output).unwrap();
        let state = crate::transfer::StateRef::clone(pending.state());
        let failure = pending
            .run_until(reader.paused.notified())
            .await
            .unwrap_err();
        assert_eq!(
            failure.partial.hashed,
            if weak { ByteCount(4) } else { ByteCount(0) }
        );
        let record =
            super::super::record::Record::decode(&fs::read(state.path().join("state")).unwrap())
                .unwrap();
        assert!(record.reader.unwrap().prefix.is_none());
        assert!(record.partial.actual.is_none());
        assert!(matches!(
            build().resume_from(state).to_file(&output),
            Err(ReaderRequestError::NoResumeAnchor)
        ));
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observer_panic_drops_range_bodies_but_keeps_native_and_aggregate_leases_until_worker_cleanup()
 {
    use core::convert::Infallible;
    use core::panic::AssertUnwindSafe;
    use core::pin::Pin;
    use core::task::{Context, Poll, Waker};
    use std::os::fd::AsRawFd as _;
    use std::os::unix::fs::DirBuilderExt as _;

    use futures::{FutureExt as _, Stream};
    use tokio::sync::oneshot;

    use crate::transfer::native::{Native, NativeError};
    use crate::transfer::observation::{Observation, Observer, ProgressFacts};
    use crate::transfer::{OperationOwner, Partial, StateRef};

    struct Controlled {
        opened: Cell<usize>,
        dropped: Cell<usize>,
        polls: Cell<usize>,
        tails_ready: Cell<bool>,
        tail_wakers: RefCell<Vec<Waker>>,
    }
    struct Body<'read> {
        reader: &'read Controlled,
        head: bool,
        yielded: bool,
    }
    impl Stream for Body<'_> {
        type Item = Result<Bytes, Infallible>;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.reader.polls.set(self.reader.polls.get() + 1);
            if self.yielded {
                return Poll::Pending;
            }
            if !self.head && !self.reader.tails_ready.get() {
                self.reader
                    .tail_wakers
                    .borrow_mut()
                    .push(Waker::clone(cx.waker()));
                return Poll::Pending;
            }
            self.yielded = true;
            Poll::Ready(Some(Ok(Bytes::from_static(b"a"))))
        }
    }
    impl Drop for Body<'_> {
        fn drop(&mut self) {
            self.reader.dropped.set(self.reader.dropped.get() + 1);
        }
    }
    impl<'read> RangeReader<'read> for Controlled {
        type Error = Infallible;
        type Body = Body<'read>;
        type Read = Ready<Result<ReadResponse<'read, Self::Body>, Self::Error>>;
        fn size(&self) -> ByteCount {
            ByteCount(16)
        }
        fn evidence(&'read self) -> Evidence<'read> {
            Evidence::Enforced {
                representation: "bytes",
                version: b"one",
            }
        }
        fn read(&'read self, window: Window) -> Self::Read {
            self.opened.set(self.opened.get() + 1);
            ready(Ok(ReadResponse {
                window,
                evidence: self.evidence(),
                body: Body {
                    reader: self,
                    head: window.start == ByteCount(0),
                    yielded: false,
                },
            }))
        }
    }
    struct PanicAfterWrite<'a>(&'a Cell<usize>);
    impl Observer for PanicAfterWrite<'_> {
        fn observe(&mut self, facts: ProgressFacts) {
            self.0.set(self.0.get() + 1);
            assert_eq!(
                facts.written(),
                ByteCount(0),
                "fixture observer panic after a tail write"
            );
        }
    }
    struct SettledRegistration {
        owner: Option<OperationOwner>,
        settled: Option<oneshot::Sender<()>>,
    }
    impl Drop for SettledRegistration {
        fn drop(&mut self) {
            // Signal actual owner destruction after its stable aggregate lease has unlocked.
            drop(self.owner.take());
            if let Some(settled) = self.settled.take() {
                let _ = settled.send(());
            }
        }
    }

    let directory = Directory::create();
    let store = directory.0.join(crate::transfer::OWNED_RUN_STORE);
    fs::DirBuilder::new().mode(0o700).create(&store).unwrap();
    let lease = Native::lease_directory(PathBuf::clone(&store))
        .await
        .unwrap();
    let (settled, mut finished) = oneshot::channel();
    let owner: OperationOwner = Arc::new(SettledRegistration {
        owner: Some(lease),
        settled: Some(settled),
    });
    let reader = Controlled {
        opened: Cell::new(0),
        dropped: Cell::new(0),
        polls: Cell::new(0),
        tails_ready: Cell::new(false),
        tail_wakers: RefCell::new(Vec::new()),
    };
    let output = directory.0.join("output");
    let pending = ReaderTransfer::from_reader(&reader)
        .measurement(Measurement::None)
        .to_file(&output)
        .unwrap();
    let state = StateRef::clone(pending.state());
    let super::PendingReader {
        size,
        evidence,
        plan,
        session,
        resume,
        ..
    } = pending;
    let native = Native::acquire_reader(plan, session, resume, Some(owner))
        .await
        .unwrap();
    let (checks, restored) = native.restore_reader().await.unwrap();
    assert_eq!(restored, ByteCount(0));
    // The existing single-consumer native gate holds the first head write physically running.
    // Other bodies cannot yield until the driver has observed that barrier being entered.
    let held = native.hooks().write.install();
    let callbacks = Cell::new(0);
    let mut observer = PanicAfterWrite(&callbacks);
    let reader_ref = &reader;
    let mut run = Box::pin(
        AssertUnwindSafe(async move {
            let mut partial = Partial::default();
            let mut observation =
                Observation::start(&mut observer, crate::transfer::Length::Known(size));
            super::receive(
                reader_ref,
                size,
                evidence,
                &native,
                &mut partial,
                checks,
                core::future::pending(),
                &mut observation,
            )
            .await
        })
        .catch_unwind(),
    );
    tokio::time::timeout(core::time::Duration::from_secs(5), async {
        tokio::select! {
            _ = &mut run => panic!("head write must remain physically held"),
            () = held.entered.notified() => {}
        }
    })
    .await
    .unwrap();
    assert_eq!(reader.opened.get(), 4);
    assert_eq!(reader.dropped.get(), 0);
    reader.tails_ready.set(true);
    for waker in reader.tail_wakers.borrow_mut().drain(..) {
        waker.wake();
    }
    let outcome = tokio::time::timeout(core::time::Duration::from_secs(5), run.as_mut())
        .await
        .unwrap();
    let Err(payload) = outcome else {
        panic!("a settled tail write must invoke the panicking observer");
    };
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("unexpected panic payload");
    assert!(message.contains("fixture observer panic after a tail write"));
    drop(payload);
    drop(run);
    assert_eq!(
        reader.dropped.get(),
        4,
        "every admitted body is destroyed on unwind"
    );
    let polls_after_unwind = reader.polls.get();
    let callbacks_after_unwind = callbacks.get();
    assert!(matches!(
        finished.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        state.reconcile().await.unwrap_err().kind,
        FailureKind::Busy
    ));
    assert!(matches!(
        Native::lease_directory(PathBuf::clone(&store)).await,
        Err(NativeError::Busy)
    ));
    let workspace_probe = fs::File::open(state.path().join("lease")).unwrap();
    // SAFETY: this independently opened descriptor stays owned through both flock probes.
    assert_eq!(
        unsafe { libc::flock(workspace_probe.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        -1
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EWOULDBLOCK)
    );
    assert_eq!(reader.polls.get(), polls_after_unwind);
    assert_eq!(callbacks.get(), callbacks_after_unwind);
    assert!(!output.exists());
    held.release();
    tokio::time::timeout(core::time::Duration::from_secs(5), finished)
        .await
        .unwrap()
        .unwrap();
    let aggregate = Native::lease_directory(store).await.unwrap();
    drop(aggregate);
    // SAFETY: settlement has completed and this is still the independent probe descriptor.
    assert_eq!(
        unsafe { libc::flock(workspace_probe.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    // SAFETY: release only the probe's just-acquired lock, never a native operation's descriptor.
    assert_eq!(
        unsafe { libc::flock(workspace_probe.as_raw_fd(), libc::LOCK_UN) },
        0
    );
    assert!(state.reconcile().await.is_ok());
    assert_eq!(reader.polls.get(), polls_after_unwind);
    assert_eq!(callbacks.get(), callbacks_after_unwind);
    assert!(!output.exists());
}
