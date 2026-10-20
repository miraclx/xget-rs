//! Public advanced live entrances, independent of Client and native file publication.

use core::cell::Cell;
use core::future::Future as _;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::collections::VecDeque;
use std::io;
use std::rc::Rc;

use bytes::Bytes;
use futures::executor::block_on;
use futures::task::noop_waker_ref;
use futures::{Stream, StreamExt as _, stream};
use tokio::io::AsyncWrite;
use xget::Transfer;
use xget::transfer::live::{BodyError, BodyFailure, DrainError, FinishError, PendingWriter};
use xget::transfer::{ByteCount, Digest, Length, Measurement, Provenance, RequestError};

const ABC_SHA256: &str = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
const EMPTY_SHA256: &str =
    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

#[test]
fn public_body_checks_real_empty_and_abc_vectors_without_a_runtime() {
    for (bytes, expected) in [
        (b"".as_slice(), EMPTY_SHA256),
        (b"abc".as_slice(), ABC_SHA256),
    ] {
        for length in [
            Length::Unknown,
            Length::Known(ByteCount(bytes.len() as u64)),
        ] {
            block_on(async {
                let source = stream::iter([Ok::<_, io::Error>(Bytes::copy_from_slice(bytes))]);
                let expected: Digest = expected.parse().unwrap();
                let mut body = Transfer::from_body(source, length)
                    .expect(expected)
                    .measurement(Measurement::None)
                    .into_body()
                    .unwrap();
                let mut received = Vec::new();
                while let Some(chunk) = body.next().await {
                    received.extend_from_slice(&chunk.unwrap());
                }
                let receipt = body.finish().unwrap();
                assert_eq!(received, bytes);
                assert_eq!(receipt.progress().received, ByteCount(bytes.len() as u64));
                assert_eq!(receipt.progress().released, receipt.progress().received);
                assert_eq!(receipt.progress().hashed, receipt.progress().received);
                assert_eq!(receipt.checks().measured(), None);
                assert_eq!(
                    receipt
                        .checks()
                        .matched_requirements()
                        .next()
                        .unwrap()
                        .digest,
                    expected
                );
            });
        }
    }
}

#[test]
fn public_measurement_defaults_to_real_sha256_but_none_does_no_hidden_hashing() {
    block_on(async {
        for measurement in [Measurement::default(), Measurement::None] {
            let source = stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"abc"))]);
            let mut body = Transfer::from_body(source, Length::Unknown)
                .measurement(measurement)
                .into_body()
                .unwrap();
            assert_eq!(body.next().await.unwrap().unwrap(), b"abc".as_slice());
            assert!(body.next().await.is_none());
            let receipt = body.finish().unwrap();
            assert_eq!(receipt.checks().matched_requirements().count(), 0);
            match measurement {
                Measurement::None => {
                    assert_eq!(receipt.progress().hashed, ByteCount(0));
                    assert_eq!(receipt.checks().measured(), None);
                }
                Measurement::Algorithm(_) => {
                    assert_eq!(receipt.progress().hashed, ByteCount(3));
                    assert_eq!(
                        receipt.checks().measured(),
                        Some(ABC_SHA256.parse().unwrap())
                    );
                }
            }
        }
    });
}

#[test]
fn public_finish_requires_actual_eof_not_just_all_declared_bytes() {
    block_on(async {
        let source = stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"abc"))]);
        let mut body = Transfer::from_body(source, Length::Known(ByteCount(3)))
            .into_body()
            .unwrap();
        assert!(body.next().await.unwrap().is_ok());
        let failure = body.finish().unwrap_err();
        assert!(matches!(failure, FinishError::Incomplete(_)));
        assert_eq!(failure.progress().released, ByteCount(3));
    });
}

#[test]
fn public_length_and_late_source_failures_precede_clean_eof_and_carry_counters() {
    block_on(async {
        for expected in [2, 4] {
            let source = stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"abc"))]);
            let mut body = Transfer::from_body(source, Length::Known(ByteCount(expected)))
                .into_body()
                .unwrap();
            if expected == 4 {
                assert!(body.next().await.unwrap().is_ok());
            }
            let failure = body.next().await.unwrap().unwrap_err();
            assert!(matches!(failure.error, BodyError::Length {
                expected: ByteCount(value), received: ByteCount(3),
            } if value == expected));
            assert_eq!(failure.progress.received, ByteCount(3));
            assert_eq!(
                failure.progress.released,
                ByteCount(if expected == 4 { 3 } else { 0 })
            );
            assert!(body.next().await.is_none());
            assert!(matches!(body.finish(), Err(FinishError::Failed(_))));
        }
        let source = stream::iter([
            Ok(Bytes::from_static(b"abc")),
            Err(io::Error::other("late source failure")),
        ]);
        let mut body = Transfer::from_body(source, Length::Known(ByteCount(3)))
            .into_body()
            .unwrap();
        assert!(body.next().await.unwrap().is_ok());
        let failure = body.next().await.unwrap().unwrap_err();
        assert!(matches!(failure.error, BodyError::Source(_)));
        assert_eq!(failure.progress.released, ByteCount(3));
        assert!(matches!(body.finish(), Err(FinishError::Failed(_))));
    });
}

#[test]
fn public_multi_algorithm_requirements_are_all_enforced_with_measurement_none() {
    block_on(async {
        let expected: Digest = ABC_SHA256.parse().unwrap();
        let wrong_md5: Digest = "md5:d41d8cd98f00b204e9800998ecf8427e".parse().unwrap();
        let source = stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"abc"))]);
        let mut body = Transfer::from_body(source, Length::Unknown)
            .expect(expected)
            .require(expected, Provenance::Source)
            .expect(wrong_md5)
            .measurement(Measurement::None)
            .into_body()
            .unwrap();
        assert!(body.next().await.unwrap().is_ok());
        let failure = body.next().await.unwrap().unwrap_err();
        let BodyError::Mismatch(mismatch) = failure.error else {
            panic!("expected mismatch")
        };
        assert_eq!(failure.progress.hashed, ByteCount(3));
        assert_eq!(
            mismatch.actual,
            "md5:900150983cd24fb0d6963f7d28e17f72".parse().unwrap()
        );
        let matched = mismatch.results.matched_requirements().next().unwrap();
        assert_eq!(matched.digest, expected);
        assert_eq!(matched.origins.iter().count(), 2);
        assert_eq!(mismatch.results.measured(), None);
        assert!(matches!(body.finish(), Err(FinishError::Failed(_))));
    });
}

struct Source {
    chunks: VecDeque<Result<Bytes, io::Error>>,
    polls: Rc<Cell<usize>>,
}

impl Source {
    fn chunks(chunks: impl IntoIterator<Item = Result<Bytes, io::Error>>) -> Self {
        Self {
            chunks: chunks.into_iter().collect(),
            polls: Rc::default(),
        }
    }
}

impl Stream for Source {
    type Item = Result<Bytes, io::Error>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.polls.set(this.polls.get() + 1);
        Poll::Ready(this.chunks.pop_front())
    }
}

#[derive(Default)]
struct Writer {
    bytes: Vec<u8>,
    mode: Mode,
    flushes: usize,
    shutdowns: usize,
}

#[derive(Default)]
enum Mode {
    #[default]
    OneByte,
    Gate(Rc<Cell<bool>>),
    FailAfterOne,
    Zero,
    FlushError,
    FlushPending(Rc<Cell<bool>>),
}

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match &this.mode {
            Mode::Gate(open) if !open.get() => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Mode::FailAfterOne if !this.bytes.is_empty() => {
                return Poll::Ready(Err(io::Error::other("write failure")));
            }
            Mode::Zero => return Poll::Ready(Ok(0)),
            _ => {}
        }
        this.bytes.push(bytes[0]);
        Poll::Ready(Ok(1))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.flushes += 1;
        match &this.mode {
            Mode::FlushError => Poll::Ready(Err(io::Error::other("flush failure"))),
            Mode::FlushPending(entered) => {
                entered.set(true);
                Poll::Pending
            }
            _ => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().shutdowns += 1;
        Poll::Ready(Ok(()))
    }
}

#[test]
fn sticky_conflicts_and_dropped_plans_do_not_poll_body_or_writer() {
    let source = Source::chunks([Ok(Bytes::from_static(b"abc"))]);
    let polls = Rc::clone(&source.polls);
    let mut writer = Writer::default();
    let request = Transfer::from_body(source, Length::Unknown)
        .expect(ABC_SHA256.parse::<Digest>().unwrap())
        .expect(EMPTY_SHA256.parse::<Digest>().unwrap())
        .expect(ABC_SHA256.parse::<Digest>().unwrap())
        .stream_to(&mut writer);
    assert!(matches!(
        &request,
        Err(RequestError::ConflictingRequirements(_))
    ));
    assert_eq!(polls.get(), 0);
    // The result may own the borrow; release it before inspecting the writer.
    drop(request);
    assert!(writer.bytes.is_empty());
    assert_eq!(writer.flushes, 0);
    assert_eq!(writer.shutdowns, 0);

    let source = Source::chunks([Ok(Bytes::from_static(b"abc"))]);
    let polls = Rc::clone(&source.polls);
    let pending: PendingWriter<'_, _, _> = Transfer::from_body(source, Length::Unknown)
        .stream_to(&mut writer)
        .unwrap();
    drop(pending);
    assert_eq!(polls.get(), 0);
    assert!(writer.bytes.is_empty());
}

#[test]
fn public_writer_backpressure_retains_one_chunk_and_never_shuts_down() {
    let source = Source::chunks([Ok(Bytes::from_static(b"abc")), Ok(Bytes::from_static(b"d"))]);
    let polls = Rc::clone(&source.polls);
    let gate = Rc::new(Cell::new(false));
    let mut writer = Writer {
        mode: Mode::Gate(Rc::clone(&gate)),
        ..Writer::default()
    };
    let pending = Transfer::from_body(source, Length::Unknown)
        .measurement(Measurement::None)
        .stream_to(&mut writer)
        .unwrap();
    assert_eq!(polls.get(), 0);
    let mut future = Box::pin(pending.run());
    let mut cx = Context::from_waker(noop_waker_ref());
    assert!(future.as_mut().poll(&mut cx).is_pending());
    assert_eq!(polls.get(), 1);
    assert!(future.as_mut().poll(&mut cx).is_pending());
    assert_eq!(polls.get(), 1);
    gate.set(true);
    let receipt = block_on(future).unwrap();
    assert_eq!(receipt.accepted(), ByteCount(4));
    assert_eq!(receipt.body().progress().hashed, ByteCount(0));
    assert_eq!(writer.bytes, b"abcd");
    assert_eq!(writer.flushes, 1);
    assert_eq!(writer.shutdowns, 0);
}

#[test]
fn public_writer_failures_preserve_actual_partial_acceptance() {
    for mode in [Mode::FailAfterOne, Mode::Zero, Mode::FlushError] {
        let source = stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"abc"))]);
        let mut writer = Writer {
            mode,
            ..Writer::default()
        };
        let failure = block_on(
            Transfer::from_body(source, Length::Unknown)
                .stream_to(&mut writer)
                .unwrap()
                .run(),
        )
        .unwrap_err();
        match failure.error {
            DrainError::Write(_) => assert_eq!(failure.accepted, ByteCount(1)),
            DrainError::WriteZero => assert_eq!(failure.accepted, ByteCount(0)),
            DrainError::Flush(_) => assert_eq!(failure.accepted, ByteCount(3)),
            other => panic!("unexpected failure: {other:?}"),
        }
        assert_eq!(failure.progress.released, ByteCount(3));
        assert_eq!(writer.shutdowns, 0);
    }
}

#[test]
fn public_late_body_and_check_errors_prevent_clean_writer_completion() {
    for required in [None, Some(EMPTY_SHA256.parse::<Digest>().unwrap())] {
        let source = Source::chunks(
            [Ok(Bytes::from_static(b"abc"))].into_iter().chain(
                required
                    .is_none()
                    .then(|| Err(io::Error::other("late body failure"))),
            ),
        );
        let mut transfer = Transfer::from_body(source, Length::Known(ByteCount(3)));
        if let Some(required) = required {
            transfer = transfer.expect(required);
        }
        let mut writer = Writer::default();
        let failure = block_on(transfer.stream_to(&mut writer).unwrap().run()).unwrap_err();
        match failure.error {
            DrainError::Body(BodyFailure {
                error: BodyError::Source(_),
                ..
            }) if required.is_none() => {}
            DrainError::Body(BodyFailure {
                error: BodyError::Mismatch(_),
                ..
            }) if required.is_some() => {}
            other => panic!("unexpected terminal error: {other:?}"),
        }
        assert_eq!(failure.accepted, ByteCount(3));
        assert_eq!(writer.bytes, b"abc");
        assert_eq!(writer.flushes, 0);
        assert_eq!(writer.shutdowns, 0);
    }
}

#[test]
fn public_borrowed_non_error_local_causes_are_not_stringified_or_made_static() {
    struct Cause<'a> {
        text: &'a str,
        identity: Rc<()>,
    }
    let text = String::from("application cause");
    let identity = Rc::new(());
    let source = stream::once(async {
        Err::<Bytes, _>(Cause {
            text: &text,
            identity: Rc::clone(&identity),
        })
    });
    let mut writer = Writer::default();
    let result = block_on(
        Transfer::from_body(source, Length::Unknown)
            .stream_to(&mut writer)
            .unwrap()
            .run(),
    );
    match result {
        Err(failure) => {
            let DrainError::Body(BodyFailure {
                error: BodyError::Source(cause),
                ..
            }) = failure.error
            else {
                panic!("expected concrete borrowed cause")
            };
            assert!(core::ptr::eq(cause.text.as_ptr(), text.as_ptr()));
            assert!(Rc::ptr_eq(&cause.identity, &identity));
            assert_eq!(failure.accepted, ByteCount(0));
        }
        Ok(_) => panic!("expected source failure"),
    }
    assert_eq!(writer.flushes, 0);
    assert_eq!(writer.shutdowns, 0);
}

#[test]
fn public_stack_borrowing_body_and_writer_need_no_tokio_runtime() {
    let payload = vec![b'a', b'b', b'c'];
    let source = stream::iter(
        payload
            .chunks(1)
            .map(|bytes| Ok::<_, io::Error>(Bytes::copy_from_slice(bytes))),
    );
    let mut writer = Writer::default();
    let pending = Transfer::from_body(source, Length::Unknown)
        .stream_to(&mut writer)
        .unwrap();
    let receipt = block_on(pending.run()).unwrap();
    assert_eq!(receipt.accepted(), ByteCount(3));
    assert_eq!(writer.bytes, payload);
}

#[test]
fn dropping_a_pending_writer_future_stops_admission_without_shutdown_or_rollback_claims() {
    let source = Source::chunks([Ok(Bytes::from_static(b"abc")), Ok(Bytes::from_static(b"d"))]);
    let polls = Rc::clone(&source.polls);
    let gate = Rc::new(Cell::new(false));
    let mut writer = Writer {
        mode: Mode::Gate(gate),
        ..Writer::default()
    };
    let mut future = Box::pin(
        Transfer::from_body(source, Length::Unknown)
            .stream_to_pinned(Pin::new(&mut writer))
            .unwrap()
            .run(),
    );
    let mut cx = Context::from_waker(noop_waker_ref());
    assert!(future.as_mut().poll(&mut cx).is_pending());
    assert_eq!(polls.get(), 1);
    drop(future);
    assert_eq!(polls.get(), 1);
    assert!(writer.bytes.is_empty());
    assert_eq!(writer.flushes, 0);
    assert_eq!(writer.shutdowns, 0);
}

struct DropSource<S> {
    inner: S,
    drops: Rc<Cell<usize>>,
}

impl<S: Stream + Unpin> Stream for DropSource<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().inner).poll_next(cx)
    }
}

impl<S> Drop for DropSource<S> {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

#[test]
fn dropping_during_acquisition_or_hashing_destroys_the_owned_source() {
    let mut cx = Context::from_waker(noop_waker_ref());
    let drops = Rc::new(Cell::new(0));
    let source = DropSource {
        inner: stream::pending::<Result<Bytes, io::Error>>(),
        drops: Rc::clone(&drops),
    };
    let mut body = Transfer::from_body(source, Length::Unknown)
        .into_body()
        .unwrap();
    assert!(Pin::new(&mut body).poll_next(&mut cx).is_pending());
    assert_eq!(drops.get(), 0);
    drop(body);
    assert_eq!(drops.get(), 1);

    let drops = Rc::new(Cell::new(0));
    let source = DropSource {
        inner: stream::iter([Ok::<_, io::Error>(Bytes::from(vec![0; 128 * 1024]))]),
        drops: Rc::clone(&drops),
    };
    let mut body = Transfer::from_body(source, Length::Unknown)
        .into_body()
        .unwrap();
    assert!(Pin::new(&mut body).poll_next(&mut cx).is_pending());
    assert_eq!(body.progress().hashed, ByteCount(64 * 1024));
    assert_eq!(body.progress().released, ByteCount(0));
    drop(body);
    assert_eq!(drops.get(), 1);
}

#[test]
fn dropping_after_partial_acceptance_destroys_source_without_flushing_or_shutdown() {
    let drops = Rc::new(Cell::new(0));
    let source = DropSource {
        inner: stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"abc"))]),
        drops: Rc::clone(&drops),
    };
    let mut writer = Writer::default();
    let mut future = Box::pin(
        Transfer::from_body(source, Length::Unknown)
            .measurement(Measurement::None)
            .stream_to(&mut writer)
            .unwrap()
            .run(),
    );
    let mut cx = Context::from_waker(noop_waker_ref());
    assert!(future.as_mut().poll(&mut cx).is_pending());
    assert_eq!(drops.get(), 0);
    drop(future);
    assert_eq!(drops.get(), 1);
    assert_eq!(writer.bytes, b"a");
    assert_eq!(writer.flushes, 0);
    assert_eq!(writer.shutdowns, 0);
}

#[test]
fn source_is_destroyed_before_pending_flush_and_drop_does_not_shutdown_writer() {
    let drops = Rc::new(Cell::new(0));
    let entered = Rc::new(Cell::new(false));
    let source = DropSource {
        inner: stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"abc"))]),
        drops: Rc::clone(&drops),
    };
    let mut writer = Writer {
        mode: Mode::FlushPending(Rc::clone(&entered)),
        ..Writer::default()
    };
    let mut future = Box::pin(
        Transfer::from_body(source, Length::Unknown)
            .measurement(Measurement::None)
            .stream_to(&mut writer)
            .unwrap()
            .run(),
    );
    let mut cx = Context::from_waker(noop_waker_ref());
    for _ in 0..16 {
        assert!(future.as_mut().poll(&mut cx).is_pending());
        if entered.get() {
            break;
        }
    }
    assert!(
        entered.get(),
        "the bounded fixture must reach pending flush"
    );
    assert_eq!(drops.get(), 1);
    drop(future);
    assert_eq!(drops.get(), 1);
    assert_eq!(writer.bytes, b"abc");
    assert_eq!(writer.flushes, 1);
    assert_eq!(writer.shutdowns, 0);
}

#[test]
fn public_body_and_writer_need_not_be_unpin() {
    struct PinnedWriter {
        accepted: Cell<u64>,
        flushes: Cell<usize>,
        shutdowns: Cell<usize>,
        pin: core::marker::PhantomPinned,
    }
    impl AsyncWrite for PinnedWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.accepted.set(self.accepted.get() + bytes.len() as u64);
            Poll::Ready(Ok(bytes.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.flushes.set(self.flushes.get() + 1);
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.shutdowns.set(self.shutdowns.get() + 1);
            Poll::Ready(Ok(()))
        }
    }
    let mut writer = Box::pin(PinnedWriter {
        accepted: Cell::new(0),
        flushes: Cell::new(0),
        shutdowns: Cell::new(0),
        pin: core::marker::PhantomPinned,
    });
    let source = stream::once(async { Ok::<_, io::Error>(Bytes::from_static(b"abc")) });
    let receipt = block_on(
        Transfer::from_body(source, Length::Unknown)
            .stream_to_pinned(writer.as_mut())
            .unwrap()
            .run(),
    )
    .unwrap();
    assert_eq!(receipt.accepted(), ByteCount(3));
    assert_eq!(writer.accepted.get(), 3);
    assert_eq!(writer.flushes.get(), 1);
    assert_eq!(writer.shutdowns.get(), 0);
    let _ = &writer.pin;
}

#[test]
fn public_owned_body_and_writer_execution_remain_send() {
    fn assert_send<T: Send>(_: &T) {}
    let source = stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"abc"))]);
    let body = Transfer::from_body(source, Length::Unknown)
        .into_body()
        .unwrap();
    assert_send(&body);
    let future = async move {
        // Vec's AsyncWrite needs no runtime and contains no local test instrumentation.
        let mut writer = Vec::new();
        let receipt = body.drain(Pin::new(&mut writer)).await.unwrap();
        (writer, receipt.accepted())
    };
    assert_send(&future);
    let (bytes, accepted) = std::thread::spawn(move || block_on(future)).join().unwrap();
    assert_eq!(bytes, b"abc");
    assert_eq!(accepted, ByteCount(3));

    let future = async {
        let source = stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"abc"))]);
        let mut writer = Vec::new();
        Transfer::from_body(source, Length::Unknown)
            .stream_to(&mut writer)
            .unwrap()
            .run()
            .await
            .unwrap()
    };
    assert_send(&future);
    assert_eq!(
        std::thread::spawn(move || block_on(future))
            .join()
            .unwrap()
            .accepted(),
        ByteCount(3)
    );
}
