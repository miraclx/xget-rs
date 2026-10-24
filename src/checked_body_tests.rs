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

use super::{BodyError, BodyFailure, CheckedBody, DrainError, FinishError};
use crate::checks::{Algorithm, Digest, Measurement, Provenance, Requirements};
use crate::transfer::{ByteCount, Length};

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const ABC_SHA256: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

struct Fixture {
    chunks: VecDeque<Result<Bytes, io::Error>>,
    polls: Rc<Cell<usize>>,
}

impl Fixture {
    fn body(chunks: impl IntoIterator<Item = Result<Bytes, io::Error>>) -> Self {
        Self {
            chunks: chunks.into_iter().collect(),
            polls: Rc::default(),
        }
    }

    fn checked(self, length: Length) -> CheckedBody<Self> {
        CheckedBody::new(self, length, Requirements::default(), Measurement::None)
    }
}

impl Stream for Fixture {
    type Item = Result<Bytes, io::Error>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.polls.set(this.polls.get() + 1);
        Poll::Ready(this.chunks.pop_front())
    }
}

#[test]
fn actual_checks_match_empty_and_abc_vectors_for_known_and_unknown_lengths() {
    for (bytes, digest) in [
        (b"".as_slice(), EMPTY_SHA256),
        (b"abc".as_slice(), ABC_SHA256),
    ] {
        for length in [
            Length::Unknown,
            Length::Known(ByteCount(bytes.len() as u64)),
        ] {
            block_on(async {
                let digest = Digest::parse(Algorithm::Sha256, digest).unwrap();
                let required = Requirements::single(digest, Provenance::Caller);
                let fixture = Fixture::body([Ok(Bytes::copy_from_slice(bytes))]);
                let mut body = CheckedBody::new(fixture, length, required, Measurement::None);
                let mut output = Vec::new();
                while let Some(chunk) = body.next().await {
                    output.extend_from_slice(&chunk.unwrap());
                }
                let receipt = body.finish().unwrap();
                assert_eq!(output, bytes);
                assert_eq!(receipt.progress.received, ByteCount(bytes.len() as u64));
                assert_eq!(receipt.progress.released, receipt.progress.received);
                assert_eq!(receipt.progress.hashed, receipt.progress.received);
                assert_eq!(
                    receipt.checks.matched_requirements().next().unwrap().digest,
                    digest
                );
                assert_eq!(receipt.checks.measured(), None);
            });
        }
    }
}

#[test]
fn optional_measurement_is_real_but_none_does_no_hidden_hashing() {
    block_on(async {
        for measurement in [Measurement::None, Measurement::Algorithm(Algorithm::Sha256)] {
            let fixture = Fixture::body([Ok(Bytes::from_static(b"abc"))]);
            let mut body = CheckedBody::new(
                fixture,
                Length::Unknown,
                Requirements::default(),
                measurement,
            );
            assert_eq!(body.next().await.unwrap().unwrap(), b"abc".as_slice());
            assert!(body.next().await.is_none());
            let receipt = body.finish().unwrap();
            match measurement {
                Measurement::None => {
                    assert_eq!(receipt.progress.hashed, ByteCount(0));
                    assert_eq!(receipt.checks.measured(), None);
                }
                Measurement::Algorithm(_) => {
                    assert_eq!(receipt.progress.hashed, ByteCount(3));
                    assert_eq!(
                        receipt.checks.measured(),
                        Some(Digest::parse(Algorithm::Sha256, ABC_SHA256).unwrap())
                    );
                }
            }
        }
    });
}

#[test]
fn every_required_algorithm_runs_even_when_measurement_is_disabled() {
    block_on(async {
        let sha = Digest::parse(Algorithm::Sha256, ABC_SHA256).unwrap();
        let wrong_md5 = Digest::parse(Algorithm::Md5, "d41d8cd98f00b204e9800998ecf8427e").unwrap();
        let mut required = Requirements::single(sha, Provenance::Caller);
        required.insert(wrong_md5, Provenance::Source).unwrap();
        let fixture = Fixture::body([Ok(Bytes::from_static(b"abc"))]);
        let mut body = CheckedBody::new(fixture, Length::Unknown, required, Measurement::None);
        assert!(body.next().await.unwrap().is_ok());
        let error = body.next().await.unwrap().unwrap_err();
        let BodyError::Mismatch(mismatch) = error.error else {
            panic!("expected required mismatch")
        };
        assert_eq!(
            mismatch.actual,
            Digest::parse(Algorithm::Md5, "900150983cd24fb0d6963f7d28e17f72").unwrap()
        );
        assert_eq!(
            mismatch
                .results
                .matched_requirements()
                .next()
                .unwrap()
                .digest,
            sha
        );
        assert!(matches!(body.finish(), Err(FinishError::Failed(_))));
    });
}

#[test]
fn short_and_overlong_bodies_error_before_clean_eof() {
    block_on(async {
        for expected in [2, 4] {
            let mut body = Fixture::body([Ok(Bytes::from_static(b"abc"))])
                .checked(Length::Known(ByteCount(expected)));
            if expected == 4 {
                assert!(body.next().await.unwrap().is_ok());
            }
            assert!(
                matches!(body.next().await, Some(Err(BodyFailure { error: BodyError::Length {
                expected: ByteCount(value), received: ByteCount(3),
            }, .. })) if value == expected)
            );
            assert!(body.next().await.is_none());
            assert!(matches!(body.finish(), Err(FinishError::Failed(_))));
        }
    });
}

#[test]
fn a_late_source_error_after_declared_length_cannot_be_hidden() {
    block_on(async {
        let mut body = Fixture::body([
            Ok(Bytes::from_static(b"abc")),
            Err(io::Error::other("late body error")),
        ])
        .checked(Length::Known(ByteCount(3)));
        assert!(body.next().await.unwrap().is_ok());
        assert!(matches!(
            body.next().await,
            Some(Err(BodyFailure {
                error: BodyError::Source(_),
                ..
            }))
        ));
        assert!(matches!(body.finish(), Err(FinishError::Failed(_))));
    });
}

#[test]
fn finish_refuses_unpolled_prefix_and_exact_declared_bytes_without_eof() {
    block_on(async {
        for consume in [false, true] {
            let mut body = Fixture::body([Ok(Bytes::from_static(b"abc"))])
                .checked(Length::Known(ByteCount(3)));
            if consume {
                assert!(body.next().await.unwrap().is_ok());
            }
            assert!(matches!(body.finish(), Err(FinishError::Incomplete(_))));
        }
    });
}

#[test]
fn overflow_is_terminal_without_releasing_the_offending_chunk() {
    block_on(async {
        let mut body = Fixture::body([Ok(Bytes::from_static(b"abc"))]).checked(Length::Unknown);
        // Direct private-state setup exercises a boundary without allocating u64::MAX bytes.
        body.progress.received = ByteCount(u64::MAX - 1);
        assert!(matches!(
            body.next().await,
            Some(Err(BodyFailure {
                error: BodyError::Overflow,
                ..
            }))
        ));
        assert_eq!(body.progress().released, ByteCount(0));
        assert!(matches!(body.finish(), Err(FinishError::Failed(_))));
    });
}

#[test]
fn large_chunks_are_hashed_in_bounded_polls_without_polling_the_source_again() {
    let fixture = Fixture::body([Ok(Bytes::from(vec![7; 131_073]))]);
    let polls = Rc::clone(&fixture.polls);
    let mut body = CheckedBody::new(
        fixture,
        Length::Unknown,
        Requirements::default(),
        Measurement::default(),
    );
    let mut cx = Context::from_waker(noop_waker_ref());
    assert!(Pin::new(&mut body).poll_next(&mut cx).is_pending());
    assert_eq!(body.progress().received, ByteCount(131_073));
    assert_eq!(body.progress().released, ByteCount(0));
    assert_eq!(body.progress().hashed, ByteCount(65_536));
    assert!(Pin::new(&mut body).poll_next(&mut cx).is_pending());
    assert_eq!(body.progress().hashed, ByteCount(131_072));
    assert!(matches!(
        Pin::new(&mut body).poll_next(&mut cx),
        Poll::Ready(Some(Ok(_)))
    ));
    assert_eq!(polls.get(), 1);
    assert_eq!(body.progress().released, ByteCount(131_073));
}

#[test]
fn empty_and_ready_tiny_sources_cooperate_without_read_ahead() {
    for bytes in [Bytes::new(), Bytes::from_static(b"x")] {
        let fixture = Fixture::body([Ok(Bytes::clone(&bytes)), Ok(bytes)]);
        let polls = Rc::clone(&fixture.polls);
        let mut body = fixture.checked(Length::Unknown);
        let mut cx = Context::from_waker(noop_waker_ref());
        let _ = Pin::new(&mut body).poll_next(&mut cx);
        assert_eq!(polls.get(), 1);
        assert!(Pin::new(&mut body).poll_next(&mut cx).is_pending());
        assert!(polls.get() <= 2);
    }
}

#[test]
fn a_borrowed_local_cause_keeps_its_value_and_rc_identity() {
    struct Cause<'a> {
        text: &'a str,
        identity: Rc<()>,
    }
    let text = String::from("borrowed cause");
    let identity = Rc::new(());
    let source = stream::iter([Err::<Bytes, _>(Cause {
        text: &text,
        identity: Rc::clone(&identity),
    })]);
    let mut body = CheckedBody::new(
        source,
        Length::Unknown,
        Requirements::default(),
        Measurement::None,
    );
    let error = block_on(body.next()).unwrap();
    match error {
        Err(BodyFailure {
            error: BodyError::Source(cause),
            ..
        }) => {
            assert!(core::ptr::eq(cause.text.as_ptr(), text.as_ptr()));
            assert!(Rc::ptr_eq(&cause.identity, &identity));
        }
        _ => panic!("expected retained borrowed source cause"),
    }
    assert!(matches!(body.finish(), Err(FinishError::Failed(_))));
}

#[test]
fn a_stack_borrowing_source_drains_to_a_borrowed_writer_without_a_runtime() {
    block_on(async {
        let payload = vec![b'a', b'b', b'c'];
        let source = stream::iter(
            payload
                .chunks(1)
                .map(|chunk| Ok::<_, io::Error>(Bytes::copy_from_slice(chunk))),
        );
        let body = CheckedBody::new(
            source,
            Length::Unknown,
            Requirements::default(),
            Measurement::None,
        );
        let mut writer = Writer::default();
        let receipt = body.drain(Pin::new(&mut writer)).await.unwrap();
        assert_eq!(receipt.accepted, ByteCount(3));
        assert_eq!(writer.bytes, payload);
    });
}

#[test]
fn a_static_rc_cause_is_preserved_in_the_standard_error_chain() {
    #[derive(Debug, thiserror::Error)]
    #[error("local source cause")]
    struct Cause(Rc<()>);
    let identity = Rc::new(());
    let source = stream::iter([Err::<Bytes, _>(Cause(Rc::clone(&identity)))]);
    let mut body = CheckedBody::new(
        source,
        Length::Unknown,
        Requirements::default(),
        Measurement::None,
    );
    let error = block_on(body.next()).unwrap().unwrap_err();
    let kind = core::error::Error::source(&error).unwrap();
    let cause = kind.source().unwrap().downcast_ref::<Cause>().unwrap();
    assert!(Rc::ptr_eq(&cause.0, &identity));
}

#[test]
fn a_borrowed_local_cause_also_survives_the_drain_error_family() {
    struct Cause<'a>(&'a str, Rc<()>);
    let text = String::from("borrowed");
    let identity = Rc::new(());
    let source = stream::iter([Err::<Bytes, _>(Cause(&text, Rc::clone(&identity)))]);
    let body = CheckedBody::new(
        source,
        Length::Unknown,
        Requirements::default(),
        Measurement::None,
    );
    let mut writer = Writer::default();
    let result = block_on(body.drain(Pin::new(&mut writer)));
    match result {
        Err(super::DrainFailure {
            error:
                DrainError::Body(BodyFailure {
                    error: BodyError::Source(cause),
                    ..
                }),
            accepted,
            ..
        }) => {
            assert!(core::ptr::eq(cause.0.as_ptr(), text.as_ptr()));
            assert!(Rc::ptr_eq(&cause.1, &identity));
            assert_eq!(accepted, ByteCount(0));
        }
        _ => panic!("expected retained borrowed drain cause"),
    }
    assert_eq!(writer.flushes, 0);
    assert_eq!(writer.shutdowns, 0);
}

#[test]
fn an_unpin_requirement_is_not_imposed_on_the_body() {
    block_on(async {
        let payload = Bytes::from_static(b"abc");
        let source = stream::once(async { Ok::<_, io::Error>(payload) });
        let mut body = CheckedBody::new(
            source,
            Length::Unknown,
            Requirements::default(),
            Measurement::None,
        );
        assert!(body.next().await.unwrap().is_ok());
        assert!(body.next().await.is_none());
        assert_eq!(body.finish().unwrap().progress.released, ByteCount(3));
    });
}

#[derive(Default)]
struct Writer {
    bytes: Vec<u8>,
    mode: WriterMode,
    flushes: usize,
    shutdowns: usize,
}

#[derive(Default)]
enum WriterMode {
    #[default]
    OneByte,
    PendingOnce,
    FailAfterOne,
    Zero,
    FlushError,
}

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match this.mode {
            WriterMode::PendingOnce => {
                this.mode = WriterMode::OneByte;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            WriterMode::FailAfterOne if !this.bytes.is_empty() => {
                return Poll::Ready(Err(io::Error::other("write failure")));
            }
            WriterMode::Zero => return Poll::Ready(Ok(0)),
            _ => {}
        }
        this.bytes.push(bytes[0]);
        Poll::Ready(Ok(1))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.flushes += 1;
        if matches!(this.mode, WriterMode::FlushError) {
            return Poll::Ready(Err(io::Error::other("flush failure")));
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().shutdowns += 1;
        Poll::Ready(Ok(()))
    }
}

#[test]
fn pending_one_byte_writer_applies_backpressure_without_shutdown() {
    let fixture = Fixture::body([Ok(Bytes::from_static(b"abc")), Ok(Bytes::from_static(b"d"))]);
    let polls = Rc::clone(&fixture.polls);
    let body = fixture.checked(Length::Unknown);
    let mut writer = Writer {
        mode: WriterMode::PendingOnce,
        ..Writer::default()
    };
    let mut drain = Box::pin(body.drain(Pin::new(&mut writer)));
    let mut cx = Context::from_waker(noop_waker_ref());
    assert!(drain.as_mut().poll(&mut cx).is_pending());
    assert_eq!(polls.get(), 1);
    assert!(drain.as_mut().poll(&mut cx).is_pending());
    assert_eq!(polls.get(), 1);
    let receipt = block_on(drain).unwrap();
    assert_eq!(receipt.accepted, ByteCount(4));
    assert_eq!(receipt.body.progress.released, ByteCount(4));
    assert_eq!(writer.bytes, b"abcd");
    assert_eq!(writer.flushes, 1);
    assert_eq!(writer.shutdowns, 0);
}

#[test]
fn partial_write_zero_and_flush_failures_retain_actual_accepted_counts() {
    for mode in [
        WriterMode::FailAfterOne,
        WriterMode::Zero,
        WriterMode::FlushError,
    ] {
        let body = Fixture::body([Ok(Bytes::from_static(b"abc"))]).checked(Length::Unknown);
        let mut writer = Writer {
            mode,
            ..Writer::default()
        };
        let failure = block_on(body.drain(Pin::new(&mut writer))).unwrap_err();
        match failure.error {
            DrainError::Write(_) => assert_eq!(failure.accepted, ByteCount(1)),
            DrainError::WriteZero => assert_eq!(failure.accepted, ByteCount(0)),
            DrainError::Flush(_) => assert_eq!(failure.accepted, ByteCount(3)),
            other => panic!("unexpected drain failure: {other:?}"),
        }
        assert_eq!(failure.progress.received, ByteCount(3));
        assert_eq!(writer.shutdowns, 0);
    }
}

#[test]
fn body_failure_prevents_clean_writer_flush_after_irreversible_prefix() {
    let body = Fixture::body([
        Ok(Bytes::from_static(b"abc")),
        Err(io::Error::other("late")),
    ])
    .checked(Length::Known(ByteCount(3)));
    let mut writer = Writer::default();
    let failure = block_on(body.drain(Pin::new(&mut writer))).unwrap_err();
    assert!(matches!(
        failure.error,
        DrainError::Body(BodyFailure {
            error: BodyError::Source(_),
            ..
        })
    ));
    assert_eq!(failure.accepted, ByteCount(3));
    assert_eq!(writer.bytes, b"abc");
    assert_eq!(writer.flushes, 0);
    assert_eq!(writer.shutdowns, 0);
}

#[test]
fn owned_native_components_produce_a_send_body_and_drain_future() {
    fn assert_send<T: Send>(_: &T) {}
    let source = stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"abc"))]);
    let body = CheckedBody::new(
        source,
        Length::Unknown,
        Requirements::default(),
        Measurement::default(),
    );
    assert_send(&body);
    let future = async move {
        let mut writer = Writer::default();
        body.drain(Pin::new(&mut writer)).await
    };
    assert_send(&future);
    assert_eq!(block_on(future).unwrap().accepted, ByteCount(3));
}
