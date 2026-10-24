//! Ordered live delivery. Released prefixes cannot be recalled after a terminal failure.

use core::pin::Pin;
use core::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;
use tokio::io;

use crate::checks::{Checks, Measurement, Mismatch, Outcome, Requirements};
use crate::transfer::{ByteCount, Length};

#[cfg(test)]
#[path = "checked_body_tests.rs"]
mod tests;

/// One consumed source, without erasing its body or imposing executor/lifetime bounds.
///
/// A chunk is hashed before release, but no prefix is expectation-verified. Only actual
/// clean source EOF, exact length and every required check permit clean API EOF.
/// After a terminal error the stream is exhausted, but [`Self::finish`] permanently fails.
/// Always check stream errors and consume `finish` before treating the body as successful.
///
/// Acquisition is demand-driven, retaining at most one source-provided chunk. Large
/// chunks are hashed over bounded cooperative polls without polling the source ahead.
/// No payload ceiling, hidden spool, source reopening or runtime is introduced. The
/// body may borrow or be local/non-Send; owned Send components remain Send.
#[must_use]
pub struct CheckedBody<B> {
    // One allocation permits arbitrary !Unpin sources without unsafe pin projection.
    source: Pin<Box<B>>,
    length: Length,
    checks: Option<Checks>,
    pending: Option<Bytes>,
    offset: usize,
    progress: Progress,
    state: State,
    turn: Turn,
}

impl<B> CheckedBody<B> {
    /// Freezes check policy before the first source poll. No bytes are acquired here.
    pub(crate) fn new(
        source: B,
        length: Length,
        requirements: Requirements,
        measurement: Measurement,
    ) -> Self {
        Self {
            source: Box::pin(source),
            length,
            checks: Some(requirements.start(measurement)),
            pending: None,
            offset: 0,
            progress: Progress::default(),
            state: State::Reading,
            turn: Turn::Read,
        }
    }

    /// Received, released and actually hashed bytes are deliberately separate facts.
    pub fn progress(&self) -> Progress {
        self.progress
    }

    /// Consumes the body; even consuming exactly the declared bytes is insufficient.
    pub fn finish(self) -> Result<Receipt, FinishError> {
        match self.state {
            State::Complete(checks) => Ok(Receipt {
                progress: self.progress,
                checks,
            }),
            State::Reading => Err(FinishError::Incomplete(self.progress)),
            State::Failed => Err(FinishError::Failed(self.progress)),
        }
    }

    fn fail<E>(&mut self, error: BodyError<E>) -> Poll<Option<Result<Bytes, BodyFailure<E>>>> {
        self.state = State::Failed;
        self.pending = None;
        self.checks = None;
        Poll::Ready(Some(Err(BodyFailure {
            error,
            progress: self.progress,
        })))
    }
}

impl<B, E> CheckedBody<B>
where
    B: Stream<Item = Result<Bytes, E>>,
{
    /// Drains into a borrowed, possibly pinned writer, counting accepted partial writes.
    ///
    /// Clean completion flushes but never shuts down the caller's writer. Cancellation
    /// cannot recall accepted bytes or promise settlement of arbitrary writer-owned I/O.
    pub async fn drain<W>(
        mut self,
        mut writer: Pin<&mut W>,
    ) -> Result<DrainReceipt, DrainFailure<E>>
    where
        W: io::AsyncWrite + ?Sized,
    {
        let mut accepted = ByteCount(0);
        loop {
            let next = futures::future::poll_fn(|cx| Pin::new(&mut self).poll_next(cx)).await;
            let chunk = match next {
                Some(Ok(chunk)) => chunk,
                Some(Err(error)) => {
                    return Err(DrainFailure {
                        error: DrainError::Body(error),
                        progress: self.progress,
                        accepted,
                    });
                }
                None => break,
            };
            let mut offset = 0;
            while offset < chunk.len() {
                let written =
                    futures::future::poll_fn(|cx| writer.as_mut().poll_write(cx, &chunk[offset..]))
                        .await
                        .map_err(|cause| DrainFailure {
                            error: DrainError::Write(cause),
                            progress: self.progress,
                            accepted,
                        })?;
                if written == 0 {
                    return Err(DrainFailure {
                        error: DrainError::WriteZero,
                        progress: self.progress,
                        accepted,
                    });
                }
                // AsyncWrite's contract bounds written by the supplied slice. Defend
                // against a broken implementation before slicing or accounting.
                if written > chunk.len() - offset {
                    return Err(DrainFailure {
                        error: DrainError::InvalidWriteCount,
                        progress: self.progress,
                        accepted,
                    });
                }
                offset += written;
                // Accepted bytes are a subset of released bytes, already checked for overflow.
                accepted.0 += written as u64;
                // Even an always-ready one-byte writer must relinquish the executor.
                Cooperate::default().await;
            }
        }
        let receipt = self.finish().map_err(|error| DrainFailure {
            progress: error.progress(),
            error: DrainError::Finish(error),
            accepted,
        })?;
        futures::future::poll_fn(|cx| writer.as_mut().poll_flush(cx))
            .await
            .map_err(|cause| DrainFailure {
                error: DrainError::Flush(cause),
                progress: receipt.progress,
                accepted,
            })?;
        Ok(DrainReceipt {
            body: receipt,
            accepted,
        })
    }
}

impl<B, E> Stream for CheckedBody<B>
where
    B: Stream<Item = Result<Bytes, E>>,
{
    type Item = Result<Bytes, BodyFailure<E>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.state {
            State::Complete(_) | State::Failed => return Poll::Ready(None),
            State::Reading => {}
        }
        if let Turn::Yield = this.turn {
            this.turn = Turn::Read;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        if this.pending.is_none() {
            match this.source.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(cause))) => return this.fail(BodyError::Source(cause)),
                Poll::Ready(Some(Ok(chunk))) => {
                    let Ok(length) = u64::try_from(chunk.len()) else {
                        return this.fail(BodyError::Overflow);
                    };
                    let Some(received) = this.progress.received.0.checked_add(length) else {
                        return this.fail(BodyError::Overflow);
                    };
                    this.progress.received = ByteCount(received);
                    if let Length::Known(expected) = this.length
                        && received > expected.0
                    {
                        return this.fail(BodyError::Length {
                            expected,
                            received: this.progress.received,
                        });
                    }
                    if chunk.is_empty() {
                        // Do not spin on an unbounded always-ready sequence of empty chunks.
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    this.pending = Some(chunk);
                    this.offset = 0;
                }
                Poll::Ready(None) => {
                    if let Length::Known(expected) = this.length
                        && this.progress.received != expected
                    {
                        return this.fail(BodyError::Length {
                            expected,
                            received: this.progress.received,
                        });
                    }
                    let Some(checks) = this.checks.take() else {
                        unreachable!("reading state owns unfinished checks");
                    };
                    match checks.finish() {
                        Ok(outcome) => {
                            this.state = State::Complete(outcome);
                            return Poll::Ready(None);
                        }
                        Err(mismatch) => return this.fail(BodyError::Mismatch(mismatch)),
                    }
                }
            }
        }
        let Some(chunk) = this.pending.as_ref() else {
            unreachable!("a nonterminal source chunk is retained until release");
        };
        let Some(checks) = this.checks.as_mut() else {
            unreachable!("reading state owns unfinished checks");
        };
        if checks.active() {
            // Five supported algorithms bound work per slice. This is a scheduling
            // quantum, not a payload ceiling or a source chunk-size requirement.
            const HASH_SLICE: usize = 64 * 1024;
            let end = this.offset + (chunk.len() - this.offset).min(HASH_SLICE);
            checks.update(&chunk[this.offset..end]);
            this.progress.hashed.0 += (end - this.offset) as u64;
            this.offset = end;
            if end != chunk.len() {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
        this.progress.released = this.progress.received;
        // Bound ready-stream work even when every provided chunk is tiny.
        this.turn = Turn::Yield;
        Poll::Ready(this.pending.take().map(Ok))
    }
}

/// Payload counters, not claims about sink acceptance or expectation verification.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Progress {
    /// Bytes admitted from the source, including a rejected overlong chunk.
    pub received: ByteCount,
    /// Bytes handed to the stream consumer, not necessarily accepted by its sink.
    pub released: ByteCount,
    /// Payload bytes fed to active checks, counted once across all algorithms.
    /// Zero when no requirement or measurement requested hashing.
    pub hashed: ByteCount,
}

/// Evidence available only after observed clean EOF and every required check.
///
/// This is live-body completion, not sink acceptance, publication or durability.
/// Private fields prevent callers from manufacturing completion evidence.
#[derive(Debug)]
pub struct Receipt {
    pub(crate) progress: Progress,
    pub(crate) checks: Outcome,
}

impl Receipt {
    /// Final body counters; released bytes remain irreversible.
    pub fn progress(&self) -> Progress {
        self.progress
    }

    /// Whole-payload measurement and matched requirements, not authenticity evidence.
    pub fn checks(&self) -> Outcome {
        self.checks
    }
}

enum Turn {
    Read,
    Yield,
}

// The fixed-size outcome stays inline rather than adding an allocation at clean EOF.
// Its variant binds completion and its evidence together without an optional side slot.
#[allow(clippy::large_enum_variant)]
enum State {
    Reading,
    Complete(Outcome),
    Failed,
}

/// A terminal stream error carries its own counters even if the consumer drops the body.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct BodyFailure<E> {
    /// Typed terminal error, retaining the concrete source cause when present.
    #[source]
    pub error: BodyError<E>,
    /// Snapshot at failure, not a completion receipt.
    pub progress: Progress,
}

/// The concrete source cause is retained by value, without Clone or static bounds.
/// Error/source chaining is available when E meets the standard Error contract;
/// borrowed causes remain directly matchable even when they cannot be downcast.
#[derive(Debug, thiserror::Error)]
pub enum BodyError<E> {
    /// Source acquisition failed, including a late error after all declared bytes.
    #[error("body source failed; inspect the retained cause")]
    Source(#[source] E),
    /// The received length differs from the declared exact length.
    #[error("body length differs from {expected:?}; inspect the received count")]
    Length {
        /// Exact length supplied at construction.
        expected: ByteCount,
        /// Admitted source bytes, including an offending overlong chunk.
        received: ByteCount,
    },
    /// The payload count cannot be represented; the offending chunk is not released.
    #[error("body byte count overflow; use a representable payload")]
    Overflow,
    /// At least one required check failed after actual clean source EOF.
    #[error("required body check failed; inspect the digest mismatch")]
    Mismatch(#[source] Box<Mismatch>),
}

/// A partial or failed stream cannot manufacture a completion receipt.
#[derive(Debug, thiserror::Error)]
pub enum FinishError {
    /// Consumption stopped before actual clean source EOF and final checks.
    #[error("body eof not observed; consume the stream to completion")]
    Incomplete(Progress),
    /// A prior stream error permanently disallows completion.
    #[error("body failed; inspect the terminal stream error")]
    Failed(Progress),
}

impl FinishError {
    /// Last observed counters, without a success claim.
    pub fn progress(&self) -> Progress {
        match self {
            Self::Incomplete(progress) | Self::Failed(progress) => *progress,
        }
    }
}

/// Completion of body checks and a successful flush, not writer shutdown or publication.
#[derive(Debug)]
pub struct DrainReceipt {
    pub(crate) body: Receipt,
    pub(crate) accepted: ByteCount,
}

impl DrainReceipt {
    /// Successful body EOF and checks; not publication or physical-I/O settlement evidence.
    pub fn body(&self) -> &Receipt {
        &self.body
    }

    /// Bytes accepted by the writer before its successful flush.
    pub fn accepted(&self) -> ByteCount {
        self.accepted
    }
}

/// Partial writer acceptance survives all terminal failures.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct DrainFailure<E> {
    /// Concrete body or writer failure, without erasing local or borrowed causes.
    #[source]
    pub error: DrainError<E>,
    /// Body counters at failure, independent of writer acceptance.
    pub progress: Progress,
    /// Confirmed successful write counts, including a prefix before a write failure.
    pub accepted: ByteCount,
}

/// Writer errors remain concrete I/O causes; body errors retain their source family.
#[derive(Debug, thiserror::Error)]
pub enum DrainError<E> {
    /// Source, length or required-check failure before clean completion.
    #[error("live body failed; inspect the terminal body error")]
    Body(#[source] BodyFailure<E>),
    /// A write failed after the reported successfully accepted prefix.
    #[error("writer failed; inspect the i/o cause")]
    Write(#[source] std::io::Error),
    /// The writer made no progress on a nonempty slice.
    #[error("writer accepted zero bytes; provide a writable sink")]
    WriteZero,
    /// A broken writer reported more bytes than the supplied slice.
    #[error("writer returned an invalid count; fix the async writer implementation")]
    InvalidWriteCount,
    /// Final flushing failed after clean body completion.
    #[error("writer flush failed; inspect the i/o cause")]
    Flush(#[source] std::io::Error),
    /// Completion evidence was unavailable; no flush was attempted.
    #[error("body completion unavailable; inspect the finish error")]
    Finish(#[source] FinishError),
}

#[derive(Default)]
enum Cooperate {
    #[default]
    Yield,
    Ready,
}

impl core::future::Future for Cooperate {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match *self {
            Self::Ready => Poll::Ready(()),
            Self::Yield => {
                *self = Self::Ready;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}
