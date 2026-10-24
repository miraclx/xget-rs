//! Ordered live delivery from an already-open body, without native file publication.
//!
//! [`Transfer::into_body`] hands the checked stream to an application-owned sink.
//! [`Transfer::stream_to`] drains it to a borrowed Tokio `AsyncWrite` implementation.
//! Neither entrance creates a runtime, spawns a pump, reopens the source, buffers the
//! entire payload, or shuts down the caller's writer. Neither requires a Tokio runtime
//! when the supplied body and writer do not require one.
//!
//! Live prefixes are irreversible. A prefix being hashed does not make it
//! expectation-verified: a late source error, length failure or required digest
//! mismatch can still fail the transfer. Successful completion requires actual clean
//! source EOF and every required match. A writer transfer additionally requires its
//! final flush. There is no rollback, file publication or power-loss durability claim.
//!
//! # Borrow an application writer
//!
//! ```
//! use bytes::Bytes;
//! use futures::{executor::block_on, stream};
//! use xget::Transfer;
//! use xget::transfer::{ByteCount, Digest, Length, Measurement};
//!
//! # fn main() -> Result<(), Box<dyn core::error::Error>> {
//! let expected: Digest =
//!     "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".parse()?;
//! let source = stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"abc"))]);
//! let mut writer = Vec::new();
//! let pending = Transfer::from_body(source, Length::Known(ByteCount(3)))
//!     .expect(expected)
//!     .measurement(Measurement::None)
//!     .stream_to(&mut writer)?;
//! let receipt = block_on(pending.run())?;
//! assert_eq!(receipt.accepted(), ByteCount(3));
//! assert_eq!(receipt.body().checks().matched_requirements().count(), 1);
//! assert_eq!(writer, b"abc");
//! # Ok(())
//! # }
//! ```
//!
//! # Own the consumption loop
//!
//! ```
//! use bytes::Bytes;
//! use futures::{executor::block_on, stream, StreamExt as _};
//! use xget::Transfer;
//! use xget::transfer::{ByteCount, Length};
//!
//! # fn main() -> Result<(), Box<dyn core::error::Error>> {
//! let source = stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"abc"))]);
//! let mut body = Transfer::from_body(source, Length::Unknown).into_body()?;
//! let receipt = block_on(async {
//!     let mut output = Vec::new();
//!     while let Some(chunk) = body.next().await {
//!         output.extend_from_slice(&chunk?);
//!     }
//!     // This consumes the body and refuses partial consumption or any prior failure.
//!     let receipt = body.finish()?;
//!     Ok::<_, Box<dyn core::error::Error>>((output, receipt))
//! })?;
//! assert_eq!(receipt.0, b"abc");
//! assert_eq!(receipt.1.progress().released, ByteCount(3));
//! # Ok(())
//! # }
//! ```
//!
//! # Ownership boundaries
//!
//! A borrowed writer cannot escape its owner's lifetime:
//!
//! ```compile_fail
//! use core::future::Future;
//! use bytes::Bytes;
//! use futures::stream;
//! use xget::Transfer;
//! use xget::transfer::Length;
//! use xget::transfer::live::{DrainFailure, DrainReceipt};
//!
//! fn escape_writer() -> impl Future<Output = Result<DrainReceipt, DrainFailure<std::io::Error>>> {
//!     let mut writer = Vec::new();
//!     let source = stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"abc"))]);
//!     Transfer::from_body(source, Length::Unknown).stream_to(&mut writer).unwrap().run()
//! }
//! ```
//!
//! Owning the checked wrapper does not extend a borrowed source's lifetime:
//!
//! ```compile_fail
//! use bytes::Bytes;
//! use futures::{stream, Stream};
//! use xget::Transfer;
//! use xget::transfer::Length;
//! use xget::transfer::live::CheckedBody;
//!
//! fn escape_body() -> CheckedBody<impl Stream<Item = Result<Bytes, std::io::Error>>> {
//!     let payload = vec![b'a', b'b', b'c'];
//!     let source = stream::iter(payload.chunks(1).map(|chunk| {
//!         Ok::<_, std::io::Error>(Bytes::copy_from_slice(chunk))
//!     }));
//!     Transfer::from_body(source, Length::Unknown).into_body().unwrap()
//! }
//! ```
//!
//! Local components are supported, not silently made Send:
//!
//! ```compile_fail
//! use core::future::Future;
//! use std::rc::Rc;
//! use bytes::Bytes;
//! use futures::stream;
//! use xget::Transfer;
//! use xget::transfer::Length;
//!
//! fn require_send(_: impl Future + Send) {}
//! let source = stream::iter([Err::<Bytes, _>(Rc::new(()))]);
//! let future = async move {
//!     let mut writer = Vec::new();
//!     let _ = Transfer::from_body(source, Length::Unknown)
//!         .stream_to(&mut writer).unwrap().run().await;
//! };
//! require_send(future);
//! ```
//!
//! These are advanced `Transfer` entrances, not an ordinary Client implementation.
//! Whole-body requirements and optional measurement use the same check engine as
//! native publication. Provenance is not source authenticity or version continuity.

use core::pin::Pin;

use bytes::Bytes;
use futures::Stream;
use tokio::io;

use super::{RequestError, Transfer};
pub use crate::checked_body::{
    BodyError, BodyFailure, CheckedBody, DrainError, DrainFailure, DrainReceipt, FinishError,
    Progress, Receipt,
};

impl<B> Transfer<B> {
    /// Consume this transfer into a demand-driven checked live stream.
    ///
    /// Conflicting requirements fail before source polling. Required checks run even
    /// with optional measurement disabled. Whole-payload failures are emitted before
    /// clean API EOF, not before each irreversible prefix. Consume
    /// [`CheckedBody::finish`] after the loop to obtain completion evidence; merely
    /// consuming the declared byte count is insufficient.
    ///
    /// No `Send`, `Sync`, `Unpin`, static lifetime or runtime is required of the body.
    pub fn into_body(self) -> Result<CheckedBody<B>, RequestError> {
        let Self {
            body,
            length,
            requirements,
            measurement,
        } = self;
        let requirements = requirements.map_err(RequestError::ConflictingRequirements)?;
        Ok(CheckedBody::new(body, length, requirements, measurement))
    }

    /// Plan live delivery into a borrowed writer without polling or writing anything.
    ///
    /// [`PendingWriter::run`] applies writer backpressure, counts partial accepted
    /// writes and flushes only after clean body completion. It never calls shutdown.
    /// Conflicting requirements fail here, before either body or writer is polled.
    /// For a writer that is not `Unpin`, use [`Self::stream_to_pinned`].
    pub fn stream_to<W>(self, writer: &mut W) -> Result<PendingWriter<'_, B, W>, RequestError>
    where
        W: io::AsyncWrite + Unpin + ?Sized,
    {
        self.stream_to_pinned(Pin::new(writer))
    }

    /// Plan live delivery into an already-pinned borrowed writer.
    ///
    /// This is the same live operation as [`Self::stream_to`], without requiring
    /// `Unpin`. The writer remains caller-owned and is never shut down by this operation.
    pub fn stream_to_pinned<W>(
        self,
        writer: Pin<&mut W>,
    ) -> Result<PendingWriter<'_, B, W>, RequestError>
    where
        W: io::AsyncWrite + ?Sized,
    {
        Ok(PendingWriter {
            body: self.into_body()?,
            writer,
        })
    }
}

/// One consumed checked body and one borrowed writer, with no background pump.
///
/// This value and its execution future preserve concrete body/error/writer families.
/// Local and borrowed components are supported; owned Send components and a Send
/// writer borrow yield a Send future. No executor, spawn or universal static bound
/// is introduced. Dropping before `run` performs no source polling or writer calls.
#[must_use]
pub struct PendingWriter<'a, B, W: ?Sized> {
    body: CheckedBody<B>,
    writer: Pin<&'a mut W>,
}

impl<B, W> PendingWriter<'_, B, W>
where
    W: io::AsyncWrite + ?Sized,
{
    /// Deliver ordered bytes, require actual clean body EOF/checks, then flush.
    ///
    /// Body failures occur before a clean completion/flush. Successful partial
    /// writes are counted even when a later write or flush fails. No source poll
    /// occurs while writer acceptance for the preceding chunk is pending.
    ///
    /// Dropping this future stops further polling. Bytes already accepted remain
    /// irreversible; arbitrary caller-writer physical I/O is not promised settled
    /// on Drop. The caller owns writer shutdown, sink commit and durability policy.
    pub async fn run<E>(self) -> Result<DrainReceipt, DrainFailure<E>>
    where
        B: Stream<Item = Result<Bytes, E>>,
    {
        self.body.drain(self.writer).await
    }
}
