//! libxget: a chunked, parallel, resumable, verified fetch engine.
//!
//! The engine plans a resource into chunks, fetches them in parallel, resumes a dropped chunk from its
//! offset, reassembles them in order, gates on the exact length, and streams the bytes through a hash
//! so a reported digest is trustworthy. It never certifies bytes it did not verify: a source that
//! ignores a range, or a length that does not match, is a typed [`Error`], not a silent bad file.
//!
//! The byte source is pluggable behind [`Source`]: HTTP range GET today, S3 or a theia/bifrost peer
//! tomorrow. The engine is written once and works over any source that can report a length and serve a
//! byte range; a source that cannot serve ranges is fetched as a single stream.

use core::time::Duration;

use bytes::Bytes;
use futures::stream::BoxStream;

mod checksum;
mod engine;
mod http;
mod plan;

#[cfg(test)]
mod http_tests;
#[cfg(test)]
mod plan_tests;

pub use crate::checksum::{Checksum, UnknownChecksum};
pub use crate::engine::{Report, download};
pub use crate::http::HttpSource;
pub use crate::plan::plan;

/// How a [`download`] is tuned: parallelism, retries, which checksum to verify with, and an optional
/// inactivity timeout.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Options {
    /// Maximum number of chunks fetched in parallel. A source that cannot serve ranges ignores this and
    /// is fetched as one stream.
    pub parts: u32,
    /// Retries for a dropped chunk, each resuming from the offset it reached.
    pub retries: u32,
    /// The checksum algorithm to verify the download with, or [`Checksum::None`] to skip it.
    pub checksum: Checksum,
    /// Fail a read that stalls for this long, so a retry can resume the chunk; `None` waits forever.
    pub timeout: Option<Duration>,
    /// Resume an interrupted download: keep the bytes already in `output`, fetch only what remains, and
    /// fold the existing prefix into the checksum by reading it back once (concurrently with the live
    /// fetch, so it never stalls the download). Requires a range-capable source.
    pub resume: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            parts: 5,
            retries: 10,
            checksum: Checksum::Sha256,
            timeout: None,
            resume: false,
        }
    }
}

/// A half-open byte range `[start, end)`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ByteRange {
    /// First byte offset, inclusive.
    pub start: u64,
    /// End offset, exclusive.
    pub end: u64,
}

impl ByteRange {
    /// The number of bytes in the range.
    pub const fn len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    /// Whether the range covers no bytes.
    pub const fn is_empty(&self) -> bool {
        self.start >= self.end
    }
}

/// What a [`Source`] reports about a resource before fetching.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Probe {
    /// Total length in bytes.
    pub length: u64,
    /// Whether the source can serve byte ranges, and so be fetched in parallel chunks. When false the
    /// engine fetches the whole resource as a single stream.
    pub supports_ranges: bool,
}

/// A stream of byte chunks from a source, or a fetch error.
pub type ByteStream = BoxStream<'static, Result<Bytes, Error>>;

/// A pluggable byte source: it reports a resource's size and serves byte ranges.
///
/// Implementations exist per protocol (HTTP, S3, a bifrost peer). The engine validates that a source
/// honored the exact range it was asked for, so a misbehaving source becomes an [`Error`], never a
/// silent corruption.
#[allow(async_fn_in_trait)]
pub trait Source {
    /// Probe the resource for its length and range support.
    async fn probe(&self) -> Result<Probe, Error>;

    /// Fetch the given byte range as a stream. The engine requires the returned bytes to cover exactly
    /// `range`; a source that serves a different range is rejected.
    async fn fetch(&self, range: ByteRange) -> Result<ByteStream, Error>;

    /// Fetch the whole resource as a single stream, used when [`Probe::supports_ranges`] is false and
    /// the resource cannot be split into parallel chunks.
    async fn fetch_all(&self) -> Result<ByteStream, Error>;
}

/// Reports download progress. The engine calls [`Progress::start`] once with the planned chunk sizes,
/// then [`Progress::advance`] as each chunk's bytes arrive, then [`Progress::finish`]. Every method
/// takes `&self` and defaults to nothing, so a caller opts in only to what it wants and passes `()` for
/// none. The engine drives these on a single task, so an implementation need not be `Sync`.
pub trait Progress {
    /// The planned chunk sizes, in order, before fetching begins.
    fn start(&self, _chunks: &[u64]) {}
    /// `bytes` more bytes arrived for chunk `index`.
    fn advance(&self, _index: usize, _bytes: u64) {}
    /// The download finished.
    fn finish(&self) {}
}

impl Progress for () {}

/// A fetch error. The variants name the failure modes that must never pass silently: a source that
/// ignores a range, a length that does not match, or a transport failure carried by its source.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The source did not serve the exact range requested (for example answered `200` with the whole
    /// body instead of `206` with a matching `Content-Range`).
    #[error("source did not honor the requested range {requested:?}")]
    RangeNotHonored {
        /// The range the engine asked for.
        requested: ByteRange,
    },
    /// The bytes received did not match the resource's declared length.
    #[error("length mismatch: expected {expected} bytes, received {received}")]
    LengthMismatch {
        /// The declared length.
        expected: u64,
        /// The length actually received.
        received: u64,
    },
    /// The underlying transport (HTTP, S3, overlay) failed.
    #[error("fetch failed")]
    Transport(#[source] BoxError),
}

/// A boxed underlying error, kept as the source of an [`Error`].
pub type BoxError = Box<dyn core::error::Error + Send + Sync + 'static>;
