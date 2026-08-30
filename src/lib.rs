//! xget: a chunked, parallel, resumable, verified fetch engine.
//!
//! The engine plans a resource into chunks, fetches them in parallel, resumes a dropped chunk from its
//! offset, reassembles them in order, gates on the exact length, and streams the bytes through a hash
//! so a reported digest is trustworthy. It never certifies bytes it did not verify: a source that
//! ignores a range, or a length that does not match, is a typed [`Error`], not a silent bad file.
//!
//! The byte source is pluggable behind [`Source`]: HTTP range GET today, S3 or a theia/bifrost peer
//! tomorrow. The engine is written once and works over any source that can report a length and serve a
//! byte range; a source that cannot serve ranges is fetched as a single stream.
//!
//! Where things live: this root holds the public vocabulary ([`Source`], [`Progress`], [`Options`],
//! [`Output`], [`Probe`], [`Error`]) that everything else speaks. The `source` module holds the shipped
//! sources ([`HttpSource`] is the reference one); `engine` is the machine that drives any source; and
//! `plan`, `control`, and `checksum` are the parts it leans on (chunk planning, the `.xget` resume
//! trailer, and the hashing). A good reading order is this doc, then [`Source`] and [`HttpSource`], then
//! the engine.

use core::time::Duration;

use bytes::Bytes;
use futures::stream::BoxStream;

mod builder;
mod checksum;
mod control;
mod engine;
mod plan;
mod source;

#[cfg(test)]
mod plan_tests;

pub use crate::builder::{Download, from, get};
pub use crate::checksum::{Checksum, UnknownChecksum};
pub use crate::engine::Report;

/// Whether an interrupted download left a resumable partial beside `output`: its `.xget` file with a
/// valid control trailer. A caller can use this to resume automatically without an explicit request. The
/// naming and format of the partial are the engine's to own, so a caller need not know them.
pub async fn resumable(output: &std::path::Path) -> bool {
    crate::control::is_resumable(&crate::engine::part_path(output)).await
}

/// The source URL recorded inside a `.xget` control file, if `control_path` is a valid control that
/// stored one. This lets a caller resume from the partial alone, with no URL given again: read the URL,
/// rebuild the source from it, and finish the download. `control_path` is the `.xget` file itself, not
/// the output it belongs to.
pub async fn control_source(control_path: &std::path::Path) -> Option<String> {
    crate::control::read(control_path)
        .await
        .and_then(|control| control.source)
}

/// A read-only summary of a `.xget` control file, for inspecting a partial without touching the network.
#[derive(Clone, Debug)]
pub struct Inspection {
    /// The source URL recorded in the control, if the source had a re-openable one.
    pub source: Option<String>,
    /// The resource's total length in bytes.
    pub total: u64,
    /// Bytes already on disk: the union of the ranges recorded as written (overlaps counted once).
    pub downloaded: u64,
    /// The resource validator (an `ETag`/`Last-Modified`/CID), if one was recorded.
    pub validator: Option<String>,
    /// The checksum algorithm the download was verifying with, if the control recorded one.
    pub checksum: Option<Checksum>,
}

/// Summarize a `.xget` control file, or `None` if `control_path` is not a valid control. Offline: it
/// reads only the local file and never probes the network, so it can identify a stray partial. Powers
/// `xget --info`.
pub async fn inspect(control_path: &std::path::Path) -> Option<Inspection> {
    let control = crate::control::read(control_path).await?;
    Some(Inspection {
        source: control.source,
        total: control.total,
        downloaded: union_len(&control.done),
        validator: control.validator,
        checksum: control.checksum,
    })
}

/// The number of distinct bytes covered by `ranges`, merging any overlaps so a byte recorded twice (a
/// checkpoint then a completion) is counted once.
fn union_len(ranges: &[ByteRange]) -> u64 {
    let mut sorted: Vec<ByteRange> = ranges.iter().copied().filter(|r| !r.is_empty()).collect();
    sorted.sort_by_key(|range| range.start);
    let mut total = 0u64;
    let mut covered_to = 0u64;
    for range in sorted {
        let start = range.start.max(covered_to);
        if range.end > start {
            total += range.end - start;
        }
        covered_to = covered_to.max(range.end);
    }
    total
}

pub use crate::plan::plan;
#[cfg(feature = "ipfs")]
pub use crate::source::IpfsSource;
#[cfg(feature = "s3")]
pub use crate::source::S3Source;
pub use crate::source::{HttpSource, Mirrors};

/// How a download is tuned: parallelism, retries, which checksum to verify with, and an optional
/// inactivity timeout. Internal: the [`Download`] builder is the public way to set these.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct Options {
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
    /// fold the existing prefix into the checksum during the in-order verify pass. Requires a
    /// range-capable source.
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

/// Where a download's verified bytes go.
///
/// A [`download`] scatters and verifies into a seekable scratch, then finalizes to the chosen sink. A
/// sink with a file ([`Output::File`] or [`Output::Tee`]) leaves a persistent `.xget` and so is
/// resumable; a lone [`Output::Writer`] or an [`Output::Discard`] has nothing to come back to and runs
/// fresh.
///
/// A sink is at heart a writer; the one thing a file adds is a persistent, resumable artifact. So there
/// is exactly one composition worth having, [`Output::Tee`]: keep a resumable file and pass the same
/// verified bytes to one writer. Fanning out to more files would just be a copy, and fanning out to more
/// writers is the receiving writer's own job, so neither is a sink the engine needs to model.
pub enum Output<'a> {
    /// Persist to a file: scatter into `<path>.xget`, atomic-rename on success. Resumable.
    File(&'a std::path::Path),
    /// Stream verified bytes to a writer (stdout, a socket, a child's stdin). Not resumable.
    Writer(&'a mut (dyn tokio::io::AsyncWrite + Unpin)),
    /// Verify and keep nothing (a speed test, or /dev/null). Not resumable.
    Discard,
    /// Persist to a resumable file and hand the same verified bytes to a writer: the file is finalized by
    /// rename, the writer receives the verified image. Build it with [`Output::tee`]. Resumable, via the
    /// file.
    Tee {
        /// The resumable file, finalized by atomic rename on success.
        file: &'a std::path::Path,
        /// A writer that receives the verified bytes.
        writer: &'a mut (dyn tokio::io::AsyncWrite + Unpin),
    },
}

impl<'a> Output<'a> {
    /// Persist the download to `path`, scattering into `<path>.xget` and atomic-renaming on success.
    pub fn file(path: &'a std::path::Path) -> Self {
        Output::File(path)
    }

    /// Stream the download's verified bytes to `writer` as they are confirmed.
    pub fn writer(writer: &'a mut (dyn tokio::io::AsyncWrite + Unpin)) -> Self {
        Output::Writer(writer)
    }

    /// Persist to a resumable `file` and also hand the verified bytes to `writer`: keep a copy and pipe
    /// it onward at once.
    pub fn tee(
        file: &'a std::path::Path,
        writer: &'a mut (dyn tokio::io::AsyncWrite + Unpin),
    ) -> Self {
        Output::Tee { file, writer }
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
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Probe {
    /// Total length in bytes.
    pub length: u64,
    /// Whether the source can serve byte ranges, and so be fetched in parallel chunks. When false the
    /// engine fetches the whole resource as a single stream.
    pub supports_ranges: bool,
    /// A filename the source suggests for the resource (from an HTTP `Content-Disposition`), if any and
    /// after stripping any path components. A caller may use it to name the output.
    pub filename: Option<String>,
    /// The resource's media type (from an HTTP `Content-Type`), if the source reports one.
    pub content_type: Option<String>,
    /// A validator identifying this exact version of the resource: an HTTP `ETag` when offered, else
    /// `Last-Modified`, else a source's own immutable identity (an IPFS CID). Recorded when a partial is
    /// written and compared on resume, so a partial from a since-changed resource is discarded rather
    /// than stitched. `None` when the source offers nothing to identify the version by, in which case
    /// resume falls back to the length alone.
    pub validator: Option<String>,
    /// A checksum the source vouches for, as an algorithm and lowercase hex digest (e.g. an S3 stored
    /// checksum). When present, a download can be verified against it with no hash supplied out of band.
    pub checksum: Option<(Checksum, String)>,
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

    /// Fetch a byte range as a stream, or the whole resource when `range` is `None`.
    ///
    /// A `Some(range)` fetch must be honored exactly: the engine rejects a source that serves different
    /// bytes, which is what makes a parallel chunked download trustworthy. A `None` fetch is the whole
    /// resource in one stream, used when [`Probe::supports_ranges`] is false.
    async fn fetch(&self, range: Option<ByteRange>) -> Result<ByteStream, Error>;

    /// A re-openable reference to this resource (its URL, e.g. `https://…`, `s3://…`, `ipfs://…`), if the
    /// source has one. The engine records it in the resume control file, so a later run can rebuild the
    /// source from the `.xget` alone and finish the download without the URL being given again. Defaults
    /// to `None`, for a source that cannot be named by a string; the engine then records nothing.
    fn identity(&self) -> Option<String> {
        None
    }
}

/// Reports download progress. The engine calls [`Progress::start`] once with the planned chunk sizes,
/// then, as bytes flow through the two-stage pipeline, [`Progress::received`] when a chunk's bytes
/// arrive from the source and [`Progress::wrote`] when they are written and hashed in order, then
/// [`Progress::finish`]. The two stages let a display shade bytes received (buffered ahead) apart from
/// bytes confirmed. Every method takes `&self` and defaults to nothing, so a caller opts in only to
/// what it wants and passes `()` for none. The engine drives these on a single task, so an
/// implementation need not be `Sync`.
pub trait Progress {
    /// The planned chunk sizes, in order, before fetching begins.
    fn start(&self, _chunks: &[u64]) {}
    /// Bytes already present per chunk before fetching begins, in plan order: a resumed download's
    /// chunks already on disk from an earlier run. A reporter shades these as downloaded-but-not-yet
    /// -verified, so the bar opens where the previous run left off and the verify pass sweeps the
    /// confirmed frontier up through them. These bytes did not arrive over the network this run, so they
    /// are not [`Progress::received`] and never enter a speed estimate. Defaults to nothing.
    fn restore(&self, _present: &[u64]) {}
    /// `bytes` more bytes arrived from the source for chunk `index`, buffered ahead of writing.
    fn received(&self, _index: usize, _bytes: u64) {}
    /// `bytes` more bytes of chunk `index` were written to the output and folded into the hash.
    fn wrote(&self, _index: usize, _bytes: u64) {}
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
