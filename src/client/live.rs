use core::fmt;
use core::pin::Pin;
use core::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;

use super::http::{HttpBody, Opened};
use super::{Error, HttpError, VersionConsistency};
use crate::checks::Requirements;
use crate::inventory::{File, FileId};
use crate::transfer::{Digest, Length, Measurement, Provenance, live as body};

/// One consumed GET response and its metadata. Dropping it drops the body owner and releases admission.
/// No payload bytes are polled until its checked body is consumed.
#[must_use]
pub struct OpenedFile {
    opened: Opened,
    file: File,
    requirements: Requirements,
    measurement: Measurement,
}

impl OpenedFile {
    pub(super) fn new(file: File, opened: Opened) -> Self {
        let requirements = file.selected.description.requirements;
        Self {
            opened,
            file,
            requirements,
            measurement: Measurement::default(),
        }
    }

    /// Original selected member context, not a local output assignment.
    pub fn file(&self) -> &File {
        &self.file
    }

    /// Length declared by this GET, or a strong-version-bound HEAD when GET omits it.
    pub fn length(&self) -> Length {
        self.opened.length
    }

    /// Version continuity established before any body bytes are released.
    pub fn version(&self) -> VersionConsistency {
        self.opened.version
    }

    /// Adds a whole-payload requirement before consuming the body. Conflict drops this response.
    /// A requirement determines successful EOF, not permission to release an individual prefix.
    pub fn expect(mut self, required: impl Into<Digest>) -> Result<Self, Error> {
        self.requirements
            .insert(required.into(), Provenance::Caller)
            .map_err(Error::ConflictingRequirements)?;
        Ok(self)
    }

    /// Optional measurement policy, independent of required checks.
    pub fn measurement(mut self, measurement: Measurement) -> Self {
        self.measurement = measurement;
        self
    }

    /// Transfers single ownership into the common ordered checking implementation.
    /// No second GET, hidden file spool, writer or publication is involved.
    pub fn into_body(self) -> CheckedBody {
        let Self {
            opened,
            file,
            requirements,
            measurement,
        } = self;
        CheckedBody {
            inner: body::CheckedBody::new(opened.body, opened.length, requirements, measurement),
            file,
            version: opened.version,
        }
    }
}

impl fmt::Debug for OpenedFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenedFile")
            .field("length", &self.opened.length)
            .field("version", &self.opened.version)
            .finish()
    }
}

/// HTTP source context around the common checked live stream, not a second checking engine.
///
/// Stream failures and progress use the same public types as [`crate::transfer::live`]. Released
/// prefixes are irreversible. Actual source EOF, exact declared length and every required match
/// precede clean API EOF. Dropping early yields no success receipt and drops the HTTP body owner.
#[must_use]
pub struct CheckedBody {
    inner: body::CheckedBody<HttpBody>,
    file: File,
    version: VersionConsistency,
}

impl CheckedBody {
    /// Selected source context retained independently of any local output or publication.
    pub fn file(&self) -> &File {
        &self.file
    }

    /// Common received, released and actually hashed counters, without reinterpretation.
    pub fn progress(&self) -> body::Progress {
        self.inner.progress()
    }

    /// Consumes the body after clean EOF, preserving HTTP context around the common receipt.
    /// Neither this context nor the underlying body receipt claims sink commit or publication.
    pub fn finish(self) -> Result<LiveReceipt, body::FinishError> {
        let Self {
            inner,
            file,
            version,
        } = self;
        let receipt = inner.finish()?;
        Ok(LiveReceipt {
            file_id: FileId::clone(file.id()),
            receipt,
            version,
        })
    }
}

impl Stream for CheckedBody {
    type Item = Result<Bytes, body::BodyFailure<HttpError>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().inner).poll_next(cx)
    }
}

impl fmt::Debug for CheckedBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CheckedBody")
            .field("progress", &self.progress())
            .field("version", &self.version)
            .finish()
    }
}

/// HTTP occurrence/version context for common streamed completion, never native publication.
#[derive(Debug)]
pub struct LiveReceipt {
    file_id: FileId,
    receipt: body::Receipt,
    version: VersionConsistency,
}

impl LiveReceipt {
    /// Occurrence-local correlation, not a repeatable CLI selector.
    pub fn file_id(&self) -> &FileId {
        &self.file_id
    }

    /// The unchanged common EOF/check receipt; there is no duplicate check or progress record.
    pub fn body(&self) -> &body::Receipt {
        &self.receipt
    }

    /// GET version consistency, separate from whole-payload content checks.
    pub fn version(&self) -> VersionConsistency {
        self.version
    }
}
