//! Exact HTTP sessions feed the existing bounded reader coordinator, never a second scheduler.

use core::pin::Pin;
use core::task::{Context, Poll};
use std::path::Path;

use bytes::Bytes;
use futures::Stream;
use futures::future::BoxFuture;

use super::Error;
use super::http::{Http, HttpBody};
use super::saved::Owned;
use crate::checks::Requirements;
use crate::input::{InputKind, RangeSupport};
use crate::inventory::File;
use crate::transfer::reader::{Evidence, RangeReader, ReadResponse, ReaderReceipt, Window};
use crate::transfer::{ByteCount, Length, Measurement, Transfer};

/// Frozen execution choice. Old aggregate records always mean Whole, including when rebound to
/// an endpoint that now advertises ranges. No digest or fresh payload probe grants range capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Mode {
    Whole,
    Ranges,
}

impl Mode {
    pub(super) fn of(file: &File) -> Self {
        if file.input.kind() == InputKind::Http
            && file.input.ranges == RangeSupport::Bytes
            && Self::supports(
                file.length(),
                file.selected
                    .description
                    .etag
                    .as_deref()
                    .map(String::as_str),
            )
        {
            Self::Ranges
        } else {
            Self::Whole
        }
    }

    pub(super) fn supports(length: Length, etag: Option<&str>) -> bool {
        // The reader's safe session token has a stricter bound than advisory HTTP headers. Long
        // valid pins retain whole-body semantics rather than truncating or inventing reader identity.
        matches!(length, Length::Known(ByteCount(size)) if size > 0)
            && etag.is_some_and(|etag| etag.len() <= 256)
    }
}

// The one-task pipeline keeps its borrowed observation separate from native operation ownership.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run<O: crate::transfer::observation::Observer>(
    http: &Http,
    file: &File,
    destination: &Path,
    requirements: Requirements,
    measurement: Measurement,
    owned: &mut Owned,
    index: usize,
    authority: super::Authority,
    targets: std::sync::Arc<[std::path::PathBuf]>,
    observer: &mut O,
) -> Result<ReaderReceipt, Error> {
    let Length::Known(size) = file.length() else {
        return Err(Error::InvalidMetadata);
    };
    let etag = file
        .selected
        .description
        .etag
        .as_deref()
        .ok_or(Error::InvalidMetadata)?;
    let reader = Reader {
        http,
        file,
        size,
        etag,
    };
    let mut transfer = Transfer::from_reader(&reader).measurement(measurement);
    for requirement in requirements.iter() {
        for provenance in requirement.origins.iter() {
            transfer = transfer.require(requirement.digest, provenance);
        }
    }
    let restart = match authority.attempt {
        super::AttemptPolicy::Continue => None,
        super::AttemptPolicy::Restart => owned.restart_state(index)?,
    };
    if matches!(authority.attempt, super::AttemptPolicy::Continue)
        && let Some(state) = owned.reader_state(index)
    {
        // Only native resume authority can decide whether this locator still describes an owned,
        // anchored receiving prefix. A refusal never falls back to refetch or a writable sealed file.
        transfer = transfer.resume_from(state);
    }
    let pending = transfer
        .to_file(destination)
        .map_err(Error::RangeRequest)?
        .authorized(authority.publication, restart)
        .protect_targets(targets);
    match authority.attempt {
        super::AttemptPolicy::Continue => owned.submit_reader(index, pending.state()).await?,
        super::AttemptPolicy::Restart => owned.submit(index, pending.state()).await?,
    }
    match pending
        .run_registered_observed(Some(owned.registration()), observer)
        .await
    {
        Ok(receipt) => Ok(receipt),
        Err(transfer) => {
            if let Err(checkpoint) = owned.retained(index).await {
                return Err(Error::RangeCheckpoint {
                    transfer,
                    checkpoint,
                });
            }
            Err(Error::RangeTransfer(transfer))
        }
    }
}

struct Reader<'session> {
    http: &'session Http,
    file: &'session File,
    size: ByteCount,
    etag: &'session str,
}

impl<'read, 'session: 'read> RangeReader<'read> for Reader<'session> {
    type Error = Error;
    type Body = Body;
    // Reqwest's opening future is opaque. Erasure stays inside this built-in transport adapter;
    // the engine still receives one borrow's named body/error/future families.
    type Read = BoxFuture<'read, Result<ReadResponse<'read, Self::Body>, Self::Error>>;

    fn size(&self) -> ByteCount {
        self.size
    }

    fn evidence(&'read self) -> Evidence<'read> {
        Evidence::Enforced {
            representation: "http identity bytes",
            version: self.etag.as_bytes(),
        }
    }

    fn read(&'read self, window: Window) -> Self::Read {
        Box::pin(async move {
            let body = self
                .http
                .open_range(self.file, window, self.size, self.etag)
                .await?;
            Ok(ReadResponse {
                window,
                evidence: self.evidence(),
                body: Body(body),
            })
        })
    }
}

struct Body(HttpBody);

impl Stream for Body {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().0)
            .poll_next(cx)
            .map(|item| item.map(|result| result.map_err(Error::Http)))
    }
}

#[cfg(all(test, unix))]
#[path = "range_tests.rs"]
mod tests;
