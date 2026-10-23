//! Concrete dispatch for the two owning built-in bodies, not an external source framework.

use core::pin::Pin;
use core::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;

use super::VersionConsistency;
use super::http::{self, HttpBody, HttpError};
use crate::transfer::Length;

pub(super) struct Opened {
    pub(super) body: Body,
    pub(super) length: Length,
    pub(super) version: VersionConsistency,
}

impl From<http::Opened> for Opened {
    fn from(opened: http::Opened) -> Self {
        Self {
            body: Body::Http(opened.body),
            length: opened.length,
            version: opened.version,
        }
    }
}

pub(super) enum Body {
    Http(HttpBody),
    #[cfg(feature = "s3")]
    S3(super::s3::S3Body),
}

impl Stream for Body {
    type Item = Result<Bytes, SourceError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            Self::Http(body) => Pin::new(body)
                .poll_next(cx)
                .map(|item| item.map(|result| result.map_err(SourceError::Http))),
            #[cfg(feature = "s3")]
            Self::S3(body) => Pin::new(body)
                .poll_next(cx)
                .map(|item| item.map(|result| result.map_err(SourceError::S3))),
        }
    }
}

/// Concrete built-in ordered-body failure. HTTP reader errors retain their separate HTTP family.
/// Foreign SDK and HTTP diagnostics remain behind their transport's redaction boundary.
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    /// A consumed HTTP-backed response failed.
    #[error("http body failed; inspect the safe transport category")]
    Http(#[source] HttpError),
    /// A consumed S3 SDK response failed.
    #[cfg(feature = "s3")]
    #[error("s3 body failed; inspect the safe transport category")]
    S3(#[source] super::s3::S3Error),
}
