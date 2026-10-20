use core::fmt;
use core::num::NonZeroUsize;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use futures::Stream;
use futures::stream::BoxStream;
use reqwest::header::{
    ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, ETAG, IF_MATCH,
    TRANSFER_ENCODING,
};
use reqwest::{Method, Response};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use zeroize::Zeroizing;

use super::{Error, VersionConsistency};
use crate::checks::Requirements;
use crate::input::Input;
use crate::inventory::{Description, File};
use crate::transfer::{ByteCount, Length};

const HEADERS_DEADLINE: Duration = Duration::from_secs(15);
const MAX_HEADERS: usize = 64 * 1024;
const MAX_PATH: usize = 4096;

pub(super) struct Http {
    client: reqwest::Client,
    admission: Arc<Semaphore>,
}

impl Http {
    pub(super) fn new(maximum: NonZeroUsize) -> Result<Self, Error> {
        if maximum.get() > Semaphore::MAX_PERMITS {
            return Err(Error::AdmissionLimit);
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .http1_only()
            .connect_timeout(HEADERS_DEADLINE)
            .build()
            .map_err(HttpError::new)?;
        Ok(Self {
            client,
            admission: Arc::new(Semaphore::new(maximum.get())),
        })
    }

    pub(super) fn runtime() -> Result<(), Error> {
        tokio::runtime::Handle::try_current().map_err(|_| Error::RuntimeUnavailable)?;
        Ok(())
    }

    pub(super) async fn describe(&self, input: &Input) -> Result<Description, Error> {
        let path = input.parsed_reference().path();
        if path.len() > MAX_PATH {
            return Err(Error::Inventory(crate::inventory::InventoryError::Limit));
        }
        let source_path = Zeroizing::new(path.strip_prefix('/').unwrap_or(path).to_owned());
        // Invalid/missing hints need not prevent an explicit local file target. Only assignment
        // turns this untrusted candidate into a validated local name.
        let name = path
            .rsplit('/')
            .next()
            .filter(|name| !name.is_empty())
            .and_then(|name| {
                percent_encoding::percent_decode_str(name)
                    .decode_utf8()
                    .ok()
            })
            .map(|name| Zeroizing::new(name.into_owned()));
        let admitted = self.headers(input, Method::HEAD, None).await?;
        let response = &admitted.response;
        let status = response.status();
        if matches!(status.as_u16(), 405 | 501) {
            drop(admitted);
            return Ok(Description {
                source_path,
                name,
                length: Length::Unknown,
                etag: None,
                requirements: Requirements::default(),
            });
        }
        Self::status(response, RequestStage::Metadata)?;
        Self::validate_headers(response)?;
        let length = Self::length(response)?;
        let etag = Self::strong_etag(response)?;
        drop(admitted);
        Ok(Description {
            source_path,
            name,
            length,
            etag,
            requirements: Requirements::default(),
        })
    }

    pub(super) async fn open(&self, file: &File) -> Result<Opened, Error> {
        let description = &file.selected.description;
        let admitted = self
            .headers(
                &file.input,
                Method::GET,
                description.etag.as_deref().map(String::as_str),
            )
            .await?;
        let response = &admitted.response;
        if response.status().as_u16() == 412 && description.etag.is_some() {
            return Err(Error::VersionChanged);
        }
        Self::status(response, RequestStage::Payload)?;
        Self::validate_headers(response)?;
        if response.headers().contains_key(CONTENT_RANGE) {
            return Err(Error::InvalidMetadata);
        }
        let mut length = Self::length(response)?;
        let version = if let Some(expected) = &description.etag {
            let actual = Self::strong_etag(response)?;
            if actual.as_ref().map(|value| value.as_str()) != Some(expected.as_str()) {
                return Err(Error::VersionChanged);
            }
            if let Length::Known(expected_length) = description.length {
                match length {
                    Length::Known(actual) if actual != expected_length => {
                        return Err(Error::VersionChanged);
                    }
                    Length::Unknown => length = Length::Known(expected_length),
                    Length::Known(_) => {}
                }
            }
            VersionConsistency::StrongEtagMatched
        } else {
            VersionConsistency::Unpinned
        };
        // Erasure is confined to the built-in HTTP adapter: reqwest exposes an opaque owning stream,
        // while callers need a nameable body that can retain the shared admission permit.
        let AdmittedResponse { response, permit } = admitted;
        let stream = Box::pin(response.bytes_stream());
        Ok(Opened {
            body: HttpBody {
                stream: Some(stream),
                permit: Some(permit),
            },
            length,
            version,
        })
    }

    async fn headers(
        &self,
        input: &Input,
        method: Method,
        etag: Option<&str>,
    ) -> Result<AdmittedResponse, Error> {
        Self::runtime()?;
        let operation = async {
            let permit = Arc::clone(&self.admission)
                .acquire_owned()
                .await
                .map_err(|_| Error::AdmissionLimit)?;
            let mut request = self
                .client
                .request(method, url::Url::clone(input.parsed_reference()))
                .header(ACCEPT_ENCODING, "identity");
            if let Some(etag) = etag {
                request = request.header(IF_MATCH, etag);
            }
            let response = request.send().await.map_err(HttpError::new)?;
            let admitted = AdmittedResponse { response, permit };
            Self::header_budget(&admitted.response)?;
            Ok(admitted)
        };
        // Only admission/connect/headers are timed. No total payload or sink-inactivity deadline is
        // silently introduced by keeping a reqwest request timeout attached to the response body.
        tokio::time::timeout(HEADERS_DEADLINE, operation)
            .await
            .map_err(|_| Error::HeaderTimeout)?
    }

    fn status(response: &Response, stage: RequestStage) -> Result<(), Error> {
        let status = response.status();
        if status.is_redirection() {
            return Err(Error::Redirect);
        }
        if status.as_u16() != 200 {
            return Err(Error::Status {
                stage,
                status: status.as_u16(),
            });
        }
        Ok(())
    }

    fn header_budget(response: &Response) -> Result<(), Error> {
        // This bounds accepted/retained metadata after HTTP parsing, not a claim about the foreign
        // transport's parser allocation. HTTP/1 transport parser limits apply before this seam.
        let bytes = response
            .headers()
            .iter()
            .fold(0_usize, |total, (name, value)| {
                total
                    .saturating_add(name.as_str().len())
                    .saturating_add(value.as_bytes().len())
            });
        if bytes > MAX_HEADERS {
            return Err(Error::Inventory(crate::inventory::InventoryError::Limit));
        }
        Ok(())
    }

    fn validate_headers(response: &Response) -> Result<(), Error> {
        if response
            .headers()
            .get_all(CONTENT_ENCODING)
            .iter()
            .any(|encoding| !encoding.as_bytes().eq_ignore_ascii_case(b"identity"))
        {
            return Err(Error::ContentEncoding);
        }
        if response.headers().contains_key(TRANSFER_ENCODING)
            && response.headers().contains_key(CONTENT_LENGTH)
        {
            return Err(Error::InvalidMetadata);
        }
        Ok(())
    }

    fn length(response: &Response) -> Result<Length, Error> {
        let mut values = response.headers().get_all(CONTENT_LENGTH).iter();
        let Some(value) = values.next() else {
            return Ok(Length::Unknown);
        };
        if values.next().is_some() {
            return Err(Error::InvalidMetadata);
        }
        let text = value.to_str().map_err(|_| Error::InvalidMetadata)?;
        if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(Error::InvalidMetadata);
        }
        let count = text.parse::<u64>().map_err(|_| Error::InvalidMetadata)?;
        Ok(Length::Known(ByteCount(count)))
    }

    fn strong_etag(response: &Response) -> Result<Option<Zeroizing<String>>, Error> {
        let mut values = response.headers().get_all(ETAG).iter();
        let Some(value) = values.next() else {
            return Ok(None);
        };
        if values.next().is_some() || value.as_bytes().len() > 8192 {
            return Err(Error::InvalidMetadata);
        }
        let value = value.to_str().map_err(|_| Error::InvalidMetadata)?;
        let tag = value.strip_prefix("W/").unwrap_or(value);
        if tag.len() < 2
            || !tag.starts_with('"')
            || !tag.ends_with('"')
            || tag.as_bytes()[1..tag.len() - 1]
                .iter()
                .any(|byte| *byte == b'"' || *byte < 0x21 || *byte == 0x7f)
        {
            return Err(Error::InvalidMetadata);
        }
        if value.starts_with("W/") {
            return Ok(None);
        }
        Ok(Some(Zeroizing::new(value.to_owned())))
    }
}

// One owner gives all validation/error paths the same drop order, including rejected GETs.
// Field order matters: the response body owner must be dropped before waking another admission waiter.
struct AdmittedResponse {
    response: Response,
    permit: OwnedSemaphorePermit,
}

pub(super) struct Opened {
    pub(super) body: HttpBody,
    pub(super) length: Length,
    pub(super) version: VersionConsistency,
}

pub(super) struct HttpBody {
    stream: Option<BoxStream<'static, Result<Bytes, reqwest::Error>>>,
    // Network response/stream ownership is dropped before capacity is released.
    permit: Option<OwnedSemaphorePermit>,
}

impl Stream for HttpBody {
    type Item = Result<Bytes, HttpError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(stream) = this.stream.as_mut() else {
            return Poll::Ready(None);
        };
        match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(bytes))),
            Poll::Ready(Some(Err(cause))) => {
                this.stream = None;
                this.permit = None;
                Poll::Ready(Some(Err(HttpError::new(cause))))
            }
            Poll::Ready(None) => {
                this.stream = None;
                this.permit = None;
                Poll::Ready(None)
            }
        }
    }
}

/// Safe transport phase without a URL, header value or server-provided diagnostic string.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestStage {
    /// Advisory metadata HEAD request.
    Metadata,
    /// Opening the single consumed payload GET.
    Payload,
}

impl fmt::Display for RequestStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Metadata => "metadata",
            Self::Payload => "payload",
        })
    }
}

/// Bounded transport category for diagnostics that must not render arbitrary foreign cause chains.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpErrorKind {
    /// Native transport construction failed.
    Configuration,
    /// Connection establishment failed.
    Connect,
    /// A transport timeout expired.
    Timeout,
    /// The consumed response body failed.
    Body,
    /// A request failed before usable response headers.
    Request,
}

/// A retained typed transport cause behind an audited redaction boundary.
///
/// The reqwest error is stripped of its URL immediately. Its arbitrary nested cause chain is not
/// exposed or formatted: removing the outer URL alone does not audit foreign TLS/DNS/proxy errors.
/// Caller-owned custom errors are not involved in this built-in HTTP boundary.
pub struct HttpError {
    cause: reqwest::Error,
}

impl HttpError {
    fn new(cause: reqwest::Error) -> Self {
        Self {
            cause: cause.without_url(),
        }
    }

    /// Returns safe actionable context without exposing the bearer reference or foreign chain.
    pub fn kind(&self) -> HttpErrorKind {
        if self.cause.is_timeout() {
            return HttpErrorKind::Timeout;
        }
        if self.cause.is_builder() {
            return HttpErrorKind::Configuration;
        }
        if self.cause.is_connect() {
            return HttpErrorKind::Connect;
        }
        if self.cause.is_body() {
            return HttpErrorKind::Body;
        }
        HttpErrorKind::Request
    }
}

impl fmt::Debug for HttpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpError")
            .field("kind", &self.kind())
            .finish()
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("http transport failed; check connectivity and the endpoint")
    }
}

impl core::error::Error for HttpError {}
