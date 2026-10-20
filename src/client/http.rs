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
    ACCEPT_ENCODING, ACCEPT_RANGES, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, ETAG,
    IF_MATCH, RANGE, TRANSFER_ENCODING,
};
use reqwest::{Method, Response};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use zeroize::Zeroizing;

use super::{Error, VersionConsistency};
use crate::checks::Requirements;
use crate::input::{Input, RangeSupport};
use crate::inventory::{Description, File};
use crate::transfer::reader::Window;
use crate::transfer::{ByteCount, Length};

const HEADERS_DEADLINE: Duration = Duration::from_secs(15);
const MAX_HEADERS: usize = 64 * 1024;
const MAX_PATH: usize = 4096;

pub(super) struct Http {
    client: reqwest::Client,
    admission: Arc<Semaphore>,
    // Tests map already-validated service/object authorities onto one owned loopback listener.
    // There is deliberately no production endpoint override or credential-origin bypass.
    #[cfg(test)]
    fixture: Option<url::Url>,
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
            #[cfg(test)]
            fixture: None,
        })
    }

    #[cfg(all(test, feature = "swisstransfer"))]
    pub(super) fn fixture(uri: &str, maximum: NonZeroUsize) -> Self {
        let mut http = Self::new(maximum).unwrap_or_else(|_| panic!("fixture http configuration"));
        http.fixture = Some(url::Url::parse(uri).unwrap_or_else(|_| panic!("fixture uri")));
        http
    }

    pub(super) fn runtime() -> Result<(), Error> {
        tokio::runtime::Handle::try_current().map_err(|_| Error::RuntimeUnavailable)?;
        Ok(())
    }

    pub(super) async fn describe(&self, input: &mut Input) -> Result<Description, Error> {
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
        input.ranges = if Self::accepts_ranges(response) {
            RangeSupport::Bytes
        } else {
            RangeSupport::Unknown
        };
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
        self.open_reference(file, &file.input)
            .await
            .map(|(opened, _)| opened)
    }

    pub(super) async fn open_reference(
        &self,
        file: &File,
        reference: &Input,
    ) -> Result<(Opened, Option<Zeroizing<String>>), Error> {
        let description = &file.selected.description;
        let admitted = self
            .headers(
                reference,
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
        let actual = Self::strong_etag(response)?;
        if file.input.kind() == crate::input::InputKind::SwissTransfer {
            if actual.is_none() {
                return Err(Error::InvalidMetadata);
            }
            // Whole provider responses must establish the entire frozen representation. Absence of
            // a declaration is handled by the common EOF checker against manifest length.
            match (description.length, length) {
                (Length::Known(expected), Length::Known(found)) if expected != found => {
                    return Err(Error::VersionChanged);
                }
                (Length::Known(expected), Length::Unknown) => length = Length::Known(expected),
                (Length::Unknown, _) => return Err(Error::InvalidMetadata),
                _ => {}
            }
        }
        let version = if let Some(expected) = &description.etag {
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
        Ok((
            Opened {
                body: HttpBody {
                    stream: Some(stream),
                    permit: Some(permit),
                },
                length,
                version,
            },
            actual,
        ))
    }

    /// A range never downgrades to whole-body access. Both the condition and repeated ETag bind
    /// every response to the frozen identity representation before its body reaches the coordinator.
    pub(super) async fn open_range(
        &self,
        file: &File,
        window: Window,
        size: ByteCount,
        etag: &str,
    ) -> Result<HttpBody, Error> {
        let admitted = self
            .headers_window(&file.input, Method::GET, Some(etag), Some(window))
            .await?;
        let response = &admitted.response;
        if response.status().as_u16() == 412 {
            return Err(Error::VersionChanged);
        }
        if response.status().as_u16() != 206 {
            return Err(Error::Status {
                stage: RequestStage::Payload,
                status: response.status().as_u16(),
            });
        }
        Self::validate_headers(response)?;
        if Self::strong_etag(response)?.as_deref().map(String::as_str) != Some(etag) {
            return Err(Error::VersionChanged);
        }
        let mut ranges = response.headers().get_all(CONTENT_RANGE).iter();
        let range = ranges.next().ok_or(Error::InvalidMetadata)?;
        if ranges.next().is_some()
            || !Self::matches_range(range.as_bytes(), window, size)
            || matches!(Self::length(response)?, Length::Known(count) if count.0 != window.end().0 - window.start().0)
        {
            return Err(Error::InvalidMetadata);
        }
        let AdmittedResponse { response, permit } = admitted;
        Ok(HttpBody {
            stream: Some(Box::pin(response.bytes_stream())),
            permit: Some(permit),
        })
    }

    fn accepts_ranges(response: &Response) -> bool {
        let mut values = response.headers().get_all(ACCEPT_RANGES).iter();
        values
            .next()
            .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"bytes"))
            && values.next().is_none()
    }

    fn matches_range(value: &[u8], window: Window, size: ByteCount) -> bool {
        let Ok(value) = core::str::from_utf8(value) else {
            return false;
        };
        let Some(value) = value.strip_prefix("bytes ") else {
            return false;
        };
        let Some((bounds, total)) = value.split_once('/') else {
            return false;
        };
        let Some((start, end)) = bounds.split_once('-') else {
            return false;
        };
        let decimal = |text: &str| {
            if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }
            text.parse::<u64>().ok()
        };
        decimal(start) == Some(window.start().0)
            && decimal(end) == Some(window.end().0 - 1)
            && decimal(total) == Some(size.0)
    }

    #[cfg(feature = "swisstransfer")]
    pub(super) async fn metadata(
        &self,
        input: &Input,
        maximum: usize,
    ) -> Result<Zeroizing<Vec<u8>>, Error> {
        Self::runtime()?;
        let operation = async {
            let mut admitted = self.headers(input, Method::GET, None).await?;
            Self::status(&admitted.response, RequestStage::Metadata)?;
            Self::validate_headers(&admitted.response)?;
            if admitted.response.headers().contains_key(CONTENT_RANGE) {
                return Err(Error::InvalidMetadata);
            }
            if matches!(Self::length(&admitted.response)?, Length::Known(ByteCount(count)) if count > maximum as u64)
            {
                return Err(Error::Inventory(crate::inventory::InventoryError::Limit));
            }
            let mut bytes = Zeroizing::new(Vec::new());
            while let Some(chunk) = admitted.response.chunk().await.map_err(HttpError::new)? {
                if chunk.len() > maximum.saturating_sub(bytes.len()) {
                    return Err(Error::Inventory(crate::inventory::InventoryError::Limit));
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(bytes)
        };
        // Metadata's deadline covers its body as well as admission and headers. No retained control
        // permit survives this call, including malformed, oversized and never-ready response paths.
        tokio::time::timeout(HEADERS_DEADLINE, operation)
            .await
            .map_err(|_| Error::HeaderTimeout)?
    }

    async fn headers(
        &self,
        input: &Input,
        method: Method,
        etag: Option<&str>,
    ) -> Result<AdmittedResponse, Error> {
        self.headers_window(input, method, etag, None).await
    }

    async fn headers_window(
        &self,
        input: &Input,
        method: Method,
        etag: Option<&str>,
        window: Option<Window>,
    ) -> Result<AdmittedResponse, Error> {
        Self::runtime()?;
        let operation = async {
            let permit = Arc::clone(&self.admission)
                .acquire_owned()
                .await
                .map_err(|_| Error::AdmissionLimit)?;
            let reference = url::Url::clone(input.parsed_reference());
            #[cfg(test)]
            let reference = if let Some(fixture) = &self.fixture {
                let mut target = url::Url::clone(fixture);
                target.set_path(reference.path());
                target.set_query(reference.query());
                target
            } else {
                reference
            };
            let mut request = self
                .client
                .request(method, reference)
                .header(ACCEPT_ENCODING, "identity");
            if let Some(etag) = etag {
                request = request.header(IF_MATCH, etag);
            }
            if let Some(window) = window {
                request = request.header(
                    RANGE,
                    format!("bytes={}-{}", window.start().0, window.end().0 - 1),
                );
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
    /// Opening a consumed whole-body GET or one exact conditional range response.
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
