//! Whole-object S3 access through a caller-owned SDK and the common coordinator.
//!
//! Listing is not a bucket snapshot. Each listed object's length/opaque ETag must agree with a
//! subsequent conditional HEAD; that HEAD supplies any versionId for a conditional whole GET.
//! No SDK client/configuration, credentials chain, scheduler or byte-range recovery is created here.

use core::fmt;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;
use std::io;

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt as _};
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::io::ReaderStream;
use zeroize::Zeroizing;

use super::builtin::{Body, Opened};
use super::http::Http;
use super::{DiscoveryBudget, Error, VersionConsistency};
use crate::checks::Requirements;
use crate::input::{Input, InputScope, MemberToken};
use crate::inventory::{
    Description, Directory, Discovered, File, FileId, InventoryError, Selected,
};
use crate::transfer::{ByteCount, Length};

mod metadata;

use metadata::MetadataWire;

#[cfg(test)]
#[path = "s3_tests.rs"]
mod tests;

const DEADLINE: Duration = Duration::from_secs(15);
const MAX_PAGES: usize = 128;
const MAX_TOKEN: usize = 8192;
const MAX_VERSION: usize = 1024;
const MAX_ETAG: usize = 8192;

pub(super) struct S3 {
    client: aws_sdk_s3::Client,
}

impl From<aws_sdk_s3::Client> for S3 {
    fn from(client: aws_sdk_s3::Client) -> Self {
        Self { client }
    }
}

impl S3 {
    pub(super) async fn resolve(
        &self,
        http: &Http,
        input: Input,
        budget: &mut DiscoveryBudget,
    ) -> Result<Discovered, Error> {
        Http::runtime()?;
        // The outer deadline bounds all pages and per-object HEADs, not just each individual call.
        tokio::time::timeout(DEADLINE, self.discover(http, input, budget))
            .await
            .map_err(|_| S3Error::plain(S3ErrorKind::Deadline))?
    }

    async fn discover(
        &self,
        http: &Http,
        input: Input,
        budget: &mut DiscoveryBudget,
    ) -> Result<Discovered, Error> {
        let address = Address::of(&input)?;
        if input.scope() == InputScope::Exact {
            let file = self
                .head(http, input.duplicate_reference()?, &address, None)
                .await?;
            budget.reserve(&file)?;
            return Ok(vec![file].into());
        }
        if address.version.is_some() {
            return Err(S3Error::plain(S3ErrorKind::Protocol).into());
        }
        let listed = self.list(http, &input, &address, budget).await?;
        // Validate the complete hierarchy, including members a later selector would exclude.
        super::layout::Layout::source_tree(
            listed
                .iter()
                .filter(|entry| entry.kind == ListedKind::File)
                .map(|entry| entry.path.as_str()),
            listed
                .iter()
                .filter(|entry| entry.kind == ListedKind::Directory)
                .map(|entry| entry.path.as_str()),
        )?;
        let mut discovered = Discovered::default();
        for entry in listed {
            let mut member = input.duplicate_reference()?;
            member.member = Some(MemberToken::clone(&entry.key));
            let object = Address {
                bucket: Zeroizing::clone(&address.bucket),
                key: Zeroizing::new(entry.key.as_str().to_owned()),
                version: None,
            };
            let mut file = self.head(http, member, &object, Some(&entry)).await?;
            file.selected.description.source_path = entry.path;
            match entry.kind {
                ListedKind::File => {
                    budget.reserve(&file)?;
                    discovered.files.push(file);
                }
                ListedKind::Directory => {
                    let directory = Directory {
                        input: file.input,
                        description: file.selected.description,
                    };
                    budget.reserve_directory(&directory)?;
                    discovered.directories.push(directory);
                }
            }
        }
        // Return the complete scope. Common Inventory construction freezes its accounting receipt
        // before either original-path or literal-member filtering can discard executable members.
        Ok(discovered)
    }

    async fn list(
        &self,
        http: &Http,
        input: &Input,
        address: &Address,
        budget: &DiscoveryBudget,
    ) -> Result<Vec<Listed>, Error> {
        let mut listed: Vec<Listed> = Vec::new();
        let mut tokens: Vec<Zeroizing<String>> = Vec::new();
        let mut bytes = 0_usize;
        for _ in 0..MAX_PAGES {
            let operation = async {
                let _permit = http.admit().await?;
                self.client
                    .list_objects_v2()
                    .bucket(address.bucket.as_str())
                    .prefix(address.key.as_str())
                    .max_keys(129)
                    .set_continuation_token(tokens.last().map(|token| token.to_string()))
                    .customize()
                    .interceptor(MetadataWire::Listing)
                    .send()
                    .await
                    .map_err(|cause| Error::S3(S3Error::list(cause)))
            };
            let page = tokio::time::timeout(DEADLINE, operation)
                .await
                .map_err(|_| S3Error::plain(S3ErrorKind::Deadline))??;
            if !page.common_prefixes().is_empty() {
                return Err(S3Error::plain(S3ErrorKind::Protocol).into());
            }
            for object in page.contents() {
                let key = object
                    .key()
                    .ok_or_else(|| S3Error::plain(S3ErrorKind::Protocol))?;
                let token =
                    MemberToken::s3(key).ok_or_else(|| S3Error::plain(S3ErrorKind::Protocol))?;
                if listed.iter().any(|entry| entry.key == token) {
                    return Err(S3Error::plain(S3ErrorKind::Protocol).into());
                }
                let path = address
                    .relative(key)
                    .ok_or_else(|| S3Error::plain(S3ErrorKind::Protocol))?;
                let length = Self::length(object.size())?;
                let (kind, path) = if key.ends_with('/') {
                    if length != Length::Known(ByteCount(0)) {
                        // Nonempty slash-key payload cannot be discarded as directory metadata.
                        return Err(S3Error::plain(S3ErrorKind::DirectoryKey).into());
                    }
                    let path = path.strip_suffix('/').unwrap_or(path);
                    if path.is_empty() && key != address.key.as_str() {
                        return Err(Error::InvalidName);
                    }
                    (ListedKind::Directory, path)
                } else {
                    (ListedKind::File, path)
                };
                let etag = Self::etag(object.e_tag())?;
                let entry = Listed {
                    kind,
                    key: token,
                    path: Zeroizing::new(path.to_owned()),
                    length,
                    etag,
                };
                // Debit the eventual repeated original input and retained key/path/name/ETag now,
                // before per-object HEADs and any literal/path selection. HEAD evidence is charged
                // again by reserve using its actual size; this preview is an early allocation bound.
                bytes = bytes
                    .saturating_add(input.retained_bytes())
                    .saturating_add(key.len())
                    .saturating_add(path.len().saturating_mul(2))
                    .saturating_add(entry.etag.len());
                if listed.len() >= budget.files || bytes > budget.bytes {
                    return Err(Error::Inventory(InventoryError::Limit));
                }
                listed.push(entry);
            }
            match page.is_truncated() {
                Some(false) => {
                    if page.next_continuation_token().is_some() {
                        return Err(S3Error::plain(S3ErrorKind::Protocol).into());
                    }
                    return Ok(listed);
                }
                Some(true) => {
                    let token = page
                        .next_continuation_token()
                        .filter(|token| !token.is_empty() && token.len() <= MAX_TOKEN)
                        .ok_or_else(|| S3Error::plain(S3ErrorKind::Protocol))?;
                    if tokens.iter().any(|previous| previous.as_str() == token) {
                        return Err(S3Error::plain(S3ErrorKind::Protocol).into());
                    }
                    bytes = bytes.saturating_add(token.len());
                    if bytes > budget.bytes {
                        return Err(Error::Inventory(InventoryError::Limit));
                    }
                    tokens.push(Zeroizing::new(token.to_owned()));
                }
                None => return Err(S3Error::plain(S3ErrorKind::Protocol).into()),
            }
        }
        Err(Error::Inventory(InventoryError::Limit))
    }

    async fn head(
        &self,
        http: &Http,
        mut input: Input,
        address: &Address,
        listed: Option<&Listed>,
    ) -> Result<File, Error> {
        let operation = async {
            let _permit = http.admit().await?;
            self.client
                .head_object()
                .bucket(address.bucket.as_str())
                .key(address.key.as_str())
                .set_version_id(address.version.as_ref().map(|version| version.to_string()))
                .set_if_match(listed.map(|entry| entry.etag.to_string()))
                .customize()
                .interceptor(MetadataWire::Head)
                .send()
                .await
                .map_err(|cause| Error::S3(S3Error::head(cause)))
        };
        let head = tokio::time::timeout(DEADLINE, operation)
            .await
            .map_err(|_| S3Error::plain(S3ErrorKind::Deadline))??;
        let length = Self::length(head.content_length())?;
        let etag = Self::etag(head.e_tag())?;
        Self::encoding(head.content_encoding())?;
        let version = Self::version(head.version_id())?;
        if address
            .version
            .as_deref()
            .is_some_and(|expected| version.as_deref() != Some(expected))
            || listed.is_some_and(|entry| entry.length != length || entry.etag != etag)
        {
            return Err(Error::VersionChanged);
        }
        input.s3_version = version;
        match input.member.as_ref() {
            Some(token) if token.is_s3() && token.as_str() == address.key.as_str() => {}
            Some(_) => return Err(S3Error::plain(S3ErrorKind::Protocol).into()),
            None => {
                input.member = Some(
                    MemberToken::s3(&address.key)
                        .ok_or_else(|| S3Error::plain(S3ErrorKind::Protocol))?,
                )
            }
        }
        let name = address
            .key
            .rsplit('/')
            .next()
            .filter(|name| !name.is_empty())
            .map(|name| Zeroizing::new(name.to_owned()));
        Ok(File {
            input,
            selected: Selected {
                id: FileId::fresh(),
                description: Description {
                    source_path: Zeroizing::clone(&address.key),
                    name,
                    length,
                    etag: Some(etag),
                    requirements: Requirements::default(),
                },
            },
        })
    }

    pub(super) async fn open(&self, http: &Http, file: &File) -> Result<Opened, Error> {
        Http::runtime()?;
        let address = Address::of(&file.input)?;
        let key = file
            .input
            .member
            .as_ref()
            .filter(|token| token.is_s3())
            .ok_or_else(|| S3Error::plain(S3ErrorKind::Protocol))?;
        let scoped = match file.input.scope() {
            InputScope::Exact => key.as_str() == address.key.as_str(),
            InputScope::Tree => address.relative(key.as_str()).is_some_and(|relative| {
                !key.as_str().ends_with('/') && relative == file.source_path()
            }),
            _ => false,
        };
        if !scoped
            || address
                .version
                .as_deref()
                .is_some_and(|version| file.input.s3_version.as_deref() != Some(version))
        {
            return Err(S3Error::plain(S3ErrorKind::Protocol).into());
        }
        let expected = file
            .selected
            .description
            .etag
            .as_ref()
            .ok_or_else(|| S3Error::plain(S3ErrorKind::Protocol))?;
        let operation = async {
            let permit = http.admit().await?;
            let output = self
                .client
                .get_object()
                .bucket(address.bucket.as_str())
                .key(key.as_str())
                .if_match(expected.as_str())
                .set_version_id(
                    file.input
                        .s3_version
                        .as_ref()
                        .map(|version| version.to_string()),
                )
                .customize()
                .interceptor(MetadataWire::PayloadErrors)
                .send()
                .await
                .map_err(|cause| Error::S3(S3Error::get(cause)))?;
            Ok::<_, Error>((output, permit))
        };
        let (output, permit) = tokio::time::timeout(DEADLINE, operation)
            .await
            .map_err(|_| S3Error::plain(S3ErrorKind::Deadline))??;
        // The response owns its body before the permit, so rejection/drop never wakes a new request
        // until the rejected stream is destroyed. No Content-Range is valid in this whole entrance.
        let admitted = Admitted { output, permit };
        let output = &admitted.output;
        // The repeated enforced strong ETag binds a missing GET length to the known HEAD size.
        // Checked consumption must still observe actual EOF, including errors after the final byte.
        let length = match output.content_length() {
            Some(length) => Self::length(Some(length))?,
            None => file.length(),
        };
        let actual = Self::etag(output.e_tag())?;
        Self::encoding(output.content_encoding())?;
        let version = Self::version(output.version_id())?;
        if output.content_range().is_some() {
            return Err(S3Error::plain(S3ErrorKind::Protocol).into());
        }
        if &actual != expected
            || length != file.length()
            || file
                .input
                .s3_version
                .as_deref()
                .is_some_and(|expected| version.as_deref() != Some(expected))
        {
            return Err(Error::VersionChanged);
        }
        let Admitted { output, permit } = admitted;
        let stream = ReaderStream::new(output.body.into_async_read())
            .map(|result| result.map_err(S3Error::body));
        Ok(Opened {
            body: Body::S3(S3Body {
                stream: Some(Box::pin(stream)),
                permit: Some(permit),
            }),
            length,
            version: VersionConsistency::StrongEtagMatched,
        })
    }

    fn length(value: Option<i64>) -> Result<Length, S3Error> {
        let value = value
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| S3Error::plain(S3ErrorKind::Protocol))?;
        Ok(Length::Known(ByteCount(value)))
    }

    fn etag(value: Option<&str>) -> Result<Zeroizing<String>, S3Error> {
        let value = value.ok_or_else(|| S3Error::plain(S3ErrorKind::Protocol))?;
        let bytes = value.as_bytes();
        if bytes.len() < 2
            || bytes.len() > MAX_ETAG
            || bytes.first() != Some(&b'"')
            || bytes.last() != Some(&b'"')
            || bytes[1..bytes.len() - 1]
                .iter()
                .any(|byte| *byte < 0x21 || *byte > 0x7e || *byte == b'"')
        {
            return Err(S3Error::plain(S3ErrorKind::Protocol));
        }
        // Opaque conditional version evidence only, never expected MD5 or a content anchor.
        Ok(Zeroizing::new(value.to_owned()))
    }

    fn version(value: Option<&str>) -> Result<Option<Zeroizing<String>>, S3Error> {
        value
            .map(|value| {
                if value.is_empty()
                    || value.len() > MAX_VERSION
                    || value.chars().any(char::is_control)
                {
                    return Err(S3Error::plain(S3ErrorKind::Protocol));
                }
                Ok(Zeroizing::new(value.to_owned()))
            })
            .transpose()
    }

    fn encoding(value: Option<&str>) -> Result<(), Error> {
        if value.is_some_and(|value| !value.eq_ignore_ascii_case("identity")) {
            return Err(Error::ContentEncoding);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ListedKind {
    File,
    Directory,
}

struct Listed {
    kind: ListedKind,
    key: MemberToken,
    path: Zeroizing<String>,
    length: Length,
    etag: Zeroizing<String>,
}

struct Address {
    bucket: Zeroizing<String>,
    key: Zeroizing<String>,
    version: Option<Zeroizing<String>>,
}

impl Address {
    fn relative<'a>(&self, key: &'a str) -> Option<&'a str> {
        if !key.starts_with(self.key.as_str()) {
            return None;
        }
        // A literal prefix need not end at a directory boundary. Retain the final partial component:
        // prefix `dir/fo` lists `dir/foo` as `foo`, not `o`. Repeated separators are never normalized.
        let parent = self.key.rfind('/').map_or("", |index| &self.key[..=index]);
        key.strip_prefix(parent)
    }

    fn of(input: &Input) -> Result<Self, Error> {
        let url = input.parsed_reference();
        if !url.username().is_empty() || url.password().is_some() || url.port().is_some() {
            return Err(S3Error::plain(S3ErrorKind::Protocol).into());
        }
        let (_, rest) = input
            .original_reference()
            .split_once("://")
            .ok_or_else(|| S3Error::plain(S3ErrorKind::Protocol))?;
        let hierarchy = rest.split('?').next().unwrap_or(rest);
        let (bucket, key) = hierarchy.split_once('/').unwrap_or((hierarchy, ""));
        if bucket.is_empty()
            || !bucket.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
            })
        {
            return Err(S3Error::plain(S3ErrorKind::Protocol).into());
        }
        let key = percent_encoding::percent_decode_str(key)
            .decode_utf8()
            .map_err(|_| S3Error::plain(S3ErrorKind::Protocol))?;
        if key.len() > 1024 || (input.scope() == InputScope::Exact && key.is_empty()) {
            return Err(S3Error::plain(S3ErrorKind::Protocol).into());
        }
        let mut version = None;
        for (name, value) in url.query_pairs() {
            if name != "versionId" || version.is_some() {
                return Err(S3Error::plain(S3ErrorKind::Protocol).into());
            }
            version = S3::version(Some(&value))?;
        }
        Ok(Self {
            bucket: Zeroizing::new(bucket.to_owned()),
            key: Zeroizing::new(key.into_owned()),
            version,
        })
    }
}

struct Admitted {
    output: aws_sdk_s3::operation::get_object::GetObjectOutput,
    permit: OwnedSemaphorePermit,
}

pub(super) struct S3Body {
    stream: Option<BoxStream<'static, Result<Bytes, S3Error>>>,
    // The SDK body is destroyed first, including on early drop and late read errors.
    permit: Option<OwnedSemaphorePermit>,
}

impl Stream for S3Body {
    type Item = Result<Bytes, S3Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(stream) = this.stream.as_mut() else {
            return Poll::Ready(None);
        };
        match stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(bytes))),
            Poll::Pending => Poll::Pending,
            Poll::Ready(item) => {
                this.stream = None;
                this.permit = None;
                Poll::Ready(item)
            }
        }
    }
}

/// Safe S3 operation categories. Remote service diagnostics and credential values are never rendered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum S3ErrorKind {
    /// The exact object/bucket was not found. This never authorizes listing fallback.
    Missing,
    /// Access was denied. Configure the caller's SDK credentials/permissions.
    Denied,
    /// The conditional request rejected the frozen version.
    VersionChanged,
    /// The SDK failed before a usable response. Its retry policy belongs to the caller.
    Transport,
    /// Consuming the owning SDK body failed, including late errors.
    Body,
    /// Async admission/metadata/headers exceeded the fixed deadline.
    Deadline,
    /// Local metadata response wire-byte admission failed before SDK collection/deserialization.
    MetadataLimit,
    /// Malformed metadata, pagination, reference or unsupported response shape.
    Protocol,
    /// A tree contains a nonempty slash-ending object whose payload cannot safely be discarded.
    /// Zero-byte slash markers are separate inspection-only directory metadata.
    DirectoryKey,
}

/// Concrete retained SDK causes behind an audited redaction boundary, like HttpError.
/// Its foreign source chain is deliberately not exposed: SDK service/credential diagnostics can
/// contain arbitrary remote text and access material. `kind` provides safe actionable context.
pub struct S3Error {
    kind: S3ErrorKind,
    // Retain the concrete operation family, never stringify or erase it into an arbitrary cause.
    _cause: Option<Cause>,
}

enum Cause {
    Head(Box<SdkError<HeadObjectError>>),
    List(Box<SdkError<ListObjectsV2Error>>),
    Get(Box<SdkError<GetObjectError>>),
    Body(io::Error),
}

impl S3Error {
    /// Returns a bounded category without remote text or SDK diagnostics.
    pub fn kind(&self) -> S3ErrorKind {
        match &self._cause {
            Some(Cause::Head(cause)) => Self::sdk_kind(
                cause.as_ref(),
                cause
                    .raw_response()
                    .map(|response| response.status().as_u16()),
            ),
            Some(Cause::List(cause)) => Self::sdk_kind(
                cause.as_ref(),
                cause
                    .raw_response()
                    .map(|response| response.status().as_u16()),
            ),
            Some(Cause::Get(cause)) => Self::sdk_kind(
                cause.as_ref(),
                cause
                    .raw_response()
                    .map(|response| response.status().as_u16()),
            ),
            Some(Cause::Body(cause)) => {
                // Reading the concrete error category does not expose its foreign source chain.
                let _category = cause.kind();
                S3ErrorKind::Body
            }
            None => self.kind,
        }
    }

    fn plain(kind: S3ErrorKind) -> Self {
        Self { kind, _cause: None }
    }

    fn sdk_kind(cause: &(dyn core::error::Error + 'static), status: Option<u16>) -> S3ErrorKind {
        if metadata::is_limit(cause) {
            S3ErrorKind::MetadataLimit
        } else {
            Self::status(status)
        }
    }

    fn status(status: Option<u16>) -> S3ErrorKind {
        match status {
            Some(404) => S3ErrorKind::Missing,
            Some(401 | 403) => S3ErrorKind::Denied,
            Some(412) => S3ErrorKind::VersionChanged,
            _ => S3ErrorKind::Transport,
        }
    }

    fn head(cause: SdkError<HeadObjectError>) -> Self {
        Self {
            kind: Self::sdk_kind(
                &cause,
                cause
                    .raw_response()
                    .map(|response| response.status().as_u16()),
            ),
            _cause: Some(Cause::Head(Box::new(cause))),
        }
    }

    fn list(cause: SdkError<ListObjectsV2Error>) -> Self {
        Self {
            kind: Self::sdk_kind(
                &cause,
                cause
                    .raw_response()
                    .map(|response| response.status().as_u16()),
            ),
            _cause: Some(Cause::List(Box::new(cause))),
        }
    }

    fn get(cause: SdkError<GetObjectError>) -> Self {
        Self {
            kind: Self::sdk_kind(
                &cause,
                cause
                    .raw_response()
                    .map(|response| response.status().as_u16()),
            ),
            _cause: Some(Cause::Get(Box::new(cause))),
        }
    }

    fn body(cause: io::Error) -> Self {
        Self {
            kind: S3ErrorKind::Body,
            _cause: Some(Cause::Body(cause)),
        }
    }
}

impl fmt::Debug for S3Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3Error")
            .field("kind", &self.kind)
            .finish()
    }
}

impl fmt::Display for S3Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            S3ErrorKind::Missing => "s3 object or bucket missing; check the exact reference",
            S3ErrorKind::Denied => "s3 access denied; configure the caller sdk permissions",
            S3ErrorKind::VersionChanged => "s3 version changed; discover a fresh request",
            S3ErrorKind::Transport => "s3 transport failed; check the caller sdk configuration",
            S3ErrorKind::Body => "s3 body failed; retain the partial report and inspect connectivity",
            S3ErrorKind::Deadline => "s3 metadata or opening timed out; release bodies or check the endpoint",
            S3ErrorKind::MetadataLimit => "s3 metadata wire limit exceeded; request a smaller scope or check the endpoint",
            S3ErrorKind::Protocol => "invalid s3 metadata or reference; check the endpoint and literal scope",
            S3ErrorKind::DirectoryKey => "s3 tree has a nonempty slash-key payload; request that object exactly with an explicit file target",
        })
    }
}

impl core::error::Error for S3Error {}
