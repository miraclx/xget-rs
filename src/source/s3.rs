//! An S3 [`Source`]: probe with a `HeadObject`, fetch validated byte ranges with a `GetObject`.
//!
//! The client is built from the default AWS credential chain (environment, shared config or profile,
//! and IMDS), so credentials are never hand-parsed here. An explicit `endpoint_url` with path-style
//! addressing lets the same source reach S3-compatible stores like Cloudflare R2, MinIO, and Backblaze.

use aws_credential_types::provider::ProvideCredentials as _;
use aws_sdk_s3::config::BehaviorVersion;
use aws_sdk_s3::types::ChecksumMode;
use futures::StreamExt as _;
use tokio_util::io::ReaderStream;

use crate::{ByteRange, ByteStream, Checksum, Error, Probe, Source};

/// A byte source backed by an object in an S3 (or S3-compatible) bucket.
pub struct S3Source {
    client: aws_sdk_s3::Client,
    bucket: String,
    key: String,
}

impl S3Source {
    /// Build a source for `key` in `bucket`. Credentials come from the default AWS chain (environment
    /// variables, shared config or profile, and IMDS) and are used to sign requests when the chain
    /// resolves any; when it resolves none, requests go out unsigned so a public bucket still works.
    /// Presence of credentials decides it, not a flag. When `endpoint_url` is set the client targets
    /// that endpoint with path-style addressing, so an S3-compatible store (R2, MinIO, Backblaze) works
    /// without a bucket-as-subdomain DNS name.
    pub async fn new(
        bucket: impl Into<String>,
        key: impl Into<String>,
        endpoint_url: Option<String>,
    ) -> Self {
        let shared = aws_config::defaults(BehaviorVersion::latest()).load().await;
        // Sign only if the chain actually yields credentials; otherwise reload without them so the
        // client sends anonymous requests instead of failing on a missing credential provider.
        let signed = match shared.credentials_provider() {
            Some(provider) => provider.provide_credentials().await.is_ok(),
            None => false,
        };
        let shared = if signed {
            shared
        } else {
            aws_config::defaults(BehaviorVersion::latest())
                .no_credentials()
                .load()
                .await
        };
        let mut builder = aws_sdk_s3::config::Builder::from(&shared);
        if let Some(endpoint_url) = endpoint_url {
            builder = builder.endpoint_url(endpoint_url).force_path_style(true);
        }
        let client = aws_sdk_s3::Client::from_conf(builder.build());
        Self {
            client,
            bucket: bucket.into(),
            key: key.into(),
        }
    }
}

impl Source for S3Source {
    fn identity(&self) -> Option<String> {
        Some(format!("s3://{}/{}", self.bucket, self.key))
    }

    async fn probe(&self) -> Result<Probe, Error> {
        let head = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .map_err(Error::transport)?;

        // S3 always reports the object's size on a successful head; a missing length is a broken
        // response we cannot plan a chunked download against, so it is an error rather than a guess.
        let length = head
            .content_length()
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| Error::detail("HeadObject response has no content length"))?;
        let supports_ranges = head
            .accept_ranges()
            .is_some_and(|value| value.eq_ignore_ascii_case("bytes"));
        let content_type = head
            .content_type()
            .map(str::to_owned)
            .filter(|value| !value.is_empty());
        let filename = head
            .content_disposition()
            .and_then(super::http::content_disposition_name);
        // A stored checksum, if the object was uploaded with one. S3 gives it base64-encoded; the engine
        // works in hex, so decode and re-encode. Skip a composite multipart checksum (a `-N` suffix): it
        // is a checksum of part checksums, not of the whole object, so it cannot verify the download.
        let checksum = stored_checksum(&head);
        // The object's ETag identifies this version, so a resume can tell it apart from a later upload
        // to the same key. S3 always returns one on a successful head.
        let validator = head
            .e_tag()
            .map(str::to_owned)
            .filter(|value| !value.is_empty());

        Ok(Probe {
            length,
            supports_ranges,
            filename,
            content_type,
            checksum,
            validator,
        })
    }

    async fn fetch(&self, range: Option<ByteRange>) -> Result<ByteStream, Error> {
        let mut request = self.client.get_object().bucket(&self.bucket).key(&self.key);
        if let Some(range) = range {
            // S3 ranges are inclusive on both ends; our ByteRange end is exclusive.
            request = request.range(format!("bytes={}-{}", range.start, range.end - 1));
        }
        let output = request.send().await.map_err(Error::transport)?;

        if let Some(range) = range {
            // Guard against an S3-compatible store (R2, MinIO, ...) that ignored the range and returned
            // the whole object: those bytes would scatter at the wrong offsets. A range GET answers with
            // a `Content-Range`; require its start to match, or, if the store omits it, the returned
            // length to equal the requested window. Otherwise reject as a range not honored, the same
            // typed error the HTTP source raises.
            let honored = output
                .content_range()
                .and_then(super::http::content_range_start)
                .map_or(
                    output.content_length() == Some(range.len() as i64),
                    |start| start == range.start,
                );
            if !honored {
                return Err(Error::RangeNotHonored { requested: range });
            }
        }

        // Adapt the SDK's ByteStream into ours: read it as an AsyncRead and re-chunk with ReaderStream,
        // mapping each read error into our transport error.
        let reader = output.body.into_async_read();
        Ok(Box::pin(
            ReaderStream::new(reader).map(|chunk| chunk.map_err(Error::transport)),
        ))
    }
}

/// Read a whole-object stored checksum from a `HeadObject` response as an algorithm and lowercase hex
/// digest. Prefers SHA-256, then SHA-1 (the two S3 algorithms the engine also computes); CRC algorithms
/// and composite multipart checksums (a `-N` suffix) are ignored.
fn stored_checksum(
    head: &aws_sdk_s3::operation::head_object::HeadObjectOutput,
) -> Option<(Checksum, String)> {
    let candidates = [
        (Checksum::Sha256, head.checksum_sha256()),
        (Checksum::Sha1, head.checksum_sha1()),
    ];
    for (algorithm, value) in candidates {
        let Some(value) = value else { continue };
        if value.contains('-') {
            continue; // composite multipart checksum, not a whole-object digest
        }
        if let Ok(bytes) = aws_smithy_types::base64::decode(value) {
            return Some((algorithm, hex::encode(bytes)));
        }
    }
    None
}
