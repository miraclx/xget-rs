//! An HTTP [`Source`]: probe with a length request, fetch validated byte ranges.

use futures::StreamExt as _;
use reqwest::header::{CONTENT_DISPOSITION, CONTENT_RANGE, CONTENT_TYPE, RANGE};

use crate::{ByteRange, ByteStream, Error, Probe, Source};

/// A byte source backed by an HTTP(S) URL that supports range requests.
pub struct HttpSource {
    client: reqwest::Client,
    url: reqwest::Url,
}

impl HttpSource {
    /// Build a source for `url`, sharing one connection pool across every chunk. `headers` are sent
    /// with every request.
    pub fn new(url: &str, headers: reqwest::header::HeaderMap) -> Result<Self, Error> {
        let url = reqwest::Url::parse(url).map_err(transport)?;
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .map_err(transport)?;
        Ok(Self { client, url })
    }
}

impl Source for HttpSource {
    async fn probe(&self) -> Result<Probe, Error> {
        // Ask for a single byte: a range-capable server answers 206 with the total in Content-Range.
        let response = self
            .client
            .get(self.url.clone())
            .header(RANGE, "bytes=0-0")
            .send()
            .await
            .map_err(transport)?;

        let filename = response
            .headers()
            .get(CONTENT_DISPOSITION)
            .and_then(|value| value.to_str().ok())
            .and_then(content_disposition_name);
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.split(';').next().unwrap_or(value).trim().to_owned())
            .filter(|value| !value.is_empty());

        if response.status() == reqwest::StatusCode::PARTIAL_CONTENT {
            let length = response
                .headers()
                .get(CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .and_then(content_range_total)
                .ok_or_else(|| detail("206 without a parseable Content-Range total"))?;
            return Ok(Probe {
                length,
                supports_ranges: true,
                filename,
                content_type,
                checksum: None,
            });
        }
        if response.status().is_success() {
            let length = response
                .content_length()
                .ok_or_else(|| detail("response has no Content-Length"))?;
            return Ok(Probe {
                length,
                supports_ranges: false,
                filename,
                content_type,
                checksum: None,
            });
        }
        Err(detail(&format!(
            "probe returned HTTP {}",
            response.status()
        )))
    }

    async fn fetch(&self, range: Option<ByteRange>) -> Result<ByteStream, Error> {
        let mut request = self.client.get(self.url.clone());
        if let Some(range) = range {
            // HTTP ranges are inclusive on both ends; our ByteRange end is exclusive.
            request = request.header(RANGE, format!("bytes={}-{}", range.start, range.end - 1));
        }
        let response = request.send().await.map_err(transport)?;

        match range {
            Some(range) => {
                // A 200 here means the server ignored the range and is sending the whole body: reject it
                // rather than splice whole-file bytes into the middle of a chunk.
                if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
                    return Err(Error::RangeNotHonored { requested: range });
                }
                let start = response
                    .headers()
                    .get(CONTENT_RANGE)
                    .and_then(|value| value.to_str().ok())
                    .and_then(content_range_start);
                if start != Some(range.start) {
                    return Err(Error::RangeNotHonored { requested: range });
                }
            }
            None => {
                if !response.status().is_success() {
                    return Err(detail(&format!(
                        "fetch returned HTTP {}",
                        response.status()
                    )));
                }
            }
        }

        Ok(Box::pin(
            response
                .bytes_stream()
                .map(|chunk| chunk.map_err(transport)),
        ))
    }
}

/// Parse the total length from a `Content-Range` value like `bytes 0-0/1234`.
pub(crate) fn content_range_total(value: &str) -> Option<u64> {
    value.rsplit('/').next()?.trim().parse().ok()
}

/// Parse the start offset from a `Content-Range` value like `bytes 100-199/1234`.
pub(crate) fn content_range_start(value: &str) -> Option<u64> {
    let span = value.strip_prefix("bytes ")?.split('/').next()?;
    span.split('-').next()?.trim().parse().ok()
}

/// Extract a filename from a `Content-Disposition` value, preferring the RFC 5987 `filename*` form and
/// falling back to a plain `filename`. The result is stripped to its final path component, so a server
/// cannot steer the output outside the intended directory with a value like `../../etc/passwd`. Returns
/// `None` if no usable, non-empty name is present.
pub(crate) fn content_disposition_name(value: &str) -> Option<String> {
    let extended = value
        .split(';')
        .filter_map(|part| part.trim().strip_prefix("filename*="))
        .find_map(decode_extended_filename);
    let plain = || {
        value
            .split(';')
            .filter_map(|part| part.trim().strip_prefix("filename="))
            .map(|raw| raw.trim().trim_matches('"').to_owned())
            .next()
    };
    let name = extended.or_else(plain)?;
    let base = std::path::Path::new(&name)
        .file_name()
        .and_then(std::ffi::OsStr::to_str)?;
    (!base.is_empty()).then(|| base.to_owned())
}

/// Decode an RFC 5987 extended value like `UTF-8''my%20file.zip` into `my file.zip`, percent-decoding
/// the bytes after the `charset''` prefix. Only UTF-8 output is accepted.
fn decode_extended_filename(raw: &str) -> Option<String> {
    let encoded = raw.rsplit("''").next()?;
    let mut out = Vec::with_capacity(encoded.len());
    let mut chars = encoded.bytes();
    while let Some(byte) = chars.next() {
        if byte == b'%' {
            let hi = chars.next()?;
            let lo = chars.next()?;
            let pair = [hi, lo];
            let text = core::str::from_utf8(&pair).ok()?;
            out.push(u8::from_str_radix(text, 16).ok()?);
        } else {
            out.push(byte);
        }
    }
    String::from_utf8(out).ok()
}

fn transport(error: impl core::error::Error + Send + Sync + 'static) -> Error {
    Error::Transport(Box::new(error))
}

fn detail(message: &str) -> Error {
    Error::Transport(Box::new(std::io::Error::other(message.to_owned())))
}
