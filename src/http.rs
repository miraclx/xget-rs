//! An HTTP [`Source`]: probe with a length request, fetch validated byte ranges.

use futures::StreamExt as _;
use reqwest::header::{CONTENT_RANGE, RANGE};

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

        if response.status() == reqwest::StatusCode::PARTIAL_CONTENT {
            let header = response
                .headers()
                .get(CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .and_then(content_range_total)
                .ok_or_else(|| detail("206 without a parseable Content-Range total"))?;
            return Ok(Probe {
                length: header,
                supports_ranges: true,
            });
        }
        if response.status().is_success() {
            let length = response
                .content_length()
                .ok_or_else(|| detail("response has no Content-Length"))?;
            return Ok(Probe {
                length,
                supports_ranges: false,
            });
        }
        Err(detail(&format!(
            "probe returned HTTP {}",
            response.status()
        )))
    }

    async fn fetch(&self, range: ByteRange) -> Result<ByteStream, Error> {
        // HTTP ranges are inclusive on both ends; our ByteRange end is exclusive.
        let header = format!("bytes={}-{}", range.start, range.end - 1);
        let response = self
            .client
            .get(self.url.clone())
            .header(RANGE, header)
            .send()
            .await
            .map_err(transport)?;

        // A 200 here means the server ignored the range and is sending the whole body: reject it rather
        // than splice whole-file bytes into the middle of a chunk.
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

fn transport(error: impl core::error::Error + Send + Sync + 'static) -> Error {
    Error::Transport(Box::new(error))
}

fn detail(message: &str) -> Error {
    Error::Transport(Box::new(std::io::Error::other(message.to_owned())))
}
