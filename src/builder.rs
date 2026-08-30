//! The URL-first builder, the public way to run a download. [`get`] (HTTP) or [`from`] (any [`Source`])
//! starts one, option methods chain, and [`Download::write`] runs it against a chosen [`Output`]:
//!
//! ```no_run
//! # async fn run() -> Result<(), xget::Error> {
//! use std::path::Path;
//! use xget::{Checksum, Output};
//!
//! let report = xget::get("https://example.com/big.iso")?
//!     .chunks(8)
//!     .checksum(Checksum::Blake3)
//!     .resume()
//!     .write(Output::file(Path::new("big.iso")))
//!     .await?;
//! # let _ = report;
//! # Ok(())
//! # }
//! ```
//!
//! It is a thin layer over the engine's internal download routine; this builder is the entry point.

use core::time::Duration;

use crate::engine::download;
use crate::{Checksum, Error, HttpSource, Options, Output, Progress, Report, Source};

/// Start a download from an HTTP(S) `url`. For custom headers or another protocol, build the source
/// yourself and use [`from`].
pub fn get(url: &str) -> Result<Download<'static, HttpSource>, Error> {
    Ok(Download::new(HttpSource::new(
        url,
        reqwest::header::HeaderMap::new(),
    )?))
}

/// Start a download from any [`Source`]: an [`HttpSource`] with custom headers, an S3 or IPFS source, a
/// [`Mirrors`](crate::Mirrors) set, or your own.
pub fn from<S: Source>(source: S) -> Download<'static, S> {
    Download::new(source)
}

/// A pending download: a source, its [`Options`], and a progress reporter, configured by chaining and
/// run by [`Download::write`]. Built by [`get`] or [`from`].
#[must_use = "a Download does nothing until you call .write(output)"]
pub struct Download<'p, S: Source> {
    source: S,
    options: Options,
    progress: &'p dyn Progress,
}

impl<S: Source> Download<'static, S> {
    fn new(source: S) -> Self {
        Self {
            source,
            options: Options::default(),
            progress: &(),
        }
    }
}

impl<'p, S: Source> Download<'p, S> {
    /// The maximum number of chunks fetched in parallel (default 5). Ignored by a source that cannot
    /// serve ranges.
    pub fn chunks(mut self, parts: u32) -> Self {
        self.options.parts = parts;
        self
    }

    /// Retries per dropped chunk, each resuming from the offset it reached (default 10).
    pub fn tries(mut self, retries: u32) -> Self {
        self.options.retries = retries;
        self
    }

    /// The checksum algorithm to verify with, or [`Checksum::None`] to skip it (default SHA-256).
    pub fn checksum(mut self, checksum: Checksum) -> Self {
        self.options.checksum = checksum;
        self
    }

    /// Fail a read that stalls for this long, so a retry can resume the chunk (default: wait forever).
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.options.timeout = Some(timeout);
        self
    }

    /// Resume an interrupted download from the partial beside the output (default off). Requires a
    /// range-capable source and a file output.
    pub fn resume(mut self) -> Self {
        self.options.resume = true;
        self
    }

    /// Fetch as a single ordered stream instead of parallel chunks: one connection, hashed inline,
    /// written straight to the sink with no scratch. Slower than the parallel default, but it holds no
    /// bytes on disk and delivers live, so a huge download can be piped to a consumer.
    pub fn sequential(mut self) -> Self {
        self.options.sequential = true;
        self
    }

    /// Report progress to `progress` as bytes are received and verified. Pass any [`Progress`] impl;
    /// with none set, nothing is reported.
    pub fn progress<Q: Progress>(self, progress: &Q) -> Download<'_, S> {
        Download {
            source: self.source,
            options: self.options,
            progress,
        }
    }

    /// Run the download, sending the verified bytes to `output`, and return its length and checksum.
    pub async fn write(self, output: Output<'_>) -> Result<Report, Error> {
        download(&self.source, output, self.options, self.progress).await
    }
}
