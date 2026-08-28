//! End-to-end tests for the download engine, driven by an in-process fake [`Source`].
//!
//! These exercise the real engine paths (parallel scatter into a sparse file, the continuous in-order
//! verifier, the length and range gates, control-file resume, and the non-range single-stream path)
//! without any network, so they are deterministic and fast. A misbehaving source is modeled directly
//! as a fake that ignores a range or short-serves a length, which is the exact failure the engine must
//! turn into a typed [`Error`].

use core::sync::atomic::{AtomicU32, Ordering};
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream;
use sha2::{Digest as _, Sha256};
use xget::{ByteRange, ByteStream, Checksum, Error, Options, Probe, Report, Source, download};

/// A deterministic in-memory resource, served either range by range (honestly) or with an injected
/// fault, so the engine's guarantees can be asserted without a server.
struct FakeSource {
    body: Arc<Vec<u8>>,
    supports_ranges: bool,
    behavior: Behavior,
    /// How many range fetches have been issued, so a test can drop the first attempt of a chunk and
    /// assert the retry resumes it.
    fetches: AtomicU32,
}

/// How a [`FakeSource`] answers a range fetch: honestly, or with a specific injected fault.
#[derive(Clone, Copy)]
enum Behavior {
    /// Serve exactly the bytes asked for.
    Honest,
    /// Ignore the requested range and serve the whole body, as a server that answered `200` to a
    /// range request would. The engine must reject this rather than splice whole-file bytes into a
    /// chunk.
    IgnoreRange,
    /// Serve one byte fewer than the range asks for, so the declared length is never satisfied.
    ShortByOne,
    /// Fail the very first fetch outright, then serve honestly, so a retry can recover.
    FailFirstFetch,
}

impl FakeSource {
    fn new(body: Vec<u8>, supports_ranges: bool, behavior: Behavior) -> Self {
        Self {
            body: Arc::new(body),
            supports_ranges,
            behavior,
            fetches: AtomicU32::new(0),
        }
    }

    fn honest(body: Vec<u8>) -> Self {
        Self::new(body, true, Behavior::Honest)
    }
}

impl Source for FakeSource {
    async fn probe(&self) -> Result<Probe, Error> {
        Ok(Probe {
            length: self.body.len() as u64,
            supports_ranges: self.supports_ranges,
            filename: None,
            content_type: None,
            checksum: None,
        })
    }

    async fn fetch(&self, range: Option<ByteRange>) -> Result<ByteStream, Error> {
        let attempt = self.fetches.fetch_add(1, Ordering::SeqCst);
        let Some(range) = range else {
            // A whole-resource fetch: one stream of the entire body, in small pieces so the streaming
            // path sees more than one chunk.
            return Ok(pieces(&self.body[..]));
        };
        match self.behavior {
            Behavior::IgnoreRange => Err(Error::RangeNotHonored { requested: range }),
            Behavior::ShortByOne => {
                let start = range.start as usize;
                let end = (range.end as usize).saturating_sub(1).max(start);
                Ok(pieces(&self.body[start..end.min(self.body.len())]))
            }
            Behavior::FailFirstFetch if attempt == 0 => Err(Error::Transport(Box::new(
                std::io::Error::other("injected"),
            ))),
            Behavior::Honest | Behavior::FailFirstFetch => {
                let start = range.start as usize;
                let end = (range.end as usize).min(self.body.len());
                Ok(pieces(&self.body[start..end]))
            }
        }
    }
}

/// Split a slice into a stream of small [`Bytes`] pieces, so a fetch delivers several chunks the way a
/// real transport would rather than one contiguous blob.
fn pieces(data: &[u8]) -> ByteStream {
    let chunks: Vec<Result<Bytes, Error>> = data
        .chunks(7)
        .map(|piece| Ok(Bytes::copy_from_slice(piece)))
        .collect();
    Box::pin(stream::iter(chunks))
}

/// A unique scratch path under the OS temp dir, removed on drop along with its `.xget` partial, so tests
/// never collide or leak files.
struct Scratch {
    path: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::SeqCst);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("xget-{tag}-{pid}-{unique}.bin"));
        Self { path }
    }

    /// The `.xget` partial the engine writes beside the output before finalizing it.
    fn part(&self) -> PathBuf {
        let mut part = self.path.clone().into_os_string();
        part.push(".xget");
        PathBuf::from(part)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(self.part());
    }
}

/// The lowercase SHA-256 hex of `data`, the digest the engine should compute for the default checksum.
fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// A body large enough to span several chunks under the default five-way plan, with non-trivial bytes.
fn sample_body(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 31 + 7) as u8).collect()
}

#[tokio::test]
async fn a_clean_parallel_download_verifies_to_the_right_hash() {
    let body = sample_body(4096);
    let source = FakeSource::honest(body.clone());
    let scratch = Scratch::new("clean");
    let options = Options {
        parts: 5,
        checksum: Checksum::Sha256,
        ..Options::default()
    };

    let report: Report = download(&source, &scratch.path, options, &())
        .await
        .expect("a clean parallel download succeeds");

    assert_eq!(report.length, body.len() as u64, "the whole resource");
    assert_eq!(
        report.hash.as_deref(),
        Some(sha256_hex(&body).as_str()),
        "the verified digest matches the known bytes"
    );
    let written = std::fs::read(&scratch.path).expect("the output file is in place");
    assert_eq!(written, body, "every byte landed at its offset");
}

#[tokio::test]
async fn a_source_that_ignores_the_range_is_a_typed_error() {
    let body = sample_body(2048);
    let source = FakeSource::new(body, true, Behavior::IgnoreRange);
    let scratch = Scratch::new("ignore-range");
    // Few retries: every attempt is rejected, so there is nothing to recover and the backoff should
    // not dominate the test.
    let options = Options {
        retries: 1,
        ..Options::default()
    };

    let error = download(&source, &scratch.path, options, &())
        .await
        .expect_err("a range-ignoring source must not be certified");

    assert!(
        matches!(error, Error::RangeNotHonored { .. }),
        "the failure names the range that was not honored, got {error:?}"
    );
    assert!(
        !scratch.path.exists(),
        "no output file is left when the download fails"
    );
}

#[tokio::test]
async fn a_short_served_chunk_becomes_a_length_mismatch() {
    let body = sample_body(2048);
    // One part, so the single chunk is served one byte short and the engine exhausts its retries.
    let source = FakeSource::new(body, true, Behavior::ShortByOne);
    let scratch = Scratch::new("short");
    let options = Options {
        parts: 1,
        retries: 1,
        ..Options::default()
    };

    let error = download(&source, &scratch.path, options, &())
        .await
        .expect_err("a chunk that never reaches its end cannot complete");

    assert!(
        matches!(error, Error::LengthMismatch { .. }),
        "a short chunk is a length mismatch, got {error:?}"
    );
}

#[tokio::test]
async fn a_dropped_chunk_resumes_and_still_verifies() {
    let body = sample_body(1024);
    // One part with one retry: the first fetch fails outright, the retry serves it honestly.
    let source = FakeSource::new(body.clone(), true, Behavior::FailFirstFetch);
    let scratch = Scratch::new("retry");
    let options = Options {
        parts: 1,
        retries: 3,
        checksum: Checksum::Sha256,
        ..Options::default()
    };

    let report = download(&source, &scratch.path, options, &())
        .await
        .expect("the retry recovers the dropped chunk");

    assert_eq!(
        report.hash.as_deref(),
        Some(sha256_hex(&body).as_str()),
        "the recovered download still verifies to the known hash"
    );
    assert!(
        source.fetches.load(Ordering::SeqCst) >= 2,
        "the first fetch was dropped and a retry was issued"
    );
}

#[tokio::test]
async fn resume_via_the_control_file_completes_a_partial_download() {
    let body = sample_body(4096);
    let scratch = Scratch::new("resume");
    let options = Options {
        parts: 4,
        retries: 1,
        checksum: Checksum::Sha256,
        resume: true,
        ..Options::default()
    };

    // First pass: every chunk but the last is honest, the last is short, so the download fails partway
    // and leaves a `.xget` partial whose control trailer records the completed chunks.
    struct PartialSource {
        body: Arc<Vec<u8>>,
        fail_from: u64,
    }
    impl Source for PartialSource {
        async fn probe(&self) -> Result<Probe, Error> {
            Ok(Probe {
                length: self.body.len() as u64,
                supports_ranges: true,
                filename: None,
                content_type: None,
                checksum: None,
            })
        }
        async fn fetch(&self, range: Option<ByteRange>) -> Result<ByteStream, Error> {
            let range = range.expect("a range-capable resume fetches ranges");
            if range.start >= self.fail_from {
                return Err(Error::Transport(Box::new(std::io::Error::other("drop"))));
            }
            let start = range.start as usize;
            let end = (range.end as usize).min(self.body.len());
            Ok(pieces(&self.body[start..end]))
        }
    }

    let partial = PartialSource {
        body: Arc::new(body.clone()),
        // Fail the final quarter, so the first three chunks land and are recorded as done.
        fail_from: 3072,
    };
    let first = download(&partial, &scratch.path, options, &()).await;
    assert!(first.is_err(), "the first pass fails on the dropped tail");
    assert!(
        scratch.part().exists(),
        "the partial file is kept for a resume"
    );

    // Second pass: the same source now serves everything. Resume must reuse the recorded chunks, fetch
    // only the remainder, fold the on-disk prefix into the hash, and verify to the full digest.
    let full = FakeSource::honest(body.clone());
    let report = download(&full, &scratch.path, options, &())
        .await
        .expect("the resume completes the download");

    assert_eq!(report.length, body.len() as u64);
    assert_eq!(
        report.hash.as_deref(),
        Some(sha256_hex(&body).as_str()),
        "the resumed download verifies to the same digest as a clean one"
    );
    assert_eq!(
        std::fs::read(&scratch.path).expect("output present"),
        body,
        "the resumed file holds every byte"
    );
}

#[tokio::test]
async fn resume_re_chunks_with_a_different_parts_count() {
    let body = sample_body(8192);
    let scratch = Scratch::new("resume-rechunk");

    // First pass with eight chunks; fail the tail so several early chunks land and are recorded.
    struct PartialSource {
        body: Arc<Vec<u8>>,
        fail_from: u64,
    }
    impl Source for PartialSource {
        async fn probe(&self) -> Result<Probe, Error> {
            Ok(Probe {
                length: self.body.len() as u64,
                supports_ranges: true,
                filename: None,
                content_type: None,
                checksum: None,
            })
        }
        async fn fetch(&self, range: Option<ByteRange>) -> Result<ByteStream, Error> {
            let range = range.expect("a range-capable resume fetches ranges");
            if range.start >= self.fail_from {
                return Err(Error::Transport(Box::new(std::io::Error::other("drop"))));
            }
            let start = range.start as usize;
            let end = (range.end as usize).min(self.body.len());
            Ok(pieces(&self.body[start..end]))
        }
    }

    let partial = PartialSource {
        body: Arc::new(body.clone()),
        fail_from: 5000,
    };
    let first = download(
        &partial,
        &scratch.path,
        Options {
            parts: 8,
            retries: 1,
            resume: true,
            ..Options::default()
        },
        &(),
    )
    .await;
    assert!(first.is_err(), "the first pass fails on the dropped tail");

    // Resume with a different parallelism. The plan is rebuilt from the bytes present, so the new chunk
    // count re-tiles only what is missing, and the download still verifies to the full digest.
    let full = FakeSource::honest(body.clone());
    let report = download(
        &full,
        &scratch.path,
        Options {
            parts: 3,
            retries: 1,
            resume: true,
            ..Options::default()
        },
        &(),
    )
    .await
    .expect("the resume completes with a different chunk count");

    assert_eq!(
        report.hash.as_deref(),
        Some(sha256_hex(&body).as_str()),
        "a re-chunked resume verifies to the same digest as a clean download"
    );
    assert_eq!(
        std::fs::read(&scratch.path).expect("output present"),
        body,
        "the re-chunked resume holds every byte"
    );
}

#[tokio::test]
async fn a_non_range_source_streams_and_verifies() {
    let body = sample_body(3000);
    let source = FakeSource::new(body.clone(), false, Behavior::Honest);
    let scratch = Scratch::new("stream");
    let options = Options {
        checksum: Checksum::Sha256,
        ..Options::default()
    };

    let report = download(&source, &scratch.path, options, &())
        .await
        .expect("a non-range source is fetched as a single stream");

    assert_eq!(report.length, body.len() as u64);
    assert_eq!(
        report.hash.as_deref(),
        Some(sha256_hex(&body).as_str()),
        "the streamed download verifies to the known hash"
    );
    assert_eq!(std::fs::read(&scratch.path).expect("output present"), body);
}

#[tokio::test]
async fn no_checksum_requested_returns_no_hash() {
    let body = sample_body(512);
    let source = FakeSource::honest(body.clone());
    let scratch = Scratch::new("nohash");
    let options = Options {
        checksum: Checksum::None,
        ..Options::default()
    };

    let report = download(&source, &scratch.path, options, &())
        .await
        .expect("a download with hashing off still completes");

    assert_eq!(report.length, body.len() as u64);
    assert_eq!(
        report.hash, None,
        "no digest is certified when none is asked"
    );
}
