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
use xget::{ByteRange, ByteStream, Checksum, Error, Output, Probe, Progress, Report, Source};

/// A deterministic in-memory resource, served either range by range (honestly) or with an injected
/// fault, so the engine's guarantees can be asserted without a server.
struct FakeSource {
    body: Arc<Vec<u8>>,
    supports_ranges: bool,
    behavior: Behavior,
    /// How many range fetches have been issued, so a test can drop the first attempt of a chunk and
    /// assert the retry resumes it. Shared so a test can keep a handle after moving the source into a
    /// download.
    fetches: Arc<AtomicU32>,
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
            fetches: Arc::new(AtomicU32::new(0)),
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
            validator: None,
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

/// Run a download through the public builder with the given knobs. The engine tests used to call the
/// now-internal `download` function directly; this keeps them concise while exercising the real entry
/// point. Defaults matched to `Options`: parts 5, retries 10, checksum SHA-256, no resume.
async fn run<S: Source>(
    source: S,
    output: Output<'_>,
    parts: u32,
    retries: u32,
    checksum: Checksum,
    resume: bool,
) -> Result<Report, Error> {
    let mut plan = xget::from(source)
        .chunks(parts)
        .tries(retries)
        .checksum(checksum);
    if resume {
        plan = plan.resume();
    }
    plan.write(output).await
}

#[tokio::test]
async fn a_clean_parallel_download_verifies_to_the_right_hash() {
    let body = sample_body(4096);
    let source = FakeSource::honest(body.clone());
    let scratch = Scratch::new("clean");
    let report: Report = run(
        source,
        Output::File(&scratch.path),
        5,
        10,
        Checksum::Sha256,
        false,
    )
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
async fn a_dropped_chunk_reports_a_retry_to_progress() {
    // A reporter that just counts retry callbacks, to prove the engine surfaces a retry (what a bar
    // renders above itself) and not only that the download recovers.
    #[derive(Default)]
    struct Recorder {
        retries: std::cell::Cell<u32>,
    }
    impl Progress for Recorder {
        fn retry(&self, _index: usize, _retry: u32, _max: u32, _resume_from: u64, _error: &str) {
            self.retries.set(self.retries.get() + 1);
        }
    }

    let body = sample_body(1024);
    let source = FakeSource::new(body.clone(), true, Behavior::FailFirstFetch);
    let scratch = Scratch::new("retry-report");
    let recorder = Recorder::default();
    xget::from(source)
        .chunks(1)
        .tries(3)
        .checksum(Checksum::Sha256)
        .progress(&recorder)
        .write(Output::File(&scratch.path))
        .await
        .expect("the retry recovers the dropped chunk");

    assert!(
        recorder.retries.get() >= 1,
        "the dropped first fetch was reported as a retry"
    );
}

#[tokio::test]
async fn a_source_that_ignores_the_range_is_a_typed_error() {
    let body = sample_body(2048);
    let source = FakeSource::new(body, true, Behavior::IgnoreRange);
    let scratch = Scratch::new("ignore-range");
    // Few retries: every attempt is rejected, so there is nothing to recover and the backoff should
    // not dominate the test.
    let error = run(
        source,
        Output::File(&scratch.path),
        5,
        1,
        Checksum::Sha256,
        false,
    )
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
    let error = run(
        source,
        Output::File(&scratch.path),
        1,
        1,
        Checksum::Sha256,
        false,
    )
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
    let fetches = Arc::clone(&source.fetches);
    let scratch = Scratch::new("retry");
    let report = run(
        source,
        Output::File(&scratch.path),
        1,
        3,
        Checksum::Sha256,
        false,
    )
    .await
    .expect("the retry recovers the dropped chunk");

    assert_eq!(
        report.hash.as_deref(),
        Some(sha256_hex(&body).as_str()),
        "the recovered download still verifies to the known hash"
    );
    assert!(
        fetches.load(Ordering::SeqCst) >= 2,
        "the first fetch was dropped and a retry was issued"
    );
}

#[tokio::test]
async fn resume_via_the_control_file_completes_a_partial_download() {
    let body = sample_body(4096);
    let scratch = Scratch::new("resume");

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
                validator: None,
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
    let first = run(
        partial,
        Output::File(&scratch.path),
        4,
        1,
        Checksum::Sha256,
        true,
    )
    .await;
    assert!(first.is_err(), "the first pass fails on the dropped tail");
    assert!(
        scratch.part().exists(),
        "the partial file is kept for a resume"
    );

    // Second pass: the same source now serves everything. Resume must reuse the recorded chunks, fetch
    // only the remainder, fold the on-disk prefix into the hash, and verify to the full digest.
    let full = FakeSource::honest(body.clone());
    let report = run(
        full,
        Output::File(&scratch.path),
        4,
        1,
        Checksum::Sha256,
        true,
    )
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
                validator: None,
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
    let first = run(
        partial,
        Output::File(&scratch.path),
        8,
        1,
        Checksum::Sha256,
        true,
    )
    .await;
    assert!(first.is_err(), "the first pass fails on the dropped tail");

    // Resume with a different parallelism. The plan is rebuilt from the bytes present, so the new chunk
    // count re-tiles only what is missing, and the download still verifies to the full digest.
    let full = FakeSource::honest(body.clone());
    let report = run(
        full,
        Output::File(&scratch.path),
        3,
        1,
        Checksum::Sha256,
        true,
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
async fn a_changed_validator_discards_the_partial_and_restarts_clean() {
    // A range source that carries a validator and can drop everything from an offset, so a first pass
    // leaves a partial tagged with its validator.
    struct TaggedSource {
        body: Arc<Vec<u8>>,
        validator: String,
        fail_from: u64,
    }
    impl Source for TaggedSource {
        async fn probe(&self) -> Result<Probe, Error> {
            Ok(Probe {
                length: self.body.len() as u64,
                supports_ranges: true,
                filename: None,
                content_type: None,
                checksum: None,
                validator: Some(self.validator.clone()),
            })
        }
        async fn fetch(&self, range: Option<ByteRange>) -> Result<ByteStream, Error> {
            let range = range.expect("a range-capable fetch");
            if range.start >= self.fail_from {
                return Err(Error::Transport(Box::new(std::io::Error::other("drop"))));
            }
            let start = range.start as usize;
            let end = (range.end as usize).min(self.body.len());
            Ok(pieces(&self.body[start..end]))
        }
    }

    let scratch = Scratch::new("validator-change");

    // First pass: version one lands its early chunks then drops, leaving a partial tagged "v1".
    let v1 = sample_body(4096);
    let first = run(
        TaggedSource {
            body: Arc::new(v1.clone()),
            validator: "\"v1\"".to_owned(),
            fail_from: 3072,
        },
        Output::File(&scratch.path),
        4,
        1,
        Checksum::Sha256,
        true,
    )
    .await;
    assert!(first.is_err(), "the first pass drops its tail");
    assert!(scratch.part().exists(), "a partial is left behind");

    // Second pass: the resource is now a different version of the *same length*, so the length check
    // alone cannot tell it changed; only the validator can. A resume that reused the v1 prefix would
    // splice v1 and v2 bytes into a corrupt file. The validator mismatch must force a clean restart, so
    // the output is exactly v2.
    let v2: Vec<u8> = v1.iter().map(|byte| byte ^ 0xff).collect();
    let report = run(
        TaggedSource {
            body: Arc::new(v2.clone()),
            validator: "\"v2\"".to_owned(),
            fail_from: u64::MAX,
        },
        Output::File(&scratch.path),
        4,
        1,
        Checksum::Sha256,
        true,
    )
    .await
    .expect("the restart completes against the new version");

    assert_eq!(
        report.hash.as_deref(),
        Some(sha256_hex(&v2).as_str()),
        "the output is wholly the new version, not a v1/v2 splice"
    );
    assert_eq!(
        std::fs::read(&scratch.path).expect("output present"),
        v2,
        "every byte on disk is the new version's"
    );
}

#[tokio::test]
async fn a_non_range_source_streams_and_verifies() {
    let body = sample_body(3000);
    let source = FakeSource::new(body.clone(), false, Behavior::Honest);
    let scratch = Scratch::new("stream");
    let report = run(
        source,
        Output::File(&scratch.path),
        5,
        10,
        Checksum::Sha256,
        false,
    )
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
    let report = run(
        source,
        Output::File(&scratch.path),
        5,
        10,
        Checksum::None,
        false,
    )
    .await
    .expect("a download with hashing off still completes");

    assert_eq!(report.length, body.len() as u64);
    assert_eq!(
        report.hash, None,
        "no digest is certified when none is asked"
    );
}

#[tokio::test]
async fn discard_verifies_but_writes_no_file() {
    let body = sample_body(4096);
    let source = FakeSource::honest(body.clone());
    let scratch = Scratch::new("discard");
    // A discard scatters and verifies exactly as a file would, so the report is identical, but it keeps
    // nothing: no output file and no `.xget` beside the (unused) scratch path.
    let report = run(source, Output::Discard, 5, 10, Checksum::Sha256, false)
        .await
        .expect("a discard download verifies");

    assert_eq!(report.length, body.len() as u64, "the whole resource");
    assert_eq!(
        report.hash.as_deref(),
        Some(sha256_hex(&body).as_str()),
        "a discard certifies the same digest a file would"
    );
    assert!(!scratch.path.exists(), "a discard leaves no output file");
    assert!(
        !scratch.part().exists(),
        "a discard leaves no partial beside the output"
    );
}

#[tokio::test]
async fn writer_streams_the_exact_bytes_and_hash() {
    let body = sample_body(4096);

    // Stream the verified bytes into an in-memory buffer.
    let mut buf: Vec<u8> = Vec::new();
    let streamed = {
        let source = FakeSource::honest(body.clone());
        run(
            source,
            Output::Writer(&mut buf),
            5,
            10,
            Checksum::Sha256,
            false,
        )
        .await
        .expect("a writer download streams the verified bytes")
    };

    // The same source written to a file, to compare the reports side by side.
    let scratch = Scratch::new("writer-ref");
    let file = {
        let source = FakeSource::honest(body.clone());
        run(
            source,
            Output::File(&scratch.path),
            5,
            10,
            Checksum::Sha256,
            false,
        )
        .await
        .expect("the reference file download succeeds")
    };

    assert_eq!(buf, body, "the writer received every byte in order");
    assert_eq!(
        streamed.hash, file.hash,
        "a writer certifies the same digest a file does"
    );
    assert_eq!(
        streamed.hash.as_deref(),
        Some(sha256_hex(&body).as_str()),
        "the streamed digest matches the known bytes"
    );
    assert_eq!(streamed.length, body.len() as u64);
}

#[tokio::test]
async fn a_tee_delivers_the_same_bytes_to_a_file_and_a_writer() {
    let body = sample_body(4096);
    let source = FakeSource::honest(body.clone());
    let scratch = Scratch::new("tee");

    // Tee the verified bytes to a file and an in-memory writer at once. The file is finalized by
    // rename; the writer receives every byte. Both must be byte-exact and share the one digest.
    let mut buf: Vec<u8> = Vec::new();
    let report = {
        let sink = Output::tee(&scratch.path, &mut buf);
        run(source, sink, 5, 10, Checksum::Sha256, false)
            .await
            .expect("a tee download verifies and finalizes")
    };

    let on_disk = std::fs::read(&scratch.path).expect("the tee's file was finalized into place");
    assert_eq!(on_disk, body, "the file received every byte in order");
    assert_eq!(buf, body, "the writer received every byte in order");
    assert_eq!(
        report.hash.as_deref(),
        Some(sha256_hex(&body).as_str()),
        "the tee certifies the digest of the bytes both sinks received"
    );
    assert_eq!(report.length, body.len() as u64);
    assert!(
        !scratch.part().exists(),
        "the scratch was renamed away, not left beside the output"
    );
}

#[tokio::test]
async fn the_control_file_records_the_source_url_for_a_standalone_resume() {
    // A range source that advertises a re-openable identity (its URL) and drops its tail, so a first
    // pass leaves a partial whose control should carry that URL for `xget path/to/file.xget`.
    struct TaggedSource {
        body: Arc<Vec<u8>>,
        fail_from: u64,
    }
    impl Source for TaggedSource {
        fn identity(&self) -> Option<String> {
            Some("https://example.com/thing.bin".to_owned())
        }
        async fn probe(&self) -> Result<Probe, Error> {
            Ok(Probe {
                length: self.body.len() as u64,
                supports_ranges: true,
                ..Probe::default()
            })
        }
        async fn fetch(&self, range: Option<ByteRange>) -> Result<ByteStream, Error> {
            let range = range.expect("a range fetch");
            if range.start >= self.fail_from {
                return Err(Error::Transport(Box::new(std::io::Error::other("drop"))));
            }
            let start = range.start as usize;
            let end = (range.end as usize).min(self.body.len());
            Ok(pieces(&self.body[start..end]))
        }
    }

    let scratch = Scratch::new("control-url");
    let _ = run(
        TaggedSource {
            body: Arc::new(sample_body(4096)),
            fail_from: 3072,
        },
        Output::File(&scratch.path),
        4,
        1,
        Checksum::Sha256,
        true,
    )
    .await;

    assert!(scratch.part().exists(), "a partial is left behind");
    assert_eq!(
        xget::control_source(&scratch.part()).await.as_deref(),
        Some("https://example.com/thing.bin"),
        "the .xget records the source URL so a standalone resume can rebuild the source"
    );

    // The offline inspection reads the same control without any network.
    let info = xget::inspect(&scratch.part())
        .await
        .expect("a valid control inspects");
    assert_eq!(
        info.source.as_deref(),
        Some("https://example.com/thing.bin")
    );
    assert_eq!(
        info.checksum,
        Some(Checksum::Sha256),
        "the .xget records the algorithm so a standalone resume verifies the same way"
    );
    assert_eq!(info.total, 4096);
    assert!(
        info.downloaded >= 3072 && info.downloaded < info.total,
        "the completed chunks are recorded as downloaded, got {}",
        info.downloaded
    );
    assert!(
        xget::inspect(&scratch.path).await.is_none(),
        "a plain (non-control) file does not inspect as one"
    );
}

#[tokio::test]
async fn the_builder_drives_a_download_like_the_function() {
    let body = sample_body(4096);
    let scratch = Scratch::new("builder");

    // The URL-first builder over any Source: from(source).chunks(..).checksum(..).write(output).
    let report = xget::from(FakeSource::honest(body.clone()))
        .chunks(4)
        .checksum(Checksum::Sha256)
        .write(Output::File(&scratch.path))
        .await
        .expect("the builder download succeeds");

    assert_eq!(report.length, body.len() as u64);
    assert_eq!(
        report.hash.as_deref(),
        Some(sha256_hex(&body).as_str()),
        "the builder verifies to the same digest the function does"
    );
    assert_eq!(
        std::fs::read(&scratch.path).expect("output present"),
        body,
        "the builder wrote every byte"
    );
}
