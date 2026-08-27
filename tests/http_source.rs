//! Integration tests for [`HttpSource`] over a real loopback HTTP server.
//!
//! Unlike the engine tests (which drive a fake in-process [`Source`] for determinism), these mount an
//! in-process mock server so the HTTP source's own request building and response validation run over a
//! real socket: a range request must yield a `206` with a matching `Content-Range`, and a server that
//! answers `200` to a range request must be rejected as [`Error::RangeNotHonored`]. No live network is
//! touched.

use libxget::{Checksum, Error, HttpSource, Options, Source, download};
use sha2::{Digest as _, Sha256};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

/// A deterministic body large enough to span several chunks under the default plan.
fn sample_body(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 17 + 3) as u8).collect()
}

/// The lowercase SHA-256 hex of `data`.
fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Parse an HTTP `bytes=start-end` (inclusive) range header into `(start, end_inclusive)`.
fn parse_range(value: &str) -> Option<(usize, usize)> {
    let span = value.strip_prefix("bytes=")?;
    let (start, end) = span.split_once('-')?;
    Some((start.trim().parse().ok()?, end.trim().parse().ok()?))
}

/// A mock server that honors byte ranges: it answers a range request with `206` and a matching
/// `Content-Range`, and a whole-body request with `200`. This is the well-behaved case an
/// `HttpSource` is designed to consume.
fn range_honoring_response(body: Vec<u8>) -> impl Fn(&Request) -> ResponseTemplate {
    move |request: &Request| {
        let total = body.len();
        let Some(range) = request
            .headers
            .get("range")
            .and_then(|value| value.to_str().ok())
            .and_then(parse_range)
        else {
            return ResponseTemplate::new(200).set_body_bytes(body.clone());
        };
        let (start, end_inclusive) = range;
        let end = (end_inclusive + 1).min(total);
        let start = start.min(end);
        ResponseTemplate::new(206)
            .insert_header(
                "content-range",
                format!("bytes {start}-{}/{total}", end.saturating_sub(1)),
            )
            .set_body_bytes(body[start..end].to_vec())
    }
}

/// The [`HttpSource`] for a mounted server's URL, with no extra headers.
fn source(server: &MockServer) -> HttpSource {
    match HttpSource::new(&server.uri(), reqwest::header::HeaderMap::new()) {
        Ok(source) => source,
        Err(error) => panic!("the mock server URL is valid: {error:?}"),
    }
}

#[tokio::test]
async fn probe_reports_length_and_range_support() {
    let body = sample_body(2048);
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(range_honoring_response(body.clone()))
        .mount(&server)
        .await;

    let probe = source(&server)
        .probe()
        .await
        .expect("a range-capable server probes cleanly");

    assert_eq!(probe.length, body.len() as u64, "the total length");
    assert!(probe.supports_ranges, "the 206 marks range support");
}

#[tokio::test]
async fn a_clean_parallel_http_download_verifies_to_the_right_hash() {
    let body = sample_body(4096);
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(range_honoring_response(body.clone()))
        .mount(&server)
        .await;

    let dir = std::env::temp_dir().join(format!(
        "libxget-http-clean-{}-{}.bin",
        std::process::id(),
        line!()
    ));
    let options = Options {
        parts: 4,
        checksum: Checksum::Sha256,
        ..Options::default()
    };
    let report = download(&source(&server), &dir, options, &())
        .await
        .expect("a parallel HTTP download succeeds");

    assert_eq!(report.length, body.len() as u64);
    assert_eq!(
        report.hash.as_deref(),
        Some(sha256_hex(&body).as_str()),
        "the HTTP download verifies to the known hash"
    );
    assert_eq!(
        std::fs::read(&dir).expect("output present"),
        body,
        "every ranged chunk landed at its offset"
    );
    let _ = std::fs::remove_file(&dir);
}

#[tokio::test]
async fn a_server_that_ignores_the_range_is_rejected() {
    let body = sample_body(2048);
    let server = MockServer::start().await;
    // Always answer 200 with the whole body, even to a range request: the source must reject this
    // rather than splice whole-file bytes into a chunk.
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body.clone()))
        .mount(&server)
        .await;

    let source = source(&server);
    // The probe sees 200 and reports no range support, so drive fetch directly to exercise the range
    // validation an engine chunk would hit. The Ok variant is a boxed stream and not `Debug`, so match
    // rather than `expect_err`.
    match source
        .fetch(Some(libxget::ByteRange { start: 0, end: 100 }))
        .await
    {
        Err(Error::RangeNotHonored { .. }) => {}
        Err(other) => panic!("expected RangeNotHonored, got {other:?}"),
        Ok(_) => panic!("a 200 answer to a range request must not be accepted"),
    }
}
