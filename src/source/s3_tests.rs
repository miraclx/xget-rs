//! Unit tests for [`S3Source`]'s response-range validation, over a loopback mock server.
//!
//! The public constructor reads the ambient AWS chain (environment, shared config, IMDS), which a test
//! must not touch, so these assemble the source directly with an explicit client: fixed test
//! credentials, a fixed region, retries disabled, and path-style addressing against a wiremock endpoint.
//! The module is a child of `s3`, so it reaches the private `client` field. No live network is touched.

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_smithy_types::retry::RetryConfig;
use futures::StreamExt as _;
use wiremock::matchers::{header, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::S3Source;
use crate::{ByteRange, Error, Source};

/// The bucket and key every test source names; the mock server ignores both.
const BUCKET: &str = "xget-test-bucket";
const KEY: &str = "object.bin";

/// A deterministic body: byte `i` depends on `i`, so a window landing at the wrong offset is visible.
fn sample_body(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 31 + 7) as u8).collect()
}

/// Build a source with a fully explicit client pointed at `server`'s loopback endpoint, so nothing is
/// read from the ambient AWS configuration.
fn source(server: &MockServer) -> S3Source {
    let config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(Credentials::new(
            "xget-test-access-key",
            "xget-test-secret-key",
            None,
            None,
            "xget-s3-tests",
        ))
        .region(Region::new("us-east-1"))
        .endpoint_url(server.uri())
        .force_path_style(true)
        .retry_config(RetryConfig::disabled())
        .build();
    S3Source {
        client: aws_sdk_s3::Client::from_conf(config),
        bucket: BUCKET.to_owned(),
        key: KEY.to_owned(),
    }
}

/// Drain a fetch expected to succeed into the bytes it yielded.
async fn fetched_bytes(source: &S3Source, range: ByteRange) -> Vec<u8> {
    let mut stream = match source.fetch(Some(range)).await {
        Ok(stream) => stream,
        Err(error) => panic!("a valid window must be accepted, got {error:?}"),
    };
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => out.extend_from_slice(&bytes),
            Err(error) => panic!("a yielded chunk must be readable, got {error:?}"),
        }
    }
    out
}

/// Assert a fetch is rejected as a range the store did not serve, naming the range it was asked for.
async fn assert_range_not_honored(source: &S3Source, range: ByteRange) {
    match source.fetch(Some(range)).await {
        Err(Error::RangeNotHonored { requested }) => assert_eq!(requested, range),
        Err(other) => panic!("expected a RangeNotHonored error, got {other:?}"),
        Ok(_) => panic!("a store that did not serve the requested window must be rejected"),
    }
}

#[tokio::test]
async fn a_matching_content_range_is_accepted_and_serves_the_window() {
    let object = sample_body(1000);
    let range = ByteRange {
        start: 100,
        end: 200,
    };
    let server = MockServer::start().await;
    // The store answers `206` with a `Content-Range` naming the requested start, and a window body. The
    // range-header matcher pins that our inclusive-end request is `bytes=100-199`.
    Mock::given(method("GET"))
        .and(header("range", "bytes=100-199"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("content-range", "bytes 100-199/1000")
                .set_body_bytes(object[100..200].to_vec()),
        )
        .mount(&server)
        .await;

    let bytes = fetched_bytes(&source(&server), range).await;
    assert_eq!(
        bytes.as_slice(),
        &object[100..200],
        "the exact window is served"
    );
}

#[tokio::test]
async fn a_content_range_that_names_a_different_start_is_rejected() {
    let range = ByteRange {
        start: 100,
        end: 200,
    };
    let server = MockServer::start().await;
    // `Content-Range` is present but points at offset 0, not the requested 100: the bytes would land at
    // the wrong offsets, so the fetch must be rejected.
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("content-range", "bytes 0-99/1000")
                .set_body_bytes(sample_body(100)),
        )
        .mount(&server)
        .await;

    assert_range_not_honored(&source(&server), range).await;
}

#[tokio::test]
async fn an_omitted_content_range_with_the_exact_length_is_accepted() {
    let range = ByteRange {
        start: 100,
        end: 200,
    };
    let server = MockServer::start().await;
    // No `Content-Range`; the body is exactly the requested window, the fallback evidence that the store
    // served it.
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(sample_body(100)))
        .mount(&server)
        .await;

    let bytes = fetched_bytes(&source(&server), range).await;
    assert_eq!(
        bytes.as_slice(),
        sample_body(100).as_slice(),
        "the whole requested window arrives, byte for byte"
    );
}

#[tokio::test]
async fn an_omitted_content_range_with_a_mismatched_length_is_rejected() {
    let range = ByteRange {
        start: 100,
        end: 200,
    };
    let server = MockServer::start().await;
    // No `Content-Range`, and a body shorter than the 100-byte window: with no evidence it served the
    // window, the fetch must be rejected.
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(sample_body(64)))
        .mount(&server)
        .await;

    assert_range_not_honored(&source(&server), range).await;
}

#[tokio::test]
async fn an_omitted_content_range_with_an_omitted_length_is_rejected() {
    let range = ByteRange {
        start: 100,
        end: 200,
    };
    let server = MockServer::start().await;
    // A chunked response carries neither `Content-Range` nor `Content-Length`, so the store offers
    // nothing to prove it served the window: the length fallback sees `None` and must reject. The body
    // is deliberately the exact window length, so an accepted fetch here would mean a length was present.
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("transfer-encoding", "chunked")
                .set_body_bytes(sample_body(100)),
        )
        .mount(&server)
        .await;

    assert_range_not_honored(&source(&server), range).await;
}

#[tokio::test]
async fn a_whole_object_response_that_ignores_the_range_is_rejected() {
    let range = ByteRange {
        start: 100,
        end: 200,
    };
    let server = MockServer::start().await;
    // A store that answered the ranged GET with the whole object as a `200`: the length is the object's,
    // not the window's, so it must be rejected rather than spliced.
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(sample_body(1000)))
        .mount(&server)
        .await;

    assert_range_not_honored(&source(&server), range).await;
}

#[tokio::test]
async fn a_matching_content_range_start_is_trusted_over_a_disagreeing_length() {
    let range = ByteRange {
        start: 100,
        end: 200,
    };
    let server = MockServer::start().await;
    // A `Content-Range` that names the requested start decides it, even if the body is short: the source's
    // job is to catch a window served at the wrong offset, and the engine separately gates the streamed
    // length against the chunk, so a short body here is the engine's error, not the source's.
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("content-range", "bytes 100-149/1000")
                .set_body_bytes(sample_body(50)),
        )
        .mount(&server)
        .await;

    let bytes = fetched_bytes(&source(&server), range).await;
    assert_eq!(bytes.len(), 50, "what the store served is passed through");
}
