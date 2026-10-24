//! Authored actual-SDK loopback fixtures only. No execution result is claimed by this source.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use core::num::NonZeroUsize;
use core::sync::atomic::{AtomicU64, Ordering};
use core::task::Poll;
use core::time::Duration;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt as _;
use std::path::{Path, PathBuf};

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_smithy_types::retry::RetryConfig;
use futures::StreamExt as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{Address, S3ErrorKind};
#[cfg(unix)]
use crate::client::SavedRun;
use crate::client::{BodyError, Client, Error, SourceError};
use crate::input::Input;
use crate::transfer::{ByteCount, Digest, Length, Measurement};

const BUCKET: &str = "fixture-bucket";
const ABC: &str = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

fn sdk(endpoint: &str) -> aws_sdk_s3::Client {
    let config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(Credentials::new(
            "fixture-access-marker",
            "fixture-secret-marker",
            Some("fixture-bearer-marker".to_owned()),
            None,
            "common-s3-loopback",
        ))
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .retry_config(RetryConfig::disabled())
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

fn client(endpoint: &str) -> Client {
    Client::builder().s3_client(sdk(endpoint)).build().unwrap()
}

async fn head(server: &MockServer, key: &str, length: u64) {
    Mock::given(method("HEAD"))
        .and(path(format!("/{BUCKET}/{key}")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", length.to_string())
                .insert_header("etag", "\"opaque-v1\"")
                .insert_header("x-amz-version-id", "version-one"),
        )
        .mount(server)
        .await;
}

async fn get(server: &MockServer, key: &str, bytes: &'static [u8]) {
    Mock::given(method("GET"))
        .and(path(format!("/{BUCKET}/{key}")))
        .and(header("if-match", "\"opaque-v1\""))
        .and(query_param("versionId", "version-one"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"opaque-v1\"")
                .insert_header("x-amz-version-id", "version-one")
                .set_body_bytes(bytes),
        )
        .mount(server)
        .await;
}

fn xml_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn listing(keys: &[&str], truncated: &str, next: Option<&str>) -> String {
    let entries: Vec<_> = keys.iter().map(|key| (*key, 3)).collect();
    listing_entries(&entries, truncated, next)
}

fn listing_entries(entries: &[(&str, u64)], truncated: &str, next: Option<&str>) -> String {
    let mut xml = format!(
        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{BUCKET}</Name><IsTruncated>{truncated}</IsTruncated>"
    );
    for (key, size) in entries {
        xml.push_str(&format!("<Contents><Key>{}</Key><Size>{size}</Size><ETag>&quot;opaque-v1&quot;</ETag></Contents>", xml_text(key)));
    }
    if let Some(next) = next {
        xml.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            xml_text(next)
        ));
    }
    xml.push_str("</ListBucketResult>");
    xml
}

async fn list(server: &MockServer, xml: String) {
    Mock::given(method("GET"))
        .and(query_param("list-type", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(xml, "application/xml"))
        .mount(server)
        .await;
}

async fn count(server: &MockServer, method: &str) -> usize {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|request| request.method.as_str() == method)
        .count()
}

#[test]
fn object_address_preserves_literal_key_and_explicit_version_independently() {
    let input = Input::parse("s3://fixture-bucket/a%2Fb%252Fc?versionId=rev%2B1").unwrap();
    let address = Address::of(&input).unwrap();
    assert_eq!(address.bucket.as_str(), BUCKET);
    assert_eq!(address.key.as_str(), "a/b%2Fc");
    assert_eq!(
        address.version.as_deref().map(String::as_str),
        Some("rev+1")
    );
    assert_eq!(
        input.original_reference(),
        "s3://fixture-bucket/a%2Fb%252Fc?versionId=rev%2B1"
    );
    for reference in [
        "s3://user@fixture-bucket/a",
        "s3://fixture-bucket/a?token=bearer",
        "s3://fixture-bucket/a?versionId=a&versionId=b",
        "s3://fixture-bucket/",
    ] {
        assert!(Address::of(&Input::parse(reference).unwrap()).is_err());
    }
}

#[test]
fn literal_prefix_keeps_partial_leaf_and_never_normalizes_key_separators() {
    for (prefix, key, expected) in [
        ("dir/fo", "dir/foo", "foo"),
        ("dir/", "dir/foo", "foo"),
        ("fo", "foo", "foo"),
        ("", "/foo", "/foo"),
        ("dir/", "dir//foo", "/foo"),
    ] {
        let input = Input::tree(&format!("s3://fixture-bucket/{prefix}")).unwrap();
        let address = Address::of(&input).unwrap();
        assert_eq!(address.relative(key), Some(expected));
    }
}

#[tokio::test]
async fn exact_sdk_head_then_one_conditional_whole_get_uses_frozen_selected_metadata() {
    let server = MockServer::start().await;
    head(&server, "nested/object", 3).await;
    get(&server, "nested/object", b"abc").await;
    let client = client(&server.uri());
    let file = client
        .resolve("s3://fixture-bucket/nested/object")
        .await
        .unwrap()
        .select_one(|_| true)
        .unwrap();
    assert_eq!(file.source_path(), "nested/object");
    assert_eq!(file.member_token().unwrap().as_str(), "nested/object");
    assert_eq!(file.length(), Length::Known(ByteCount(3)));
    assert_eq!(count(&server, "GET").await, 0);
    let mut body = client
        .open(file)
        .await
        .unwrap()
        .expect(ABC.parse::<Digest>().unwrap())
        .unwrap()
        .measurement(Measurement::None)
        .into_body();
    let mut bytes = Vec::new();
    while let Some(chunk) = body.next().await {
        bytes.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(bytes, b"abc");
    let receipt = body.finish().unwrap();
    assert_eq!(receipt.body().checks().matched_requirements().count(), 1);
    assert!(receipt.body().checks().measured().is_none());
    assert_eq!(count(&server, "HEAD").await, 1);
    assert_eq!(count(&server, "GET").await, 1);
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| !request.headers.contains_key("range"))
    );
}

fn oversized_listing() -> String {
    // Without the local wire cap this is a valid complete EMPTY SDK listing: retained-text/count
    // checks alone cannot reject its large ignored XML field before SDK collection/deserialization.
    listing(&[], "false", None).replace(
        "</ListBucketResult>",
        &format!(
            "<Ignored>{}</Ignored></ListBucketResult>",
            "x".repeat(super::metadata::MAX_WIRE_BYTES)
        ),
    )
}

#[tokio::test]
async fn fixed_length_oversized_sdk_metadata_is_rejected_before_a_complete_plan_and_releases_admission()
 {
    let server = MockServer::start().await;
    list(&server, oversized_listing()).await;
    Mock::given(method("HEAD"))
        .and(path("/probe"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let client = Client::builder()
        .max_requests(NonZeroUsize::MIN)
        .s3_client(sdk(&server.uri()))
        .build()
        .unwrap();
    let root = Directory::new();
    let result = client
        .download(Input::tree("s3://fixture-bucket/prefix/").unwrap())
        .to_directory(root.path().join("absent"))
        .plan()
        .await;
    let error = result.unwrap_err();
    assert!(
        matches!(error, Error::S3(ref error) if error.kind() == S3ErrorKind::MetadataLimit),
        "{error:?}"
    );
    assert_eq!(count(&server, "GET").await, 1);
    assert_eq!(count(&server, "HEAD").await, 0);
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
    // Retaining the concrete SDK error must not retain a rejected response stream/permit.
    tokio::time::timeout(
        Duration::from_secs(2),
        client.resolve(format!("{}/probe", server.uri())),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(error, Error::S3(_)));
}

#[tokio::test]
async fn chunked_oversized_sdk_metadata_is_bounded_before_collection_or_xml_deserialization() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            assert!(request.len() < 16384, "bounded fixture request headers");
            if socket.read(&mut byte).await.unwrap() == 0 {
                return;
            }
            request.push(byte[0]);
        }
        if socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.is_err() { return; }
        let xml = oversized_listing();
        for chunk in xml.as_bytes().chunks(8192) {
            if socket
                .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                .await
                .is_err()
                || socket.write_all(chunk).await.is_err()
                || socket.write_all(b"\r\n").await.is_err()
            {
                return;
            }
        }
        let _closed = socket.write_all(b"0\r\n\r\n").await;
    });
    let client = client(&endpoint);
    let root = Directory::new();
    let result = client
        .download(Input::tree("s3://fixture-bucket/prefix/").unwrap())
        .to_directory(root.path().join("absent"))
        .plan()
        .await;
    let error = result.unwrap_err();
    assert!(
        matches!(error, Error::S3(ref error) if error.kind() == S3ErrorKind::MetadataLimit),
        "{error:?}"
    );
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn large_head_object_lengths_and_successful_get_payloads_are_not_metadata_wire_capped() {
    let server = MockServer::start().await;
    let length = super::metadata::MAX_WIRE_BYTES + 17;
    head(&server, "large", length as u64).await;
    Mock::given(method("GET"))
        .and(path(format!("/{BUCKET}/large")))
        .and(header("if-match", "\"opaque-v1\""))
        .and(query_param("versionId", "version-one"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"opaque-v1\"")
                .insert_header("x-amz-version-id", "version-one")
                .set_body_bytes(vec![b'x'; length]),
        )
        .mount(&server)
        .await;
    let client = client(&server.uri());
    let mut body = client
        .open("s3://fixture-bucket/large")
        .await
        .unwrap()
        .measurement(Measurement::None)
        .into_body();
    let mut received = 0;
    while let Some(chunk) = body.next().await {
        received += chunk.unwrap().len();
    }
    assert_eq!(received, length);
    assert_eq!(
        body.finish().unwrap().body().progress().released,
        ByteCount(length as u64)
    );
}

#[tokio::test]
async fn missing_and_denied_exact_objects_never_list_or_open_a_payload() {
    for (status, kind) in [(404, S3ErrorKind::Missing), (403, S3ErrorKind::Denied)] {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .respond_with(ResponseTemplate::new(status))
            .mount(&server)
            .await;
        let error = client(&server.uri())
            .resolve("s3://fixture-bucket/absent")
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::S3(ref error) if error.kind() == kind),
            "{error:?}"
        );
        assert_eq!(count(&server, "GET").await, 0);
    }
}

#[tokio::test]
async fn exact_version_etag_and_size_disagreement_fail_before_payload_release() {
    for case in ["etag", "version", "size", "conditional"] {
        let server = MockServer::start().await;
        head(&server, "object", 3).await;
        let status = if case == "conditional" { 412 } else { 200 };
        Mock::given(method("GET"))
            .and(header("if-match", "\"opaque-v1\""))
            .and(query_param("versionId", "version-one"))
            .respond_with(
                ResponseTemplate::new(status)
                    .insert_header(
                        "etag",
                        if case == "etag" {
                            "\"other\""
                        } else {
                            "\"opaque-v1\""
                        },
                    )
                    .insert_header(
                        "x-amz-version-id",
                        if case == "version" {
                            "other"
                        } else {
                            "version-one"
                        },
                    )
                    .set_body_bytes(if case == "size" {
                        b"abcd".as_slice()
                    } else {
                        b"abc".as_slice()
                    }),
            )
            .mount(&server)
            .await;
        let error = client(&server.uri())
            .open("s3://fixture-bucket/object")
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::VersionChanged)
                || matches!(error, Error::S3(ref error) if error.kind() == S3ErrorKind::VersionChanged),
            "{case}: {error:?}"
        );
    }
}

#[tokio::test]
async fn explicit_version_query_must_be_repeated_by_head_and_is_never_treated_as_a_digest() {
    let server = MockServer::start().await;
    head(&server, "object", 3).await;
    let error = client(&server.uri())
        .resolve("s3://fixture-bucket/object?versionId=other")
        .await
        .unwrap_err();
    assert!(matches!(error, Error::VersionChanged));
    assert_eq!(count(&server, "GET").await, 0);
}

#[tokio::test]
async fn tree_uses_real_continuation_tokens_and_completes_before_path_or_literal_selection() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(query_param("list-type", "2"))
        .respond_with(|request: &wiremock::Request| {
            let continuation = request
                .url
                .query_pairs()
                .find(|(name, _)| name == "continuation-token")
                .map(|(_, token)| token.into_owned());
            let xml = match continuation.as_deref() {
                None => listing(&["prefix/a"], "true", Some("real+opaque/token=")),
                Some("real+opaque/token=") => listing(&["prefix/nested/b"], "false", None),
                _ => return ResponseTemplate::new(400),
            };
            ResponseTemplate::new(200).set_body_raw(xml, "application/xml")
        })
        .mount(&server)
        .await;
    for key in ["prefix/a", "prefix/nested/b"] {
        head(&server, key, 3).await;
    }
    let client = client(&server.uri());
    let input = || {
        Input::tree("s3://fixture-bucket/prefix/")
            .unwrap()
            .include("nested/**")
            .unwrap()
    };
    let full = client
        .resolve(Input::tree("s3://fixture-bucket/prefix/").unwrap())
        .await
        .unwrap();
    let accounted = full.metadata_bytes();
    let selected = client.resolve(input()).await.unwrap();
    assert_eq!(selected.files().len(), 1);
    assert!(selected.metadata_bytes() >= accounted);
    let file = selected.files().next().unwrap();
    assert_eq!(file.source_path(), "nested/b");
    assert_eq!(file.member_token().unwrap().as_str(), "prefix/nested/b");
    let literal = client
        .resolve(
            Input::tree("s3://fixture-bucket/prefix/")
                .unwrap()
                .select_id("prefix/a")
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(literal.files().len(), 1);
    assert_eq!(literal.files().next().unwrap().source_path(), "a");
    assert_eq!(count(&server, "HEAD").await, 6);
}

#[tokio::test]
async fn two_page_literal_selection_preserves_the_complete_accounted_charge_including_markers() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(query_param("list-type", "2"))
        .respond_with(|request: &wiremock::Request| {
            let next = request
                .url
                .query_pairs()
                .find(|(name, _)| name == "continuation-token")
                .map(|(_, token)| token.into_owned());
            let xml = match next.as_deref() {
                None => listing_entries(
                    &[("prefix/a", 3), ("prefix/nested/", 0)],
                    "true",
                    Some("second-page"),
                ),
                Some("second-page") => listing_entries(
                    &[("prefix/nested/b", 3), ("prefix/empty/", 0)],
                    "false",
                    None,
                ),
                _ => return ResponseTemplate::new(400),
            };
            ResponseTemplate::new(200).set_body_raw(xml, "application/xml")
        })
        .mount(&server)
        .await;
    for (key, length) in [
        ("prefix/a", 3),
        ("prefix/nested/", 0),
        ("prefix/nested/b", 3),
        ("prefix/empty/", 0),
    ] {
        head(&server, key, length).await;
    }
    let client = client(&server.uri());
    let input = || Input::tree("s3://fixture-bucket/prefix/").unwrap();
    let complete = client.resolve(input()).await.unwrap();
    let charge = complete.metadata_bytes();
    assert_eq!(complete.files().len(), 2);
    assert_eq!(complete.directories().len(), 2);
    let one = client
        .resolve(input().select_id("prefix/a").unwrap())
        .await
        .unwrap();
    assert_eq!(one.files().len(), 1);
    assert_eq!(one.directories().len(), 2);
    assert_eq!(one.metadata_bytes(), charge);
    let marker = client
        .resolve(input().select_id("prefix/empty/").unwrap())
        .await
        .unwrap();
    assert_eq!(marker.files().len(), 0);
    assert_eq!(marker.directories().len(), 2);
    assert_eq!(marker.metadata_bytes(), charge);
    assert_eq!(count(&server, "GET").await, 6, "listing pages only");
    assert_eq!(
        count(&server, "HEAD").await,
        12,
        "all metadata before selection"
    );
}

#[tokio::test]
async fn each_positive_include_matches_the_complete_file_scope_before_exclusions() {
    let server = MockServer::start().await;
    list(&server, listing(&["prefix/A"], "false", None)).await;
    head(&server, "prefix/A", 3).await;
    let client = client(&server.uri());
    let error = client
        .resolve(
            Input::tree("s3://fixture-bucket/prefix/")
                .unwrap()
                .include("A")
                .unwrap()
                .include("B")
                .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::PathSelection));
    let duplicate = client
        .resolve(
            Input::tree("s3://fixture-bucket/prefix/")
                .unwrap()
                .include("A")
                .unwrap()
                .include("A")
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(duplicate.files().len(), 1);
    server.reset().await;
    list(&server, listing(&["prefix/A", "prefix/B"], "false", None)).await;
    head(&server, "prefix/A", 3).await;
    head(&server, "prefix/B", 3).await;
    let selected = client
        .resolve(
            Input::tree("s3://fixture-bucket/prefix/")
                .unwrap()
                .include("A")
                .unwrap()
                .include("B")
                .unwrap()
                .exclude("A")
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(selected.files().next().unwrap().source_path(), "B");
    assert_eq!(selected.files().len(), 1);
}

#[tokio::test]
async fn selecting_a_frozen_file_keeps_prior_criterion_proofs_but_new_positives_need_subset_matches()
 {
    let server = MockServer::start().await;
    list(&server, listing(&["prefix/A", "prefix/B"], "false", None)).await;
    head(&server, "prefix/A", 3).await;
    head(&server, "prefix/B", 3).await;
    let client = client(&server.uri());
    let selected = client
        .resolve(
            Input::tree("s3://fixture-bucket/prefix/")
                .unwrap()
                .include("A")
                .unwrap()
                .include("B")
                .unwrap(),
        )
        .await
        .unwrap()
        .select_one(|file| file.source_path() == "A")
        .unwrap();
    let plan = client
        .download(selected)
        .to_file("unused")
        .plan()
        .await
        .unwrap();
    let file = plan.files.into_iter().next().unwrap().file;
    let input = Input::from(file).include("B").unwrap();
    assert!(matches!(
        client.download(input).to_file("unused").plan().await,
        Err(Error::PathSelection)
    ));
    assert_eq!(count(&server, "HEAD").await, 2);
    assert_eq!(count(&server, "GET").await, 1);
}

#[tokio::test]
async fn positive_on_empty_is_unmatched_but_exclusion_only_empty_is_a_noop() {
    let server = MockServer::start().await;
    list(&server, listing(&[], "false", None)).await;
    let client = client(&server.uri());
    let error = client
        .resolve(
            Input::tree("s3://fixture-bucket/prefix/")
                .unwrap()
                .include("A")
                .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::PathSelection));
    let empty = client
        .resolve(
            Input::tree("s3://fixture-bucket/prefix/")
                .unwrap()
                .exclude("A")
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(empty.files().len(), 0);
    assert_eq!(count(&server, "HEAD").await, 0);
}

#[tokio::test]
async fn resolve_all_shares_entry_budget_and_does_not_let_one_occurrence_hide_another_include() {
    let server = MockServer::start().await;
    list(&server, listing(&["prefix/A"], "false", None)).await;
    head(&server, "prefix/A", 3).await;
    let client = client(&server.uri());
    let error = client
        .resolve_all([
            Input::tree("s3://fixture-bucket/prefix/")
                .unwrap()
                .include("A")
                .unwrap(),
            Input::tree("s3://fixture-bucket/prefix/")
                .unwrap()
                .include("B")
                .unwrap(),
        ])
        .await
        .unwrap_err();
    assert!(matches!(error, Error::PathSelection));
    assert_eq!(
        count(&server, "HEAD").await,
        2,
        "complete both scopes before selection"
    );
    server.reset().await;
    let keys: Vec<_> = (0..128)
        .map(|index| format!("prefix/file-{index}"))
        .collect();
    let refs: Vec<_> = keys.iter().map(String::as_str).collect();
    list(&server, listing(&refs, "false", None)).await;
    head(&server, "first", 3).await;
    let error = client
        .resolve_all([
            Input::parse("s3://fixture-bucket/first").unwrap(),
            Input::tree("s3://fixture-bucket/prefix/")
                .unwrap()
                .exclude("**")
                .unwrap(),
        ])
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Inventory(_)));
    assert_eq!(count(&server, "HEAD").await, 1);
}

#[tokio::test]
async fn complete_nonempty_tree_with_unmatched_patterns_or_stale_key_is_not_empty_success() {
    let server = MockServer::start().await;
    list(&server, listing(&["prefix/a"], "false", None)).await;
    head(&server, "prefix/a", 3).await;
    let client = client(&server.uri());
    let error = client
        .resolve(
            Input::tree("s3://fixture-bucket/prefix/")
                .unwrap()
                .include("nothing")
                .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::PathSelection));
    let error = client
        .resolve(
            Input::tree("s3://fixture-bucket/prefix/")
                .unwrap()
                .select_id("prefix/stale")
                .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Selection));
    assert_eq!(count(&server, "HEAD").await, 2);
}

#[tokio::test]
async fn duplicate_keys_token_loops_incomplete_pages_and_member_overbudget_never_produce_a_plan() {
    let too_many: Vec<_> = (0..129)
        .map(|index| format!("prefix/file-{index}"))
        .collect();
    let too_many_refs: Vec<_> = too_many.iter().map(String::as_str).collect();
    let cases = [
        listing(&["prefix/a", "prefix/a"], "false", None),
        listing(&["prefix/a"], "true", None),
        listing(&[], "true", Some("loop")),
        listing(&["prefix/a"], "false", Some("unexpected")),
        listing(&[], "false", None).replace("<IsTruncated>false</IsTruncated>", ""),
        listing(&too_many_refs, "false", None),
    ];
    for xml in cases {
        let server = MockServer::start().await;
        list(&server, xml).await;
        let result = client(&server.uri())
            .download(
                Input::tree("s3://fixture-bucket/prefix/")
                    .unwrap()
                    .include("nothing")
                    .unwrap(),
            )
            .to_directory("unused")
            .plan()
            .await;
        assert!(matches!(
            result,
            Err(Error::S3(_)) | Err(Error::Inventory(_))
        ));
        assert_eq!(count(&server, "HEAD").await, 0);
        assert!(count(&server, "GET").await <= 2);
    }
}

#[tokio::test]
async fn retained_text_and_shared_cross_input_member_budgets_apply_before_filtering() {
    let server = MockServer::start().await;
    let keys: Vec<_> = (0..128)
        .map(|index| format!("prefix/{}-{index}", "a".repeat(950)))
        .collect();
    let refs: Vec<_> = keys.iter().map(String::as_str).collect();
    list(&server, listing(&refs, "false", None)).await;
    // Repeated pattern intent is charged per discovered member, not only once for the input.
    let mut input = Input::tree("s3://fixture-bucket/prefix/").unwrap();
    for _ in 0..10 {
        input = input.include(&"x".repeat(1024)).unwrap();
    }
    let result = client(&server.uri()).resolve(input).await;
    assert!(matches!(result, Err(Error::Inventory(_))));
    assert_eq!(count(&server, "HEAD").await, 0);

    let server = MockServer::start().await;
    let keys: Vec<_> = (0..128)
        .map(|index| format!("prefix/file-{index}"))
        .collect();
    let refs: Vec<_> = keys.iter().map(String::as_str).collect();
    list(&server, listing(&refs, "false", None)).await;
    for key in &keys {
        head(&server, key, 3).await;
    }
    head(&server, "first", 3).await;
    let result = client(&server.uri())
        .download_all([
            Input::parse("s3://fixture-bucket/first").unwrap(),
            Input::tree("s3://fixture-bucket/prefix/")
                .unwrap()
                .include("nothing")
                .unwrap(),
        ])
        .to_directory("unused")
        .plan()
        .await;
    assert!(matches!(result, Err(Error::Inventory(_))));
    assert_eq!(count(&server, "HEAD").await, 1);
}

#[tokio::test]
async fn zero_byte_markers_are_genuine_directory_metadata_separate_from_file_selection() {
    use crate::inventory::{DirectoryKind, VersionEvidence};

    let server = MockServer::start().await;
    let entries = [
        ("prefix/", 0),
        ("prefix/nested/", 0),
        ("prefix/nested/object", 3),
        ("prefix/empty/", 0),
    ];
    list(&server, listing_entries(&entries, "false", None)).await;
    for (key, size) in entries {
        head(&server, key, size).await;
    }
    let client = client(&server.uri());
    let input = || {
        Input::tree("s3://fixture-bucket/prefix/")
            .unwrap()
            .under("group")
            .unwrap()
    };
    let inventory = client.resolve(input()).await.unwrap();
    assert_eq!(inventory.files().len(), 1);
    assert_eq!(inventory.directories().len(), 3);
    assert_eq!(
        inventory
            .directories()
            .map(|directory| directory.source_path())
            .collect::<Vec<_>>(),
        ["", "nested", "empty"]
    );
    for directory in inventory.directories() {
        assert_eq!(directory.kind(), DirectoryKind::S3FolderMarker);
        assert_eq!(directory.version(), VersionEvidence::StrongEtag);
        assert_eq!(directory.namespace().unwrap().as_str(), "group");
        let token = directory.member_token().unwrap();
        assert_eq!(token.kind(), crate::input::InputKind::S3);
        assert!(token.as_str().ends_with('/'));
        assert!(!format!("{directory:?}").contains("prefix"));
    }
    assert_eq!(inventory.into_files().count(), 1);
    let markers = client
        .resolve(input().select_id("prefix/nested/").unwrap())
        .await
        .unwrap();
    assert_eq!(markers.files().len(), 0);
    assert_eq!(markers.directories().len(), 3);
    assert_eq!(count(&server, "HEAD").await, 8);
    assert_eq!(
        count(&server, "GET").await,
        2,
        "only listing GETs, no marker payloads"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn directory_only_inspection_keeps_metadata_but_execution_creates_no_root_or_directory_jobs()
{
    let server = MockServer::start().await;
    list(
        &server,
        listing_entries(&[("prefix/", 0), ("prefix/empty/", 0)], "false", None),
    )
    .await;
    head(&server, "prefix/", 0).await;
    head(&server, "prefix/empty/", 0).await;
    let client = client(&server.uri());
    let input = || {
        Input::tree("s3://fixture-bucket/prefix/")
            .unwrap()
            .exclude("nothing")
            .unwrap()
    };
    let inventory = client.resolve(input()).await.unwrap();
    assert_eq!(inventory.files().len(), 0);
    assert_eq!(inventory.directories().len(), 2);
    assert!(inventory.metadata_bytes() > 0);
    let root = Directory::new();
    let output = root.path().join("absent");
    let report = client
        .download(input())
        .to_directory(&output)
        .run()
        .await
        .unwrap();
    assert_eq!(report.completed().len(), 0);
    assert!(report.saved_run().is_none());
    assert!(!output.exists());
    assert_eq!(count(&server, "GET").await, 2);
}

#[tokio::test]
async fn inspected_prefix_example_handles_empty_and_marker_only_scopes_before_download_all() {
    for entries in [&[][..], &[("prefix/", 0)][..]] {
        let server = MockServer::start().await;
        list(&server, listing_entries(entries, "false", None)).await;
        for (key, size) in entries {
            head(&server, key, *size).await;
        }
        let client = client(&server.uri());
        let input = Input::tree("s3://fixture-bucket/prefix/").unwrap();
        let inventory = client.resolve(input).await.unwrap();
        assert_eq!(inventory.directories().len(), entries.len());
        let root = Directory::new();
        let output = root.path().join("absent");
        let completed = if inventory.files().len() == 0 {
            0
        } else {
            client
                .download_all(inventory.into_files())
                .to_directory(&output)
                .run()
                .await
                .unwrap()
                .completed()
                .len()
        };
        assert_eq!(completed, 0);
        assert!(!output.exists());
        assert!(!root.path().join(".xget-runs").exists());
        assert_eq!(count(&server, "GET").await, 1);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn mixed_marker_metadata_does_not_create_empty_directories_or_saved_directory_jobs() {
    let server = MockServer::start().await;
    let entries = [
        ("prefix/", 0),
        ("prefix/empty/", 0),
        ("prefix/nested/object", 3),
    ];
    list(&server, listing_entries(&entries, "false", None)).await;
    for (key, size) in entries {
        head(&server, key, size).await;
    }
    get(&server, "prefix/nested/object", b"abc").await;
    let root = Directory::new();
    let report = client(&server.uri())
        .download(Input::tree("s3://fixture-bucket/prefix/").unwrap())
        .to_directory(root.path())
        .run()
        .await
        .unwrap();
    assert_eq!(report.completed().len(), 1);
    assert_eq!(fs::read(root.path().join("nested/object")).unwrap(), b"abc");
    assert!(!root.path().join("empty").exists());
    assert_eq!(
        SavedRun::inspect(report.saved_run().unwrap())
            .await
            .unwrap()
            .members()
            .len(),
        1
    );
    assert_eq!(count(&server, "GET").await, 2);
}

#[tokio::test]
async fn marker_entries_share_count_and_text_bounds_before_any_file_filtering_or_head() {
    let keys: Vec<_> = (0..129)
        .map(|index| format!("prefix/dir-{index}/"))
        .collect();
    let rows: Vec<_> = keys.iter().map(|key| (key.as_str(), 0)).collect();
    let server = MockServer::start().await;
    list(&server, listing_entries(&rows, "false", None)).await;
    let error = client(&server.uri())
        .resolve(
            Input::tree("s3://fixture-bucket/prefix/")
                .unwrap()
                .include("nothing")
                .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Inventory(_)));
    assert_eq!(count(&server, "HEAD").await, 0);

    let server = MockServer::start().await;
    list(&server, listing_entries(&rows[..128], "false", None)).await;
    let mut input = Input::tree("s3://fixture-bucket/prefix/").unwrap();
    for _ in 0..10 {
        input = input.include(&"x".repeat(1024)).unwrap();
    }
    let error = client(&server.uri()).resolve(input).await.unwrap_err();
    assert!(matches!(error, Error::Inventory(_)));
    assert_eq!(count(&server, "HEAD").await, 0);
}

#[tokio::test]
async fn a_zero_listing_marker_must_still_have_a_matching_conditional_head() {
    let server = MockServer::start().await;
    list(
        &server,
        listing_entries(&[("prefix/dir/", 0)], "false", None),
    )
    .await;
    Mock::given(method("HEAD"))
        .and(path(format!("/{BUCKET}/prefix/dir/")))
        .and(header("if-match", "\"opaque-v1\""))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", "1")
                .insert_header("etag", "\"opaque-v1\""),
        )
        .mount(&server)
        .await;
    let error = client(&server.uri())
        .resolve(Input::tree("s3://fixture-bucket/prefix/").unwrap())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::VersionChanged));
    assert_eq!(count(&server, "GET").await, 1);
}

#[tokio::test]
async fn nonempty_slash_key_payloads_are_explicit_not_silently_discarded_as_directories() {
    for key in ["prefix/", "prefix/folder/"] {
        let server = MockServer::start().await;
        list(&server, listing(&[key], "false", None)).await;
        let error = client(&server.uri())
            .resolve(Input::tree("s3://fixture-bucket/prefix/").unwrap())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::S3(ref error) if error.kind() == S3ErrorKind::DirectoryKey));
        assert_eq!(count(&server, "HEAD").await, 0);
    }
    let server = MockServer::start().await;
    head(&server, "folder/", 3).await;
    get(&server, "folder/", b"abc").await;
    let client = client(&server.uri());
    let plan = client
        .download("s3://fixture-bucket/folder/")
        .to_file("explicit.bin")
        .plan()
        .await
        .unwrap();
    assert_eq!(plan.files().len(), 1);
    let mut body = client
        .open(plan.files.into_iter().next().unwrap().file)
        .await
        .unwrap()
        .into_body();
    while let Some(chunk) = body.next().await {
        chunk.unwrap();
    }
    body.finish().unwrap();
}

#[tokio::test]
async fn zero_one_and_many_tree_inventories_preserve_nested_layout_and_reject_collisions() {
    let root = Directory::new();
    for keys in [
        vec![],
        vec!["prefix/nested/a"],
        vec!["prefix/a", "prefix/nested/b"],
    ] {
        let server = MockServer::start().await;
        list(&server, listing(&keys, "false", None)).await;
        for key in &keys {
            head(&server, key, 3).await;
        }
        let plan = client(&server.uri())
            .download(Input::tree("s3://fixture-bucket/prefix/").unwrap())
            .to_directory(root.path())
            .plan()
            .await
            .unwrap();
        assert_eq!(plan.files().len(), keys.len());
        for planned in plan.files() {
            assert_eq!(
                planned.destination(),
                root.path().join(planned.file().source_path())
            );
        }
    }
    for keys in [["prefix/a", "prefix/A"], ["prefix/a", "prefix/a/child"]] {
        let server = MockServer::start().await;
        list(&server, listing(&keys, "false", None)).await;
        let result = client(&server.uri())
            .resolve(
                Input::tree("s3://fixture-bucket/prefix/")
                    .unwrap()
                    .exclude("**")
                    .unwrap(),
            )
            .await;
        assert!(matches!(result, Err(Error::LayoutConflict)));
        assert_eq!(count(&server, "HEAD").await, 0);
    }
}

#[tokio::test]
async fn cap_one_is_shared_with_http_and_clones_until_s3_body_eof_or_drop() {
    let server = MockServer::start().await;
    head(&server, "object", 3).await;
    get(&server, "object", b"abc").await;
    Mock::given(method("HEAD"))
        .and(path("/http"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let client = Client::builder()
        .max_requests(NonZeroUsize::MIN)
        .s3_client(sdk(&server.uri()))
        .build()
        .unwrap();
    let sibling = Client::clone(&client);
    let opened = client.open("s3://fixture-bucket/object").await.unwrap();
    let reference = format!("{}/http", server.uri());
    let mut waiting = Box::pin(sibling.resolve(reference.as_str()));
    assert!(matches!(futures::poll!(waiting.as_mut()), Poll::Pending));
    assert_eq!(count(&server, "HEAD").await, 1);
    drop(opened);
    tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .unwrap()
        .unwrap();
    let mut body = client
        .open("s3://fixture-bucket/object")
        .await
        .unwrap()
        .into_body();
    while let Some(chunk) = body.next().await {
        chunk.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(2), sibling.resolve(reference.as_str()))
        .await
        .unwrap()
        .unwrap();
    body.finish().unwrap();
}

#[tokio::test]
async fn sdk_late_chunk_framing_failure_after_all_declared_bytes_is_not_clean_checked_eof() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                if socket.read(&mut byte).await.unwrap() == 0 {
                    break;
                }
                request.push(byte[0]);
            }
            if request.starts_with(b"HEAD ") {
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nETag: \"opaque-v1\"\r\nx-amz-version-id: version-one\r\nConnection: close\r\n\r\n").await.unwrap();
            } else {
                socket.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nETag: \"opaque-v1\"\r\nx-amz-version-id: version-one\r\nConnection: close\r\n\r\n3\r\nabc\r\nnot-a-chunk\r\n").await.unwrap();
            }
        }
    });
    let client = client(&endpoint);
    let mut body = client
        .open("s3://fixture-bucket/object")
        .await
        .unwrap()
        .into_body();
    let mut late = false;
    while let Some(chunk) = body.next().await {
        if let Err(failure) = chunk {
            assert!(
                matches!(failure.error, BodyError::Source(SourceError::S3(ref cause))
                if cause.kind() == S3ErrorKind::Body)
            );
            late = true;
        }
    }
    assert!(late);
    assert!(body.finish().is_err());
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn actual_sdk_publication_is_checked_and_whole_mode_reentry_never_relists_or_reheads() {
    let server = MockServer::start().await;
    list(&server, listing(&["prefix/nested/object"], "false", None)).await;
    head(&server, "prefix/nested/object", 3).await;
    Mock::given(method("GET"))
        .and(path(format!("/{BUCKET}/prefix/nested/object")))
        .respond_with(ResponseTemplate::new(503).set_body_string("remote-diagnostic-marker"))
        .mount(&server)
        .await;
    let root = Directory::new();
    let client = client(&server.uri());
    let input = || {
        Input::tree("s3://fixture-bucket/prefix/")
            .unwrap()
            .include("nested/**")
            .unwrap()
    };
    let failure = client
        .download(input())
        .to_directory(root.path())
        .run()
        .await
        .unwrap_err();
    let saved_path = failure.report.saved_run().unwrap().to_path_buf();
    let state = fs::read(saved_path.join("state")).unwrap();
    assert_eq!(&state[8..12], &4_u32.to_le_bytes());
    for marker in [
        "fixture-access-marker",
        "fixture-secret-marker",
        "fixture-bearer-marker",
        "remote-diagnostic-marker",
        "s3://fixture-bucket",
    ] {
        assert!(
            !state
                .windows(marker.len())
                .any(|bytes| bytes == marker.as_bytes())
        );
        assert!(!format!("{failure:?} {failure}").contains(marker));
    }
    server.reset().await;
    get(&server, "prefix/nested/object", b"abc").await;
    let report = client
        .download(input())
        .to_directory(root.path())
        .run()
        .await
        .unwrap();
    assert_eq!(report.saved_run(), Some(saved_path.as_path()));
    assert_eq!(fs::read(root.path().join("nested/object")).unwrap(), b"abc");
    assert_eq!(count(&server, "HEAD").await, 0);
    assert_eq!(count(&server, "GET").await, 1);
    let saved = SavedRun::inspect(&saved_path).await.unwrap();
    let key = saved.members().next().unwrap().key();
    let wrong = client
        .resume(saved)
        .input(key, Input::tree("s3://fixture-bucket/prefix/").unwrap())
        .run()
        .await
        .unwrap_err();
    assert!(matches!(
        wrong.error,
        Error::Saved(crate::client::SavedError::Binding)
    ));
    assert_eq!(count(&server, "GET").await, 1);
}

#[cfg(unix)]
#[tokio::test]
async fn reference_only_binding_reconstructs_frozen_tree_namespace_patterns_without_new_discovery()
{
    let server = MockServer::start().await;
    list(&server, listing(&["prefix/nested/object"], "false", None)).await;
    head(&server, "prefix/nested/object", 3).await;
    Mock::given(method("GET"))
        .and(path(format!("/{BUCKET}/prefix/nested/object")))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let root = Directory::new();
    let client = client(&server.uri());
    let reference = "s3://fixture-bucket/prefix/";
    let input = Input::tree(reference)
        .unwrap()
        .under("store")
        .unwrap()
        .include("nested/**")
        .unwrap()
        .include("nested/object")
        .unwrap()
        .exclude("*.tmp")
        .unwrap();
    let failure = client
        .download(input)
        .to_directory(root.path())
        .run()
        .await
        .unwrap_err();
    let saved_path = failure.report.saved_run().unwrap().to_path_buf();
    let saved = SavedRun::inspect(&saved_path).await.unwrap();
    let member = saved.members().next().unwrap();
    assert!(matches!(
        member.binding_input("s3://fixture-bucket/other/"),
        Err(crate::client::SavedError::Binding)
    ));
    let input = member.binding_input(reference).unwrap();
    assert_eq!(input.scope(), crate::input::InputScope::Tree);
    assert_eq!(input.namespace().unwrap().as_str(), "store");
    assert_eq!(
        input
            .selection
            .as_ref()
            .unwrap()
            .includes()
            .collect::<Vec<_>>(),
        ["nested/**", "nested/object"]
    );
    let key = member.key();
    let state = fs::read(saved_path.join("state")).unwrap();
    assert!(
        !state
            .windows(reference.len())
            .any(|bytes| bytes == reference.as_bytes())
    );
    server.reset().await;
    get(&server, "prefix/nested/object", b"abc").await;
    let report = client.resume(saved).input(key, input).run().await.unwrap();
    assert_eq!(report.completed().len(), 1);
    assert_eq!(
        fs::read(root.path().join("store/nested/object")).unwrap(),
        b"abc"
    );
    assert_eq!(count(&server, "HEAD").await, 0);
    assert_eq!(count(&server, "GET").await, 1);
}

#[cfg(unix)]
#[tokio::test]
async fn s3_required_mismatch_preserves_destination_and_does_not_publish() {
    let server = MockServer::start().await;
    head(&server, "object", 3).await;
    get(&server, "object", b"abc").await;
    let root = Directory::new();
    let destination = root.path().join("sentinel");
    fs::write(&destination, b"sentinel").unwrap();
    let failure = client(&server.uri())
        .download("s3://fixture-bucket/object")
        .expect(Digest::parse(crate::transfer::Algorithm::Sha256, &"00".repeat(32)).unwrap())
        .measurement(Measurement::None)
        .to_file(&destination)
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.error, Error::Transfer(_)));
    assert_eq!(fs::read(destination).unwrap(), b"sentinel");
    assert_eq!(failure.report.completed().len(), 0);
}

#[cfg(all(unix, feature = "swisstransfer"))]
#[tokio::test]
async fn mixed_http_swiss_and_s3_publish_through_one_common_layout_and_v4_record() {
    use std::sync::Arc;

    use crate::client::http::Http;

    let server = MockServer::start().await;
    let share = "aaaaaaaa-1111-2222-3333-444444444444";
    let member = "11111111-1111-2222-3333-444444444444";
    let html = format!(
        "<html><script data-page=app type=application/json>{}</script></html>",
        serde_json::json!({"component":"link/show","props":{"link":{"id":share},"transfer":{"files":[{"id":member,"path":"nested/swiss","size":3}]}}})
    );
    Mock::given(method("GET"))
        .and(path(format!("/dl/{share}")))
        .respond_with(ResponseTemplate::new(200).set_body_string(html))
        .mount(&server)
        .await;
    let date = time::OffsetDateTime::now_utc()
        .format(time::macros::format_description!(
            "[year][month][day]T[hour][minute][second]Z"
        ))
        .unwrap();
    let signed = format!(
        "https://objects.swisstransfer.infomaniak.cloud/swiss-object?X-Amz-Date={date}&X-Amz-Expires=3600&X-Amz-Signature=fixture-swiss-bearer-marker"
    );
    Mock::given(method("GET"))
        .and(path(format!("/api/1/links/{share}/files/{member}")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"data":{"url":signed}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/swiss-object"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"swiss-v1\"")
                .set_body_bytes(b"abc"),
        )
        .mount(&server)
        .await;
    Mock::given(method("HEAD"))
        .and(path("/http-object"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"http-v1\"")
                .insert_header("content-length", "3"),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/http-object"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"http-v1\"")
                .set_body_bytes(b"abc"),
        )
        .mount(&server)
        .await;
    list(&server, listing(&["prefix/nested/s3"], "false", None)).await;
    head(&server, "prefix/nested/s3", 3).await;
    get(&server, "prefix/nested/s3", b"abc").await;
    let mut http = Http::fixture(&server.uri(), NonZeroUsize::MIN);
    http.s3 = Some(Arc::new(super::S3::from(sdk(&server.uri()))));
    let client = Client {
        http: Arc::new(http),
    };
    let root = Directory::new();
    let report = client
        .download_all([
            Input::parse("https://example.test/http-object")
                .unwrap()
                .under("net")
                .unwrap(),
            Input::parse(&format!("https://www.swisstransfer.com/dl/{share}"))
                .unwrap()
                .under("share")
                .unwrap(),
            Input::tree("s3://fixture-bucket/prefix/")
                .unwrap()
                .under("store")
                .unwrap(),
        ])
        .to_directory(root.path())
        .run()
        .await
        .unwrap();
    assert_eq!(report.completed().len(), 3);
    for relative in ["net/http-object", "share/nested/swiss", "store/nested/s3"] {
        assert_eq!(fs::read(root.path().join(relative)).unwrap(), b"abc");
    }
    let bytes = fs::read(report.saved_run().unwrap().join("state")).unwrap();
    assert_eq!(&bytes[8..12], &4_u32.to_le_bytes());
    for marker in ["fixture-swiss-bearer-marker", "fixture-bearer-marker"] {
        assert!(
            !bytes
                .windows(marker.len())
                .any(|bytes| bytes == marker.as_bytes())
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn genuine_empty_tree_creates_no_root_but_zero_byte_exact_is_a_real_publication() {
    let server = MockServer::start().await;
    list(&server, listing(&[], "false", None)).await;
    let root = Directory::new();
    let output = root.path().join("absent");
    let client = client(&server.uri());
    let report = client
        .download(Input::tree("s3://fixture-bucket/prefix/").unwrap())
        .to_directory(&output)
        .run()
        .await
        .unwrap();
    assert_eq!(report.completed().len(), 0);
    assert!(!output.exists());
    head(&server, "zero", 0).await;
    get(&server, "zero", b"").await;
    let report = client
        .download("s3://fixture-bucket/zero")
        .to_file(root.path().join("zero"))
        .run()
        .await
        .unwrap();
    assert_eq!(report.completed().len(), 1);
    assert_eq!(fs::metadata(root.path().join("zero")).unwrap().len(), 0);
}

#[test]
fn native_s3_futures_are_send_without_imposing_bounds_on_custom_body_families() {
    fn send<T: Send>(_: T) {}
    let client = client("http://127.0.0.1:1");
    send(client.resolve("s3://fixture-bucket/object"));
    send(
        client
            .download("s3://fixture-bucket/object")
            .to_file("unused")
            .run(),
    );
}

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "xget-common-s3-fixture-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        #[cfg(unix)]
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        #[cfg(not(unix))]
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        // This guard owns only its freshly created empty fixture root, never an owner download.
        let _removed = fs::remove_dir_all(&self.0);
    }
}
