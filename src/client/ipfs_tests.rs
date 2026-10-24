#![allow(clippy::expect_used, clippy::unwrap_used)]

use core::sync::atomic::{AtomicU64, Ordering};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt as _;
use std::path::PathBuf;

use cid::Cid;
use cid::multihash::Multihash;
use futures::StreamExt as _;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::client::{BodyError, Client, SavedRun, VersionConsistency};
use crate::input::Input;
use crate::transfer::Measurement;

fn raw_cid() -> Cid {
    let hash =
        hex::decode("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad").unwrap();
    Cid::new_v1(0x55, Multihash::<64>::wrap(0x12, &hash).unwrap())
}

fn client(server: &MockServer) -> Client {
    Client::builder()
        .ipfs_gateway(&server.uri())
        .unwrap()
        .build()
        .unwrap()
}

async fn metadata(server: &MockServer, resource: &str, etag: Option<&str>) {
    let mut response = ResponseTemplate::new(200)
        .insert_header("content-length", "3")
        .insert_header("accept-ranges", "bytes");
    if let Some(etag) = etag {
        response = response.insert_header("etag", etag);
    }
    Mock::given(method("HEAD"))
        .and(path(resource))
        .respond_with(response)
        .expect(1)
        .mount(server)
        .await;
}

#[tokio::test]
async fn raw_cid_check_is_required_with_measurement_none_and_a_mismatch_never_finishes() {
    for payload in [b"abc".as_slice(), b"abd".as_slice()] {
        let server = MockServer::start().await;
        let cid = raw_cid();
        let resource = format!("/ipfs/{cid}");
        metadata(&server, &resource, Some("\"v1\"")).await;
        Mock::given(method("GET"))
            .and(path(&resource))
            .and(header("if-match", "\"v1\""))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v1\"")
                    .set_body_bytes(payload),
            )
            .expect(1)
            .mount(&server)
            .await;
        let mut body = client(&server)
            .open(format!("ipfs://{cid}"))
            .await
            .unwrap()
            .measurement(Measurement::None)
            .into_body();
        let mut mismatch = false;
        while let Some(chunk) = body.next().await {
            match chunk {
                Ok(_) => {}
                Err(failure) => {
                    assert!(matches!(failure.error, BodyError::Mismatch(_)));
                    mismatch = true;
                }
            }
        }
        if payload == b"abc" {
            let receipt = body.finish().unwrap();
            assert_eq!(receipt.body().checks().matched_requirements().count(), 1);
            assert_eq!(receipt.body().checks().measured(), None);
            assert!(!mismatch);
        } else {
            assert!(mismatch);
            assert!(body.finish().is_err());
        }
    }
}

#[tokio::test]
async fn unixfs_gateway_body_does_not_claim_the_dag_root_is_a_flat_file_digest() {
    let server = MockServer::start().await;
    let cid = Cid::new_v1(0x70, Multihash::<64>::wrap(0x12, &[1; 32]).unwrap());
    let resource = format!("/ipfs/{cid}/folder/file.txt");
    metadata(&server, &resource, None).await;
    Mock::given(method("GET"))
        .and(path(&resource))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"abc"))
        .expect(1)
        .mount(&server)
        .await;
    let reference = format!("ipfs://{cid}/folder/file.txt");
    let inventory = client(&server).resolve(reference.as_str()).await.unwrap();
    let file = inventory.into_files().next().unwrap();
    assert_eq!(file.source_path(), "folder/file.txt");
    assert_eq!(
        file.member_token().unwrap().as_str(),
        format!("{cid}/folder/file.txt")
    );
    let mut body = client(&server)
        .open(file)
        .await
        .unwrap()
        .measurement(Measurement::None)
        .into_body();
    while let Some(chunk) = body.next().await {
        chunk.unwrap();
    }
    let receipt = body.finish().unwrap();
    assert_eq!(receipt.version(), VersionConsistency::Unpinned);
    assert_eq!(receipt.body().checks().matched_requirements().count(), 0);
    assert_eq!(receipt.body().checks().measured(), None);
}

#[tokio::test]
async fn maximum_ipfs_identity_does_not_spend_its_path_budget_on_gateway_routing() {
    let server = MockServer::start().await;
    let cid = Cid::new_v1(0x70, Multihash::<64>::wrap(0x12, &[3; 32]).unwrap()).to_string();
    let source_path = format!(
        "{}/file.txt",
        "x".repeat(4096 - cid.len() - 1 - "/file.txt".len())
    );
    let token = format!("{cid}/{source_path}");
    assert_eq!(token.len(), 4096);
    let resource = format!("/gateway-prefix/ipfs/{token}");
    metadata(&server, &resource, Some("\"v1\"")).await;
    Mock::given(method("GET"))
        .and(path(&resource))
        .and(header("if-match", "\"v1\""))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"v1\"")
                .set_body_bytes(b"abc"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let client = Client::builder()
        .ipfs_gateway(&format!("{}/gateway-prefix", server.uri()))
        .unwrap()
        .build()
        .unwrap();
    let inventory = client.resolve(format!("ipfs://{token}")).await.unwrap();
    let file = inventory.into_files().next().unwrap();
    assert_eq!(file.source_path(), source_path);
    assert_eq!(file.member_token().unwrap().as_str(), token);
    let mut body = client
        .open(file)
        .await
        .unwrap()
        .measurement(Measurement::None)
        .into_body();
    while let Some(chunk) = body.next().await {
        chunk.unwrap();
    }
    body.finish().unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn exact_ipfs_remote_ancestors_are_not_local_destination_components() {
    let root = Scratch::new();
    let server = MockServer::start().await;
    let cid = Cid::new_v1(0x70, Multihash::<64>::wrap(0x12, &[1; 32]).unwrap());
    for ancestor in ["a:b".to_owned(), "a".repeat(256)] {
        let resource = format!("/ipfs/{cid}/{ancestor}/file.txt");
        metadata(&server, &resource, Some("\"v1\"")).await;
        Mock::given(method("GET"))
            .and(path(&resource))
            .and(header("if-match", "\"v1\""))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v1\"")
                    .set_body_bytes(b"abc"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let destination = root.0.join(format!("file-{}.txt", ancestor.len()));
        let report = client(&server)
            .download(format!("ipfs://{cid}/{ancestor}/file.txt"))
            .to_file(&destination)
            .run()
            .await
            .unwrap();
        assert_eq!(fs::read(&destination).unwrap(), b"abc");
        // Inspect round-trips the saved codec without applying local rules to remote routing ancestors.
        let saved = SavedRun::inspect(report.saved_run().unwrap())
            .await
            .unwrap();
        saved
            .members()
            .next()
            .unwrap()
            .binding_input(&format!("ipfs://{cid}/{ancestor}/file.txt"))
            .unwrap();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn frozen_raw_ipfs_reentry_renews_gateway_access_without_a_head_or_stored_gateway_url() {
    let root = Scratch::new();
    let first = MockServer::start().await;
    let cid = raw_cid();
    let resource = format!("/ipfs/{cid}");
    metadata(&first, &resource, Some("\"v1\"")).await;
    Mock::given(method("GET"))
        .and(path(&resource))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&first)
        .await;
    let reference = format!("ipfs://{cid}");
    let failure = client(&first)
        .download(reference.as_str())
        .measurement(Measurement::None)
        .to_directory(&root.0)
        .run()
        .await
        .unwrap_err();
    let state = failure.report.saved_run().unwrap();
    let bytes = fs::read(state.join("state")).unwrap();
    assert!(
        !bytes
            .windows(first.uri().len())
            .any(|window| window == first.uri().as_bytes())
    );
    let saved = SavedRun::inspect(state).await.unwrap();
    let key = saved.members().next().unwrap().key();
    let rebound = saved
        .members()
        .next()
        .unwrap()
        .binding_input(&reference)
        .unwrap();
    let second = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&second)
        .await;
    Mock::given(method("GET"))
        .and(path(&resource))
        .and(header("if-match", "\"v1\""))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"v1\"")
                .set_body_bytes(b"abc"),
        )
        .expect(1)
        .mount(&second)
        .await;
    let report = client(&second)
        .resume(saved)
        .input(key, rebound)
        .run()
        .await
        .unwrap();
    let published = report.completed().next().unwrap();
    assert!(published.range_stats().is_none());
    assert_eq!(
        published.receipt().checks().matched_requirements().count(),
        1
    );
    assert_eq!(published.receipt().checks().measured(), None);
    assert_eq!(fs::read(root.0.join(cid.to_string())).unwrap(), b"abc");
}

#[test]
fn gateway_configuration_has_no_access_credentials_or_ambient_lookup() {
    for gateway in [
        "file:///tmp/data",
        "https://user:secret@example.test",
        "https://example.test?token=secret",
        "https://example.test/#secret",
    ] {
        assert!(Client::builder().ipfs_gateway(gateway).is_err());
    }
    assert!(
        Client::builder()
            .ipfs_gateway("http://127.0.0.1:8080/prefix")
            .is_ok()
    );
    assert!(Input::tree(&format!("ipfs://{}", raw_cid())).is_err());
}

#[cfg(unix)]
struct Scratch(PathBuf);
#[cfg(unix)]
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "xget-ipfs-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self(path)
    }
}
#[cfg(unix)]
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn ipfs_path_patterns_keep_remote_ancestors_and_frozen_reentry_separate_from_local_names() {
    let root = Scratch::new();
    let server = MockServer::start().await;
    let cid = Cid::new_v1(0x70, Multihash::<64>::wrap(0x12, &[1; 32]).unwrap());
    for ancestor in ["a:b".to_owned(), "a".repeat(256)] {
        let pattern = format!("{}/*.txt", ancestor.replace(':', "?"));
        let resource = format!("/ipfs/{cid}/{ancestor}/file.txt");
        let reference = format!("ipfs://{cid}/{ancestor}/file.txt");
        metadata(&server, &resource, Some("\"v1\"")).await;
        Mock::given(method("GET"))
            .and(path(&resource))
            .and(header("if-match", "\"v1\""))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&server)
            .await;
        let input = Input::parse(&reference)
            .unwrap()
            .include(&pattern)
            .unwrap()
            .exclude("**/*.tmp")
            .unwrap();
        let destination = root.0.join(format!("filtered-{}.txt", ancestor.len()));
        let failure = client(&server)
            .download(input)
            .to_file(&destination)
            .run()
            .await
            .unwrap_err();
        assert!(!destination.exists());
        let saved = SavedRun::inspect(failure.report.saved_run().unwrap())
            .await
            .unwrap();
        let member = saved.members().next().unwrap();
        let key = member.key();
        let binding = member.binding_input(&reference).unwrap();
        assert_eq!(
            binding
                .selection
                .as_ref()
                .unwrap()
                .includes()
                .collect::<Vec<_>>(),
            vec![String::clone(&pattern)]
        );
        assert_eq!(
            binding
                .selection
                .as_ref()
                .unwrap()
                .excludes()
                .collect::<Vec<_>>(),
            vec!["**/*.tmp"]
        );
        server.reset().await;
        Mock::given(method("GET"))
            .and(path(&resource))
            .and(header("if-match", "\"v1\""))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v1\"")
                    .set_body_bytes(b"abc"),
            )
            .expect(1)
            .mount(&server)
            .await;
        client(&server)
            .resume(saved)
            .input(key, binding)
            .run()
            .await
            .unwrap();
        assert_eq!(fs::read(&destination).unwrap(), b"abc");
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method.as_str(), "GET");
        server.reset().await;
        metadata(&server, &resource, Some("\"v1\"")).await;
        let removed = Input::parse(&reference)
            .unwrap()
            .include(&pattern)
            .unwrap()
            .exclude("**")
            .unwrap();
        assert!(matches!(
            client(&server).resolve(removed).await,
            Err(crate::client::Error::PathSelection)
        ));
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method.as_str(), "HEAD");
        server.reset().await;
    }
}
