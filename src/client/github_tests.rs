//! Owned-loopback public Client fixtures. Authored source only, not executed by the source author.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use core::num::NonZeroUsize;
#[cfg(unix)]
use core::sync::atomic::{AtomicU64, Ordering};
#[cfg(unix)]
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt as _;
#[cfg(unix)]
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use futures::StreamExt as _;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{Address, GithubError, Json};
use crate::client::http::Http;
use crate::client::{Client, Error};
use crate::input::{GithubEvidence, Input, MemberToken};
use crate::transfer::{ByteCount, Length, Measurement};

const REFERENCE: &str = "https://github.com/example/project/releases/latest";
const API: &str = "https://api.github.com/repos/example/project";
const ABC: &str = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
const SIGNED: &str = "https://release-assets.githubusercontent.com/github-production-release-asset/123/abcd?sig=private-bearer-marker";

fn client(server: &MockServer) -> Client {
    Client {
        http: Arc::new(Http::fixture(&server.uri(), NonZeroUsize::MIN)),
    }
}
fn asset(id: u64, name: &str, digest: Option<&str>) -> Value {
    json!({"id": id, "url": format!("{API}/releases/assets/{id}"), "name": name,
        "size": 3, "state": "uploaded", "updated_at": "2026-10-20T12:00:00Z", "digest": digest})
}
fn assets(count: u64) -> Vec<Value> {
    (1..=count)
        .map(|id| asset(id, &format!("artifact-{id:03}.bin"), None))
        .collect()
}
async fn release(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/repos/example/project/releases/latest"))
        .and(header("accept", "application/vnd.github+json"))
        // The embedded first page is deliberately empty; only the dedicated asset endpoint counts.
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"id": 42, "draft": false,
            "tag_name": "v1", "url": format!("{API}/releases/42"),
            "assets_url": format!("{API}/releases/42/assets"), "assets": []})),
        )
        .mount(server)
        .await;
}
async fn page(server: &MockServer, number: usize, values: &[Value], link: Option<&str>) {
    let mut response = ResponseTemplate::new(200).set_body_json(values);
    if let Some(link) = link {
        response = response.insert_header("link", link);
    }
    Mock::given(method("GET"))
        .and(path("/repos/example/project/releases/42/assets"))
        .and(query_param("per_page", "30"))
        .and(query_param("page", number.to_string()))
        .respond_with(response)
        .mount(server)
        .await;
}
async fn catalogue(server: &MockServer, values: &[Value]) {
    release(server).await;
    for (index, chunk) in values.chunks(30).enumerate() {
        page(server, index + 1, chunk, None).await;
    }
    if values.len().is_multiple_of(30) {
        page(server, values.len() / 30 + 1, &[], None).await;
    }
}
async fn member(server: &MockServer, value: &Value, response: ResponseTemplate) {
    let id = value["id"].as_u64().unwrap();
    let route = format!("/repos/example/project/releases/assets/{id}");
    Mock::given(method("GET"))
        .and(path(&route))
        .and(header("accept", "application/vnd.github+json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(value))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(&route))
        .and(header("accept", "application/octet-stream"))
        .respond_with(response)
        .mount(server)
        .await;
}
async fn requests(server: &MockServer, route: &str) -> usize {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|request| request.url.path() == route)
        .count()
}

#[test]
fn stable_asset_tokens_are_numeric_not_rows_names_urls_or_zero() {
    for token in ["1", "18446744073709551615"] {
        let input = Input::parse(REFERENCE).unwrap().select_id(token).unwrap();
        assert_eq!(input.member.as_ref().unwrap().as_str(), token);
        assert!(input.member.as_ref().unwrap().is_github());
    }
    for token in [
        "0",
        "01",
        "-1",
        "18446744073709551616",
        "artifact-001.bin",
        SIGNED,
    ] {
        assert!(Input::parse(REFERENCE).unwrap().select_id(token).is_err());
    }
    assert!(MemberToken::github("1").unwrap() != MemberToken::s3("1").unwrap());
    for reference in [
        "https://user@github.com/example/project/releases/latest",
        "https://github.com/example/project/releases/latest?token=secret",
    ] {
        assert!(Input::parse(reference).is_err());
    }
    assert!(GithubEvidence::parse(42, "2026-02-29T00:00:00Z").is_none());
    assert!(GithubEvidence::parse(42, "2024-02-29T23:59:59Z").is_some());
    assert!(GithubEvidence::parse(0, "2024-02-29T23:59:59Z").is_none());
}

#[test]
fn foreign_controls_are_capped_before_typed_json_parsing() {
    assert!(Json::bounded(&vec![b' '; super::MAX_CONTROL + 1]).is_err());
    assert!(Json::bounded(format!("{{\"text\":\"{}\"}}", "x".repeat(16385)).as_bytes()).is_err());
    assert!(Json::bounded(format!("{}0{}", "[".repeat(33), "]".repeat(33)).as_bytes()).is_err());
    assert!(Json::bounded(serde_json::to_string(&assets(31)).unwrap().as_bytes()).is_err());
    assert!(Json::bounded(br#"[{"unknown":"escaped \" [ , ]"}]"#).is_ok());
}

#[test]
fn api_links_and_asset_redirects_require_evidenced_authorities_and_routes() {
    let address = Address::input(&Input::parse(REFERENCE).unwrap()).unwrap();
    let next = format!("<{API}/releases/42/assets?per_page=30&page=2>; rel=\"next\"");
    assert!(address.links(42, 1, 30, Some(&next)).unwrap());
    for link in [
        next.replace("api.github.com", "api.github.com.evil.test"),
        next.replace("/42/", "/43/"),
        next.replace("page=2", "page=1"),
        next.replace("per_page=30", "per_page=100"),
        next.replace("page=2", "page=129"),
        format!("{next}, {next}"),
        next.replace("rel=\"next\"", "rel=\"last\""),
    ] {
        assert!(address.links(42, 1, 30, Some(&link)).is_err(), "{link}");
    }
    let tagged = Address::input(
        &Input::parse("https://github.com/example/project/releases/tag/feature%2Fbuild").unwrap(),
    )
    .unwrap();
    assert_eq!(
        tagged.release().unwrap().path(),
        "/repos/example/project/releases/tags/feature%2Fbuild"
    );
    assert!(
        Address::input(&Input::parse("https://github.com/example/project/releases").unwrap())
            .is_err()
    );
    assert!(address.redirect(SIGNED).is_ok());
    for location in [
        SIGNED.replace(
            "release-assets.githubusercontent.com",
            "release-assets.githubusercontent.com.evil.test",
        ),
        SIGNED.replace("https:", "http:"),
        SIGNED.replace("/github-production-release-asset/", "/other/"),
        SIGNED.replace("https://", "https://user@"),
        SIGNED.replace("/123/", "/123/%2e%2e/"),
        "https://objects.githubusercontent.com/unproven?sig=private".to_owned(),
    ] {
        assert!(address.redirect(&location).is_err());
    }
}

#[test]
fn pagination_last_relations_use_the_current_cardinality_without_relaxing_page_bounds() {
    let address = Address::input(&Input::parse(REFERENCE).unwrap()).unwrap();
    let link = |page: usize, relation: &str| {
        format!("<{API}/releases/42/assets?per_page=30&page={page}>; rel=\"{relation}\"")
    };
    let previous_last = link(1, "last");
    assert!(!address.links(42, 2, 0, Some(&previous_last)).unwrap());
    assert!(address.links(42, 2, 1, Some(&previous_last)).is_err());
    assert!(address.links(42, 2, 30, Some(&previous_last)).is_err());
    assert!(address.links(42, 3, 0, Some(&previous_last)).is_err());
    let future_last = link(3, "last");
    let future_next = link(3, "next");
    for entries in [0, 1, 29] {
        assert!(address.links(42, 2, entries, Some(&future_last)).is_err());
        assert!(address.links(42, 2, entries, Some(&future_next)).is_err());
        assert!(
            address
                .links(
                    42,
                    2,
                    entries,
                    Some(&format!("{future_last}, {future_next}"))
                )
                .is_err()
        );
    }
    for invalid in [
        previous_last.replace("api.github.com", "evil.test"),
        previous_last.replace("/42/", "/43/"),
        previous_last.replace("per_page=30", "per_page=31"),
        previous_last.replace("page=1", "page=0"),
        format!("{previous_last}, {previous_last}"),
        link(1, "unknown"),
        link(1, "next"),
        link(3, "prev"),
    ] {
        assert!(
            address.links(42, 2, 0, Some(&invalid)).is_err(),
            "{invalid}"
        );
    }
    // This is the parser's independent page bound, not a claim that 128 populated pages fit
    // the separate common 128-entry inventory budget.
    assert!(address.links(42, super::MAX_PAGES, 30, None).is_ok());
    assert!(address.links(42, super::MAX_PAGES + 1, 30, None).is_err());
    assert!(address.links(42, 0, 30, None).is_err());
    assert!(address.links(42, 1, 31, None).is_err());
    assert!(
        address
            .links(
                42,
                super::MAX_PAGES,
                30,
                Some(&link(super::MAX_PAGES + 1, "next"))
            )
            .is_err()
    );
}

#[tokio::test]
async fn exactly_thirty_assets_accept_an_empty_completion_probe_linking_the_populated_last_page() {
    let server = MockServer::start().await;
    release(&server).await;
    page(&server, 1, &assets(30), None).await;
    let last = format!("<{API}/releases/42/assets?per_page=30&page=1>; rel=\"last\"");
    page(&server, 2, &[], Some(&last)).await;
    let inventory = client(&server).resolve(REFERENCE).await.unwrap();
    assert_eq!(inventory.files().len(), 30);
    assert_eq!(inventory.directories().len(), 0);
    assert_eq!(
        inventory
            .files()
            .last()
            .unwrap()
            .member_token()
            .unwrap()
            .as_str(),
        "30"
    );
    assert_eq!(
        requests(&server, "/repos/example/project/releases/42/assets").await,
        2
    );
}

#[tokio::test]
async fn control_bodies_admit_the_exact_cumulative_byte_limit_and_refuse_further_requests() {
    let server = MockServer::start().await;
    let mut bytes = b"[]".to_vec();
    bytes.resize(super::MAX_CONTROL, b' ');
    Mock::given(path("/repos/example/project/releases/42/assets"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(bytes))
        .mount(&server)
        .await;
    let client = client(&server);
    let address = Address::input(&Input::parse(REFERENCE).unwrap()).unwrap();
    let url = address.page(42, 1).unwrap();
    let mut controls = super::Controls {
        http: &client.http,
        remaining: super::MAX_TOTAL,
    };
    const {
        assert!(super::MAX_TOTAL.is_multiple_of(super::MAX_CONTROL));
    }
    for _ in 0..super::MAX_TOTAL / super::MAX_CONTROL {
        assert_eq!(
            controls.bytes(&url).await.unwrap().bytes.len(),
            super::MAX_CONTROL
        );
    }
    assert_eq!(controls.remaining, 0);
    assert!(matches!(
        controls.bytes(&url).await,
        Err(Error::Github(GithubError::Limit))
    ));
    assert_eq!(
        requests(&server, "/repos/example/project/releases/42/assets").await,
        super::MAX_TOTAL / super::MAX_CONTROL
    );
    let mut insufficient = super::Controls {
        http: &client.http,
        remaining: super::MAX_CONTROL - 1,
    };
    assert!(matches!(
        insufficient.bytes(&url).await,
        Err(Error::Inventory(crate::inventory::InventoryError::Limit))
    ));
    assert_eq!(insufficient.remaining, super::MAX_CONTROL - 1);
}

#[tokio::test]
async fn fifty_actual_assets_list_select_and_plan_all_without_an_implicit_first_choice() {
    let server = MockServer::start().await;
    let values = assets(50);
    catalogue(&server, &values).await;
    let client = client(&server);
    let inventory = client.resolve(REFERENCE).await.unwrap();
    assert_eq!(inventory.files().len(), 50);
    assert_eq!(inventory.directories().len(), 0);
    assert_eq!(
        inventory
            .files()
            .last()
            .unwrap()
            .member_token()
            .unwrap()
            .as_str(),
        "50"
    );
    let full_charge = inventory.metadata_bytes();
    let selected = client
        .resolve(Input::parse(REFERENCE).unwrap().select_id("50").unwrap())
        .await
        .unwrap();
    assert_eq!(selected.files().len(), 1);
    assert_eq!(selected.metadata_bytes(), full_charge);
    assert_eq!(
        selected.files().next().unwrap().source_path(),
        "artifact-050.bin"
    );
    assert!(matches!(
        client.download(REFERENCE).to_directory("out").plan().await,
        Err(Error::CatalogueChoice)
    ));
    assert!(matches!(
        client.open(REFERENCE).await,
        Err(Error::CatalogueChoice)
    ));
    assert_eq!(
        client
            .download(REFERENCE)
            .all()
            .to_directory("out")
            .plan()
            .await
            .unwrap()
            .files()
            .len(),
        50
    );
    assert_eq!(
        client
            .download(REFERENCE)
            .include("artifact-050.bin")
            .to_file("one.bin")
            .plan()
            .await
            .unwrap()
            .files()
            .len(),
        1
    );
    assert_eq!(
        client
            .download(selected.select_one(|_| true).unwrap())
            .to_file("chosen.bin")
            .plan()
            .await
            .unwrap()
            .files()
            .len(),
        1
    );
    assert_eq!(
        requests(&server, "/repos/example/project/releases/42/assets").await,
        10
    );
    assert_eq!(
        requests(&server, "/repos/example/project/releases/assets/1").await,
        0
    );
}

#[tokio::test]
async fn each_positive_criterion_and_stale_id_must_match_the_complete_original_scope() {
    let server = MockServer::start().await;
    catalogue(&server, &assets(50)).await;
    let client = client(&server);
    assert!(matches!(
        client
            .resolve(Input::parse(REFERENCE).unwrap().select_id("99").unwrap())
            .await,
        Err(Error::Selection)
    ));
    let both = Input::parse(REFERENCE)
        .unwrap()
        .select_id("50")
        .unwrap()
        .include("artifact-001.bin")
        .unwrap();
    assert!(client.resolve(both).await.is_err());
    let positive = Input::parse(REFERENCE)
        .unwrap()
        .include("missing")
        .unwrap()
        .include("artifact-050.bin")
        .unwrap();
    assert!(matches!(
        client.resolve(positive).await,
        Err(Error::PathSelection)
    ));
    let removed = Input::parse(REFERENCE)
        .unwrap()
        .include("*.bin")
        .unwrap()
        .exclude("*.bin")
        .unwrap();
    assert!(matches!(
        client.resolve(removed).await,
        Err(Error::PathSelection)
    ));
    let one = Input::parse(REFERENCE)
        .unwrap()
        .include("artifact-050.bin")
        .unwrap();
    let inventory = client.resolve(one).await.unwrap();
    assert_eq!(inventory.files().len(), 1);
    assert!(inventory.metadata_bytes() > inventory.files().next().unwrap().retained_bytes());
}

#[tokio::test]
async fn zero_one_and_reordered_names_never_select_a_position_or_invent_directory_rows() {
    let server = MockServer::start().await;
    catalogue(&server, &[]).await;
    let client = client(&server);
    assert_eq!(client.resolve(REFERENCE).await.unwrap().files().len(), 0);
    assert_eq!(
        client
            .download(REFERENCE)
            .all()
            .to_directory("out")
            .plan()
            .await
            .unwrap()
            .files()
            .len(),
        0
    );
    assert!(matches!(
        client
            .resolve(Input::parse(REFERENCE).unwrap().include("**").unwrap())
            .await,
        Err(Error::PathSelection)
    ));
    server.reset().await;
    catalogue(&server, &[asset(71, "not-native.bin", None)]).await;
    assert!(matches!(
        client.download(REFERENCE).to_file("one").plan().await,
        Err(Error::CatalogueChoice)
    ));
    server.reset().await;
    catalogue(
        &server,
        &[asset(2, "first.bin", None), asset(1, "last.bin", None)],
    )
    .await;
    let selected = client
        .resolve(Input::parse(REFERENCE).unwrap().select_id("1").unwrap())
        .await
        .unwrap();
    assert_eq!(selected.files().next().unwrap().source_path(), "last.bin");
}

#[tokio::test]
async fn duplicate_ids_name_and_path_collisions_fail_before_selection_can_hide_them() {
    for values in [
        vec![asset(1, "one.bin", None), asset(1, "two.bin", None)],
        vec![asset(1, "same.bin", None), asset(2, "same.bin", None)],
        vec![asset(1, "case.bin", None), asset(2, "CASE.bin", None)],
        vec![asset(1, "dir/file.bin", None)],
    ] {
        let server = MockServer::start().await;
        catalogue(&server, &values).await;
        assert!(
            client(&server)
                .resolve(Input::parse(REFERENCE).unwrap().select_id("1").unwrap())
                .await
                .is_err()
        );
    }
    let server = MockServer::start().await;
    let values = assets(30);
    release(&server).await;
    page(&server, 1, &values, None).await;
    page(
        &server,
        2,
        &[asset(1, "duplicated-across-page.bin", None)],
        None,
    )
    .await;
    let result = client(&server).resolve(REFERENCE).await;
    assert!(
        matches!(result, Err(Error::Github(GithubError::Duplicate))),
        "expected duplicate identity, got {result:?}"
    );
}

#[tokio::test]
async fn pagination_status_count_byte_and_incomplete_links_cannot_yield_a_complete_prefix() {
    for status in [301, 403, 404, 429, 500] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(status))
            .mount(&server)
            .await;
        assert!(client(&server).resolve(REFERENCE).await.is_err());
    }
    let server = MockServer::start().await;
    release(&server).await;
    let next = format!("<{API}/releases/42/assets?per_page=30&page=2>; rel=\"next\"");
    page(&server, 1, &assets(1), Some(&next)).await;
    assert!(matches!(
        client(&server).resolve(REFERENCE).await,
        Err(Error::Github(GithubError::Incomplete))
    ));
    server.reset().await;
    release(&server).await;
    page(&server, 1, &assets(31), None).await;
    assert!(client(&server).resolve(REFERENCE).await.is_err());
    server.reset().await;
    release(&server).await;
    Mock::given(path("/repos/example/project/releases/42/assets"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string("x".repeat(super::MAX_CONTROL + 1)),
        )
        .mount(&server)
        .await;
    assert!(client(&server).resolve(REFERENCE).await.is_err());
    server.reset().await;
    catalogue(&server, &assets(129)).await;
    assert!(
        client(&server)
            .resolve(Input::parse(REFERENCE).unwrap().select_id("1").unwrap())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn mixed_original_occurrences_keep_grouping_and_require_namespaces_for_equal_groups() {
    let server = MockServer::start().await;
    catalogue(&server, &assets(2)).await;
    Mock::given(method("HEAD"))
        .and(path("/exact.bin"))
        .respond_with(ResponseTemplate::new(200).insert_header("content-length", "3"))
        .mount(&server)
        .await;
    let client = client(&server);
    let inputs = [
        Input::parse(REFERENCE)
            .unwrap()
            .all()
            .under("release")
            .unwrap(),
        Input::parse("https://example.test/exact.bin")
            .unwrap()
            .under("exact")
            .unwrap(),
    ];
    let plan = client
        .download_all(inputs)
        .to_directory("out")
        .plan()
        .await
        .unwrap();
    let destinations: Vec<_> = plan
        .files()
        .map(|file| file.destination().to_path_buf())
        .collect();
    assert!(destinations.contains(&PathBuf::from("out/release/artifact-001.bin")));
    assert!(destinations.contains(&PathBuf::from("out/exact/exact.bin")));
    let same = [
        Input::parse(REFERENCE).unwrap().all(),
        Input::parse(REFERENCE).unwrap().all(),
    ];
    assert!(matches!(
        client.download_all(same).to_directory("out").plan().await,
        Err(Error::LayoutConflict)
    ));
}

#[tokio::test]
async fn direct_asset_api_uses_known_size_and_reports_unpinned_without_guessing_a_validator() {
    let server = MockServer::start().await;
    let value = asset(5, "plain.bin", None);
    catalogue(&server, core::slice::from_ref(&value)).await;
    member(
        &server,
        &value,
        ResponseTemplate::new(200).set_body_bytes(b"abc"),
    )
    .await;
    let opened = client(&server)
        .open(Input::parse(REFERENCE).unwrap().select_id("5").unwrap())
        .await
        .unwrap();
    assert_eq!(opened.file().length(), Length::Known(ByteCount(3)));
    assert_eq!(
        opened.version(),
        crate::client::VersionConsistency::Unpinned
    );
    let mut body = opened.into_body();
    assert_eq!(body.next().await.unwrap().unwrap(), &b"abc"[..]);
    assert!(body.next().await.is_none());
    body.finish().unwrap();
    for request in server.received_requests().await.unwrap() {
        assert!(!request.headers.contains_key("authorization"));
        assert!(!request.headers.contains_key("cookie"));
        assert!(!request.headers.contains_key("range"));
    }
}

#[cfg(unix)]
static NEXT: AtomicU64 = AtomicU64::new(0);
#[cfg(unix)]
struct Root(PathBuf);
#[cfg(unix)]
impl Root {
    fn new() -> Self {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("xget-github-{}-{id}", std::process::id()));
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self(fs::canonicalize(path).unwrap())
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
#[cfg(unix)]
impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn fifty_assets_use_the_real_common_serial_checked_publication_path() {
    let server = MockServer::start().await;
    let root = Root::new();
    let values = assets(50);
    catalogue(&server, &values).await;
    for value in &values {
        member(
            &server,
            value,
            ResponseTemplate::new(200).set_body_bytes(b"abc"),
        )
        .await;
    }
    let report = client(&server)
        .download(REFERENCE)
        .all()
        .to_directory(root.path())
        .run()
        .await
        .unwrap();
    assert_eq!(report.completed().len(), 50);
    for file in report.completed() {
        assert_eq!(fs::read(file.destination()).unwrap(), b"abc");
        assert!(file.range_stats().is_none());
    }
    let saved = crate::client::SavedRun::inspect(report.saved_run().unwrap())
        .await
        .unwrap();
    assert_eq!(saved.members().len(), 50);
    assert!(saved.is_complete());
}

#[cfg(unix)]
#[tokio::test]
async fn provider_digest_is_required_with_measurement_none_and_failure_never_publishes() {
    for payload in [b"abc".as_slice(), b"bad".as_slice()] {
        let server = MockServer::start().await;
        let root = Root::new();
        let value = asset(1, "checked.bin", Some(ABC));
        catalogue(&server, core::slice::from_ref(&value)).await;
        member(
            &server,
            &value,
            ResponseTemplate::new(200).set_body_bytes(payload),
        )
        .await;
        let result = client(&server)
            .download(REFERENCE)
            .select_id("1")
            .measurement(Measurement::None)
            .to_directory(root.path())
            .run()
            .await;
        if payload == b"abc" {
            let report = result.unwrap();
            assert_eq!(report.completed().len(), 1);
            assert_eq!(fs::read(root.path().join("checked.bin")).unwrap(), b"abc");
        } else {
            assert!(result.is_err());
            assert!(!root.path().join("checked.bin").exists());
        }
    }
    let server = MockServer::start().await;
    let root = Root::new();
    catalogue(&server, &[asset(1, "checked.bin", Some(ABC))]).await;
    let conflicting: crate::transfer::Digest =
        "sha256:0000000000000000000000000000000000000000000000000000000000000000"
            .parse()
            .unwrap();
    assert!(matches!(
        client(&server)
            .download(REFERENCE)
            .select_id("1")
            .expect(conflicting)
            .to_directory(root.path())
            .plan()
            .await,
        Err(Error::ConflictingRequirements(_))
    ));
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
    assert_eq!(
        requests(&server, "/repos/example/project/releases/assets/1").await,
        0
    );
}

#[cfg(unix)]
#[tokio::test]
async fn frozen_reentry_uses_asset_id_without_latest_or_selection_rediscovery_and_never_stores_bearers()
 {
    let server = MockServer::start().await;
    let root = Root::new();
    let value = asset(7, "frozen.bin", Some(ABC));
    catalogue(&server, core::slice::from_ref(&value)).await;
    member(&server, &value, ResponseTemplate::new(503)).await;
    let client = client(&server);
    let failure = client
        .download(REFERENCE)
        .select_id("7")
        .to_directory(root.path())
        .run()
        .await
        .unwrap_err();
    let saved_path = failure.report.saved_run().unwrap().to_path_buf();
    let before = fs::read(saved_path.join("state")).unwrap();
    server.reset().await;
    member(
        &server,
        &value,
        ResponseTemplate::new(302).insert_header("location", SIGNED),
    )
    .await;
    Mock::given(path("/github-production-release-asset/123/abcd"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"abc"))
        .mount(&server)
        .await;
    let report = client
        .download(REFERENCE)
        .select_id("7")
        .to_directory(root.path())
        .run()
        .await
        .unwrap();
    assert_eq!(report.saved_run(), Some(saved_path.as_path()));
    assert_eq!(fs::read(root.path().join("frozen.bin")).unwrap(), b"abc");
    assert_eq!(
        requests(&server, "/repos/example/project/releases/latest").await,
        0
    );
    assert_eq!(
        requests(&server, "/repos/example/project/releases/42/assets").await,
        0
    );
    let after = fs::read(saved_path.join("state")).unwrap();
    for bytes in [&before, &after] {
        assert!(
            !bytes
                .windows(REFERENCE.len())
                .any(|window| window == REFERENCE.as_bytes())
        );
        // Darwin's owned temporary root can legitimately contain /private/tmp. Audit the actual
        // bearer sentinel and complete signed locator, not a common filesystem component.
        assert!(
            !bytes
                .windows(b"private-bearer-marker".len())
                .any(|window| window == b"private-bearer-marker")
        );
        assert!(
            !bytes
                .windows(SIGNED.len())
                .any(|window| window == SIGNED.as_bytes())
        );
        assert!(
            !bytes
                .windows(b"githubusercontent".len())
                .any(|window| window == b"githubusercontent")
        );
    }
    for request in server.received_requests().await.unwrap() {
        assert!(!request.headers.contains_key("authorization"));
        assert!(!request.headers.contains_key("cookie"));
        assert!(!request.headers.contains_key("referer"));
    }
    // Fresh deliberately rediscovers instead of matching the completed frozen run; a missing
    // catalogue endpoint fails rather than overwriting/adopting the already published destination.
    assert!(
        client
            .download(REFERENCE)
            .select_id("7")
            .fresh(crate::client::Fresh)
            .to_directory(root.path())
            .run()
            .await
            .is_err()
    );
    assert_eq!(fs::read(root.path().join("frozen.bin")).unwrap(), b"abc");
}

#[cfg(unix)]
#[tokio::test]
async fn frozen_metadata_changes_or_live_denials_never_open_payload_or_fall_back_to_names() {
    for change in [
        "id",
        "name",
        "name_case",
        "size",
        "updated_at",
        "digest",
        "denied",
    ] {
        let server = MockServer::start().await;
        let root = Root::new();
        let value = asset(1, "frozen.bin", Some(ABC));
        catalogue(&server, core::slice::from_ref(&value)).await;
        member(&server, &value, ResponseTemplate::new(503)).await;
        let client = client(&server);
        let failure = client
            .download(REFERENCE)
            .select_id("1")
            .to_directory(root.path())
            .run()
            .await
            .unwrap_err();
        let saved = failure.report.saved_run().unwrap().to_path_buf();
        server.reset().await;
        let mut changed = Value::clone(&value);
        match change {
            "id" => changed["id"] = json!(2),
            "name" => changed["name"] = json!("renamed.bin"),
            "name_case" => changed["name"] = json!("FROZEN.BIN"),
            "size" => changed["size"] = json!(4),
            "updated_at" => changed["updated_at"] = json!("2026-10-21T12:00:00Z"),
            "digest" => changed["digest"] = Value::Null,
            "denied" => {}
            _ => unreachable!(),
        }
        Mock::given(path("/repos/example/project/releases/assets/1"))
            .and(header("accept", "application/vnd.github+json"))
            .respond_with(if change == "denied" {
                ResponseTemplate::new(403)
            } else {
                ResponseTemplate::new(200).set_body_json(changed)
            })
            .mount(&server)
            .await;
        assert!(
            client
                .download(REFERENCE)
                .select_id("1")
                .to_directory(root.path())
                .run()
                .await
                .is_err()
        );
        assert_eq!(
            requests(&server, "/repos/example/project/releases/latest").await,
            0
        );
        assert_eq!(
            requests(&server, "/repos/example/project/releases/assets/1").await,
            1
        );
        assert!(!root.path().join("frozen.bin").exists());
        assert!(saved.join("state").is_file());
    }
}

#[cfg(unix)]
#[tokio::test]
async fn explicit_completed_reentry_requires_native_evidence_not_a_new_asset_open_or_hidden_hash() {
    for digest in [Some(ABC), None] {
        let server = MockServer::start().await;
        let root = Root::new();
        let value = asset(1, "complete.bin", digest);
        catalogue(&server, core::slice::from_ref(&value)).await;
        member(
            &server,
            &value,
            ResponseTemplate::new(200).set_body_bytes(b"abc"),
        )
        .await;
        let client = client(&server);
        let report = client
            .download(REFERENCE)
            .select_id("1")
            .measurement(Measurement::None)
            .to_directory(root.path())
            .run()
            .await
            .unwrap();
        let saved = crate::client::SavedRun::inspect(report.saved_run().unwrap())
            .await
            .unwrap();
        let member = saved.members().next().unwrap();
        let key = member.key();
        let binding = member.binding_input(REFERENCE).unwrap();
        server.reset().await;
        let result = client.resume(saved).input(key, binding).run().await;
        if digest.is_some() {
            assert_eq!(result.unwrap().completed().len(), 1);
        } else {
            assert!(matches!(
                result.unwrap_err().error,
                Error::Saved(crate::client::SavedError::NotRevalidatable)
            ));
        }
        assert_eq!(server.received_requests().await.unwrap().len(), 0);
        assert_eq!(fs::read(root.path().join("complete.bin")).unwrap(), b"abc");
    }
}

#[tokio::test]
async fn redirect_authority_and_second_hop_redirect_are_rejected_before_body_admission() {
    for location in ["https://evil.test/asset?secret=marker", SIGNED] {
        let server = MockServer::start().await;
        let value = asset(1, "asset.bin", None);
        catalogue(&server, core::slice::from_ref(&value)).await;
        member(
            &server,
            &value,
            ResponseTemplate::new(302).insert_header("location", location),
        )
        .await;
        Mock::given(path("/github-production-release-asset/123/abcd"))
            .respond_with(ResponseTemplate::new(302).insert_header("location", SIGNED))
            .mount(&server)
            .await;
        assert!(matches!(
            client(&server)
                .open(Input::parse(REFERENCE).unwrap().select_id("1").unwrap())
                .await,
            Err(Error::Github(GithubError::Redirect))
        ));
        assert_eq!(requests(&server, "/asset").await, 0);
    }
}

#[tokio::test]
async fn actual_eof_short_long_and_errors_after_the_frozen_length_are_checked() {
    for tail in ["0\r\n\r\n", "1\r\nx\r\n0\r\n\r\n", "not-a-chunk\r\n"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let uri = format!("http://{}", listener.local_addr().unwrap());
        let value = asset(1, "late.bin", Some(ABC));
        let value_bytes = serde_json::to_vec(&value).unwrap();
        let listing = serde_json::to_vec(&vec![value]).unwrap();
        let release = serde_json::to_vec(&json!({"id":42,"draft":false,"tag_name":"v1",
            "url":format!("{API}/releases/42"),"assets_url":format!("{API}/releases/42/assets")}))
        .unwrap();
        let worker = tokio::spawn(async move {
            for _ in 0..4 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    stream.read_exact(&mut byte).await.unwrap();
                    request.push(byte[0]);
                    assert!(request.len() < 16384);
                }
                let text = core::str::from_utf8(&request).unwrap();
                if text
                    .to_ascii_lowercase()
                    .contains("accept: application/octet-stream")
                {
                    stream.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n3\r\nabc\r\n").await.unwrap();
                    stream.write_all(tail.as_bytes()).await.unwrap();
                } else {
                    let body = if text.starts_with("GET /repos/example/project/releases/latest ") {
                        &release
                    } else if text.starts_with("GET /repos/example/project/releases/42/assets?") {
                        &listing
                    } else {
                        &value_bytes
                    };
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    stream.write_all(headers.as_bytes()).await.unwrap();
                    stream.write_all(body).await.unwrap();
                }
                stream.shutdown().await.unwrap();
            }
        });
        let client = Client {
            http: Arc::new(Http::fixture(&uri, NonZeroUsize::MIN)),
        };
        let mut body = client
            .open(Input::parse(REFERENCE).unwrap().select_id("1").unwrap())
            .await
            .unwrap()
            .measurement(Measurement::None)
            .into_body();
        let mut failed = false;
        while let Some(item) = body.next().await {
            if item.is_err() {
                failed = true;
                break;
            }
        }
        if tail == "0\r\n\r\n" {
            assert!(!failed);
            body.finish().unwrap();
        } else {
            assert!(failed);
            assert!(body.finish().is_err());
        }
        worker.await.unwrap();
    }
    let server = MockServer::start().await;
    let value = asset(1, "short.bin", Some(ABC));
    catalogue(&server, core::slice::from_ref(&value)).await;
    member(
        &server,
        &value,
        ResponseTemplate::new(200).set_body_bytes(b"ab"),
    )
    .await;
    assert!(matches!(
        client(&server)
            .open(Input::parse(REFERENCE).unwrap().select_id("1").unwrap())
            .await,
        Err(Error::VersionChanged)
    ));
}

#[test]
fn api_repository_case_does_not_relax_authority_routes_identifiers_or_query() {
    let address = Address::input(
        &Input::parse("https://github.com/EXAMPLE/Project/releases/latest").unwrap(),
    )
    .unwrap();
    for repository in ["example/project", "EXAMPLE/Project", "ExAmPlE/pRoJeCt"] {
        assert!(address.api_matches(
            &format!("https://api.github.com/repos/{repository}/releases/assets/31"),
            "/releases/assets/31"
        ));
    }
    for value in [
        "https://api.github.com.evil.test/repos/example/project/releases/assets/31",
        "https://user@api.github.com/repos/example/project/releases/assets/31",
        "https://api.github.com:443/repos/example/project/releases/assets/31",
        "https://api.github.com/REPOS/example/project/releases/assets/31",
        "https://api.github.com/repos/example/project/Releases/assets/31",
        "https://api.github.com/repos/example/project/releases/assets/031",
        "https://api.github.com/repos/example/project/releases/assets/31?extra=1",
        "https://api.github.com/repos/example/project/releases/assets/31#fragment",
        "https://api.github.com/repos/example/other/releases/assets/31",
    ] {
        assert!(
            !address.api_matches(value, "/releases/assets/31"),
            "{value}"
        );
    }
    assert!(!address.api_matches(
        "https://api.github.com/repos/example/project/releases/tags/v1",
        "/releases/tags/V1"
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn case_varied_repository_accepts_canonical_control_pages_and_frozen_asset_reentry() {
    let server = MockServer::start().await;
    let root = Root::new();
    let reference = "https://github.com/EXAMPLE/Project/releases/latest";
    let repository = "/repos/EXAMPLE/Project";
    Mock::given(method("GET"))
        .and(path(format!("{repository}/releases/latest")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": 42, "draft": false, "tag_name": "V1", "url": format!("{API}/releases/42"),
            "assets_url": format!("{API}/releases/42/assets")
        })))
        .expect(1)
        .mount(&server)
        .await;
    let next = format!("<{API}/releases/42/assets?per_page=30&page=2>; rel=\"next\"");
    Mock::given(method("GET"))
        .and(path(format!("{repository}/releases/42/assets")))
        .and(query_param("page", "1"))
        .and(query_param("per_page", "30"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("link", next)
                .set_body_json(assets(30)),
        )
        .expect(1)
        .mount(&server)
        .await;
    let value = asset(31, "Artifact.BIN", Some(ABC));
    Mock::given(method("GET"))
        .and(path(format!("{repository}/releases/42/assets")))
        .and(query_param("page", "2"))
        .and(query_param("per_page", "30"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec![Value::clone(&value)]))
        .expect(1)
        .mount(&server)
        .await;
    let route = format!("{repository}/releases/assets/31");
    Mock::given(method("GET"))
        .and(path(&route))
        .and(header("accept", "application/vnd.github+json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(Value::clone(&value)))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(&route))
        .and(header("accept", "application/octet-stream"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;
    let client = client(&server);
    let inventory = client
        .resolve(Input::parse(reference).unwrap().select_id("31").unwrap())
        .await
        .unwrap();
    let file = inventory.into_files().next().unwrap();
    assert_eq!(file.input.original_reference(), reference);
    assert_eq!(file.source_path(), "Artifact.BIN");
    let destination = root.path().join("result.bin");
    let failure = client
        .download(file)
        .to_file(&destination)
        .run()
        .await
        .unwrap_err();
    let saved = crate::client::SavedRun::inspect(failure.report.saved_run().unwrap())
        .await
        .unwrap();
    let member = saved.members().next().unwrap();
    assert!(
        member.binding_input(REFERENCE).is_err(),
        "original binding spelling remains exact"
    );
    let key = member.key();
    let binding = member.binding_input(reference).unwrap();
    assert_eq!(binding.original_reference(), reference);
    server.reset().await;
    Mock::given(method("GET"))
        .and(path(&route))
        .and(header("accept", "application/vnd.github+json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(value))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(&route))
        .and(header("accept", "application/octet-stream"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"abc"))
        .expect(1)
        .mount(&server)
        .await;
    client
        .resume(saved)
        .input(key, binding)
        .run()
        .await
        .unwrap();
    assert_eq!(fs::read(destination).unwrap(), b"abc");
    let observed = server.received_requests().await.unwrap();
    assert_eq!(observed.len(), 2);
    assert!(observed.iter().all(|request| request.url.path() == route));
}

#[tokio::test]
async fn repository_case_acceptance_does_not_fold_release_tags() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/EXAMPLE/Project/releases/tags/V1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": 42, "draft": false, "tag_name": "v1", "url": format!("{API}/releases/42"),
            "assets_url": format!("{API}/releases/42/assets")
        })))
        .expect(1)
        .mount(&server)
        .await;
    assert!(matches!(
        client(&server)
            .resolve("https://github.com/EXAMPLE/Project/releases/tag/V1")
            .await,
        Err(Error::Github(GithubError::Malformed))
    ));
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

async fn control_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        stream.read_exact(&mut byte).await.unwrap();
        request.push(byte[0]);
        assert!(request.len() <= 16384);
    }
    request
}

enum ControlTail {
    Stalled,
    Malformed,
}

// Keep the first socket open until the successor has arrived: a stalled control cannot acquire
// accidental EOF, and a late malformed chunk cannot depend on the server closing its connection.
async fn control_successor(
    listener: tokio::net::TcpListener,
    tail: ControlTail,
    written: tokio::sync::oneshot::Sender<()>,
) {
    let (mut first, _) = listener.accept().await.unwrap();
    let request = control_request(&mut first).await;
    assert!(request.starts_with(b"GET /repos/example/project/releases/latest HTTP/1.1\r\n"));
    let release = serde_json::to_vec(&json!({"id": 42, "draft": false, "tag_name": "v1",
        "url": format!("{API}/releases/42"), "assets_url": format!("{API}/releases/42/assets")}))
    .unwrap();
    first
        .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    first
        .write_all(format!("{:x}\r\n", release.len()).as_bytes())
        .await
        .unwrap();
    first.write_all(&release).await.unwrap();
    first.write_all(b"\r\n").await.unwrap();
    if matches!(tail, ControlTail::Malformed) {
        first.write_all(b"not-a-chunk\r\n").await.unwrap();
    }
    written.send(()).unwrap();
    let (mut successor, _) = listener.accept().await.unwrap();
    let request = control_request(&mut successor).await;
    let request = core::str::from_utf8(&request).unwrap();
    assert!(
        request.starts_with("GET /repos/example/project/releases/assets/999 HTTP/1.1\r\n"),
        "{request}"
    );
    assert!(
        request
            .to_ascii_lowercase()
            .contains("accept: application/vnd.github+json\r\n")
    );
    successor
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n[]")
        .await
        .unwrap();
    successor.shutdown().await.unwrap();
    drop(first);
}

#[tokio::test]
async fn dropping_or_timing_out_a_stalled_control_releases_its_only_slot_without_discovery_advancing()
 {
    for caller_timeout in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let uri = format!("http://{}", listener.local_addr().unwrap());
        let (written, ready) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn(control_successor(listener, ControlTail::Stalled, written));
        let client = Client {
            http: Arc::new(Http::fixture(&uri, NonZeroUsize::MIN)),
        };
        let mut discovery = Box::pin(client.resolve(REFERENCE));
        tokio::select! {
            biased;
            result = &mut discovery => panic!("stalled control unexpectedly returned: {result:?}"),
            result = ready => result.unwrap(),
        }
        assert!(futures::poll!(discovery.as_mut()).is_pending());
        let timeout = if caller_timeout {
            Some(
                tokio::time::timeout(core::time::Duration::from_millis(10), discovery.as_mut())
                    .await
                    .unwrap_err(),
            )
        } else {
            None
        };
        drop(discovery);
        let url = url::Url::parse(&format!("{API}/releases/assets/999")).unwrap();
        let control = tokio::time::timeout(
            core::time::Duration::from_secs(5),
            client.http.github_control(&url, super::MAX_CONTROL),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(control.bytes.as_slice(), b"[]");
        // Retain the caller's timeout until after admission succeeds, rather than clearing errors first.
        assert_eq!(timeout.is_some(), caller_timeout);
        worker.await.unwrap();
    }
}

#[tokio::test]
async fn late_control_body_error_after_complete_json_releases_its_only_slot_while_error_is_retained()
 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}", listener.local_addr().unwrap());
    let (written, ready) = tokio::sync::oneshot::channel();
    let worker = tokio::spawn(control_successor(listener, ControlTail::Malformed, written));
    let client = Client {
        http: Arc::new(Http::fixture(&uri, NonZeroUsize::MIN)),
    };
    let failure = tokio::time::timeout(
        core::time::Duration::from_secs(5),
        client.resolve(REFERENCE),
    )
    .await
    .unwrap()
    .unwrap_err();
    ready.await.unwrap();
    assert!(matches!(&failure, Error::Http(_)), "{failure:?}");
    let url = url::Url::parse(&format!("{API}/releases/assets/999")).unwrap();
    let successor = tokio::time::timeout(
        core::time::Duration::from_secs(5),
        client.http.github_control(&url, super::MAX_CONTROL),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(successor.bytes.as_slice(), b"[]");
    assert!(matches!(failure, Error::Http(_)));
    worker.await.unwrap();
}
