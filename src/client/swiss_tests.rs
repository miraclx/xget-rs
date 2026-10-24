//! Authored loopback fixtures through public Client entrances; no live service or execution claim.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use core::num::NonZeroUsize;
use core::sync::atomic::{AtomicU64, Ordering};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::StreamExt as _;
use time::OffsetDateTime;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{Access, Manifest};
#[cfg(unix)]
use crate::client::SavedRun;
use crate::client::http::Http;
use crate::client::{Client, Error, VersionConsistency};
use crate::input::{Input, InputScope};
use crate::transfer::{Digest, Measurement};

const SHARE: &str = "aaaaaaaa-1111-2222-3333-444444444444";
const SHARE_B: &str = "bbbbbbbb-1111-2222-3333-444444444444";
const FILE_A: &str = "11111111-1111-2222-3333-444444444444";
const FILE_B: &str = "22222222-1111-2222-3333-444444444444";
const ABC: &str = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

fn reference(share: &str) -> String {
    format!("https://www.swisstransfer.com/dl/{share}")
}

fn page(share: &str, files: &[(&str, &str, u64)]) -> String {
    let files: Vec<_> = files
        .iter()
        .map(|(id, path, size)| serde_json::json!({"id": id, "path": path, "size": size}))
        .collect();
    format!(
        "<html><script data-page=app type=application/json>{}</script></html>",
        serde_json::json!({"component":"link/show","props":{"link":{"id":share},"transfer":{"files":files}}})
    )
}

fn client(server: &MockServer) -> Client {
    Client {
        http: Arc::new(Http::fixture(&server.uri(), NonZeroUsize::MIN)),
    }
}

async fn manifest(server: &MockServer, share: &str, files: &[(&str, &str, u64)]) {
    Mock::given(method("GET"))
        .and(path(format!("/dl/{share}")))
        .respond_with(ResponseTemplate::new(200).set_body_string(page(share, files)))
        .expect(1)
        .mount(server)
        .await;
}

fn signed(now: OffsetDateTime, object: &str) -> String {
    let date = now
        .format(time::macros::format_description!(
            "[year][month][day]T[hour][minute][second]Z"
        ))
        .unwrap();
    format!(
        "https://objects.swisstransfer.infomaniak.cloud/{object}?X-Amz-Date={date}&X-Amz-Expires=3600&X-Amz-Signature=fixture-bearer-secret"
    )
}

async fn access(server: &MockServer, share: &str, file: &str, object: &str, count: u64) {
    Mock::given(method("GET"))
        .and(path(format!("/api/1/links/{share}/files/{file}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data":{"url":signed(OffsetDateTime::now_utc(), object)}
        })))
        .expect(count)
        .mount(server)
        .await;
}

async fn object(server: &MockServer, name: &str, bytes: &[u8], count: u64) {
    Mock::given(method("GET"))
        .and(path(format!("/{name}")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"v1\"")
                .set_body_bytes(bytes),
        )
        .expect(count)
        .mount(server)
        .await;
}

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "xget-swiss-common-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt as _;
            fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        }
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
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn singleton_collection_keeps_nested_path_and_planning_spends_no_access_or_payload() {
    let server = MockServer::start().await;
    manifest(&server, SHARE, &[(FILE_A, "folder/one.bin", 3)]).await;
    let client = client(&server);
    let root = Directory::new();
    let output = root.path().join("absent");
    let inventory = client.resolve(reference(SHARE)).await.unwrap();
    assert_eq!(inventory.files().len(), 1);
    let file = inventory.into_files().next().unwrap();
    assert_eq!(file.input.scope(), InputScope::Collection);
    assert_eq!(file.member_token().unwrap().as_str(), FILE_A);
    assert_eq!(file.source_path(), "folder/one.bin");
    // Selected-file re-entry retains metadata and does not fetch the page again.
    let plan = client
        .download(file)
        .to_directory(&output)
        .plan()
        .await
        .unwrap();
    assert_eq!(
        plan.files().next().unwrap().destination(),
        output.join("folder/one.bin")
    );
    assert!(!output.exists());
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn default_singleton_collection_uses_readable_fallback_not_a_uuid_or_basename() {
    let server = MockServer::start().await;
    manifest(&server, SHARE, &[(FILE_A, "nested/one", 0)]).await;
    let plan = client(&server)
        .download(reference(SHARE))
        .plan()
        .await
        .unwrap();
    assert_eq!(
        plan.files().next().unwrap().destination(),
        Path::new("./SwissTransfer/nested/one")
    );
}

#[cfg(unix)]
#[tokio::test]
async fn mixed_exact_and_multiple_shares_publish_through_the_common_coordinator() {
    let server = MockServer::start().await;
    manifest(
        &server,
        SHARE,
        &[(FILE_A, "nested/a", 3), (FILE_B, "empty", 0)],
    )
    .await;
    manifest(&server, SHARE_B, &[(FILE_A, "nested/a", 3)]).await;
    access(&server, SHARE, FILE_A, "a", 1).await;
    access(&server, SHARE, FILE_B, "zero", 1).await;
    access(&server, SHARE_B, FILE_A, "b", 1).await;
    object(&server, "a", b"abc", 1).await;
    object(&server, "zero", b"", 1).await;
    object(&server, "b", b"abc", 1).await;
    Mock::given(method("HEAD"))
        .and(path("/exact"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"v1\"")
                .insert_header("content-length", "3"),
        )
        .expect(1)
        .mount(&server)
        .await;
    object(&server, "exact", b"abc", 1).await;
    let root = Directory::new();
    let inputs = [
        Input::parse(&reference(SHARE))
            .unwrap()
            .under("first")
            .unwrap(),
        Input::parse(&reference(SHARE_B))
            .unwrap()
            .under("second")
            .unwrap(),
        Input::parse("https://example.org/exact").unwrap(),
    ];
    let report = client(&server)
        .download_all(inputs)
        .to_directory(root.path())
        .run()
        .await
        .unwrap();
    assert_eq!(report.completed().len(), 4);
    assert_eq!(
        fs::read(root.path().join("first/nested/a")).unwrap(),
        b"abc"
    );
    assert_eq!(
        fs::read(root.path().join("second/nested/a")).unwrap(),
        b"abc"
    );
    assert_eq!(fs::read(root.path().join("first/empty")).unwrap(), b"");
    assert_eq!(fs::read(root.path().join("exact")).unwrap(), b"abc");
}

#[tokio::test]
async fn equal_fallback_groups_conflict_even_with_disjoint_member_names() {
    let server = MockServer::start().await;
    manifest(&server, SHARE, &[(FILE_A, "a", 3)]).await;
    manifest(&server, SHARE_B, &[(FILE_B, "b", 3)]).await;
    let root = Directory::new();
    let result = client(&server)
        .download_all([reference(SHARE), reference(SHARE_B)])
        .to_directory(root.path())
        .plan()
        .await;
    assert!(matches!(result, Err(Error::LayoutConflict)));
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn literal_selection_keeps_the_complete_share_metadata_charge() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/dl/{SHARE}")))
        .respond_with(ResponseTemplate::new(200).set_body_string(page(
            SHARE,
            &[(FILE_A, "nested/a", 3), (FILE_B, "other/b", 3)],
        )))
        .expect(2)
        .mount(&server)
        .await;
    let client = client(&server);
    let complete = client.resolve(reference(SHARE)).await.unwrap();
    let charge = complete.metadata_bytes();
    let selected = client
        .resolve(
            Input::parse(&reference(SHARE))
                .unwrap()
                .select_id(FILE_B)
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(complete.files().len(), 2);
    assert_eq!(selected.files().len(), 1);
    assert_eq!(
        selected
            .files()
            .next()
            .unwrap()
            .member_token()
            .unwrap()
            .as_str(),
        FILE_B
    );
    assert_eq!(selected.metadata_bytes(), charge);
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        2,
        "metadata pages only"
    );
}

#[tokio::test]
async fn explicit_selection_is_scoped_stale_safe_and_global_expectation_checks_selected_count() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/dl/{SHARE}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(page(SHARE, &[(FILE_A, "a", 3), (FILE_B, "b", 3)])),
        )
        .expect(3)
        .mount(&server)
        .await;
    let client = client(&server);
    let result = client
        .download(reference(SHARE))
        .expect(ABC.parse::<Digest>().unwrap())
        .plan()
        .await;
    assert!(matches!(result, Err(Error::GlobalRequirementCardinality)));
    let plan = client
        .download(reference(SHARE))
        .select_id(FILE_B)
        .expect(ABC.parse::<Digest>().unwrap())
        .to_file("chosen")
        .plan()
        .await
        .unwrap();
    assert_eq!(
        plan.files()
            .next()
            .unwrap()
            .file()
            .member_token()
            .unwrap()
            .as_str(),
        FILE_B
    );
    let result = client
        .download(reference(SHARE))
        .select_id(SHARE_B)
        .plan()
        .await;
    assert!(matches!(result, Err(Error::Selection)));
}

#[tokio::test]
async fn open_owns_the_actual_body_and_pins_before_releasing_bytes() {
    let server = MockServer::start().await;
    manifest(&server, SHARE, &[(FILE_A, "one", 3)]).await;
    access(&server, SHARE, FILE_A, "a", 1).await;
    object(&server, "a", b"abc", 1).await;
    let client = client(&server);
    let opened = client.open(reference(SHARE)).await.unwrap();
    assert_eq!(opened.version(), VersionConsistency::StrongEtagEstablished);
    assert_eq!(
        opened.file().version(),
        crate::inventory::VersionEvidence::StrongEtag
    );
    let mut body = opened
        .expect(ABC.parse::<Digest>().unwrap())
        .unwrap()
        .measurement(Measurement::None)
        .into_body();
    let mut bytes = Vec::new();
    while let Some(chunk) = body.next().await {
        bytes.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(bytes, b"abc");
    assert!(body.finish().is_ok());
}

#[cfg(unix)]
#[tokio::test]
async fn empty_inventory_is_a_noop_without_output_creation_but_zero_byte_is_a_real_job() {
    let server = MockServer::start().await;
    manifest(&server, SHARE, &[]).await;
    let root = Directory::new();
    let absent = root.path().join("absent");
    let report = client(&server)
        .download(reference(SHARE))
        .to_directory(&absent)
        .run()
        .await
        .unwrap();
    assert_eq!(report.completed().len(), 0);
    assert!(!absent.exists());
    assert!(report.saved_run().is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn automatic_reentry_maps_one_share_to_many_frozen_members_without_manifest_rediscovery() {
    let server = MockServer::start().await;
    manifest(&server, SHARE, &[(FILE_A, "one", 3), (FILE_B, "two", 3)]).await;
    access(&server, SHARE, FILE_A, "a", 2).await;
    access(&server, SHARE, FILE_B, "b", 1).await;
    let calls = Arc::new(AtomicU64::new(0));
    let count = Arc::clone(&calls);
    Mock::given(method("GET"))
        .and(path("/a"))
        .respond_with(move |_: &wiremock::Request| {
            let status = if count.fetch_add(1, Ordering::Relaxed) == 0 {
                503
            } else {
                200
            };
            ResponseTemplate::new(status)
                .insert_header("etag", "\"v1\"")
                .set_body_bytes(b"abc")
        })
        .expect(2)
        .mount(&server)
        .await;
    object(&server, "b", b"abc", 1).await;
    let root = Directory::new();
    let client = client(&server);
    let failure = client
        .download(reference(SHARE))
        .to_directory(root.path())
        .run()
        .await
        .unwrap_err();
    let saved_path = failure.report.saved_run().unwrap().to_path_buf();
    let saved = SavedRun::inspect(&saved_path).await.unwrap();
    assert_eq!(saved.members().len(), 2);
    let bytes = fs::read(saved_path.join("state")).unwrap();
    assert!(
        !bytes
            .windows(b"fixture-bearer-secret".len())
            .any(|part| part == b"fixture-bearer-secret")
    );
    assert!(
        !bytes
            .windows(b"https://".len())
            .any(|part| part == b"https://")
    );
    let report = client
        .download(reference(SHARE))
        .to_directory(root.path())
        .run()
        .await
        .unwrap();
    assert_eq!(report.completed().len(), 2);
    assert_eq!(fs::read(root.path().join("one")).unwrap(), b"abc");
    assert_eq!(fs::read(root.path().join("two")).unwrap(), b"abc");
}

#[cfg(unix)]
#[tokio::test]
async fn a_known_version_change_cannot_be_overridden_by_an_expected_digest_on_frozen_rebind() {
    let server = MockServer::start().await;
    manifest(&server, SHARE, &[(FILE_A, "one", 3)]).await;
    access(&server, SHARE, FILE_A, "a", 2).await;
    let calls = Arc::new(AtomicU64::new(0));
    let count = Arc::clone(&calls);
    Mock::given(method("GET"))
        .and(path("/a"))
        .respond_with(move |request: &wiremock::Request| {
            if count.fetch_add(1, Ordering::Relaxed) == 0 {
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v1\"")
                    .set_body_bytes(b"xyz")
            } else {
                assert_eq!(request.headers.get("if-match").unwrap(), "\"v1\"");
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v2\"")
                    .set_body_bytes(b"abc")
            }
        })
        .expect(2)
        .mount(&server)
        .await;
    let root = Directory::new();
    let client = client(&server);
    let failure = client
        .download(reference(SHARE))
        .expect(ABC.parse::<Digest>().unwrap())
        .to_directory(root.path())
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.error, Error::Transfer(_)));
    let saved_path = failure.report.saved_run().unwrap().to_path_buf();
    let saved = SavedRun::inspect(&saved_path).await.unwrap();
    assert_eq!(
        saved.members().next().unwrap().version(),
        crate::inventory::VersionEvidence::StrongEtag
    );
    let key = saved.members().next().unwrap().key();
    let failure = client
        .resume(saved)
        .input(key, reference(SHARE))
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.error, Error::VersionChanged));
    assert!(!root.path().join("one").exists());
    let saved = SavedRun::inspect(saved_path).await.unwrap();
    assert_eq!(saved.members().next().unwrap().attempts().len(), 1);
}

#[tokio::test]
async fn live_denial_does_not_refresh_or_reveal_bearer_values_in_any_error_chain() {
    let server = MockServer::start().await;
    manifest(&server, SHARE, &[(FILE_A, "one", 3)]).await;
    access(&server, SHARE, FILE_A, "a", 1).await;
    Mock::given(method("GET"))
        .and(path("/a"))
        .respond_with(ResponseTemplate::new(403))
        .expect(1)
        .mount(&server)
        .await;
    let error = client(&server).open(reference(SHARE)).await.unwrap_err();
    assert!(matches!(error, Error::Status { status: 403, .. }));
    let mut source: Option<&dyn core::error::Error> = Some(&error);
    while let Some(error) = source {
        let rendered = format!("{error:?} {error}");
        assert!(!rendered.contains("fixture-bearer-secret"));
        assert!(!rendered.contains("X-Amz"));
        assert!(!rendered.contains(SHARE));
        source = error.source();
    }
}

#[test]
fn manifest_sizes_remain_exact_u64_values_and_ignored_depth_is_byte_bounded() {
    let input = Input::parse(&reference(SHARE)).unwrap();
    for size in [0, (1_u64 << 53) + 1, u64::MAX] {
        let body = page(SHARE, &[(FILE_A, "one", size)]);
        let members = Manifest::parse(&input, body.as_bytes()).unwrap();
        assert_eq!(members[0].size, crate::transfer::ByteCount(size));
    }
    let valid = page(SHARE, &[(FILE_A, "one", 0)]);
    assert!(valid.contains("\"size\":0"));
    for invalid in ["18446744073709551616", "-1", "0.5", "null", "\"3\""] {
        let body = valid.replace("\"size\":0", &format!("\"size\":{invalid}"));
        assert!(Manifest::parse(&input, body.as_bytes()).is_err());
    }
    // IgnoredAny skips deeply nested unknown data iteratively. The inbound byte cap, not a
    // universal small JSON-depth claim, bounds this accepted unknown field.
    let ignored = format!("{}0{}", "[".repeat(4096), "]".repeat(4096));
    let body = valid.replacen('{', &format!("{{\"ignored\":{ignored},"), 1);
    assert_eq!(Manifest::parse(&input, body.as_bytes()).unwrap().len(), 1);
}

#[test]
fn complete_manifest_validation_precedes_selection_and_rejects_unsupported_or_incomplete_shapes() {
    let input = Input::parse(&reference(SHARE)).unwrap();
    for body in [
        "",
        "<script data-page=app type=application/json>{}</script>",
        "<script data-page=app type=application/json>{\"component\":\"link/password\"}</script>",
    ] {
        assert!(Manifest::parse(&input, body.as_bytes()).is_err());
    }
    let valid = page(SHARE, &[(FILE_A, "one", 0)]);
    assert_eq!(Manifest::parse(&input, valid.as_bytes()).unwrap().len(), 1);
    assert!(Manifest::parse(&input, format!("{valid}{valid}").as_bytes()).is_err());
    assert!(Manifest::parse(&input, page(SHARE_B, &[]).as_bytes()).is_err());
    for files in [
        vec![(FILE_A, "one", 0), (FILE_A, "two", 1)],
        vec![(FILE_A, "one", 0), (FILE_B, "one", 1)],
        vec![(FILE_A, "one", 0), (FILE_B, "one/two", 1)],
        vec![(FILE_A, "a", 0), (FILE_B, "A", 1)],
        vec![(FILE_A, "../escape", 1)],
        vec![(FILE_A, ".xget-runs/x", 1)],
    ] {
        assert!(Manifest::parse(&input, page(SHARE, &files).as_bytes()).is_err());
    }
    let too_many = vec![(FILE_A, "one", 0); crate::inventory::MAX_FILES + 1];
    assert!(Manifest::parse(&input, page(SHARE, &too_many).as_bytes()).is_err());
    assert!(Manifest::parse(&input, &vec![b' '; super::MAX_PAGE + 1]).is_err());
    assert!(Manifest::parse(&input, &[0xff]).is_err());
}

#[tokio::test]
async fn expired_denial_renews_once_while_a_healthy_open_body_outlives_its_lease() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/dl/{SHARE}")))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(page(SHARE, &[(FILE_A, "one", 3)])),
        )
        .expect(2)
        .mount(&server)
        .await;
    let leases = Arc::new(AtomicU64::new(0));
    let lease_count = Arc::clone(&leases);
    Mock::given(method("GET"))
        .and(path(format!("/api/1/links/{SHARE}/files/{FILE_A}")))
        .respond_with(move |_: &wiremock::Request| {
            let index = lease_count.fetch_add(1, Ordering::Relaxed);
            let url = signed(OffsetDateTime::now_utc(), "renew");
            let url = if index == 0 || index == 2 {
                url.replace("X-Amz-Expires=3600", "X-Amz-Expires=2")
            } else {
                url
            };
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"data":url}))
        })
        .expect(3)
        .mount(&server)
        .await;
    let objects = Arc::new(AtomicU64::new(0));
    let object_count = Arc::clone(&objects);
    Mock::given(method("GET"))
        .and(path("/renew"))
        .respond_with(move |_: &wiremock::Request| {
            if object_count.fetch_add(1, Ordering::Relaxed) == 0 {
                ResponseTemplate::new(403).set_delay(core::time::Duration::from_secs(3))
            } else {
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v1\"")
                    .set_body_bytes(b"abc")
            }
        })
        .expect(3)
        .mount(&server)
        .await;
    let client = client(&server);
    let mut body = client.open(reference(SHARE)).await.unwrap().into_body();
    while let Some(chunk) = body.next().await {
        let _ = chunk.unwrap();
    }
    body.finish().unwrap();
    let opened = client.open(reference(SHARE)).await.unwrap();
    tokio::time::sleep(core::time::Duration::from_secs(3)).await;
    let mut body = opened.into_body();
    let mut bytes = Vec::new();
    while let Some(chunk) = body.next().await {
        bytes.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(bytes, b"abc");
    body.finish().unwrap();
    assert_eq!(leases.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn selected_file_iterators_keep_one_collection_group_without_refetching_the_manifest() {
    let server = MockServer::start().await;
    manifest(
        &server,
        SHARE,
        &[(FILE_A, "one/a", 3), (FILE_B, "two/b", 3)],
    )
    .await;
    let client = client(&server);
    let inventory = client.resolve(reference(SHARE)).await.unwrap();
    let plan = client
        .download_all(inventory.into_files())
        .to_directory("chosen")
        .plan()
        .await
        .unwrap();
    let paths: Vec<_> = plan.files().map(|file| file.destination()).collect();
    assert_eq!(
        paths,
        [Path::new("chosen/one/a"), Path::new("chosen/two/b")]
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn metadata_and_count_limits_fail_without_spending_access_or_payload_requests() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/dl/{SHARE}")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![b' '; super::MAX_PAGE + 1]))
        .expect(1)
        .mount(&server)
        .await;
    assert!(matches!(
        client(&server).resolve(reference(SHARE)).await,
        Err(Error::Inventory(_))
    ));
    server.reset().await;
    let ids: Vec<_> = (0..128)
        .map(|index| format!("{index:08x}-1111-2222-3333-444444444444"))
        .collect();
    let names: Vec<_> = (0..128).map(|index| format!("file-{index}")).collect();
    let rows: Vec<_> = ids
        .iter()
        .zip(&names)
        .map(|(id, name)| (id.as_str(), name.as_str(), 0))
        .collect();
    manifest(&server, SHARE, &rows).await;
    Mock::given(method("HEAD"))
        .and(path("/exact"))
        .respond_with(ResponseTemplate::new(200).insert_header("content-length", "0"))
        .expect(1)
        .mount(&server)
        .await;
    let result = client(&server)
        .download_all(["https://example.org/exact".to_owned(), reference(SHARE)])
        .to_directory("unused")
        .plan()
        .await;
    assert!(matches!(result, Err(Error::Inventory(_))));
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

#[test]
fn access_supports_both_shapes_and_refuses_ambiguous_expired_or_foreign_authorities() {
    let now = OffsetDateTime::now_utc();
    let url = signed(now, "one");
    for data in [
        serde_json::json!({"data":url}),
        serde_json::json!({"data":{"url":url}}),
    ] {
        assert!(Access::parse(&serde_json::to_vec(&data).unwrap(), now).is_ok());
    }
    for bad in [
        url.replace("objects.swisstransfer.infomaniak.cloud", "evil.test"),
        url.replace("https://", "http://"),
        url.replace("https://", "https://user:secret@"),
        format!("{url}#fragment"),
        format!("{url}&X-Amz-Expires=5"),
        format!("{url}&X-Amz-Date=20261020T000000Z"),
        url.replace("infomaniak.cloud/", "infomaniak.cloud:8443/"),
        signed(now + time::Duration::minutes(10), "future"),
        url.replace("X-Amz-Expires=3600", "X-Amz-Expires=0"),
        url.replace("X-Amz-Expires=3600", "X-Amz-Expires=604801"),
        signed(now - time::Duration::hours(2), "expired"),
    ] {
        let bytes = serde_json::to_vec(&serde_json::json!({"data":bad})).unwrap();
        let error = Access::parse(&bytes, now).err().unwrap();
        let rendered = format!("{error} {error:?}");
        assert!(!rendered.contains("secret"));
        assert!(!rendered.contains("X-Amz"));
    }
}
