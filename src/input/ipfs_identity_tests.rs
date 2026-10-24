#![allow(clippy::expect_used, clippy::unwrap_used)]

use cid::Cid;
use cid::multihash::Multihash;

use super::{IpfsIdentity, IpfsIdentityError};
use crate::checks::{Algorithm, Digest};

fn cid(codec: u64, code: u64, bytes: &[u8]) -> Cid {
    Cid::new_v1(codec, Multihash::<64>::wrap(code, bytes).unwrap())
}

#[test]
fn raw_content_address_is_a_required_flat_digest_and_not_a_measurement() {
    let hash =
        hex::decode("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad").unwrap();
    let raw = cid(0x55, 0x12, &hash);
    let parsed = IpfsIdentity::parse(&raw.to_string()).unwrap();
    assert_eq!(parsed.as_str(), raw.to_string());
    assert_eq!(parsed.source_path(), raw.to_string());
    assert_eq!(
        parsed.required(),
        Some(Digest::parse(Algorithm::Sha256, &hex::encode(hash)).unwrap())
    );
    for (code, size) in [(0x11, 20), (0x12, 32), (0x13, 64), (0x1e, 32)] {
        assert!(
            IpfsIdentity::parse(&cid(0x55, code, &vec![0; size]).to_string())
                .unwrap()
                .required()
                .is_some()
        );
    }
}

#[test]
fn dag_root_and_case_sensitive_v0_do_not_become_flat_file_checks() {
    let hash = Multihash::<64>::wrap(0x12, &[1; 32]).unwrap();
    let v0 = Cid::new_v0(hash).unwrap();
    let text = v0.to_string();
    assert!(text.chars().any(char::is_uppercase));
    let parsed = IpfsIdentity::parse(&text).unwrap();
    assert_eq!(parsed.as_str(), text);
    assert_eq!(parsed.required(), None);
    assert!(IpfsIdentity::parse(&text.to_ascii_lowercase()).is_err());
    let named = IpfsIdentity::parse(&format!("{text}/folder/%C3%A9.txt")).unwrap();
    assert_eq!(named.source_path(), "folder/é.txt");
    assert!(named.as_str().ends_with("folder/%C3%A9.txt"));
    assert!(!format!("{named:?}").contains("folder"));
}

#[test]
fn unsupported_algorithms_codecs_and_raw_namespaces_are_refused() {
    for raw in [cid(0x55, 0x12, &[0; 16]), cid(0x55, 0x99, &[0; 32])] {
        assert!(matches!(
            IpfsIdentity::parse(&raw.to_string()),
            Err(IpfsIdentityError::UnsupportedDigest)
        ));
    }
    assert!(matches!(
        IpfsIdentity::parse(&cid(0x71, 0x12, &[0; 32]).to_string()),
        Err(IpfsIdentityError::UnsupportedCodec)
    ));
    let raw = cid(0x55, 0x12, &[0; 32]);
    assert!(matches!(
        IpfsIdentity::parse(&format!("{raw}/file")),
        Err(IpfsIdentityError::RawPath)
    ));
    for spelling in ["", "not-a-cid", "user:secret@cid", "cid:8080"] {
        assert!(IpfsIdentity::parse(spelling).is_err());
    }
}

#[test]
fn path_spelling_is_not_normalized_or_allowed_to_hide_directory_scope() {
    let dag = cid(0x70, 0x12, &[2; 32]);
    for path in [
        "",
        ".",
        "..",
        "a/../b",
        "a//b",
        "a/",
        "/a",
        "%",
        "%0",
        "%gg",
        "%2f",
        "%5c",
        "%25",
        "%00",
        "%2e",
        "%2e%2e",
        "a?token=secret",
        "a#fragment",
        "é",
        "%ff",
    ] {
        assert!(
            matches!(
                IpfsIdentity::parse(&format!("{dag}/{path}")),
                Err(IpfsIdentityError::Path)
            ),
            "{path}"
        );
    }
    let source = format!("{dag}/a%20b/file.txt");
    let parsed = IpfsIdentity::parse(&source).unwrap();
    assert_eq!(parsed.as_str(), source);
    assert_eq!(parsed.source_path(), "a b/file.txt");
}

#[test]
fn spelling_and_component_limits_accept_the_boundary_then_refuse_the_next() {
    let dag = cid(0x70, 0x12, &[3; 32]).to_string();
    let exact = format!("{dag}/{}", "x".repeat(4096 - dag.len() - 1));
    assert_eq!(IpfsIdentity::parse(&exact).unwrap().as_str().len(), 4096);
    assert!(matches!(
        IpfsIdentity::parse(&(exact + "x")),
        Err(IpfsIdentityError::Limit)
    ));
    let path = vec!["x"; 64].join("/");
    assert!(IpfsIdentity::parse(&format!("{dag}/{path}")).is_ok());
    assert!(matches!(
        IpfsIdentity::parse(&format!("{dag}/{path}/x")),
        Err(IpfsIdentityError::Path)
    ));
}
