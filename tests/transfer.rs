//! External use of the actual new public API, not a test facade.
#![cfg(unix)]
#![allow(clippy::unwrap_used)]

use core::convert::Infallible;
use std::fs;
use std::os::unix::fs::DirBuilderExt as _;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::stream;
use xget::Transfer;
use xget::transfer::{ByteCount, Length, Reconciliation, RequiredSha256, StateRef};

#[tokio::test]
async fn an_external_caller_can_publish_and_reconcile_without_internal_types() {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "xget-native-external-{}-{time:x}",
        std::process::id()
    ));
    fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
    let required: RequiredSha256 =
        "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            .parse()
            .unwrap();
    let body = stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"abc"))]);
    let pending = Transfer::from_body(body, Length::Known(ByteCount(3)))
        .expect(required)
        .to_file(root.join("output"))
        .unwrap();
    let state = StateRef::clone(pending.state());
    assert!(!state.path().exists());
    let receipt = require_send(pending.run()).await.unwrap();
    assert_eq!(receipt.partial.hashed, ByteCount(3));
    assert_eq!(receipt.partial.actual, Some(required.digest()));
    assert_eq!(fs::read(root.join("output")).unwrap(), b"abc");
    assert!(matches!(
        StateRef::from(state.path()).reconcile().await.unwrap(),
        Reconciliation::Published(_)
    ));
    fs::remove_dir_all(root).unwrap();
}

fn require_send<T: Send>(value: T) -> T {
    value
}
