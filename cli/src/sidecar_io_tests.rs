//! Authored local-reader fixtures; no runtime evidence is claimed by their author.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use core::sync::atomic::{AtomicU64, Ordering};
use std::path::PathBuf;

use xget::transfer::Algorithm;

use super::LocalSidecar;

struct Directory(PathBuf);

impl Directory {
    fn fresh() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "xget-sidecar-fixture-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn actual_file_reads_are_capped_and_non_utf8_is_an_input_error() {
    let directory = Directory::fresh();
    for bytes in [vec![b'x'; 65_537], vec![0xff]] {
        let path = directory.0.join("checks");
        std::fs::write(&path, bytes).unwrap();
        let failure = LocalSidecar {
            path,
            algorithm: Algorithm::Sha256,
        }
        .read()
        .await
        .err()
        .unwrap();
        assert_eq!(failure.code(), 2);
    }
}

#[tokio::test]
async fn symlink_roles_are_rejected_without_reading_the_target() {
    let directory = Directory::fresh();
    let target = directory.0.join("target");
    std::fs::write(&target, b"not read or interpreted").unwrap();
    let link = directory.0.join("checks");
    std::os::unix::fs::symlink(&target, &link).unwrap();
    assert!(
        LocalSidecar {
            path: link,
            algorithm: Algorithm::Sha256
        }
        .read()
        .await
        .is_err()
    );
}

#[tokio::test]
async fn a_fifo_sidecar_with_no_writer_is_rejected_without_waiting_for_one() {
    use std::os::unix::ffi::OsStrExt as _;

    let directory = Directory::fresh();
    let fifo = directory.0.join("checks");
    let spelling = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
    // SAFETY: the C string is owned and valid; the path is exclusively in this new fixture directory.
    assert_eq!(unsafe { libc::mkfifo(spelling.as_ptr(), 0o600) }, 0);
    let failure = LocalSidecar {
        path: fifo,
        algorithm: Algorithm::Sha256,
    }
    .read()
    .await
    .err()
    .unwrap();
    assert_eq!(failure.code(), 2);
}
