//! The control writer owns fresh allocation so no caller can truncate its trailer out of order.

use core::sync::atomic::{AtomicU32, Ordering};
use std::path::PathBuf;

use crate::control::{Control, Writer};
use crate::{ByteRange, Checksum, Error};

#[tokio::test]
async fn creating_control_allocates_the_data_before_its_trailer() {
    for total in [0, 1, 4096] {
        let scratch = Scratch::new();
        let mut writer = Writer::create(
            &scratch.path,
            total,
            Some("\"v1\""),
            Some("https://example.com/file"),
            Checksum::Sha256,
        )
        .await
        .expect("a fresh writer creates its own data region");
        let control = Control::read(&scratch.path).await.expect("valid trailer");
        assert_eq!(control.total, total);
        assert_eq!(control.validator.as_deref(), Some("\"v1\""));
        assert_eq!(control.source.as_deref(), Some("https://example.com/file"));
        assert_eq!(control.checksum, Some(Checksum::Sha256));
        assert!(control.done.is_empty());
        let bytes = std::fs::read(&scratch.path).expect("allocated file");
        assert!(bytes[..total as usize].iter().all(|byte| *byte == 0));
        writer.finish().await.expect("remove the trailer");
        assert_eq!(
            std::fs::metadata(&scratch.path)
                .expect("finished file")
                .len(),
            total
        );
        assert!(Control::read(&scratch.path).await.is_none());
    }
}

#[tokio::test]
async fn creating_fresh_control_discards_stale_bytes_and_ranges() {
    let scratch = Scratch::new();
    let mut old = Writer::create(&scratch.path, 4096, Some("old"), None, Checksum::Sha256)
        .await
        .expect("old writer");
    old.append(ByteRange {
        start: 0,
        end: 4096,
    })
    .await
    .expect("old range");
    drop(old);

    let _fresh = Writer::create(&scratch.path, 16, Some("new"), None, Checksum::Sha256)
        .await
        .expect("restart with a smaller resource");
    let control = Control::read(&scratch.path).await.expect("new trailer");
    assert_eq!(control.total, 16);
    assert_eq!(control.validator.as_deref(), Some("new"));
    assert!(control.done.is_empty(), "no old completed range survives");
    assert!(
        std::fs::read(&scratch.path).expect("fresh data")[..16]
            .iter()
            .all(|byte| *byte == 0)
    );
}

#[tokio::test]
async fn reopening_control_keeps_data_and_completed_ranges() {
    let scratch = Scratch::new();
    let mut writer = Writer::create(&scratch.path, 4, None, None, Checksum::Sha256)
        .await
        .expect("fresh writer");
    // Write the data without touching the trailer, as the engine's scatter handles do.
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&scratch.path)
        .expect("data handle");
    std::io::Write::write_all(&mut &file, b"data").expect("scatter bytes");
    writer
        .append(ByteRange { start: 0, end: 2 })
        .await
        .expect("first range");
    drop(writer);

    let mut resumed = Writer::open(&scratch.path, 4).await.expect("resume writer");
    resumed
        .append(ByteRange { start: 2, end: 4 })
        .await
        .expect("remaining range");
    let control = Control::read(&scratch.path)
        .await
        .expect("appended control");
    assert_eq!(
        control.done,
        [
            ByteRange { start: 0, end: 2 },
            ByteRange { start: 2, end: 4 }
        ]
    );
    resumed.finish().await.expect("finalize");
    assert_eq!(
        std::fs::read(&scratch.path).expect("finished data"),
        b"data"
    );
}

#[tokio::test]
async fn a_missing_parent_is_a_transport_error_not_a_partial_control() {
    let scratch = Scratch::new();
    let child = scratch.path.join("missing.xget");
    let result = Writer::create(&child, 4, None, None, Checksum::Sha256).await;
    assert!(matches!(result, Err(Error::Transport(_))));
    assert!(!child.exists());
}

struct Scratch {
    path: PathBuf,
}

impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
        Self {
            path: std::env::temp_dir().join(format!(
                "xget-control-test-{}-{sequence}.xget",
                std::process::id()
            )),
        }
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
