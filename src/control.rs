//! A control file recording a scatter download's plan and which chunks are fully written, so a later
//! run can resume. It sits beside the `.part` (as `<part>.st`) and is append-only, so the parallel
//! chunk completions that record into it never race, and it is deleted once the download succeeds.
//!
//! With scatter writes the `.part` is preallocated to full length from the start, so its size says
//! nothing about progress: this file is the source of truth for what has actually been downloaded.

use std::path::{Path, PathBuf};

use tokio::io::AsyncWriteExt as _;

use crate::Error;

/// The resumable state beside a `.part`: the plan the download was made with and the indices of chunks
/// whose whole region is on disk.
pub(crate) struct Control {
    /// The resource's total length, to detect a stale control against a changed resource.
    pub total: u64,
    /// How many chunks the remaining range was split into.
    pub parts: u32,
    /// The byte offset the chunk plan starts at (the resumed prefix ends here).
    pub start: u64,
    /// The indices of chunks that are fully written.
    pub completed: Vec<usize>,
}

/// The control file path for a given `.part`.
pub(crate) fn path(part: &Path) -> PathBuf {
    let mut name = part.as_os_str().to_owned();
    name.push(".st");
    PathBuf::from(name)
}

/// Read the control file for `part`, or `None` if it is missing or unparseable.
pub(crate) async fn read(part: &Path) -> Option<Control> {
    let text = tokio::fs::read_to_string(path(part)).await.ok()?;
    let (mut total, mut parts, mut start) = (None, None, None);
    let mut completed = Vec::new();
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        match (fields.next(), fields.next()) {
            (Some("total"), Some(value)) => total = value.parse().ok(),
            (Some("parts"), Some(value)) => parts = value.parse().ok(),
            (Some("start"), Some(value)) => start = value.parse().ok(),
            (Some("done"), Some(value)) => {
                if let Ok(index) = value.parse() {
                    completed.push(index);
                }
            }
            _ => {}
        }
    }
    Some(Control {
        total: total?,
        parts: parts?,
        start: start?,
        completed,
    })
}

/// Write a fresh control header for `part`, replacing any earlier one.
pub(crate) async fn begin(part: &Path, total: u64, parts: u32, start: u64) -> Result<(), Error> {
    let body = format!("total {total}\nparts {parts}\nstart {start}\n");
    tokio::fs::write(path(part), body).await.map_err(io)
}

/// Record chunk `index` as fully written by appending a line, so two completions never clobber.
pub(crate) async fn mark_done(part: &Path, index: usize) -> Result<(), Error> {
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(path(part))
        .await
        .map_err(io)?;
    file.write_all(format!("done {index}\n").as_bytes())
        .await
        .map_err(io)
}

/// Delete the control file once the download is complete. Best effort.
pub(crate) async fn remove(part: &Path) {
    let _ = tokio::fs::remove_file(path(part)).await;
}

fn io(error: std::io::Error) -> Error {
    Error::Transport(Box::new(error))
}
