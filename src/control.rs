//! A control file recording a scatter download's plan and which chunks are fully written, so a later
//! run can resume. It sits beside the `.part` (as `<part>.st`) and is append-only, so the parallel
//! chunk completions that record into it never race, and it is deleted once the download succeeds.
//!
//! With scatter writes the `.part` is preallocated to full length from the start, so its size says
//! nothing about progress: this file is the source of truth for what has actually been downloaded.

use std::path::{Path, PathBuf};

use tokio::io::AsyncWriteExt as _;

use crate::{ByteRange, Error};

/// The resumable state beside a `.part`: the resource's length and the byte ranges whose whole region is
/// on disk. Ranges rather than chunk indices, so a resume can re-chunk with a different parallelism than
/// the run that started it: the plan is rebuilt from the bytes present, not from the old chunk count.
pub(crate) struct Control {
    /// The resource's total length, to detect a stale control against a changed resource.
    pub total: u64,
    /// The byte ranges that are fully written, each an absolute `[start, end)` in the resource.
    pub done: Vec<ByteRange>,
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
    let mut total = None;
    let mut done = Vec::new();
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        match fields.next() {
            Some("total") => total = fields.next().and_then(|value| value.parse().ok()),
            Some("done") => {
                if let (Some(Ok(start)), Some(Ok(end))) =
                    (fields.next().map(str::parse), fields.next().map(str::parse))
                {
                    done.push(ByteRange { start, end });
                }
            }
            _ => {}
        }
    }
    Some(Control {
        total: total?,
        done,
    })
}

/// Write a fresh control header for `part`, replacing any earlier one.
pub(crate) async fn begin(part: &Path, total: u64) -> Result<(), Error> {
    tokio::fs::write(path(part), format!("total {total}\n"))
        .await
        .map_err(io)
}

/// Record `range` as fully written by appending a line, so two completions never clobber.
pub(crate) async fn mark_done(part: &Path, range: ByteRange) -> Result<(), Error> {
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(path(part))
        .await
        .map_err(io)?;
    file.write_all(format!("done {} {}\n", range.start, range.end).as_bytes())
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
