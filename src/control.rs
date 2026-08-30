//! Resume control embedded in the download's own `.xget` file, so there is one artifact, not a data file
//! plus a sidecar to keep in step.
//!
//! The layout is `[data: 0..total][trailer: completed-range text][footer: fixed]`. The sparse data
//! occupies `[0, total)`; a text trailer records, one per line, an optional leading `tag <validator>`
//! (the resource validator, so a resume can tell the resource apart from a changed one) followed by
//! `done <start> <end>` lines for the byte ranges written and flushed; a fixed footer at the very end
//! carries a magic, the total, and the trailer length, so a later run can find the trailer without
//! knowing the total in advance. Chunk writes only ever touch the
//! data region, and control appends only ever touch the trailer and footer, so the two never collide.
//! On success the trailer and footer are truncated away, leaving the file a byte-exact image to rename
//! into place. A file whose footer does not validate (a foreign or truncated file) is simply not
//! resumable.

use core::convert::TryInto as _;
use core::fmt::Write as _;
use core::str::FromStr as _;
use std::io::SeekFrom;
use std::path::Path;

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};

use crate::{ByteRange, Checksum, Error};

/// Magic marking a valid xget control footer.
const MAGIC: [u8; 8] = *b"XGETCTL1";
/// The fixed footer: magic (8) + total (8) + trailer length (8) + reserved (8).
const FOOTER: u64 = 32;

/// The resumable state read from a `.xget` file: the resource length and the completed byte ranges.
pub(crate) struct Control {
    /// The resource's total length, to detect a stale control against a changed resource.
    pub total: u64,
    /// The resource validator recorded when the partial was written: an HTTP `ETag` when the server
    /// offered one, else `Last-Modified`, else the source's own immutable identity (an IPFS CID). On
    /// resume it is compared against the resource as probed now; a mismatch means the resource changed,
    /// so the partial is discarded rather than stitched into a corrupt file. `None` when the source
    /// offered no validator, in which case resume falls back to the length alone.
    pub validator: Option<String>,
    /// The source's re-openable reference (its URL) when it had one, so a resume can rebuild the source
    /// from this file alone: `xget path/to/file.xget` finishes the download without the URL being given
    /// again. `None` for a source with no string address.
    pub source: Option<String>,
    /// The checksum algorithm the download was verifying with, so a standalone resume reports the same
    /// digest the original run would have rather than defaulting. `None` for an older control without it.
    pub checksum: Option<Checksum>,
    /// The byte ranges recorded as written, each an absolute `[start, end)`. May overlap or be partial
    /// (a checkpoint of an in-flight chunk); the planner clamps and merges them.
    pub done: Vec<ByteRange>,
}

/// Read the embedded control from `path`, or `None` if the file is missing, too short, or its footer
/// does not validate.
pub(crate) async fn read(path: &Path) -> Option<Control> {
    let mut file = File::open(path).await.ok()?;
    let size = file.metadata().await.ok()?.len();
    if size < FOOTER {
        return None;
    }
    file.seek(SeekFrom::Start(size - FOOTER)).await.ok()?;
    let mut footer = [0u8; FOOTER as usize];
    file.read_exact(&mut footer).await.ok()?;
    if footer[0..8] != MAGIC {
        return None;
    }
    let total = u64::from_le_bytes(footer[8..16].try_into().ok()?);
    let trailer = u64::from_le_bytes(footer[16..24].try_into().ok()?);
    // The layout must add up exactly, or this is not a control we wrote.
    if total.checked_add(trailer)?.checked_add(FOOTER)? != size {
        return None;
    }
    file.seek(SeekFrom::Start(total)).await.ok()?;
    let mut text = vec![0u8; trailer as usize];
    file.read_exact(&mut text).await.ok()?;
    let text = String::from_utf8(text).ok()?;

    let mut done = Vec::new();
    let mut validator = None;
    let mut source = None;
    let mut checksum = None;
    for line in text.lines() {
        // The value is the rest of the line, so it may carry spaces (a `Last-Modified` date does, and a
        // URL may too once decoded).
        if let Some(value) = line.strip_prefix("url ") {
            source = Some(value.to_owned());
            continue;
        }
        if let Some(value) = line.strip_prefix("tag ") {
            validator = Some(value.to_owned());
            continue;
        }
        if let Some(value) = line.strip_prefix("algo ") {
            checksum = Checksum::from_str(value).ok();
            continue;
        }
        let mut fields = line.split_whitespace();
        if fields.next() == Some("done")
            && let (Some(Ok(start)), Some(Ok(end))) =
                (fields.next().map(str::parse), fields.next().map(str::parse))
        {
            done.push(ByteRange { start, end });
        }
    }
    Some(Control {
        total,
        validator,
        source,
        checksum,
        done,
    })
}

/// Whether `path` holds a resumable download: a valid control footer of the given total.
pub(crate) async fn is_resumable(path: &Path) -> bool {
    read(path).await.is_some()
}

/// A handle over an open `.xget` file that appends completed or checkpointed byte ranges to its trailer,
/// rewriting the footer after each, and truncates them away on finish. Held behind an async mutex and
/// shared by the fetchers, so their appends serialize even though they interleave at await points.
pub(crate) struct Writer {
    file: File,
    total: u64,
    /// The current trailer length in bytes (the footer sits immediately after it).
    trailer: u64,
}

impl Writer {
    /// Begin control for a freshly allocated file whose data region is already `set_len` to `total`:
    /// seed the trailer with the resource `source` (its URL, if any) as a `url` line and the `validator`
    /// (if any) as a `tag` line, then write the footer.
    pub(crate) async fn create(
        path: &Path,
        total: u64,
        validator: Option<&str>,
        source: Option<&str>,
        checksum: Checksum,
    ) -> Result<Writer, Error> {
        let mut file = open_rw(path).await?;
        let mut trailer_text = String::new();
        if let Some(source) = source {
            let _ = writeln!(trailer_text, "url {source}");
        }
        if let Some(value) = validator {
            let _ = writeln!(trailer_text, "tag {value}");
        }
        let _ = writeln!(trailer_text, "algo {}", checksum.name());
        let trailer = trailer_text.len() as u64;
        file.seek(SeekFrom::Start(total)).await.map_err(io)?;
        let mut buffer = trailer_text.into_bytes();
        buffer.extend_from_slice(&footer_bytes(total, trailer));
        file.write_all(&buffer).await.map_err(io)?;
        file.flush().await.map_err(io)?;
        Ok(Writer {
            file,
            total,
            trailer,
        })
    }

    /// Reopen control for a resumed file, positioned to append after its existing trailer. The caller has
    /// already validated the footer (via [`read`]) and that its total matches.
    pub(crate) async fn open(path: &Path, total: u64) -> Result<Writer, Error> {
        let file = open_rw(path).await?;
        let size = file.metadata().await.map_err(io)?.len();
        let trailer = size
            .checked_sub(total)
            .and_then(|rest| rest.checked_sub(FOOTER))
            .ok_or_else(|| detail("control file is smaller than its own layout"))?;
        Ok(Writer {
            file,
            total,
            trailer,
        })
    }

    /// Append `range` as written and re-stamp the footer. The line and the new footer are written as one
    /// contiguous buffer at the current footer's position, so the file never sits with the line in place
    /// but the footer not yet updated: a reader sees either the old trailer and footer or the new pair,
    /// never a half-written state. Recording a range twice, or a prefix then a longer one, is harmless:
    /// the planner merges them.
    pub(crate) async fn append(&mut self, range: ByteRange) -> Result<(), Error> {
        let line = format!("done {} {}\n", range.start, range.end);
        let trailer = self.trailer + line.len() as u64;
        // The line overwrites the current footer; the new footer follows it, all in one write.
        let mut buffer = line.into_bytes();
        buffer.extend_from_slice(&footer_bytes(self.total, trailer));
        self.file
            .seek(SeekFrom::Start(self.total + self.trailer))
            .await
            .map_err(io)?;
        self.file.write_all(&buffer).await.map_err(io)?;
        self.trailer = trailer;
        self.file.flush().await.map_err(io)
    }

    /// Truncate the trailer and footer away, leaving the file a byte-exact image of the resource, and
    /// flush. The caller renames it into place afterward.
    pub(crate) async fn finish(&mut self) -> Result<(), Error> {
        self.file.set_len(self.total).await.map_err(io)?;
        self.file.flush().await.map_err(io)
    }
}

/// The fixed footer bytes for a trailer of `trailer` bytes past a data region of `total` bytes: the
/// magic, the total, and the trailer length, so a resume can find and validate the trailer.
fn footer_bytes(total: u64, trailer: u64) -> [u8; FOOTER as usize] {
    let mut footer = [0u8; FOOTER as usize];
    footer[0..8].copy_from_slice(&MAGIC);
    footer[8..16].copy_from_slice(&total.to_le_bytes());
    footer[16..24].copy_from_slice(&trailer.to_le_bytes());
    footer
}

/// Open a `.xget` file for reading and writing without truncating it.
async fn open_rw(path: &Path) -> Result<File, Error> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .await
        .map_err(io)
}

fn io(error: std::io::Error) -> Error {
    Error::Transport(Box::new(error))
}

fn detail(message: &str) -> Error {
    Error::Transport(Box::new(std::io::Error::other(message.to_owned())))
}
