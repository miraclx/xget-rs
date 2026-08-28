//! Planning a resource into contiguous byte-range chunks to fetch in parallel.

use crate::ByteRange;

/// Divide a resource of `length` bytes into up to `parts` contiguous, non-overlapping chunks that tile
/// `[0, length)` exactly. Sizes differ by at most one byte, the earliest chunks taking the remainder.
/// `parts` is clamped to at least one and at most `length`, so every chunk covers at least one byte and
/// a zero-length resource plans to no chunks.
#[must_use]
pub fn plan(length: u64, parts: u32) -> Vec<ByteRange> {
    plan_range(0, length, parts)
}

/// Tile `[start, end)` into up to `parts` contiguous chunks, the same way [`plan`] tiles a whole
/// resource. Used to plan only the bytes a resumed download still needs. An empty or inverted range
/// plans to no chunks.
#[must_use]
pub fn plan_range(start: u64, end: u64, parts: u32) -> Vec<ByteRange> {
    let length = end.saturating_sub(start);
    if length == 0 {
        return Vec::new();
    }
    let parts = u64::from(parts.max(1)).min(length);
    let base = length / parts;
    let extra = length % parts;
    let mut ranges = Vec::with_capacity(parts as usize);
    let mut offset = start;
    for index in 0..parts {
        let size = base + u64::from(index < extra);
        ranges.push(ByteRange {
            start: offset,
            end: offset + size,
        });
        offset += size;
    }
    ranges
}

/// Plan a resume as the same `parts` chunks a fresh run uses, each paired with how many of its bytes are
/// already on disk as a contiguous prefix. A chunk downloads sequentially from its start, so its bytes
/// on disk are a prefix `[chunk.start, chunk.start + received)`; the chunk resumes by fetching only the
/// rest, exactly like retrying a connection dropped mid-chunk. Keeping the same chunks, rather than
/// re-cutting the file around the recorded ranges, is what keeps the progress view the fixed `parts`
/// chunks each shown partially done, instead of fragmenting into many.
///
/// `done` ranges may be unsorted, overlapping, adjacent, or reach past `total`; they are clamped and
/// merged first. When `parts` differs from the run that recorded the ranges, a chunk's on-disk bytes may
/// not be a clean prefix (a hole can fall inside it); only the leading contiguous run is credited and
/// the rest is refetched.
#[must_use]
pub fn plan_resume(total: u64, parts: u32, done: &[ByteRange]) -> (Vec<ByteRange>, Vec<u64>) {
    let ranges = plan_range(0, total, parts);
    // Clamp to the resource and merge, so stale, overlapping, or adjacent ranges do not mislead.
    let clamped: Vec<ByteRange> = done
        .iter()
        .map(|range| ByteRange {
            start: range.start.min(total),
            end: range.end.min(total),
        })
        .filter(|range| range.start < range.end)
        .collect();
    let done = merge_ranges(clamped);

    let received = ranges
        .iter()
        .map(|chunk| contiguous_prefix(*chunk, &done))
        .collect();
    (ranges, received)
}

/// How many bytes of `chunk`, from its start, `done` covers without a gap. `done` is sorted and
/// non-overlapping. A hole before the chunk end stops the count there, so only the leading run is
/// credited and any covered bytes past the hole are ignored (they are refetched).
fn contiguous_prefix(chunk: ByteRange, done: &[ByteRange]) -> u64 {
    let mut frontier = chunk.start;
    for range in done {
        if range.start > frontier {
            break;
        }
        if range.end > frontier {
            frontier = range.end.min(chunk.end);
        }
        if frontier >= chunk.end {
            break;
        }
    }
    frontier - chunk.start
}

/// Sort and coalesce ranges so the result is non-overlapping and gap-free between touching ranges, which
/// keeps the resume plan's boundaries minimal.
fn merge_ranges(mut ranges: Vec<ByteRange>) -> Vec<ByteRange> {
    ranges.sort_by_key(|range| range.start);
    let mut merged: Vec<ByteRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        match merged.last_mut() {
            Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
            _ => merged.push(range),
        }
    }
    merged
}
