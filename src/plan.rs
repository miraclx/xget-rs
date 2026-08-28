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

/// Plan a resume: tile `[0, total)` into ~`parts` uniform chunks, cutting at every boundary of the byte
/// ranges already on disk so no chunk ever straddles a done/missing edge, and returning the indices of
/// the chunks that are fully on disk. Both the done regions and the gaps are subdivided to the same
/// target size, so a resume shows the same uniform chunks a fresh run would, with some already complete
/// (five chunks with two done stays five chunks, two done; switching to ten makes ten, with the covered
/// ones marked done).
///
/// This is what lets a resume re-chunk with a different `parts` than the run that started it: the plan
/// is rebuilt from the bytes actually present, not from the old chunk count. The returned ranges tile
/// `[0, total)` exactly and in order, so the engine's contiguous-prefix verify works over them unchanged.
/// `done` ranges may be unsorted, overlapping, adjacent, or reach past `total`; they are clamped and
/// merged first.
#[must_use]
pub fn plan_resume(total: u64, parts: u32, done: &[ByteRange]) -> (Vec<ByteRange>, Vec<usize>) {
    // Clamp to the resource and merge, so stale, overlapping, or adjacent ranges do not break the tiling.
    let clamped: Vec<ByteRange> = done
        .iter()
        .map(|range| ByteRange {
            start: range.start.min(total),
            end: range.end.min(total),
        })
        .filter(|range| range.start < range.end)
        .collect();
    let done = merge_ranges(clamped);

    // The target chunk size that ~`parts` chunks over the whole resource would use, so every subdivided
    // segment lands near it and the plan reads as uniform chunks regardless of what is already done.
    let target = total.div_ceil(u64::from(parts.max(1))).max(1);

    let mut ranges = Vec::new();
    let mut completed = Vec::new();
    let mut cursor = 0;
    for range in done {
        if cursor < range.start {
            subdivide(&mut ranges, cursor, range.start, target);
        }
        let first = ranges.len();
        subdivide(&mut ranges, range.start, range.end, target);
        completed.extend(first..ranges.len());
        cursor = range.end;
    }
    if cursor < total {
        subdivide(&mut ranges, cursor, total, target);
    }
    (ranges, completed)
}

/// Split `[start, end)` into contiguous chunks of about `target` bytes each and append them, so a done
/// region or a gap is tiled at the same granularity as the rest of the plan.
fn subdivide(ranges: &mut Vec<ByteRange>, start: u64, end: u64, target: u64) {
    let pieces = (end - start).div_ceil(target).clamp(1, u64::from(u32::MAX)) as u32;
    ranges.extend(plan_range(start, end, pieces));
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
