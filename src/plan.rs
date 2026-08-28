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

/// Plan a resume: tile `[0, total)` into contiguous chunks that align to the byte ranges already on
/// disk, so no chunk ever straddles a done/missing boundary. Each already-done range passes through as
/// its own chunk (its index is returned in the second field), and the gaps between them are split toward
/// `parts` chunks in proportion to their size, at least one chunk per gap.
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
    let missing = total - done.iter().map(ByteRange::len).sum::<u64>();

    let mut ranges = Vec::new();
    let mut completed = Vec::new();
    let mut cursor = 0;
    for range in done {
        if cursor < range.start {
            push_gap(&mut ranges, cursor, range.start, parts, missing);
        }
        ranges.push(range);
        completed.push(ranges.len() - 1);
        cursor = range.end;
    }
    if cursor < total {
        push_gap(&mut ranges, cursor, total, parts, missing);
    }
    (ranges, completed)
}

/// Split a missing gap `[start, end)` into chunks in proportion to its share of the `missing` bytes,
/// toward a total of `parts` chunks across all gaps, and append them. Always at least one chunk, so no
/// gap is skipped.
fn push_gap(ranges: &mut Vec<ByteRange>, start: u64, end: u64, parts: u32, missing: u64) {
    let length = end - start;
    let share = if missing == 0 {
        1
    } else {
        // Round parts * length / missing, clamped into a u32 chunk count of at least one.
        let scaled = (u128::from(parts) * u128::from(length) + u128::from(missing) / 2)
            / u128::from(missing);
        scaled.min(u128::from(u32::MAX)) as u32
    };
    ranges.extend(plan_range(start, end, share.max(1)));
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
