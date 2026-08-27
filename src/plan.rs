//! Planning a resource into contiguous byte-range chunks to fetch in parallel.

use crate::ByteRange;

/// Divide a resource of `length` bytes into up to `parts` contiguous, non-overlapping chunks that tile
/// `[0, length)` exactly. Sizes differ by at most one byte, the earliest chunks taking the remainder.
/// `parts` is clamped to at least one and at most `length`, so every chunk covers at least one byte and
/// a zero-length resource plans to no chunks.
#[must_use]
pub fn plan(length: u64, parts: u32) -> Vec<ByteRange> {
    if length == 0 {
        return Vec::new();
    }
    let parts = u64::from(parts.max(1)).min(length);
    let base = length / parts;
    let extra = length % parts;
    let mut ranges = Vec::with_capacity(parts as usize);
    let mut start = 0;
    for index in 0..parts {
        let size = base + u64::from(index < extra);
        ranges.push(ByteRange {
            start,
            end: start + size,
        });
        start += size;
    }
    ranges
}
