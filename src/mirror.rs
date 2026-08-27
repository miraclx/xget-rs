//! A [`Source`] that fails over across several equivalent sources (mirrors of one resource).

use crate::{ByteRange, ByteStream, Error, Probe, Source};

/// Several sources for the same resource, tried in order until one answers. Every operation starts at
/// the primary and falls through to each mirror on error, so a dead or throttled host is skipped
/// without failing the download. Because a dropped chunk is retried, a mid-stream failure on one mirror
/// is re-attempted from where it stopped, possibly against another.
pub struct Mirrors<S> {
    sources: Vec<S>,
}

impl<S: Source> Mirrors<S> {
    /// Build a set from the primary source and zero or more mirrors, tried in that order.
    pub fn new(primary: S, mirrors: impl IntoIterator<Item = S>) -> Self {
        let mut sources = vec![primary];
        sources.extend(mirrors);
        Self { sources }
    }
}

impl<S: Source> Source for Mirrors<S> {
    async fn probe(&self) -> Result<Probe, Error> {
        let mut last = None;
        for source in &self.sources {
            match source.probe().await {
                Ok(probe) => return Ok(probe),
                Err(error) => last = Some(error),
            }
        }
        Err(last.unwrap_or_else(no_sources))
    }

    async fn fetch(&self, range: Option<ByteRange>) -> Result<ByteStream, Error> {
        let mut last = None;
        for source in &self.sources {
            match source.fetch(range).await {
                Ok(stream) => return Ok(stream),
                Err(error) => last = Some(error),
            }
        }
        Err(last.unwrap_or_else(no_sources))
    }
}

/// The error for an empty mirror set. `Mirrors::new` always keeps the primary, so this is unreachable
/// in practice, but the loops stay total without an unwrap.
fn no_sources() -> Error {
    Error::Transport(Box::new(std::io::Error::other("no sources to fetch from")))
}
