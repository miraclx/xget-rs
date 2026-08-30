//! Progress reporting for the CLI. One [`Reporter`] dispatches to the mode the user chose; the leaf
//! reporters live in sibling modules ([`bar`], [`plain`], [`json`]), and the shared [`Meter`] and
//! [`speedometer`] windowed rate feed the two that render numbers.

use core::time::Duration;
use std::time::Instant;

use xget::Progress;

use crate::ProgressMode;

mod bar;
mod json;
mod plain;
mod speedometer;

use bar::BarProgress;
use json::JsonProgress;
use plain::PlainProgress;
use speedometer::Speedometer;

/// Dimmed style for the chrome (borders, labels), used by the bar and by the CLI preamble in `main`.
pub(crate) const DIM: anstyle::Style = anstyle::Style::new().dimmed();

/// The chosen progress reporter, dispatching to whichever mode was selected. Every variant reports the
/// same download; they differ only in how they render it.
pub(crate) enum Reporter {
    /// A live segmented bar with a size, speed, and ETA readout.
    Bar(BarProgress),
    /// A single updating text line.
    Plain(PlainProgress),
    /// One JSON event per update.
    Json(JsonProgress),
    /// No progress output.
    Silent,
}

impl Reporter {
    pub(crate) fn new(mode: ProgressMode, raw: bool) -> Self {
        match mode {
            ProgressMode::Bar | ProgressMode::Auto => Self::Bar(BarProgress::new(raw)),
            ProgressMode::Plain => Self::Plain(PlainProgress::new(raw)),
            ProgressMode::Json => Self::Json(JsonProgress::new()),
            ProgressMode::None => Self::Silent,
        }
    }
}

impl Progress for Reporter {
    fn start(&self, chunks: &[u64]) {
        match self {
            Self::Bar(bar) => bar.start(chunks),
            Self::Plain(plain) => plain.start(chunks),
            Self::Json(json) => json.start(chunks),
            Self::Silent => {}
        }
    }

    fn restore(&self, present: &[u64]) {
        match self {
            Self::Bar(bar) => bar.restore(present),
            Self::Plain(plain) => plain.restore(present),
            Self::Json(json) => json.restore(present),
            Self::Silent => {}
        }
    }

    fn received(&self, index: usize, bytes: u64) {
        match self {
            Self::Bar(bar) => bar.received(index, bytes),
            Self::Plain(plain) => plain.received(index, bytes),
            Self::Json(json) => json.received(index, bytes),
            Self::Silent => {}
        }
    }

    fn wrote(&self, index: usize, bytes: u64) {
        match self {
            Self::Bar(bar) => bar.wrote(index, bytes),
            Self::Plain(plain) => plain.wrote(index, bytes),
            Self::Json(json) => json.wrote(index, bytes),
            Self::Silent => {}
        }
    }

    fn finish(&self) {
        match self {
            Self::Bar(bar) => bar.finish(),
            Self::Plain(plain) => plain.finish(),
            Self::Json(json) => json.finish(),
            Self::Silent => {}
        }
    }
}

/// Running totals shared by the text-based reporters: how much of how many bytes have arrived, a
/// windowed speedometer over those bytes, and a throttle so a fast download does not spend its time
/// printing. Private to this module; the leaf reporters reach it as `super::Meter`.
struct Meter {
    total: u64,
    done: u64,
    started: Instant,
    last: Instant,
    speed: Speedometer,
}

impl Meter {
    fn new(total: u64) -> Self {
        let now = Instant::now();
        Self {
            total,
            done: 0,
            started: now,
            last: now,
            speed: Speedometer::new(),
        }
    }

    /// Record `bytes` more of confirmed progress, feeding the speedometer too.
    fn add(&mut self, bytes: u64) {
        self.done += bytes;
        self.speed.add(bytes);
    }

    /// Account for `bytes` already present at the start of a resumed download. They count toward the
    /// readout total but not the speedometer, since they did not arrive over the network this run.
    fn restore(&mut self, bytes: u64) {
        self.done += bytes;
    }

    /// Whether enough time has passed since the last redraw to draw again.
    fn ready(&mut self, every: Duration) -> bool {
        let now = Instant::now();
        if now.duration_since(self.last) < every {
            return false;
        }
        self.last = now;
        true
    }

    /// Record `bytes` more and report whether enough time has passed to redraw.
    fn advance(&mut self, bytes: u64, every: Duration) -> bool {
        self.add(bytes);
        self.ready(every)
    }

    /// Bytes per second across the recent window.
    fn rate(&self) -> u64 {
        self.speed.rate()
    }

    /// Seconds until completion at the current rate, or `0` if unknown.
    fn eta(&self) -> u64 {
        let rate = self.rate();
        if rate > 0 && self.total > self.done {
            (self.total - self.done) / rate
        } else {
            0
        }
    }

    /// Percent complete, `0` when the total is unknown.
    fn percent(&self) -> u64 {
        self.done
            .saturating_mul(100)
            .checked_div(self.total)
            .unwrap_or(0)
    }

    /// Percent complete as a fraction, `0.0` when the total is unknown.
    fn percent_f64(&self) -> f64 {
        if self.total > 0 {
            self.done as f64 * 100.0 / self.total as f64
        } else {
            0.0
        }
    }

    /// The ETA as a `MM:SS` clock (minutes may exceed 59 for a long transfer).
    fn eta_clock(&self) -> String {
        let eta = self.eta();
        format!("{:02}:{:02}", eta / 60, eta % 60)
    }

    /// Milliseconds since the transfer started.
    fn elapsed_ms(&self) -> u128 {
        self.started.elapsed().as_millis()
    }
}
