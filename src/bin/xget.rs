//! xget: download a URL in parallel, verified, with a live segmented progress bar.

use core::cell::RefCell;
use core::time::Duration;
use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;
use libxget::{HttpSource, Progress};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use xbytes::prelude::*;
use xprogress::{Bar, Color, DrawTarget, Style, Width};

/// Download a URL in parallel chunks, verify it, and print its SHA-256.
#[derive(Parser)]
#[command(name = "xget", version, about)]
struct Cli {
    /// The URL to download.
    url: String,
    /// Where to write the downloaded file.
    output: PathBuf,
    /// Maximum number of concurrent chunk connections.
    #[arg(short = 'n', long, default_value_t = 5)]
    chunks: u32,
    /// Number of retries for each chunk, resuming from its offset.
    #[arg(short = 't', long, default_value_t = 10)]
    tries: u32,
    /// Set a request header, e.g. `Authorization: Bearer x` (repeatable).
    #[arg(short = 'H', long = "header")]
    headers: Vec<String>,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli = Cli::parse();
    let source = HttpSource::new(&cli.url, parse_headers(&cli.headers)?)?;
    let progress = BarProgress::new();
    let report = libxget::download(&source, &cli.output, cli.chunks, cli.tries, &progress).await?;
    println!(
        "{}  {}",
        report.sha256,
        ByteSize::of(report.length, BYTE).iec()
    );
    Ok(())
}

/// Parse repeated `Name: Value` header arguments into a [`HeaderMap`].
fn parse_headers(raw: &[String]) -> eyre::Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    for entry in raw {
        let (name, value) = entry
            .split_once(':')
            .ok_or_else(|| eyre::eyre!("header must be `Name: Value`: {entry}"))?;
        headers.insert(
            name.trim().parse::<HeaderName>()?,
            value.trim().parse::<HeaderValue>()?,
        );
    }
    Ok(headers)
}

/// A live segmented progress bar: one xprogress segment per chunk, with an xbytes size, speed, and ETA
/// readout, drawn in place and throttled so a fast download does not spend its time redrawing.
struct BarProgress {
    inner: RefCell<Option<Live>>,
}

struct Live {
    bar: Bar,
    target: DrawTarget,
    width: u16,
    total: u64,
    done: u64,
    started: Instant,
    last: Instant,
}

impl BarProgress {
    fn new() -> Self {
        Self {
            inner: RefCell::new(None),
        }
    }
}

impl Progress for BarProgress {
    fn start(&self, chunks: &[u64]) {
        let bar =
            Bar::new(chunks.iter().copied()).with_style(Style::default().with_color(Color::Cyan));
        let target = DrawTarget::from_env();
        let width = Width::TerminalMinus(46).resolve(target.columns(), 32);
        let now = Instant::now();
        let mut slot = self.inner.borrow_mut();
        *slot = Some(Live {
            bar,
            target,
            width,
            total: chunks.iter().sum(),
            done: 0,
            started: now,
            last: now,
        });
        if let Some(live) = slot.as_mut() {
            draw(live, true);
        }
    }

    fn advance(&self, index: usize, bytes: u64) {
        if let Some(live) = self.inner.borrow_mut().as_mut() {
            live.bar.advance(index, bytes);
            live.done += bytes;
            draw(live, false);
        }
    }

    fn finish(&self) {
        if let Some(live) = self.inner.borrow_mut().as_mut() {
            let line = frame(live);
            let _ = live.target.finish(&line);
        }
    }
}

/// Redraw the bar, at most every 60ms unless forced.
fn draw(live: &mut Live, force: bool) {
    let now = Instant::now();
    if !force && now.duration_since(live.last) < Duration::from_millis(60) {
        return;
    }
    live.last = now;
    let line = frame(live);
    let _ = live.target.draw(&line);
}

/// Compose the bar with a `done / total  speed  eta` readout.
fn frame(live: &Live) -> String {
    let elapsed = live.started.elapsed().as_secs_f64();
    let rate = if elapsed > 0.0 {
        (live.done as f64 / elapsed) as u64
    } else {
        0
    };
    let eta = if rate > 0 && live.total > live.done {
        (live.total - live.done) / rate
    } else {
        0
    };
    format!(
        "[{}] {} / {}  {}/s  eta {}",
        live.bar.render(live.width),
        ByteSize::of(live.done, BYTE).iec(),
        ByteSize::of(live.total, BYTE).iec(),
        ByteSize::of(rate, BYTE).iec(),
        format_eta(eta),
    )
}

/// A compact `1h2m` / `3m4s` / `5s` duration for the ETA readout.
fn format_eta(seconds: u64) -> String {
    if seconds >= 3600 {
        format!("{}h{}m", seconds / 3600, (seconds % 3600) / 60)
    } else if seconds >= 60 {
        format!("{}m{}s", seconds / 60, seconds % 60)
    } else {
        format!("{seconds}s")
    }
}
