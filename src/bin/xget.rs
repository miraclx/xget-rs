//! xget: download a URL in parallel, verified, with a live segmented progress bar.

use core::cell::RefCell;
use core::time::Duration;
use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;
use libxget::{HttpSource, Progress};
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
    /// How many chunks to fetch in parallel.
    #[arg(short = 'n', long, default_value_t = 5)]
    parts: u32,
    /// How many times to retry a dropped chunk, resuming from its offset.
    #[arg(short = 'r', long, default_value_t = 3)]
    retries: u32,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli = Cli::parse();
    let source = HttpSource::new(&cli.url)?;
    let progress = BarProgress::new();
    let report = libxget::download(&source, &cli.output, cli.parts, cli.retries, &progress).await?;
    println!(
        "{}  {}",
        report.sha256,
        ByteSize::of(report.length, BYTE).iec()
    );
    Ok(())
}

/// A live segmented progress bar: one xprogress segment per chunk, with an xbytes size readout, drawn
/// in place and throttled so a fast download does not spend its time redrawing.
struct BarProgress {
    inner: RefCell<Option<Live>>,
}

struct Live {
    bar: Bar,
    target: DrawTarget,
    width: u16,
    total: u64,
    done: u64,
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
        let width = Width::TerminalMinus(28).resolve(target.columns(), 40);
        let mut slot = self.inner.borrow_mut();
        *slot = Some(Live {
            bar,
            target,
            width,
            total: chunks.iter().sum(),
            done: 0,
            last: Instant::now(),
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

/// Compose the bar with a `done / total` size readout.
fn frame(live: &Live) -> String {
    format!(
        "[{}] {} / {}",
        live.bar.render(live.width),
        ByteSize::of(live.done, BYTE).iec(),
        ByteSize::of(live.total, BYTE).iec()
    )
}
