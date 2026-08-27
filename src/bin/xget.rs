//! xget: download a URL in parallel, verified, with a live segmented progress bar.

use core::cell::RefCell;
use core::time::Duration;
use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;
use libxget::{Checksum, HttpSource, Progress};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use xbytes::prelude::*;
use xprogress::{Bar, Color, DrawTarget, Style, Width};

/// Download a URL in parallel chunks, verify it, and print its SHA-256.
#[derive(Parser)]
#[command(name = "xget", version, about)]
struct Cli {
    /// The URL to download.
    url: String,
    /// Where to write the file. If omitted or a directory, the name is taken from the URL.
    output: Option<PathBuf>,
    /// Maximum number of concurrent chunk connections.
    #[arg(short = 'n', long, default_value_t = 5)]
    chunks: u32,
    /// Retries for each chunk, resuming from its offset. `inf` for unlimited.
    #[arg(short = 't', long, default_value = "10", value_parser = parse_tries)]
    tries: u32,
    /// Save the file under this directory prefix.
    #[arg(short = 'D', long)]
    directory_prefix: Option<PathBuf>,
    /// Do not create missing directories.
    #[arg(long)]
    no_directories: bool,
    /// Overwrite an existing output file.
    #[arg(short = 'f', long)]
    overwrite: bool,
    /// Set a request header, e.g. `Authorization: Bearer x` (repeatable).
    #[arg(short = 'H', long = "header")]
    headers: Vec<String>,
    /// Checksum to verify the download with: none, md5, sha1, sha256, or sha512.
    #[arg(short = 's', long, default_value_t = Checksum::Sha256)]
    checksum: Checksum,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli = Cli::parse();
    let output = resolve_output(&cli)?;
    let source = HttpSource::new(&cli.url, parse_headers(&cli.headers)?)?;
    let progress = BarProgress::new();
    let report = libxget::download(
        &source,
        &output,
        cli.chunks,
        cli.tries,
        cli.checksum,
        &progress,
    )
    .await?;
    let size = ByteSize::of(report.length, BYTE).iec();
    match report.hash {
        Some(hash) => println!("{}:{hash}  {size}", cli.checksum),
        None => println!("{size}"),
    }
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

/// Resolve where to write: an explicit file, a name inside an explicit directory, or a name inferred
/// from the URL (under `--directory-prefix`). Refuses to clobber an existing file without `-f`, and
/// creates missing parents unless `--no-directories`.
fn resolve_output(cli: &Cli) -> eyre::Result<PathBuf> {
    let path = match &cli.output {
        Some(dir) if dir.is_dir() => dir.join(url_basename(&cli.url)?),
        Some(file) => file.to_path_buf(),
        None => {
            let name = url_basename(&cli.url)?;
            match &cli.directory_prefix {
                Some(prefix) => prefix.join(name),
                None => PathBuf::from(name),
            }
        }
    };
    if path.exists() && !cli.overwrite {
        eyre::bail!("{} already exists (use -f to overwrite)", path.display());
    }
    if !cli.no_directories {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
    }
    Ok(path)
}

/// The last path segment of the URL, for naming a downloaded file.
fn url_basename(url: &str) -> eyre::Result<String> {
    let parsed = reqwest::Url::parse(url)?;
    let name = parsed
        .path_segments()
        .and_then(Iterator::last)
        .filter(|segment| !segment.is_empty());
    match name {
        Some(name) => Ok(name.to_owned()),
        None => eyre::bail!("cannot infer a filename from {url}; give an output path"),
    }
}

/// Parse a retry count, accepting `inf`/`infinite` as unlimited.
fn parse_tries(value: &str) -> Result<u32, String> {
    match value.to_ascii_lowercase().as_str() {
        "inf" | "infinite" => Ok(u32::MAX),
        _ => value
            .parse()
            .map_err(|_| format!("expected a number or `inf`, got `{value}`")),
    }
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
