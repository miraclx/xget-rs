//! xget: download a URL in parallel, verified, with a live segmented progress bar.

use core::cell::RefCell;
use core::time::Duration;
use std::io::IsTerminal as _;
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
    /// Resume a partial download: keep the existing file, fetch only what remains, still verified.
    #[arg(short = 'c', long = "continue")]
    resume: bool,
    /// Set a request header, e.g. `Authorization: Bearer x` (repeatable).
    #[arg(short = 'H', long = "header")]
    headers: Vec<String>,
    /// Checksum to verify the download with: none, md5, sha1, sha256, or sha512.
    #[arg(short = 's', long, default_value_t = Checksum::Sha256)]
    checksum: Checksum,
    /// Fail a chunk if no data arrives for this many seconds, so its retry can resume it.
    #[arg(long, value_name = "SECS")]
    timeout: Option<f64>,
    /// Buffers each chunk may read ahead of the reassembler; trades memory for smoother throughput.
    #[arg(long, value_name = "N", default_value_t = 64)]
    cache_size: usize,
    /// Progress output: auto (a bar on a terminal, else plain lines), bar, plain, json, or none.
    #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
    progress: ProgressMode,
    /// Disable the live bar (same as `--progress plain`).
    #[arg(long)]
    no_bar: bool,
    /// Report raw byte counts instead of human-readable sizes.
    #[arg(long)]
    raw_sizes: bool,
}

/// How to report progress while downloading.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum ProgressMode {
    /// A live bar on a terminal, otherwise plain lines.
    Auto,
    /// A live segmented bar.
    Bar,
    /// A single updating text line.
    Plain,
    /// One JSON event per update, to stderr.
    Json,
    /// No progress, only the final result.
    None,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli = Cli::parse();
    let output = resolve_output(&cli)?;
    let source = HttpSource::new(&cli.url, parse_headers(&cli.headers)?)?;
    let reporter = Reporter::new(resolve_mode(&cli), cli.raw_sizes);
    let options = libxget::Options {
        parts: cli.chunks,
        retries: cli.tries,
        checksum: cli.checksum,
        timeout: cli.timeout.map(Duration::from_secs_f64),
        resume: cli.resume,
        cache: cli.cache_size,
    };
    let report = libxget::download(&source, &output, options, &reporter).await?;
    let size = fmt_size(report.length, cli.raw_sizes);
    match report.hash {
        Some(hash) => println!("{}:{hash}  {size}", cli.checksum),
        None => println!("{size}"),
    }
    Ok(())
}

/// Resolve the effective progress mode: `auto` becomes a bar on a terminal and plain lines otherwise,
/// and `--no-bar` downgrades a bar to plain.
fn resolve_mode(cli: &Cli) -> ProgressMode {
    let mut mode = cli.progress;
    if mode == ProgressMode::Auto {
        mode = if std::io::stderr().is_terminal() {
            ProgressMode::Bar
        } else {
            ProgressMode::Plain
        };
    }
    if cli.no_bar && mode == ProgressMode::Bar {
        mode = ProgressMode::Plain;
    }
    mode
}

/// Format a byte count, either raw or human-readable.
fn fmt_size(bytes: u64, raw: bool) -> String {
    if raw {
        bytes.to_string()
    } else {
        ByteSize::of(bytes, BYTE).iec().to_string()
    }
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
    if path.exists() && !cli.overwrite && !cli.resume {
        eyre::bail!(
            "{} already exists (use -f to overwrite or -c to resume)",
            path.display()
        );
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

/// The chosen progress reporter, dispatching to whichever mode was selected. Every variant reports the
/// same download; they differ only in how they render it.
enum Reporter {
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
    fn new(mode: ProgressMode, raw: bool) -> Self {
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

    fn advance(&self, index: usize, bytes: u64) {
        match self {
            Self::Bar(bar) => bar.advance(index, bytes),
            Self::Plain(plain) => plain.advance(index, bytes),
            Self::Json(json) => json.advance(index, bytes),
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

/// Running totals shared by the text-based reporters: how much of how many bytes have arrived, and a
/// throttle so a fast download does not spend its time printing.
struct Meter {
    total: u64,
    done: u64,
    started: Instant,
    last: Instant,
}

impl Meter {
    fn new(total: u64) -> Self {
        let now = Instant::now();
        Self {
            total,
            done: 0,
            started: now,
            last: now,
        }
    }

    /// Record `bytes` more and report whether enough time has passed to redraw.
    fn advance(&mut self, bytes: u64, every: Duration) -> bool {
        self.done += bytes;
        let now = Instant::now();
        if now.duration_since(self.last) < every {
            return false;
        }
        self.last = now;
        true
    }

    /// Bytes per second so far.
    fn rate(&self) -> u64 {
        let elapsed = self.started.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            (self.done as f64 / elapsed) as u64
        } else {
            0
        }
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
}

/// A single updating text line: `done / total (pct%)  speed  eta`, drawn in place on stderr and
/// throttled, with a final newline so the shell prompt lands cleanly.
struct PlainProgress {
    meter: RefCell<Option<Meter>>,
    raw: bool,
}

impl PlainProgress {
    fn new(raw: bool) -> Self {
        Self {
            meter: RefCell::new(None),
            raw,
        }
    }

    fn line(&self, meter: &Meter) -> String {
        format!(
            "\r\x1b[K{} / {} ({}%)  {}/s  eta {}",
            fmt_size(meter.done, self.raw),
            fmt_size(meter.total, self.raw),
            meter.percent(),
            fmt_size(meter.rate(), self.raw),
            format_eta(meter.eta()),
        )
    }
}

impl Progress for PlainProgress {
    fn start(&self, chunks: &[u64]) {
        let meter = Meter::new(chunks.iter().sum());
        eprint!("{}", self.line(&meter));
        let _ = std::io::Write::flush(&mut std::io::stderr());
        *self.meter.borrow_mut() = Some(meter);
    }

    fn advance(&self, _index: usize, bytes: u64) {
        if let Some(meter) = self.meter.borrow_mut().as_mut() {
            if meter.advance(bytes, Duration::from_millis(200)) {
                eprint!("{}", self.line(meter));
                let _ = std::io::Write::flush(&mut std::io::stderr());
            }
        }
    }

    fn finish(&self) {
        if let Some(meter) = self.meter.borrow().as_ref() {
            eprintln!("{}", self.line(meter));
        }
    }
}

/// One JSON event per update, to stderr: a `start`, throttled `progress` events, and a `done`. The
/// numbers are always raw bytes so a consumer can format them itself.
struct JsonProgress {
    meter: RefCell<Option<Meter>>,
}

impl JsonProgress {
    fn new() -> Self {
        Self {
            meter: RefCell::new(None),
        }
    }
}

impl Progress for JsonProgress {
    fn start(&self, chunks: &[u64]) {
        let meter = Meter::new(chunks.iter().sum());
        eprintln!(
            r#"{{"event":"start","total":{},"chunks":{}}}"#,
            meter.total,
            chunks.len()
        );
        *self.meter.borrow_mut() = Some(meter);
    }

    fn advance(&self, _index: usize, bytes: u64) {
        if let Some(meter) = self.meter.borrow_mut().as_mut() {
            if meter.advance(bytes, Duration::from_millis(200)) {
                eprintln!(
                    r#"{{"event":"progress","done":{},"total":{},"rate":{},"eta":{}}}"#,
                    meter.done,
                    meter.total,
                    meter.rate(),
                    meter.eta()
                );
            }
        }
    }

    fn finish(&self) {
        if let Some(meter) = self.meter.borrow().as_ref() {
            eprintln!(
                r#"{{"event":"done","total":{},"done":{}}}"#,
                meter.total, meter.done
            );
        }
    }
}

/// A live segmented progress bar: one xprogress segment per chunk, with an xbytes size, speed, and ETA
/// readout, drawn in place and throttled so a fast download does not spend its time redrawing.
struct BarProgress {
    inner: RefCell<Option<Live>>,
    raw: bool,
}

struct Live {
    bar: Bar,
    target: DrawTarget,
    width: u16,
    meter: Meter,
    raw: bool,
}

impl BarProgress {
    fn new(raw: bool) -> Self {
        Self {
            inner: RefCell::new(None),
            raw,
        }
    }
}

impl Progress for BarProgress {
    fn start(&self, chunks: &[u64]) {
        let bar =
            Bar::new(chunks.iter().copied()).with_style(Style::default().with_color(Color::Cyan));
        let target = DrawTarget::from_env();
        let width = Width::TerminalMinus(46).resolve(target.columns(), 32);
        let mut slot = self.inner.borrow_mut();
        *slot = Some(Live {
            bar,
            target,
            width,
            meter: Meter::new(chunks.iter().sum()),
            raw: self.raw,
        });
        if let Some(live) = slot.as_mut() {
            let line = frame(live);
            let _ = live.target.draw(&line);
        }
    }

    fn advance(&self, index: usize, bytes: u64) {
        if let Some(live) = self.inner.borrow_mut().as_mut() {
            live.bar.advance(index, bytes);
            if live.meter.advance(bytes, Duration::from_millis(60)) {
                let line = frame(live);
                let _ = live.target.draw(&line);
            }
        }
    }

    fn finish(&self) {
        if let Some(live) = self.inner.borrow_mut().as_mut() {
            let line = frame(live);
            let _ = live.target.finish(&line);
        }
    }
}

/// Compose the bar with a `done / total  speed  eta` readout.
fn frame(live: &Live) -> String {
    format!(
        "[{}] {} / {}  {}/s  eta {}",
        live.bar.render(live.width),
        fmt_size(live.meter.done, live.raw),
        fmt_size(live.meter.total, live.raw),
        fmt_size(live.meter.rate(), live.raw),
        format_eta(live.meter.eta()),
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
