//! xget: download a URL in parallel, verified, with a live segmented progress bar.

mod expect;
mod progress;
mod speedometer;

use core::time::Duration;
use std::io::IsTerminal as _;
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::Parser;
use libxget::{ByteRange, ByteStream, Checksum, Error, HttpSource, Mirrors, Probe, Source};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use xbytes::ByteSize;
use xbytes::sizes::BYTE;

use crate::expect::{parse_expect, resolve_expect};
use crate::progress::{DIM, RESET, Reporter};

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
    /// A mirror URL for the same resource, tried when the primary fails a chunk (repeatable).
    #[arg(long = "mirror", value_name = "URL")]
    mirrors: Vec<String>,
    /// Endpoint for an `s3://` URL, so an S3-compatible store (R2, MinIO, Backblaze) works. Ignored
    /// for HTTP URLs.
    #[arg(long, value_name = "URL")]
    endpoint_url: Option<String>,
    /// Checksum to verify the download with: none, md5, sha1, sha256, sha512, or blake3.
    #[arg(short = 's', long, default_value_t = Checksum::Sha256)]
    checksum: Checksum,
    /// Require the download to match this checksum: `algo:hex`, or bare `hex` using `--checksum`'s
    /// algorithm. Exits non-zero on mismatch.
    #[arg(long, value_name = "[ALGO:]HEX")]
    expect: Option<String>,
    /// Fail a chunk if no data arrives for this many seconds, so its retry can resume it.
    #[arg(long, value_name = "SECS", value_parser = parse_timeout)]
    timeout: Option<Duration>,
    /// Progress output: auto (a bar on a terminal, else plain lines), bar, plain, json, or none.
    #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
    progress: ProgressMode,
    /// Disable the live bar (same as `--progress plain`).
    #[arg(long)]
    no_bar: bool,
    /// Report raw byte counts instead of human-readable sizes.
    #[arg(long)]
    raw_sizes: bool,
    /// Verbose diagnostics on stderr: `-v` for chunk ranges, retries, and errors as they happen; `-vv`
    /// for more. Off by default.
    #[arg(short = 'v', long, action = clap::ArgAction::Count)]
    verbose: u8,
}

/// Turn on diagnostic tracing to stderr at a level chosen by `-v` count. Off when zero, so a normal run
/// emits nothing. `RUST_LOG` overrides the level if set.
fn init_tracing(verbose: u8) {
    if verbose == 0 {
        return;
    }
    let level = if verbose == 1 { "debug" } else { "trace" };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::new(format!("libxget={level},xget={level}"))
    });
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(false)
        .without_time()
        .init();
}

/// How to report progress while downloading.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum ProgressMode {
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

fn main() {
    if let Err(error) = run() {
        eprintln!("xget: {error:#}");
        std::process::exit(1);
    }
}

/// Run the download, returning any error for `main` to print. Kept apart so a failure surfaces as one
/// concise line rather than the default multi-line debug report with a source location.
#[tokio::main]
async fn run() -> eyre::Result<()> {
    // Load a .env from the working directory if present, so credentials (e.g. for an s3:// URL) can
    // live there instead of being exported by hand. A missing file is not an error.
    let _ = dotenvy::dotenv();
    let cli = Cli::parse();
    init_tracing(cli.verbose);
    let headers = parse_headers(&cli.headers)?;
    let source = build_source(&cli, &headers).await?;
    let mode = resolve_mode(&cli);

    // Probe once up front for the preamble and the output name; the download re-probes authoritatively.
    let probe = source.probe().await.ok();
    let output = resolve_output(&cli, probe.as_ref())?;
    if mode == ProgressMode::Bar {
        preamble(&cli, probe.as_ref(), &output);
    }

    // `--expect` is a literal digest or a URL to a checksum file; resolve it (fetching the sidecar if
    // needed) to an optional pinned algorithm and the expected hex.
    let mut expected = match cli.expect.as_deref() {
        Some(value) => {
            Some(resolve_expect(parse_expect(value)?, cli.endpoint_url.as_deref()).await?)
        }
        None => None,
    };
    // With no explicit --expect, adopt a checksum the source vouches for (e.g. an S3 stored checksum),
    // so the download is verified against it for free.
    if expected.is_none()
        && let Some((algo, hex)) = probe.as_ref().and_then(|probe| probe.checksum.clone())
    {
        if mode != ProgressMode::Json {
            eprintln!("{DIM}Verifying against the source's stored {algo} checksum{RESET}");
        }
        expected = Some((Some(algo), hex));
    }
    let checksum = match &expected {
        Some((Some(algo), _)) => *algo,
        Some((None, _)) if matches!(cli.checksum, Checksum::None) => {
            eyre::bail!("--expect needs an algorithm: pass --checksum or use algo:hex")
        }
        _ => cli.checksum,
    };

    let reporter = Reporter::new(mode, cli.raw_sizes);
    let options = libxget::Options {
        parts: cli.chunks,
        retries: cli.tries,
        checksum,
        timeout: cli.timeout,
        resume: cli.resume,
    };
    let started = Instant::now();
    let report = libxget::download(&source, &output, options, &reporter).await?;

    if let Some((_, want)) = &expected {
        match &report.hash {
            Some(got) if got.eq_ignore_ascii_case(want) => {}
            Some(got) => {
                eyre::bail!("checksum mismatch: expected {checksum}:{want}, got {checksum}:{got}")
            }
            None => eyre::bail!("no checksum was computed to verify against"),
        }
    }

    summary(mode, &cli, checksum, &report, started.elapsed());
    Ok(())
}

/// The byte source for this run, chosen from the URL scheme. `Source` is not object-safe (it has async
/// methods), so a `dyn Source` is impossible; this enum dispatches to whichever concrete source the URL
/// selected while still being one type the engine can be generic over.
enum AnySource {
    /// An HTTP(S) URL, with any `--mirror` URLs tried in order on failure.
    Http(Mirrors<HttpSource>),
    /// An object in an S3 (or S3-compatible) bucket.
    #[cfg(feature = "s3")]
    S3(libxget::S3Source),
}

impl Source for AnySource {
    async fn probe(&self) -> Result<Probe, Error> {
        match self {
            Self::Http(source) => source.probe().await,
            #[cfg(feature = "s3")]
            Self::S3(source) => source.probe().await,
        }
    }

    async fn fetch(&self, range: Option<ByteRange>) -> Result<ByteStream, Error> {
        match self {
            Self::Http(source) => source.fetch(range).await,
            #[cfg(feature = "s3")]
            Self::S3(source) => source.fetch(range).await,
        }
    }
}

/// Build the source for `cli.url`: an `s3://bucket/key` object when the scheme is `s3`, otherwise the
/// HTTP primary plus any `--mirror` URLs tried in order on failure. Mirrors are HTTP-only; an `s3://`
/// URL ignores them.
async fn build_source(cli: &Cli, headers: &HeaderMap) -> eyre::Result<AnySource> {
    if let Some(rest) = cli.url.strip_prefix("s3://") {
        return build_s3_source(rest, cli.endpoint_url.clone()).await;
    }
    // The primary plus any --mirror URLs, tried in order on failure. With no mirrors this is just the
    // primary, so there is one download path.
    let primary = HttpSource::new(&cli.url, headers.clone())?;
    let mirrors = cli
        .mirrors
        .iter()
        .map(|url| HttpSource::new(url, headers.clone()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AnySource::Http(Mirrors::new(primary, mirrors)))
}

/// Build an S3 source from the part of an `s3://` URL after the scheme: the first path segment is the
/// bucket and the rest is the key. Only available when built with `--features s3`.
#[cfg(feature = "s3")]
async fn build_s3_source(rest: &str, endpoint_url: Option<String>) -> eyre::Result<AnySource> {
    let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() || key.is_empty() {
        eyre::bail!("s3 URL must be s3://bucket/key");
    }
    let source = libxget::S3Source::new(bucket, key, endpoint_url).await;
    Ok(AnySource::S3(source))
}

/// Reject an `s3://` URL when the `s3` feature was not compiled in, so the failure names the fix rather
/// than the URL falling through to the HTTP path and failing obscurely.
#[cfg(not(feature = "s3"))]
async fn build_s3_source(_rest: &str, _endpoint_url: Option<String>) -> eyre::Result<AnySource> {
    eyre::bail!("s3:// requires building with --features s3")
}

/// Print the block shown before a live bar: the URL, how many chunks, the length and media type, and
/// where the file is being written.
fn preamble(cli: &Cli, probe: Option<&Probe>, output: &Path) {
    eprintln!("{DIM}URL:{RESET} {}", cli.url);
    let chunks = match probe {
        Some(probe) if probe.supports_ranges => {
            libxget::plan(probe.length, cli.chunks).len().max(1)
        }
        Some(_) => 1,
        None => cli.chunks as usize,
    };
    eprintln!("{DIM}Chunks:{RESET} {chunks}");
    match probe {
        Some(probe) => {
            let size = fmt_size(probe.length, cli.raw_sizes);
            match &probe.content_type {
                Some(kind) => eprintln!("{DIM}Length:{RESET} {size} [{kind}]"),
                None => eprintln!("{DIM}Length:{RESET} {size}"),
            }
        }
        None => eprintln!("{DIM}Length:{RESET} unknown"),
    }
    eprintln!("{DIM}Saving:{RESET} '{}'", output.display());
}

/// Print the closing summary: how much was fetched in how long, and the verified checksum if one was
/// requested. Skipped for `json` (which emits an `end` event instead).
fn summary(
    mode: ProgressMode,
    cli: &Cli,
    checksum: Checksum,
    report: &libxget::Report,
    elapsed: Duration,
) {
    if mode == ProgressMode::Json {
        return;
    }
    let size = fmt_size(report.length, cli.raw_sizes);
    println!("Downloaded {size} in {}", fmt_elapsed(elapsed));
    if let Some(hash) = &report.hash {
        println!("Hash({checksum}): {hash}");
    }
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
pub(crate) fn fmt_size(bytes: u64, raw: bool) -> String {
    if raw {
        bytes.to_string()
    } else {
        ByteSize::of(bytes, BYTE).iec().to_string()
    }
}

/// A compact wall-clock duration for the closing summary: `1h2m`, `3m4s`, or `4.29s`.
fn fmt_elapsed(elapsed: Duration) -> String {
    let seconds = elapsed.as_secs_f64();
    if seconds >= 3600.0 {
        format!(
            "{}h{}m",
            (seconds / 3600.0) as u64,
            (seconds % 3600.0 / 60.0) as u64
        )
    } else if seconds >= 60.0 {
        format!("{}m{:.0}s", (seconds / 60.0) as u64, seconds % 60.0)
    } else {
        format!("{seconds:.2}s")
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
/// from the resource (its `Content-Disposition`, else the URL) under `--directory-prefix`. Refuses to
/// clobber an existing file without `-f`, and creates missing parents unless `--no-directories`.
fn resolve_output(cli: &Cli, probe: Option<&Probe>) -> eyre::Result<PathBuf> {
    let path = match &cli.output {
        Some(dir) if dir.is_dir() => dir.join(infer_name(cli, probe)?),
        Some(file) => file.to_path_buf(),
        None => {
            let name = infer_name(cli, probe)?;
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

/// Infer an output filename: the resource's `Content-Disposition` if the server offered one in the
/// probe, otherwise the URL's last path segment.
fn infer_name(cli: &Cli, probe: Option<&Probe>) -> eyre::Result<String> {
    if let Some(name) = probe.and_then(|probe| probe.filename.clone()) {
        return Ok(name);
    }
    url_basename(&cli.url)
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

/// Parse an inactivity timeout given in seconds into a [`Duration`].
fn parse_timeout(value: &str) -> Result<Duration, String> {
    let seconds: f64 = value
        .parse()
        .map_err(|_| format!("invalid seconds: `{value}`"))?;
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(format!("invalid seconds: `{value}`"));
    }
    Ok(Duration::from_secs_f64(seconds))
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
