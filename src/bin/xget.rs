//! xget: download a URL in parallel, verified.

use std::path::PathBuf;

use clap::Parser;
use libxget::HttpSource;

/// Download a URL in parallel chunks and print its verified SHA-256.
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
    let report = libxget::download(&source, &cli.output, cli.parts, cli.retries).await?;
    println!("{}  {} bytes", report.sha256, report.length);
    Ok(())
}
