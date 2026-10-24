//! Native CLI composition and explicit command dispatch.

use clap::Parser as _;

mod args;
mod display;
mod download;
mod failure;
mod info;
mod list;
mod output;
mod reporting;
mod resume;
mod selection;
mod sidecar;

use output::{Channel, Format, Output, RunId};

fn main() {
    let cli = match args::Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            if matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            ) {
                error.exit();
            }
            // Clap diagnostics can quote complete rejected bearer references. New errors use only
            // audited text; --help remains available without constructing a runtime or a Client.
            let mut arguments = std::env::args_os().skip(1).peekable();
            let mut json = false;
            while let Some(argument) = arguments.next() {
                if argument == "--" {
                    break;
                }
                if argument == "--json"
                    || argument == "--progress=json"
                    || (argument == "--progress"
                        && arguments
                            .peek()
                            .is_some_and(|mode| mode.as_os_str() == "json"))
                {
                    json = true;
                    break;
                }
            }
            let format = if json { Format::Json } else { Format::Plain };
            let output = Output::new(RunId::fresh(), format, Channel::Diagnostic);
            let failure = failure::Failure::request(
                "invalid command or value; use xget --help and explicit -o/-D output",
            );
            let _ = output.failure(&failure);
            std::process::exit(2);
        }
    };
    let output = match &cli.command {
        Some(args::Command::Info(command)) => command.output(RunId::fresh()),
        Some(args::Command::Resume(command)) => command.output(RunId::fresh()),
        Some(args::Command::List(command)) => command.output(RunId::fresh()),
        None => cli.download.output(RunId::fresh()),
    };
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(_) => {
            let _ = output.failure(&failure::Failure::runtime(
                "cannot start runtime; check available system resources",
            ));
            std::process::exit(1);
        }
    };
    let result = runtime.block_on(async {
        let dispatch = async {
            // Offline info is deliberately dispatched before transport or SDK composition.
            match cli.command {
                Some(args::Command::Info(command)) => command.run(&output).await,
                Some(args::Command::Resume(command)) => {
                    let prepared = command.prepare().await?;
                    let client = compose_client(prepared.needs_s3(), prepared.gateway()).await?;
                    prepared.run(&client, &output).await
                }
                Some(args::Command::List(command)) => {
                    let prepared = command.prepare()?;
                    let client = compose_client(prepared.needs_s3(), prepared.gateway()).await?;
                    prepared.run(&client, &output).await
                }
                None => {
                    let prepared = cli.download.prepare()?;
                    let client = compose_client(prepared.needs_s3(), prepared.gateway()).await?;
                    prepared.run(&client, &output).await
                }
            }
        };
        tokio::select! {
            result = dispatch => result,
            signal = tokio::signal::ctrl_c() => Err(match signal {
                Ok(()) => failure::Failure::interrupted(),
                Err(_) => failure::Failure::signal(),
            }),
        }
    });
    // Do not bypass Tokio's blocking-operation shutdown with process::exit. Dropping an awaited
    // command is still not a checkpoint/pause receipt, and submitted publication may be uncertain.
    drop(runtime);
    if let Err(failure) = result {
        let _ = output.failure(&failure);
        std::process::exit(failure.code());
    }
}

async fn compose_client(
    needs_s3: bool,
    gateway: Option<&str>,
) -> Result<xget::Client, failure::Failure> {
    #[allow(unused_mut)] // Optional transport configuration is compiled only with its capability.
    let mut builder = xget::Client::builder();
    if let Some(gateway) = gateway {
        #[cfg(feature = "ipfs")]
        {
            builder = builder
                .ipfs_gateway(gateway)
                .map_err(|error| failure::Failure::library(xget::client::Error::Ipfs(error)))?;
        }
        #[cfg(not(feature = "ipfs"))]
        {
            let _ = gateway;
            return Err(failure::Failure::request(
                "ipfs gateway requires a CLI build with --features ipfs",
            ));
        }
    }
    if needs_s3 {
        #[cfg(feature = "s3")]
        {
            // The composition root alone names the SDK; ordinary HTTP and offline info never load it.
            let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            builder = builder.s3_client(aws_sdk_s3::Client::new(&config));
        }
        #[cfg(not(feature = "s3"))]
        return Err(failure::Failure::request(
            "s3 requires a CLI build with --features s3; no HTTP fallback is attempted",
        ));
    }
    builder.build().map_err(failure::Failure::library)
}
