//! Parser boundary for the common Client front door. No alternate engine is dispatched.

use core::convert::Infallible;
use core::fmt;
use core::str::FromStr;

use clap::{Args, Parser, Subcommand};
use zeroize::Zeroizing;

use crate::{download, info, list, resume};

/// Acquire files through the common library coordinator.
#[derive(Parser)]
#[command(
    name = "xget",
    version,
    args_conflicts_with_subcommands = true,
    subcommand_negates_reqs = true,
    after_help = "use -o FILE for one selected file or -D DIR for file jobs\nstdout prefixes cannot be retracted; use shell pipefail to detect late failures"
)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Option<Command>,
    #[command(flatten)]
    pub(crate) download: download::Command,
}

#[derive(Subcommand)]
pub(crate) enum Command {
    /// Inspect bounded remote metadata without opening payloads or creating outputs.
    #[command(visible_alias = "ls")]
    List(list::Command),
    /// Inspect owned saved metadata offline, without hashing or execution authority.
    Info(info::Command),
    /// Continue frozen owned work with explicit input bindings, never rediscover latest.
    Resume(resume::Command),
}

/// Original references remain text until the named preparation seam parses provider intent.
/// Discovery is not possible inside a clap value parser.
#[derive(Args, Default)]
pub(crate) struct Inputs {
    /// Exact references or recognized materialization shares; never positional output paths.
    #[arg(value_name = "URL")]
    pub(crate) references: Vec<Reference>,
    /// Explicit bounded tree scope; requires a configured supported tree adapter.
    #[arg(long = "tree", value_name = "URL")]
    pub(crate) trees: Vec<Reference>,
    /// Include original source-relative paths, using bounded *, ? and whole-component **.
    /// Selection conveniences require one original input; use separate invocations otherwise.
    #[arg(long = "select", value_name = "PATTERN")]
    pub(crate) includes: Vec<String>,
    /// Subtract original paths after includes, with the same bounded pattern grammar.
    #[arg(long = "exclude", value_name = "PATTERN")]
    pub(crate) excludes: Vec<String>,
    /// Literal repeatable member token printed by list, not an occurrence ID or URL.
    #[arg(long = "select-id", value_name = "TOKEN")]
    pub(crate) member: Option<String>,
    /// Explicitly choose all release alternatives for one original input, even for a singleton.
    /// Literal tokens and path includes/exclusions still filter; this never guesses a native asset.
    #[arg(long)]
    pub(crate) all: bool,
    /// Access header for one original HTTP/WebDAV input only; never broadcast to mixed inputs.
    /// Managed range, conditional and framing fields are rejected. Values are not saved in run records.
    #[arg(short = 'H', long = "header", value_name = "NAME: VALUE")]
    pub(crate) headers: Vec<xget::input::RequestHeader>,
}

/// Caller-selected transport access, never a stored member identity or a scheduler policy.
#[derive(Args, Default)]
pub(crate) struct Transport {
    /// HTTP gateway for exact ipfs references; does not enable redirects or DAG/directory traversal.
    #[arg(long, value_name = "URL")]
    ipfs_gateway: Option<Reference>,
}

impl Transport {
    pub(crate) fn gateway(&self) -> Option<&str> {
        self.ipfs_gateway.as_ref().map(Reference::as_str)
    }

    pub(crate) fn validate<'a>(
        &self,
        inputs: impl IntoIterator<Item = &'a xget::Input>,
    ) -> Result<(), crate::failure::Failure> {
        if self.ipfs_gateway.is_some()
            && !inputs
                .into_iter()
                .any(|input| input.kind() == xget::input::InputKind::Ipfs)
        {
            return Err(crate::failure::Failure::request(
                "ipfs gateway needs an ipfs input; remove the unused transport option",
            ));
        }
        Ok(())
    }
}

enum Purpose {
    Inspection,
    Execution,
}

impl Inputs {
    pub(crate) fn prepare(self) -> Result<Vec<xget::Input>, crate::failure::Failure> {
        self.for_purpose(Purpose::Inspection)
    }

    pub(crate) fn prepare_execution(self) -> Result<Vec<xget::Input>, crate::failure::Failure> {
        self.for_purpose(Purpose::Execution)
    }

    fn for_purpose(self, purpose: Purpose) -> Result<Vec<xget::Input>, crate::failure::Failure> {
        use crate::failure::Failure;

        let count = self.references.len().saturating_add(self.trees.len());
        if count == 0 {
            return Err(Failure::request(
                "no inputs; supply a reference or --tree URL",
            ));
        }
        if count != 1 && !self.headers.is_empty() {
            return Err(Failure::request(
                "access headers need one original input; use input-local access in the Rust API for mixed credentials",
            ));
        }
        let has_choice = self.all || self.member.is_some() || !self.includes.is_empty();
        if count != 1
            && (self.all
                || !self.includes.is_empty()
                || !self.excludes.is_empty()
                || self.member.is_some())
        {
            return Err(Failure::request(
                "selection needs one original input; use separate invocations for mixed selections",
            ));
        }
        let mut inputs = Vec::with_capacity(count);
        for reference in self.references {
            inputs.push(xget::Input::parse(reference.as_str()).map_err(|_| {
                Failure::request("invalid reference; use an authorized absolute reference, not a positional output path")
            })?);
        }
        for reference in self.trees {
            inputs.push(xget::Input::tree(reference.as_str()).map_err(|_| {
                Failure::request("invalid tree reference; use a supported explicit tree scope")
            })?);
        }
        if matches!(purpose, Purpose::Execution)
            && !has_choice
            && inputs
                .iter()
                .any(|input| input.scope() == xget::input::InputScope::Catalogue)
        {
            return Err(Failure::request(
                "catalogue execution needs an explicit choice: use --all, --select-id or a positive --select with one original input; use list to inspect first",
            ));
        }
        if count == 1 {
            let Some(mut input) = inputs.pop() else {
                return Err(Failure::request("no input; supply a reference"));
            };
            if let Some(token) = self.member {
                input = input.select_id(&token).map_err(|_| Failure::request(
                    "invalid or unsupported member token; list this input and use its literal token"
                ))?;
            }
            for pattern in self.includes {
                input = input.include(&pattern).map_err(|_| {
                    Failure::request(
                        "invalid or over-limit include; use bounded original-path patterns",
                    )
                })?;
            }
            for pattern in self.excludes {
                input = input.exclude(&pattern).map_err(|_| {
                    Failure::request(
                        "invalid or over-limit exclusion; use bounded original-path patterns",
                    )
                })?;
            }
            if self.all {
                input = input.all();
            }
            for header in self.headers {
                input = input
                    .request_header(header)
                    .map_err(|error| Failure::request(&error.to_string()))?;
            }
            inputs.push(input);
        }
        Ok(inputs)
    }
}

/// Parser-owned access text is wiped on drop and never included in Debug diagnostics.
/// Clap requires Clone for stored values; each unavoidable parser copy keeps the same wipe policy.
#[derive(Clone)]
pub(crate) struct Reference(Zeroizing<String>);

impl Reference {
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl FromStr for Reference {
    type Err = Infallible;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Ok(Self(Zeroizing::new(raw.to_owned())))
    }
}

impl fmt::Debug for Reference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Reference([redacted])")
    }
}

#[cfg(test)]
#[path = "args_tests.rs"]
mod tests;
