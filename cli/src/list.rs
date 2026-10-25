//! Bounded metadata inspection through Client, never a payload or native execution entrance.

use clap::Args;
use serde_json::json;
use xget::input::{InputKind, InputScope};
use xget::{Client, Input};

use crate::args::Inputs;
use crate::failure::Failure;
use crate::human::SelectorContext;
use crate::output::{Channel, Format, ItemId, Output, RunId};

#[derive(Args)]
pub(crate) struct Command {
    #[command(flatten)]
    inputs: Inputs,
    #[command(flatten)]
    transport: crate::args::Transport,
    /// Versioned complete metadata on stdout; no access references or validators.
    #[arg(long)]
    json: bool,
}

pub(crate) struct Prepared {
    inputs: Vec<Input>,
    transport: crate::args::Transport,
}

impl Command {
    pub(crate) fn output(&self, run: RunId) -> Output {
        Output::new(
            run,
            if self.json {
                Format::Json
            } else {
                Format::Plain
            },
            Channel::Metadata,
        )
    }

    pub(crate) fn prepare(self) -> Result<Prepared, Failure> {
        let inputs = self.inputs.prepare()?;
        self.transport.validate(&inputs)?;
        Ok(Prepared {
            inputs,
            transport: self.transport,
        })
    }
}

impl Prepared {
    pub(crate) fn gateway(&self) -> Option<&str> {
        self.transport.gateway()
    }

    pub(crate) fn needs_s3(&self) -> bool {
        self.inputs
            .iter()
            .any(|input| input.kind() == xget::input::InputKind::S3)
    }

    pub(crate) async fn run(self, client: &Client, output: &Output) -> Result<(), Failure> {
        let selector = match self.inputs.as_slice() {
            [input] if input.kind() == InputKind::S3 && input.scope() == InputScope::Exact => {
                SelectorContext::ExactInput
            }
            [_] => SelectorContext::Selectable,
            _ => SelectorContext::MixedInputs,
        };
        let inventory = client
            .resolve_all(self.inputs)
            .await
            .map_err(Failure::library)?;
        for (index, file) in inventory.files().enumerate() {
            output.human(|human| human.member(file, selector))?;
            output.emit("Member", Some(ItemId(index)), json!({
                "source_path": file.source_path(),
                "namespace": file.namespace().map(|namespace| namespace.as_str()),
                "length_bytes": Output::length(file.length()),
                "selector_token": file.member_token().map(|token| token.as_str()),
                "id_selection_available": file.member_token().is_some(),
                "version_evidence": Output::evidence(file.version()),
                "required_digests": file.requirements().map(|required| required.digest.to_string()).collect::<Vec<_>>(),
            }))?;
        }
        for directory in inventory.directories() {
            output.human(|human| human.directory(directory))?;
            output.emit(
                "Directory",
                None,
                json!({
                    "source_path": directory.source_path(),
                    "namespace": directory.namespace().map(|namespace| namespace.as_str()),
                    "version_evidence": Output::evidence(directory.version()),
                    "inspection_only": true,
                    "executable": false,
                }),
            )?;
        }
        output.human(|human| {
            human.listed(
                inventory.files().len(),
                inventory.directories().len(),
                selector == SelectorContext::Selectable
                    && inventory.files().any(|file| file.member_token().is_some()),
            )
        })?;
        output.emit(
            "End",
            None,
            json!({
                "complete": true, "files": inventory.files().len(),
                "directories": inventory.directories().len(),
                "accounted_metadata_bytes": inventory.metadata_bytes(),
                "payload_opened": false, "output_created": false,
            }),
        )
    }
}
