//! Offline saved metadata, deliberately without a Client or executable lease.

use std::path::PathBuf;

use clap::Args;
use serde_json::json;
use xget::client::SavedRun;

use crate::failure::Failure;
use crate::output::{Channel, Format, Output, RunId};

#[derive(Args)]
pub(crate) struct Command {
    /// Saved run state directory, not an incomplete payload file.
    state: PathBuf,
    /// Versioned metadata on stdout; no network, payload hashing or executable authority.
    #[arg(long)]
    json: bool,
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

    pub(crate) async fn run(self, output: &Output) -> Result<(), Failure> {
        let saved = SavedRun::inspect(self.state)
            .await
            .map_err(Failure::saved)?;
        output.human(|human| human.info_header(saved.path(), saved.is_complete()))?;
        for member in saved.members() {
            output.human(|human| human.saved_member(&member))?;
            output.emit_saved("SavedMember", saved.id(), Some(member.key()), json!({
                "binding_key": member.key().to_string(),
                "destination": Output::path(member.destination()),
                "length_bytes": Output::length(member.length()),
                "status": Output::status(member.status()),
                "measurement": Output::measurement(member.measurement()),
                "required_digests": member.requirements().map(|required| required.digest.to_string()).collect::<Vec<_>>(),
            }))?;
        }
        output.human(|human| {
            human.info_end(
                saved.members().len(),
                saved.members().map(|member| member.status()),
            )
        })?;
        output.emit_saved(
            "End",
            saved.id(),
            None,
            json!({
                "recorded_complete": saved.is_complete(),
                "members": saved.members().len(),
                "state_path": Output::path(saved.path()),
                "offline": true, "content_revalidated": false, "execution_authority": false,
            }),
        )
    }
}
