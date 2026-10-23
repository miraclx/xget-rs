//! Exact IPFS acquisition through a caller-chosen HTTP gateway, not a DAG/directory engine.

use url::Url;
use zeroize::Zeroizing;

use super::http::{Http, Opened};
use super::{DiscoveryBudget, Error};
use crate::input::{Input, IpfsIdentity, IpfsIdentityError, MemberToken};
use crate::inventory::{Discovered, File, FileId, Selected};
use crate::transfer::Provenance;

const DEFAULT_GATEWAY: &str = "https://ipfs.io";

pub(super) struct Ipfs {
    gateway: Url,
}

impl Ipfs {
    pub(super) fn new(gateway: &str) -> Result<Self, IpfsError> {
        if gateway.len() > 4096
            || gateway.trim() != gateway
            || gateway.chars().any(char::is_control)
        {
            return Err(IpfsError::Gateway);
        }
        let gateway = Url::parse(gateway).map_err(|_| IpfsError::Gateway)?;
        if !matches!(gateway.scheme(), "http" | "https")
            || gateway.host_str().is_none()
            || !gateway.username().is_empty()
            || gateway.password().is_some()
            || gateway.query().is_some()
            || gateway.fragment().is_some()
        {
            return Err(IpfsError::Gateway);
        }
        Ok(Self { gateway })
    }

    pub(super) fn default_gateway() -> Result<Self, IpfsError> {
        Self::new(DEFAULT_GATEWAY)
    }

    pub(super) async fn resolve(
        &self,
        http: &Http,
        mut input: Input,
        budget: &mut DiscoveryBudget,
    ) -> Result<Discovered, Error> {
        let token = Self::token(&input)?;
        if input
            .member
            .as_ref()
            .is_some_and(|selected| selected != &token)
        {
            return Err(Error::Selection);
        }
        let identity = token.ipfs_identity().ok_or(IpfsError::Reference)?;
        let mut access = self.access(&token)?;
        let source_path = Zeroizing::new(identity.source_path().to_owned());
        let name = identity
            .source_path()
            .rsplit('/')
            .next()
            .map(|name| Zeroizing::new(name.to_owned()));
        let mut description = http.describe_source(&mut access, source_path, name).await?;
        if let Some(required) = identity.required() {
            description
                .requirements
                .insert(required, Provenance::Source)
                .map_err(Error::ConflictingRequirements)?;
        }
        input.member = Some(token);
        let file = File {
            input,
            selected: Selected {
                id: FileId::fresh(),
                description,
            },
        };
        budget.reserve(&file)?;
        Ok(vec![file].into())
    }

    pub(super) async fn open(&self, http: &Http, file: &File) -> Result<Opened, Error> {
        let token = Self::token(&file.input)?;
        if file.input.member.as_ref() != Some(&token) {
            return Err(Error::Selection);
        }
        let identity = token.ipfs_identity().ok_or(IpfsError::Reference)?;
        if identity.source_path() != file.source_path() {
            return Err(Error::VersionChanged);
        }
        if let Some(expected) = identity.required() {
            let recorded = file
                .selected
                .description
                .requirements
                .iter()
                .any(|requirement| {
                    requirement.digest == expected
                        && requirement
                            .origins
                            .iter()
                            .any(|origin| origin == Provenance::Source)
                });
            if !recorded {
                return Err(Error::Ipfs(IpfsError::Evidence));
            }
        }
        let access = self.access(&token)?;
        http.open_reference(file, &access)
            .await
            .map(|(opened, _)| opened)
    }

    fn token(input: &Input) -> Result<MemberToken, Error> {
        let (scheme, spelling) = input
            .original_reference()
            .split_once("://")
            .ok_or(IpfsError::Reference)?;
        if !scheme.eq_ignore_ascii_case("ipfs")
            || input.parsed_reference().query().is_some()
            || input.parsed_reference().fragment().is_some()
        {
            return Err(Error::Ipfs(IpfsError::Reference));
        }
        let identity = IpfsIdentity::parse(spelling).map_err(IpfsError::Identity)?;
        Ok(MemberToken::from_ipfs(identity))
    }

    fn access(&self, token: &MemberToken) -> Result<Input, Error> {
        let mut url = Url::clone(&self.gateway);
        url.set_path(&format!(
            "{}/ipfs/{}",
            self.gateway.path().trim_end_matches('/'),
            token.as_str()
        ));
        // This real gateway locator is transient access, not the original Input or stored identity.
        let reference = Zeroizing::new(String::from(url));
        Input::parse(&reference).map_err(Into::into)
    }
}

/// Safe exact-IPFS configuration/identity failure, without a retained gateway URL or reference.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum IpfsError {
    /// Gateway access is configured independently of source identity.
    #[error(
        "invalid ipfs gateway; use a bounded http or https base without credentials, query or fragment"
    )]
    Gateway,
    /// Exact acquisition does not infer a directory/DAG traversal.
    #[error("invalid ipfs file reference; supply a cid with an optional exact file path")]
    Reference,
    /// The content address or literal file path is invalid/unsupported.
    #[error("invalid ipfs identity; inspect the typed content-address category")]
    Identity(#[from] IpfsIdentityError),
    /// Frozen raw-CID requirements cannot be silently disabled by restored metadata.
    #[error("ipfs source evidence differs; restore the original cid and required check policy")]
    Evidence,
}

#[cfg(test)]
#[path = "ipfs_tests.rs"]
mod tests;
