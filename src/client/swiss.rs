//! SwissTransfer protocol only. Common inventories, admission, layout and Transfer own the product.

use std::collections::BTreeSet;

use scraper::{Html, Selector};
use serde::Deserialize;
use time::{OffsetDateTime, PrimitiveDateTime};
use zeroize::Zeroizing;

use super::http::{Http, Opened};
use super::{DiscoveryBudget, Error, RequestStage, VersionConsistency};
use crate::checks::Requirements;
use crate::input::{Input, MemberToken};
use crate::inventory::{Description, File, FileId, Inventory, InventoryError, MAX_FILES, Selected};
use crate::transfer::{ByteCount, Length};

#[cfg(test)]
#[path = "swiss_tests.rs"]
mod tests;

const MAX_PAGE: usize = 2 * 1024 * 1024;
const MAX_ACCESS: usize = 64 * 1024;

pub(super) struct Swiss;

impl Swiss {
    pub(super) async fn resolve(
        http: &Http,
        input: Input,
        budget: &mut DiscoveryBudget,
    ) -> Result<Vec<File>, Error> {
        let page = http.metadata(&input, MAX_PAGE).await?;
        let members = Manifest::parse(&input, &page)?;
        let mut files = Vec::with_capacity(members.len());
        for member in members {
            let mut origin = input.duplicate_reference()?;
            origin.member = Some(member.token);
            let name = member
                .path
                .rsplit('/')
                .next()
                .map(|name| Zeroizing::new(name.to_owned()));
            let file = File {
                input: origin,
                selected: Selected {
                    id: FileId::fresh(),
                    description: Description {
                        source_path: member.path,
                        name,
                        length: Length::Known(member.size),
                        etag: None,
                        requirements: Requirements::default(),
                    },
                },
            };
            budget.reserve(&file)?;
            files.push(file);
        }
        // The COMPLETE inventory is accounted before applying intent. Selection cannot conceal a count
        // or retained-text overflow in the original scope.
        let inventory = Inventory::complete(files)?;
        // Common selection, after complete Inventory accounting, owns literal member filtering.
        Ok(inventory.into_files().collect())
    }

    pub(super) async fn open(http: &Http, file: &mut File) -> Result<Opened, Error> {
        let token = file.input.member.as_ref().ok_or(Error::SwissProtocol)?;
        let share = Manifest::share(&file.input)?;
        let reference = Input::parse(&format!(
            "https://www.swisstransfer.com/api/1/links/{}/files/{}",
            share.as_str(),
            token.as_str()
        ))?;
        for attempt in 0..2 {
            let bytes = http.metadata(&reference, MAX_ACCESS).await?;
            let lease = Access::parse(&bytes, OffsetDateTime::now_utc())?;
            let result = http.open_reference(file, &lease.input).await;
            let (mut opened, etag) = match result {
                // A live 403 is a denial, not permission to spend another access request. Expiry only
                // permits one renewal for this SAME member and already frozen representation.
                Err(Error::Status {
                    stage: RequestStage::Payload,
                    status: 403,
                }) if attempt == 0 && lease.expires <= OffsetDateTime::now_utc() => continue,
                other => other?,
            };
            if file.selected.description.etag.is_none() {
                file.selected.description.etag = Some(etag.ok_or(Error::InvalidMetadata)?);
                opened.version = VersionConsistency::StrongEtagEstablished;
            }
            return Ok(opened);
        }
        Err(Error::SwissProtocol)
    }
}

struct Manifest;

#[derive(Eq, PartialEq)]
struct ShareId(MemberToken);
impl ShareId {
    fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Manifest {
    fn share(input: &Input) -> Result<ShareId, Error> {
        let share = input
            .parsed_reference()
            .path()
            .strip_prefix("/dl/")
            .ok_or(Error::SwissProtocol)?;
        MemberToken::parse(share.strip_suffix('/').unwrap_or(share))
            .map(ShareId)
            .ok_or(Error::SwissProtocol)
    }

    fn parse(input: &Input, page: &[u8]) -> Result<Vec<Member>, Error> {
        if page.len() > MAX_PAGE {
            return Err(Error::Inventory(InventoryError::Limit));
        }
        let text = core::str::from_utf8(page).map_err(|_| Error::SwissProtocol)?;
        let document = Html::parse_document(text);
        let selector = Selector::parse("script[data-page=app][type=\"application/json\"]")
            .map_err(|_| Error::SwissProtocol)?;
        let mut scripts = document.select(&selector);
        let script = scripts.next().ok_or(Error::SwissProtocol)?;
        if scripts.next().is_some() {
            return Err(Error::SwissProtocol);
        }
        let json = Zeroizing::new(script.text().collect::<String>());
        // No Value or foreign serde diagnostic crosses this boundary. Unknown fields are skipped,
        // not retained. Owned external text is immediately zeroizing, including escaped JSON strings.
        let envelope: Envelope = serde_json::from_str(&json).map_err(|_| Error::SwissProtocol)?;
        if envelope.component.0.as_str() != "link/show" {
            return Err(Error::SwissProtocol);
        }
        let props = envelope.props.ok_or(Error::SwissProtocol)?;
        if MemberToken::parse(&props.link.id.0).map(ShareId) != Some(Self::share(input)?) {
            return Err(Error::SwissProtocol);
        }
        let files = match props.transfer.files {
            BoundedFiles::Complete(files) => files,
            BoundedFiles::Excess => return Err(Error::Inventory(InventoryError::Limit)),
        };
        let mut ids = BTreeSet::new();
        let mut members = Vec::with_capacity(files.len());
        for remote in files {
            let token = MemberToken::parse(&remote.id.0).ok_or(Error::SwissProtocol)?;
            if !ids.insert(MemberToken::clone(&token)) {
                return Err(Error::SwissProtocol);
            }
            // Source paths are validated by the common policy, not a second provider layout policy.
            super::layout::Layout::source_path(&remote.path.0)?;
            members.push(Member {
                token,
                path: remote.path.0,
                size: ByteCount(remote.size),
            });
        }
        super::layout::Layout::source_paths(members.iter().map(|member| member.path.as_str()))?;
        Ok(members)
    }
}

struct Member {
    token: MemberToken,
    path: Zeroizing<String>,
    size: ByteCount,
}

#[derive(serde::Deserialize)]
struct Envelope {
    component: SecretText,
    props: Option<Props>,
}
#[derive(serde::Deserialize)]
struct Props {
    link: Link,
    transfer: RemoteTransfer,
}
#[derive(serde::Deserialize)]
struct Link {
    id: SecretText,
}
#[derive(serde::Deserialize)]
struct RemoteTransfer {
    files: BoundedFiles,
}
#[derive(serde::Deserialize)]
struct RemoteFile {
    id: SecretText,
    path: SecretText,
    size: u64,
}
enum BoundedFiles {
    Complete(Vec<RemoteFile>),
    Excess,
}

impl<'de> Deserialize<'de> for BoundedFiles {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = BoundedFiles;
            fn expecting(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                formatter.write_str("a bounded file list")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut files = Vec::new();
                loop {
                    if files.len() == MAX_FILES {
                        if seq.next_element::<serde::de::IgnoredAny>()?.is_none() {
                            return Ok(BoundedFiles::Complete(files));
                        }
                        // Finish the bounded external sequence without retaining another member.
                        // A typed Excess value avoids inspecting/stringifying a serde diagnostic.
                        while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                        return Ok(BoundedFiles::Excess);
                    }
                    let Some(file) = seq.next_element()? else {
                        return Ok(BoundedFiles::Complete(files));
                    };
                    files.push(file);
                }
            }
        }
        deserializer.deserialize_seq(Visitor)
    }
}

struct Access {
    input: Input,
    expires: OffsetDateTime,
}

impl Access {
    fn parse(bytes: &[u8], now: OffsetDateTime) -> Result<Self, Error> {
        if bytes.len() > MAX_ACCESS {
            return Err(Error::Inventory(InventoryError::Limit));
        }
        let envelope: AccessEnvelope =
            serde_json::from_slice(bytes).map_err(|_| Error::SwissProtocol)?;
        let text = match envelope.data {
            AccessData::Url { url } | AccessData::Text(url) => url,
        };
        let input = Input::parse(&text.0).map_err(|_| Error::SwissProtocol)?;
        let parsed = input.parsed_reference();
        if parsed.scheme() != "https"
            || parsed.port().is_some()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.fragment().is_some()
            || !parsed.host_str().is_some_and(|host| {
                host.strip_suffix(".swisstransfer.infomaniak.cloud")
                    .is_some_and(|subdomain| {
                        !subdomain.is_empty() && subdomain.split('.').all(|label| !label.is_empty())
                    })
            })
        {
            return Err(Error::SwissProtocol);
        }
        let mut date = None;
        let mut lifetime = None;
        for (key, value) in parsed.query_pairs() {
            // Decoded query allocations can also bear secrets. Wipe owned values immediately;
            // borrowed ones remain inside Input's zeroizing reference allocations.
            let key = Zeroizing::new(key.into_owned());
            let value = Zeroizing::new(value.into_owned());
            match key.as_str() {
                "X-Amz-Date" => {
                    if date.is_some() || value.len() != 16 {
                        return Err(Error::SwissProtocol);
                    }
                    date = Some(
                        PrimitiveDateTime::parse(
                            &value,
                            time::macros::format_description!(
                                "[year][month][day]T[hour][minute][second]Z"
                            ),
                        )
                        .map_err(|_| Error::SwissProtocol)?
                        .assume_utc(),
                    );
                }
                "X-Amz-Expires" => {
                    if lifetime.is_some()
                        || value.is_empty()
                        || value.len() > 6
                        || !value.bytes().all(|byte| byte.is_ascii_digit())
                    {
                        return Err(Error::SwissProtocol);
                    }
                    lifetime = Some(value.parse::<i64>().map_err(|_| Error::SwissProtocol)?);
                }
                _ => {}
            }
        }
        let date = date.ok_or(Error::SwissProtocol)?;
        let lifetime = lifetime
            .filter(|value| (1..=604800).contains(value))
            .ok_or(Error::SwissProtocol)?;
        if date > now + time::Duration::minutes(5) {
            return Err(Error::SwissProtocol);
        }
        let expires = date
            .checked_add(time::Duration::seconds(lifetime))
            .ok_or(Error::SwissProtocol)?;
        if expires <= now {
            return Err(Error::SwissProtocol);
        }
        Ok(Self { input, expires })
    }
}

#[derive(serde::Deserialize)]
struct AccessEnvelope {
    data: AccessData,
}
enum AccessData {
    Url { url: SecretText },
    Text(SecretText),
}
impl<'de> Deserialize<'de> for AccessData {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = AccessData;
            fn expecting(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                formatter.write_str("a download access value")
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(AccessData::Text(SecretText(Zeroizing::new(
                    value.to_owned(),
                ))))
            }
            fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
                Ok(AccessData::Text(SecretText(Zeroizing::new(value))))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut url = None;
                while let Some(key) = map.next_key::<SecretText>()? {
                    if key.0.as_str() == "url" {
                        if url.is_some() {
                            return Err(serde::de::Error::custom("duplicate access field"));
                        }
                        url = Some(map.next_value()?);
                    } else {
                        map.next_value::<serde::de::IgnoredAny>()?;
                    }
                }
                Ok(AccessData::Url {
                    url: url.ok_or_else(|| serde::de::Error::custom("missing access field"))?,
                })
            }
        }
        // Unlike an untagged derive this does not first copy bearer values into serde's temporary
        // Content tree, whose owned strings would not be wiped on drop.
        deserializer.deserialize_any(Visitor)
    }
}
struct SecretText(Zeroizing<String>);
impl<'de> Deserialize<'de> for SecretText {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(|value| Self(Zeroizing::new(value)))
    }
}
