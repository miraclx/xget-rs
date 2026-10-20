use core::borrow::Borrow;
use core::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use unicode_casefold::UnicodeCaseFold as _;
use unicode_normalization::UnicodeNormalization as _;
use zeroize::Zeroizing;

use super::{Error, Plan, PlannedFile, Target};
use crate::checks::Requirements;
use crate::inventory::File;
use crate::transfer::Measurement;

pub(super) struct Layout {
    files: BTreeSet<CollisionKey>,
    directories: BTreeMap<CollisionKey, Zeroizing<String>>,
}

impl Layout {
    pub(super) fn freeze(
        files: Vec<File>,
        target: Target,
        global: Requirements,
        measurement: Measurement,
    ) -> Result<Plan, Error> {
        if let Target::File(destination) = target {
            if files.len() != 1 {
                return Err(Error::FileCardinality);
            }
            let mut files = files.into_iter();
            let Some(file) = files.next() else {
                return Err(Error::FileCardinality);
            };
            if file.namespace().is_some() {
                return Err(Error::NamespaceWithFile);
            }
            Self::explicit_file(&destination)?;
            let requirements = Self::requirements(&file, global)?;
            return Ok(Plan {
                files: vec![PlannedFile {
                    file,
                    destination,
                    requirements,
                    measurement,
                }],
                directory_root: None,
                directories: Vec::new(),
            });
        }
        let root = match target {
            Target::Default => PathBuf::from("."),
            Target::Directory(root) => root,
            Target::File(_) => unreachable!("exact file target was consumed above"),
        };
        // Explicit root authority may use ordinary native directory syntax, including a trailing
        // separator. Normalize only that caller-owned root so symlink preflight sees its final role.
        let root: PathBuf = root.components().collect();
        if root.as_os_str().is_empty() {
            return Err(Error::InvalidName);
        }
        if root.as_os_str().as_encoded_bytes().len() > 4096 {
            return Err(Error::LayoutLimit);
        }
        let mut layout = Self {
            files: BTreeSet::new(),
            directories: BTreeMap::new(),
        };
        let mut output_directories = BTreeSet::new();
        let mut planned = Vec::with_capacity(files.len());
        for file in files {
            let namespace = file.namespace().map(|namespace| namespace.as_str());
            // Bound prefix-key retention before collecting or copying any namespace prefixes.
            if namespace.is_some_and(|namespace| {
                namespace.len() > 4096 || namespace.split('/').count() >= 64
            }) {
                return Err(Error::LayoutLimit);
            }
            let mut components: Vec<_> = namespace
                .map(|namespace| namespace.split('/').collect())
                .unwrap_or_default();
            components.push(file.name_hint().ok_or(Error::MissingName)?);
            layout.claim(&components)?;
            // Claim validates every component before any join. Names cannot escape the explicit
            // root or acquire URL-decoding/path-normalization meaning at the filesystem boundary.
            let mut destination = PathBuf::clone(&root);
            for component in components {
                destination.push(component);
            }
            if destination.as_os_str().as_encoded_bytes().len() > 4096 {
                return Err(Error::LayoutLimit);
            }
            let parent = destination.parent().ok_or(Error::InvalidName)?;
            output_directories.insert(parent.to_path_buf());
            let requirements = Self::requirements(&file, global)?;
            planned.push(PlannedFile {
                file,
                destination,
                requirements,
                measurement,
            });
        }
        Ok(Plan {
            files: planned,
            directory_root: Some(root),
            directories: output_directories.into_iter().collect(),
        })
    }

    fn requirements(file: &File, global: Requirements) -> Result<Requirements, Error> {
        let mut requirements = file.selected.description.requirements;
        for requirement in global.iter() {
            for provenance in requirement.origins.iter() {
                requirements
                    .insert(requirement.digest, provenance)
                    .map_err(Error::ConflictingRequirements)?;
            }
        }
        Ok(requirements)
    }

    fn explicit_file(path: &Path) -> Result<(), Error> {
        let raw = path.as_os_str().as_encoded_bytes();
        if raw.len() > 4096 {
            return Err(Error::LayoutLimit);
        }
        if raw
            .last()
            .is_some_and(|byte| std::path::is_separator(char::from(*byte)))
            || raw.ends_with(b"/.")
            || raw.ends_with(b"\\.")
        {
            return Err(Error::InvalidName);
        }
        let Some(Component::Normal(name)) = path.components().next_back() else {
            return Err(Error::InvalidName);
        };
        let name = name.to_str().ok_or(Error::InvalidName)?;
        Self::component(name)?;
        Ok(())
    }

    fn claim(&mut self, components: &[&str]) -> Result<(), Error> {
        let mut prefix = Zeroizing::new(String::new());
        let mut spelling = Zeroizing::new(String::new());
        for (index, component) in components.iter().enumerate() {
            let key = Self::component(component)?;
            if index != 0 {
                prefix.push('/');
                spelling.push('/');
            }
            prefix.push_str(&key);
            spelling.push_str(component);
            if prefix.len() > 4096 {
                return Err(Error::LayoutLimit);
            }
            if self.files.contains(prefix.as_str()) {
                return Err(Error::LayoutConflict);
            }
            if index + 1 == components.len() {
                if self.directories.contains_key(prefix.as_str()) {
                    return Err(Error::LayoutConflict);
                }
                self.files.insert(CollisionKey(Zeroizing::clone(&prefix)));
            } else if let Some(existing) = self.directories.get(prefix.as_str()) {
                // Equivalent but differently spelled directories must not silently merge groups on
                // one filesystem while creating separate groups on another.
                if existing.as_str() != spelling.as_str() {
                    return Err(Error::LayoutConflict);
                }
            } else {
                self.directories.insert(
                    CollisionKey(Zeroizing::clone(&prefix)),
                    Zeroizing::clone(&spelling),
                );
            }
        }
        Ok(())
    }

    fn component(value: &str) -> Result<Zeroizing<String>, Error> {
        if value.is_empty()
            || value.len() > 255
            || matches!(value, "." | "..")
            || value.ends_with(['.', ' '])
            || value.chars().any(|character| {
                character.is_control()
                    || matches!(
                        character,
                        '/' | '\\' | ':' | '<' | '>' | '"' | '|' | '?' | '*'
                    )
            })
        {
            return Err(Error::InvalidName);
        }
        // This is deliberately a conservative portable conflict policy, not an assertion that a
        // particular native filesystem equates every compatibility/case-folded spelling.
        // Apply the standard library's lowercase tables as well as full folding so newer
        // one-to-one case pairs absent from the cached folding table still share a key.
        let key = Zeroizing::new(
            value
                .nfkc()
                .case_fold()
                .flat_map(char::to_lowercase)
                .nfkc()
                .collect::<String>(),
        );
        if key.is_empty()
            || key.len() > 255
            || matches!(key.as_str(), "." | "..")
            || key.ends_with(['.', ' '])
            || key.chars().any(|character| {
                character.is_control()
                    || matches!(
                        character,
                        '/' | '\\' | ':' | '<' | '>' | '"' | '|' | '?' | '*'
                    )
            })
            || key.starts_with(".xget")
            || key.ends_with(".xget")
        {
            return Err(Error::InvalidName);
        }
        let stem = key.split('.').next().unwrap_or("");
        if matches!(
            stem,
            "con"
                | "prn"
                | "aux"
                | "nul"
                | "com1"
                | "com2"
                | "com3"
                | "com4"
                | "com5"
                | "com6"
                | "com7"
                | "com8"
                | "com9"
                | "lpt1"
                | "lpt2"
                | "lpt3"
                | "lpt4"
                | "lpt5"
                | "lpt6"
                | "lpt7"
                | "lpt8"
                | "lpt9"
        ) {
            return Err(Error::InvalidName);
        }
        Ok(key)
    }
}

struct CollisionKey(Zeroizing<String>);

impl Borrow<str> for CollisionKey {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl PartialEq for CollisionKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_str() == other.0.as_str()
    }
}

impl Eq for CollisionKey {}

impl PartialOrd for CollisionKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CollisionKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.as_str().cmp(other.0.as_str())
    }
}
