use core::borrow::Borrow;
use core::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

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
    pub(super) fn source_path(path: &str) -> Result<(), Error> {
        if path.len() > 4096 || path.split('/').count() > 64 {
            return Err(Error::LayoutLimit);
        }
        for component in path.split('/') {
            Self::component(component)?;
        }
        Ok(())
    }

    #[cfg(feature = "swisstransfer")]
    pub(super) fn source_paths<'a>(paths: impl IntoIterator<Item = &'a str>) -> Result<(), Error> {
        let mut layout = Self {
            files: BTreeSet::new(),
            directories: BTreeMap::new(),
        };
        for path in paths {
            Self::source_path(path)?;
            let components: Vec<_> = path.split('/').collect();
            layout.claim(&components)?;
        }
        Ok(())
    }

    #[cfg(feature = "s3")]
    pub(super) fn source_tree<'a>(
        files: impl IntoIterator<Item = &'a str>,
        directories: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), Error> {
        let mut layout = Self {
            files: BTreeSet::new(),
            directories: BTreeMap::new(),
        };
        for path in directories {
            // Only a marker for the explicitly requested prefix root has an empty relative path.
            if path.is_empty() {
                continue;
            }
            Self::source_path(path)?;
            layout.claim_directory(&path.split('/').collect::<Vec<_>>())?;
        }
        for path in files {
            Self::source_path(path)?;
            layout.claim(&path.split('/').collect::<Vec<_>>())?;
        }
        Ok(())
    }

    #[cfg(feature = "s3")]
    fn claim_directory(&mut self, components: &[&str]) -> Result<(), Error> {
        let mut key = Zeroizing::new(String::new());
        let mut spelling = Zeroizing::new(String::new());
        for component in components {
            let canonical = Self::component(component)?;
            if !key.is_empty() {
                key.push('/');
                spelling.push('/');
            }
            key.push_str(&canonical);
            spelling.push_str(component);
            if key.len() > 4096 {
                return Err(Error::LayoutLimit);
            }
            if self.files.contains(key.as_str()) {
                return Err(Error::LayoutConflict);
            }
            if let Some(previous) = self.directories.get(key.as_str()) {
                if previous.as_str() != spelling.as_str() {
                    return Err(Error::LayoutConflict);
                }
            } else {
                self.directories.insert(
                    CollisionKey(Zeroizing::clone(&key)),
                    Zeroizing::clone(&spelling),
                );
            }
        }
        Ok(())
    }

    // Recovery validates stored assignments using exactly the same managed-role/collision policy,
    // without deriving names again or changing frozen assignments.
    #[cfg(unix)]
    pub(super) fn frozen_paths<'a>(
        root: &Path,
        paths: impl IntoIterator<Item = &'a Path>,
    ) -> Result<(), Error> {
        let mut layout = Self {
            files: BTreeSet::new(),
            directories: BTreeMap::new(),
        };
        for path in paths {
            crate::transfer::validate_file_spelling(path).map_err(|_| Error::InvalidName)?;
            let relative = path.strip_prefix(root).map_err(|_| Error::InvalidName)?;
            let components = relative
                .components()
                .map(|component| match component {
                    Component::Normal(name) => name.to_str().ok_or(Error::InvalidName),
                    _ => Err(Error::InvalidName),
                })
                .collect::<Result<Vec<_>, _>>()?;
            if components.is_empty() || components.len() > 64 {
                return Err(Error::LayoutLimit);
            }
            layout.claim(&components)?;
        }
        Ok(())
    }

    pub(super) fn freeze(
        files: Vec<File>,
        target: Target,
        global: Requirements,
        measurement: Measurement,
        input_count: usize,
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
        let explicit_directory = matches!(target, Target::Directory(_));
        let multiple_inputs = input_count > 1;
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
        let mut groups = BTreeMap::new();
        for file in files {
            let namespace = file.namespace().map(|namespace| namespace.as_str());
            // Bound prefix-key retention before collecting or copying any namespace prefixes.
            if namespace.is_some_and(|namespace| {
                namespace.len() > 4096 || namespace.split('/').count() >= 64
            }) {
                return Err(Error::LayoutLimit);
            }
            let collection = matches!(
                file.input.scope(),
                crate::input::InputScope::Collection | crate::input::InputScope::Tree
            );
            // No title field is evidenced by the service protocol. This is an honest readable
            // fallback, never a share UUID or a silently suffixed namespace.
            let group = namespace.or_else(|| {
                if !collection || (explicit_directory && !multiple_inputs) {
                    return None;
                }
                match file.input.kind() {
                    crate::input::InputKind::S3 => file.input.parsed_reference().host_str(),
                    _ => Some("SwissTransfer"),
                }
            });
            if collection && let Some(group) = group {
                let key = group
                    .split('/')
                    .map(Self::component)
                    .collect::<Result<Vec<_>, _>>()?
                    .iter()
                    .map(|part| part.as_str())
                    .collect::<Vec<_>>()
                    .join("/");
                if let Some(previous) = groups.insert(
                    CollisionKey(Zeroizing::new(key)),
                    Arc::clone(&file.input.group),
                ) && !Arc::ptr_eq(&previous, &file.input.group)
                {
                    return Err(Error::LayoutConflict);
                }
            }
            let mut components: Vec<_> = group
                .map(|group| group.split('/').collect())
                .unwrap_or_default();
            if collection {
                Self::source_path(file.source_path())?;
                components.extend(file.source_path().split('/'));
            } else {
                components.push(file.name_hint().ok_or(Error::MissingName)?);
            }
            if components.len() > 64 {
                return Err(Error::LayoutLimit);
            }
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

    pub(super) fn explicit_file(path: &Path) -> Result<(), Error> {
        let raw = path.as_os_str().as_encoded_bytes();
        if raw.len() > 4096 {
            return Err(Error::LayoutLimit);
        }
        crate::transfer::validate_file_spelling(path).map_err(|_| Error::InvalidName)?;
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
                if !self.files.insert(CollisionKey(Zeroizing::clone(&prefix))) {
                    return Err(Error::LayoutConflict);
                }
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
