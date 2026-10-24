use core::sync::atomic::{AtomicU64, Ordering};
use std::ffi::OsString;
use std::path::{Component, Path};
use std::time::{SystemTime, UNIX_EPOCH};

use super::{RequestError, RequiredSha256, StateRef};

static NEXT: AtomicU64 = AtomicU64::new(0);

pub(super) struct Plan {
    pub(super) parent: std::path::PathBuf,
    pub(super) destination: OsString,
    pub(super) workspace: OsString,
    pub(super) required: RequiredSha256,
    pub(super) state: StateRef,
}

impl Plan {
    pub(super) fn new(path: &Path, required: RequiredSha256) -> Result<Self, RequestError> {
        let raw = path.as_os_str().as_encoded_bytes();
        let ends_in_separator = raw
            .last()
            .is_some_and(|byte| std::path::is_separator(char::from(*byte)));
        let ends_in_directory_dot = raw.len() >= 2
            && raw.last() == Some(&b'.')
            && std::path::is_separator(char::from(raw[raw.len() - 2]));
        if ends_in_separator || ends_in_directory_dot {
            return Err(RequestError::InvalidDestination);
        }
        let Some(Component::Normal(name)) = path.components().next_back() else {
            return Err(RequestError::InvalidDestination);
        };
        let name = name.to_os_string();
        let display = name.to_string_lossy();
        if display.starts_with(".xget-native-")
            || display == ".xget-native.lock"
            || name.as_encoded_bytes().len() > 255
            || name.as_encoded_bytes().contains(&0)
        {
            return Err(RequestError::InvalidDestination);
        }
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        // This counter only avoids local collisions; create-new is the ownership authority.
        let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let workspace = OsString::from(format!(
            ".xget-native-{}-{time:x}-{sequence:x}",
            std::process::id()
        ));
        let state = StateRef(parent.join(&workspace));
        Ok(Self {
            parent: parent.to_path_buf(),
            destination: name,
            workspace,
            required,
            state,
        })
    }

    pub(super) fn state(&self) -> &StateRef {
        &self.state
    }
}
