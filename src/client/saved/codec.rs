//! Bounded normalized private V1/V2 encoding. No references, foreign errors or hasher state.
//! V1 round-trips as V1; V2 explicitly records input occurrence and adapter member/path bindings.

use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;

#[cfg(unix)]
use zeroize::Zeroizing;

#[cfg(unix)]
use super::{Attempt, Member, RootDevice, RootIdentity, RootInode};
use super::{AttemptStatus, Fingerprint, MAX_ATTEMPTS, MAX_RECORD, Record, SavedError};
use crate::checks::Requirements;
use crate::inventory::MAX_FILES;
use crate::transfer::{Algorithm, ByteCount, Length, Measurement, Provenance};
#[cfg(unix)]
use crate::transfer::{Digest, StateRef};

const MAGIC: &[u8; 8] = b"XGETRUN\0";
const MAX_PATH: usize = 4096;
const MAX_VALIDATOR: usize = 8192;

pub(super) fn encode(record: &Record) -> Result<Vec<u8>, SavedError> {
    if record.members.is_empty()
        || record.members.len() > MAX_FILES
        || record
            .members
            .iter()
            .any(|member| member.attempts.len() > MAX_ATTEMPTS)
    {
        return Err(SavedError::Limit);
    }
    if !matches!(record.version, 1 | 2) {
        return Err(SavedError::Version);
    }
    for (index, member) in record.members.iter().enumerate() {
        if member.occurrence.0 as usize >= MAX_FILES
            || (record.version == 1
                && (member.remote.is_some() || member.occurrence.0 as usize != index))
        {
            return Err(SavedError::Corrupt);
        }
        if let Some(remote) = &member.remote {
            crate::client::layout::Layout::source_path(&remote.source_path)
                .map_err(|_| SavedError::Corrupt)?;
            if matches!(member.length, Length::Unknown)
                || (!member.attempts.is_empty() && member.etag.is_none())
            {
                return Err(SavedError::Corrupt);
            }
        }
        for previous in &record.members[..index] {
            if previous.occurrence == member.occurrence
                && (previous.binding != member.binding
                    || previous.remote.is_none()
                    || member.remote.is_none()
                    || previous.remote.as_ref().map(|remote| remote.token)
                        == member.remote.as_ref().map(|remote| remote.token))
            {
                return Err(SavedError::Corrupt);
            }
        }
        if record.members[..index]
            .last()
            .is_some_and(|previous| previous.occurrence > member.occurrence)
        {
            return Err(SavedError::Corrupt);
        }
    }
    let mut out = Encoder(Vec::new());
    out.0.extend_from_slice(MAGIC);
    out.u32(record.version);
    out.path(&record.root)?;
    out.u64(record.identity.device.0);
    out.u64(record.identity.inode.0);
    out.u64(record.store_identity.device.0);
    out.u64(record.store_identity.inode.0);
    out.u64(record.workspace_identity.device.0);
    out.u64(record.workspace_identity.inode.0);
    match record.request {
        None => out.byte(0),
        Some(key) => {
            out.byte(1);
            out.0.extend_from_slice(&key.0);
        }
    }
    out.byte(u8::from(record.complete));
    out.u32(record.members.len() as u32);
    for member in &record.members {
        out.0.extend_from_slice(&member.binding.0);
        if record.version == 2 {
            out.u32(member.occurrence.0);
            match &member.remote {
                None => out.byte(0),
                Some(remote) => {
                    out.byte(1);
                    out.bytes(remote.token.as_str().as_bytes(), 36)?;
                    out.bytes(remote.source_path.as_bytes(), MAX_PATH)?;
                }
            }
        }
        out.path(&member.destination)?;
        match member.length {
            Length::Unknown => out.byte(0),
            Length::Known(ByteCount(length)) => {
                out.byte(1);
                out.u64(length);
            }
        }
        match &member.etag {
            None => out.byte(0),
            Some(etag) => {
                out.byte(1);
                out.bytes(etag.as_bytes(), MAX_VALIDATOR)?;
            }
        }
        out.0
            .extend_from_slice(&policy(member.requirements, member.measurement));
        out.u32(member.attempts.len() as u32);
        for attempt in &member.attempts {
            out.path(attempt.state.path())?;
            out.byte(match attempt.status {
                AttemptStatus::Submitted => 0,
                AttemptStatus::Retained => 1,
                AttemptStatus::Complete => 2,
            });
        }
    }
    if out.0.len().saturating_add(32) > MAX_RECORD {
        return Err(SavedError::Limit);
    }
    // This checksum detects damaged metadata. It never reads or fingerprints payload content.
    let checksum = Fingerprint::bytes(&out.0);
    out.0.extend_from_slice(&checksum.0);
    Ok(out.0)
}

#[cfg(unix)]
pub(super) fn decode(bytes: &[u8]) -> Result<Record, SavedError> {
    if bytes.len() > MAX_RECORD || bytes.len() < 44 {
        return Err(SavedError::Corrupt);
    }
    let mut input = Decoder(bytes);
    if input.take(8)? != MAGIC {
        return Err(SavedError::Corrupt);
    }
    let version = input.u32()?;
    if !matches!(version, 1 | 2) {
        return Err(SavedError::Version);
    }
    let payload_length = bytes.len() - 32;
    if Fingerprint::bytes(&bytes[..payload_length]).0.as_slice() != &bytes[payload_length..] {
        return Err(SavedError::Corrupt);
    }
    input.0 = &bytes[12..payload_length];
    let root = input.path()?;
    let identity = RootIdentity {
        device: RootDevice(input.u64()?),
        inode: RootInode(input.u64()?),
    };
    let store_identity = RootIdentity {
        device: RootDevice(input.u64()?),
        inode: RootInode(input.u64()?),
    };
    let workspace_identity = RootIdentity {
        device: RootDevice(input.u64()?),
        inode: RootInode(input.u64()?),
    };
    let request = match input.byte()? {
        0 => None,
        1 => Some(input.fingerprint()?),
        _ => return Err(SavedError::Corrupt),
    };
    let complete = match input.byte()? {
        0 => false,
        1 => true,
        _ => return Err(SavedError::Corrupt),
    };
    let count = input.u32()? as usize;
    if count == 0 || count > MAX_FILES {
        return Err(SavedError::Corrupt);
    }
    let mut members = Vec::with_capacity(count);
    for index in 0..count {
        let binding = input.fingerprint()?;
        let (occurrence, remote) = if version == 1 {
            (crate::input::InputOccurrence(index as u32), None)
        } else {
            let occurrence = crate::input::InputOccurrence(input.u32()?);
            let remote = match input.byte()? {
                0 => None,
                1 => Some(super::FrozenMember {
                    token: crate::input::MemberToken::parse(input.text(36)?)
                        .ok_or(SavedError::Corrupt)?,
                    source_path: Zeroizing::new(input.text(MAX_PATH)?.to_owned()),
                }),
                _ => return Err(SavedError::Corrupt),
            };
            (occurrence, remote)
        };
        let destination = input.path()?;
        let length = match input.byte()? {
            0 => Length::Unknown,
            1 => Length::Known(ByteCount(input.u64()?)),
            _ => return Err(SavedError::Corrupt),
        };
        let etag = match input.byte()? {
            0 => None,
            1 => {
                let value = input.text(MAX_VALIDATOR)?;
                let bytes = value.as_bytes();
                if bytes.len() < 2
                    || bytes.first() != Some(&b'"')
                    || bytes.last() != Some(&b'"')
                    || !bytes[1..bytes.len() - 1]
                        .iter()
                        .all(|byte| *byte == 0x21 || (0x23..=0x7e).contains(byte))
                {
                    return Err(SavedError::Corrupt);
                }
                Some(Zeroizing::new(value.to_owned()))
            }
            _ => return Err(SavedError::Corrupt),
        };
        let (requirements, measurement) = input.policy()?;
        let count = input.u32()? as usize;
        if count > MAX_ATTEMPTS {
            return Err(SavedError::Corrupt);
        }
        let mut attempts = Vec::with_capacity(count);
        for _ in 0..count {
            let state = StateRef::from(input.path()?);
            let status = match input.byte()? {
                0 => AttemptStatus::Submitted,
                1 => AttemptStatus::Retained,
                2 => AttemptStatus::Complete,
                _ => return Err(SavedError::Corrupt),
            };
            if attempts
                .iter()
                .any(|attempt: &Attempt| attempt.state == state)
            {
                return Err(SavedError::Corrupt);
            }
            if attempts
                .last()
                .is_some_and(|attempt: &Attempt| attempt.status == AttemptStatus::Complete)
            {
                return Err(SavedError::Corrupt);
            }
            attempts.push(Attempt { state, status });
        }
        if complete
            && attempts
                .last()
                .is_none_or(|attempt| attempt.status != AttemptStatus::Complete)
        {
            return Err(SavedError::Corrupt);
        }
        members.push(Member {
            occurrence,
            remote,
            binding,
            destination,
            length,
            etag,
            requirements,
            measurement,
            attempts,
        });
    }
    if !input.0.is_empty() {
        return Err(SavedError::Corrupt);
    }
    let record = Record {
        version,
        root,
        identity,
        store_identity,
        workspace_identity,
        request,
        complete,
        members,
    };
    // Exact normalized encoding rejects duplicate policies, noncanonical algorithm/origin ordering
    // and alternate spellings before the value can acquire executable authority.
    if encode(&record)? != bytes {
        return Err(SavedError::Corrupt);
    }
    Ok(record)
}

pub(super) fn policy(requirements: Requirements, measurement: Measurement) -> Vec<u8> {
    let mut out = Encoder(Vec::new());
    out.byte(match measurement {
        Measurement::None => 0,
        Measurement::Algorithm(algorithm) => algorithm_tag(algorithm),
    });
    out.byte(requirements.iter().count() as u8);
    for requirement in requirements.iter() {
        let text = requirement.digest.to_string();
        out.u32(text.len() as u32);
        out.0.extend_from_slice(text.as_bytes());
        let mut origins = 0;
        for provenance in requirement.origins.iter() {
            origins |= match provenance {
                Provenance::Caller => 1,
                Provenance::Source => 2,
                Provenance::Sidecar => 4,
            };
        }
        out.byte(origins);
    }
    out.0
}

fn algorithm_tag(algorithm: Algorithm) -> u8 {
    match algorithm {
        Algorithm::Md5 => 1,
        Algorithm::Sha1 => 2,
        Algorithm::Sha256 => 3,
        Algorithm::Sha512 => 4,
        Algorithm::Blake3 => 5,
    }
}

struct Encoder(Vec<u8>);
impl Encoder {
    fn byte(&mut self, value: u8) {
        self.0.push(value);
    }
    fn u32(&mut self, value: u32) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }
    fn u64(&mut self, value: u64) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }
    fn bytes(&mut self, bytes: &[u8], limit: usize) -> Result<(), SavedError> {
        if bytes.len() > limit
            || self.0.len().saturating_add(bytes.len()).saturating_add(4) > MAX_RECORD
        {
            return Err(SavedError::Limit);
        }
        self.u32(bytes.len() as u32);
        self.0.extend_from_slice(bytes);
        Ok(())
    }
    fn path(&mut self, path: &Path) -> Result<(), SavedError> {
        self.bytes(path.as_os_str().as_encoded_bytes(), MAX_PATH)
    }
}

#[cfg(unix)]
struct Decoder<'a>(&'a [u8]);
#[cfg(unix)]
impl<'a> Decoder<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], SavedError> {
        let (value, rest) = self.0.split_at_checked(count).ok_or(SavedError::Corrupt)?;
        self.0 = rest;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8, SavedError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, SavedError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().map_err(|_| SavedError::Corrupt)?,
        ))
    }
    fn u64(&mut self) -> Result<u64, SavedError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().map_err(|_| SavedError::Corrupt)?,
        ))
    }
    fn fingerprint(&mut self) -> Result<Fingerprint, SavedError> {
        Ok(Fingerprint(
            self.take(32)?.try_into().map_err(|_| SavedError::Corrupt)?,
        ))
    }
    fn bytes(&mut self, limit: usize) -> Result<&'a [u8], SavedError> {
        let count = self.u32()? as usize;
        if count > limit {
            return Err(SavedError::Corrupt);
        }
        self.take(count)
    }
    fn text(&mut self, limit: usize) -> Result<&'a str, SavedError> {
        core::str::from_utf8(self.bytes(limit)?).map_err(|_| SavedError::Corrupt)
    }
    fn path(&mut self) -> Result<PathBuf, SavedError> {
        let bytes = self.bytes(MAX_PATH)?;
        if bytes.is_empty() || bytes.contains(&0) {
            return Err(SavedError::Corrupt);
        }
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt as _;
            Ok(PathBuf::from(std::ffi::OsStr::from_bytes(bytes)))
        }
        #[cfg(not(unix))]
        {
            Ok(PathBuf::from(
                core::str::from_utf8(bytes).map_err(|_| SavedError::Corrupt)?,
            ))
        }
    }
    fn policy(&mut self) -> Result<(Requirements, Measurement), SavedError> {
        let measurement = match self.byte()? {
            0 => Measurement::None,
            1 => Measurement::Algorithm(Algorithm::Md5),
            2 => Measurement::Algorithm(Algorithm::Sha1),
            3 => Measurement::Algorithm(Algorithm::Sha256),
            4 => Measurement::Algorithm(Algorithm::Sha512),
            5 => Measurement::Algorithm(Algorithm::Blake3),
            _ => return Err(SavedError::Corrupt),
        };
        let count = self.byte()?;
        if count > 5 {
            return Err(SavedError::Corrupt);
        }
        let mut requirements = Requirements::default();
        for _ in 0..count {
            let digest: Digest = self.text(135)?.parse().map_err(|_| SavedError::Corrupt)?;
            let origins = self.byte()?;
            if origins == 0 || origins & !7 != 0 {
                return Err(SavedError::Corrupt);
            }
            for (bit, provenance) in [
                (1, Provenance::Caller),
                (2, Provenance::Source),
                (4, Provenance::Sidecar),
            ] {
                if origins & bit != 0 {
                    requirements
                        .insert(digest, provenance)
                        .map_err(|_| SavedError::Corrupt)?;
                }
            }
        }
        Ok((requirements, measurement))
    }
}
