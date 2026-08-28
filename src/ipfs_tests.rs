use cid::Cid;
use cid::multihash::Multihash;

use crate::Checksum;
use crate::ipfs::{IpfsSource, checksum_from_cid, resolve_gateway};

/// A boxed error so a test can `?` a fallible constructor rather than unwrap it.
type TestResult = Result<(), Box<dyn core::error::Error>>;

/// The raw-block multicodec, whose CID hashes the content bytes.
const RAW: u64 = 0x55;
/// The dag-pb multicodec, used for a UnixFS DAG root.
const DAG_PB: u64 = 0x70;

/// Build a CIDv1 with the given codec and a multihash of the given code over `digest`.
fn cid(codec: u64, hash_code: u64, digest: &[u8]) -> Result<Cid, Box<dyn core::error::Error>> {
    let hash = Multihash::<64>::wrap(hash_code, digest)?;
    Ok(Cid::new_v1(codec, hash))
}

#[test]
fn a_raw_sha256_cid_verifies_against_its_own_hash() -> TestResult {
    let digest = [0xabu8; 32];
    let checksum = checksum_from_cid(&cid(RAW, 0x12, &digest)?);
    assert_eq!(checksum, Some((Checksum::Sha256, hex::encode(digest))));
    Ok(())
}

#[test]
fn a_raw_blake3_cid_maps_to_blake3() -> TestResult {
    let digest = [0x11u8; 32];
    let checksum = checksum_from_cid(&cid(RAW, 0x1e, &digest)?);
    assert_eq!(checksum, Some((Checksum::Blake3, hex::encode(digest))));
    Ok(())
}

#[test]
fn a_dag_wrapped_cid_is_not_directly_verifiable() -> TestResult {
    // A UnixFS DAG root addresses the tree of blocks, not the raw bytes, so its CID cannot verify the
    // file by a straight hash.
    assert_eq!(checksum_from_cid(&cid(DAG_PB, 0x12, &[0u8; 32])?), None);
    Ok(())
}

#[test]
fn an_unsupported_hash_is_left_unverified() -> TestResult {
    // sha3-256 (0x16) is a hash the engine cannot compute: leave it unverified rather than mis-map it.
    assert_eq!(checksum_from_cid(&cid(RAW, 0x16, &[0u8; 32])?), None);
    Ok(())
}

#[test]
fn resolve_gateway_prefers_explicit_and_trims_a_trailing_slash() {
    assert_eq!(
        resolve_gateway(Some("http://127.0.0.1:8080/".to_owned())),
        "http://127.0.0.1:8080"
    );
}

#[test]
fn an_invalid_cid_is_rejected() {
    assert!(IpfsSource::new("definitely-not-a-cid", Some("http://localhost".to_owned())).is_err());
}

#[test]
fn a_valid_cid_builds_a_source() -> TestResult {
    let reference = cid(RAW, 0x12, &[0u8; 32])?.to_string();
    assert!(IpfsSource::new(&reference, Some("http://localhost:8080".to_owned())).is_ok());
    Ok(())
}
