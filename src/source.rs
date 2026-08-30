//! The byte sources the engine can read from. Each protocol implements the [`crate::Source`] trait, so
//! the engine is written once and works over any of them: HTTP range GET today, S3 and S3-compatible
//! stores behind the `s3` feature, IPFS through a gateway behind the `ipfs` feature, and a `Mirrors`
//! wrapper that fails a chunk over from one source to the next. The trait itself is part of the crate's
//! public vocabulary and lives in the crate root; this module holds the implementations we ship.

mod http;
#[cfg(feature = "ipfs")]
mod ipfs;
mod mirror;
#[cfg(feature = "s3")]
mod s3;

#[cfg(test)]
mod http_tests;
#[cfg(all(test, feature = "ipfs"))]
mod ipfs_tests;
#[cfg(test)]
mod mirror_tests;

pub use http::HttpSource;
#[cfg(feature = "ipfs")]
pub use ipfs::IpfsSource;
pub use mirror::Mirrors;
#[cfg(feature = "s3")]
pub use s3::S3Source;
