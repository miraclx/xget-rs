//! Reusable checked transfers through one common Client coordinator.
//!
//! Start with [`Client`] for exact HTTP inputs, enabled bounded provider inventories and frozen native
//! output planning. [`Input`] distinguishes remote intent from local grouping; [`Inventory`] exposes
//! resolved files without acquiring their payloads. The Client executes files serially and rejects
//! unsupported provider/tree inputs explicitly. Native publication defaults to absent-only on supported
//! Unix filesystems. [`SavedRun`] inspects frozen aggregate state offline; ordinary re-entry finds a
//! unique matching unfinished run before new discovery. Whole-response members open fresh bodies;
//! eligible exact-HTTP range members revalidate a recorded strong prefix and fetch only the suffix.
//! [`client::Restart`] makes a new guarded attempt for frozen receiving work without deleting old state.
//! [`client::Overwrite`] grants checked cooperating-writer replacement only to a consumed ordinary run;
//! preview Plans and public advanced Transfer entrances retain absent-only publication.
//! Borrow [`client::Observer`] through [`client::Download::run_with`], [`Client::execute_with`] or
//! [`client::Resume::run_with`] for caller-task metadata, progress and outcome events without a
//! reporting task or a static observer. Unobserved methods use the same coordinator with `()`.
//!
//! Use [`Transfer`] for an already-open application body, either checked native publication or
//! [`transfer::live`] delivery to an application-owned consumer/writer. [`Transfer::from_reader`] accepts
//! borrowed repeatable range access for checked native assembly; see [`transfer::reader`] for the
//! session contract and lifetime-specific reader families. Required checks and optional
//! measurement are separate. Irreversible streamed prefixes are not whole-payload expectation evidence.
//!
//! # Checked acquisition
//!
//! Run asynchronous operations on a caller-owned Tokio runtime with I/O and time enabled. Native
//! publication requires a suitable caller-owned parent directory and Unix hardlink support. The
//! destination below must be absent. Replace the reference, access value and complete expected digest
//! with those of a resource you are authorized to fetch; the digest shown is SHA-256 of `abc`, not a
//! claim about the example server.
//!
//! ```no_run
//! use xget::{Client, Input};
//! use xget::transfer::{Digest, Measurement};
//!
//! #[tokio::main(flavor = "current_thread")]
//! async fn main() -> Result<(), Box<dyn core::error::Error>> {
//!     let client = Client::new()?;
//!     let input = Input::parse("https://example.org/abc.bin")?
//!         .header("authorization", "Bearer caller-supplied-token")?;
//!     let inventory = client.resolve(input).await?;
//!     for file in inventory.files() {
//!         eprintln!("source path: {:?}, length: {:?}", file.source_path(), file.length());
//!     }
//!     if inventory.files().len() == 0 {
//!         return Ok(());
//!     }
//!     let file = inventory.select_one(|file| file.source_path() == "abc.bin")?;
//!     let required: Digest =
//!         "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".parse()?;
//!     let report = client.download(file)
//!         .expect(required)
//!         .measurement(Measurement::None)
//!         .to_file("abc.bin")
//!         .run().await?;
//!     assert_eq!(report.completed().len(), 1);
//!     Ok(())
//! }
//! ```
//!
//! [`Input::header`] scopes transient access to one HTTP input or WebDAV root, never all Client
//! requests. It is not persisted or used as source identity. [`Client::resolve`] opens metadata, not
//! payloads. Exact HTTP returns one file; collections can have zero or many. The empty branch avoids
//! constructing an invalid empty original-input request. [`Inventory::select_one`] consumes the
//! inventory and rejects both zero and multiple predicate matches instead of picking the first row.
//! Passing the selected [`File`] into [`Client::download`] preserves its frozen metadata, checks,
//! versions, access and original collection context; reparsing its name would lose that context.
//!
//! Optional [`transfer::Measurement`] and required [`transfer::Digest`] checks are independent:
//! `Measurement::None` does not disable `.expect(required)`. Clean EOF, declared length, settled writes,
//! sealing and all required matches precede native publication. A failure can retain completed files
//! and owned state; consult [`client::RunFailure`] rather than treating it as rollback.
//!
//! # Learning path
//!
//! - [`Input`] and [`input`] describe exact/tree intent, local grouping, scoped access and selectors.
//! - [`Inventory`] and [`inventory`] describe metadata inspection, directory facts and consuming files.
//! - [`client::Download`] covers targets, required checks, measurement, planning and execution.
//! - [`client::Observer`] covers borrowed progress and outcome callbacks without a reporting task.
//! - [`SavedRun`] covers offline inspection and explicit bindings. Normally repeat the original
//!   download request to match unfinished work before discovery. Retain `failure.report.saved_run()`
//!   when available so you can inspect the actual failed run instead of guessing its path.
//! - [`client::Overwrite`] is separate guarded replacement authority, not an inode compare-and-swap.
//! - [`Transfer`], [`transfer::reader`] and [`transfer::live`] serve application-owned bodies/readers.
//!
//! # Optional providers
//!
//! Features `s3`, `swisstransfer`, `webdav`, `github` and `ipfs` enable their common Client adapters;
//! unsupported inputs fail explicitly. For WebDAV, [`Input::tree`] requires a slash-ending HTTP(S)
//! collection root without query or fragment, for example `https://host.example/dav/files/`. It never
//! crawls websites. Other provider-specific limits live in [`client`] and [`input`].
//!
//! S3 additionally requires a caller-configured AWS SDK client. The application chooses credentials,
//! region, endpoint and SDK policies; the library does not construct an SDK client or load ambient
//! credentials, profiles or instance metadata. With `s3` enabled, hand that configured client to the
//! builder. This function is called by an application that already owns its SDK configuration/runtime:
//!
//! ```no_run
//! #[cfg(feature = "s3")]
//! async fn fetch_s3(sdk: aws_sdk_s3::Client) -> Result<(), Box<dyn core::error::Error>> {
//!     let client = xget::Client::builder().s3_client(sdk).build()?;
//!     let report = client.download("s3://example-bucket/file.bin")
//!         .to_file("file.bin")
//!         .run().await?;
//!     assert_eq!(report.completed().len(), 1);
//!     Ok(())
//! }
//!
//! fn main() {}
//! ```
//!
//! # Retained source boundary
//!
//! The earlier root Source/get/from engine is not compiled or exported. Its retained physical sources
//! are excluded from the package pending cleanup; they are not a second engine or compatibility mode.
//! The modern native V1/V2/V3/V4 state readers remain part of the checked transfer implementation.
//! Native publication requires Unix support; portable-engine Wasm support has not been established.

mod checked_body;
mod checks;
mod checksum;
pub mod client;
pub mod input;
pub mod inventory;
mod selection;
pub mod transfer;

pub use crate::checksum::{Checksum, UnknownChecksum};
pub use crate::client::{Client, SavedRun};
pub use crate::input::Input;
pub use crate::inventory::{File, Inventory};
pub use crate::transfer::Transfer;
