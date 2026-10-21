#![allow(clippy::expect_used)]

use zeroize::Zeroizing;

use super::{
    Description, File, FileId, Inventory, InventoryError, MAX_FILES, MAX_METADATA_BYTES, Selected,
    SelectionError, VersionEvidence,
};
use crate::checks::Requirements;
use crate::input::Input;
use crate::transfer::Length;

fn file(name: &str) -> File {
    File {
        input: Input::parse("https://example.test/private-parent/member?token=private-token")
            .expect("input")
            .under("local/group")
            .expect("namespace"),
        selected: Selected {
            id: FileId::fresh(),
            description: Description {
                source_path: Zeroizing::new(format!("original/{name}")),
                name: Some(Zeroizing::new(name.to_owned())),
                length: Length::Unknown,
                etag: None,
                requirements: Requirements::default(),
            },
        },
    }
}

fn directory(path: &str) -> super::Directory {
    let mut input = Input::tree("s3://bucket/prefix/")
        .expect("tree")
        .under("local/group")
        .expect("namespace");
    input.member = Some(crate::input::MemberToken::s3(&format!("prefix/{path}/")).expect("key"));
    input.s3_version = Some(Zeroizing::new("version-one".to_owned()));
    super::Directory {
        input,
        description: Description {
            source_path: Zeroizing::new(path.to_owned()),
            name: None,
            length: Length::Known(crate::transfer::ByteCount(0)),
            etag: Some(Zeroizing::new("\"marker-version\"".to_owned())),
            requirements: Requirements::default(),
        },
    }
}

#[test]
fn directory_inspection_is_separate_from_files_and_preserves_original_group_and_version_facts() {
    let selected = file("one");
    let id = FileId::clone(selected.id());
    let mut inventory = Inventory::discovered(super::Discovered {
        files: vec![selected],
        directories: vec![directory("private-dir")],
    })
    .expect("complete scope");
    let accounted = inventory.metadata_bytes();
    let marker = inventory.directories().next().expect("directory metadata");
    assert_eq!(marker.kind(), super::DirectoryKind::S3FolderMarker);
    assert_eq!(marker.source_path(), "private-dir");
    assert_eq!(
        marker.member_token().expect("key").as_str(),
        "prefix/private-dir/"
    );
    assert_eq!(marker.namespace().expect("group").as_str(), "local/group");
    assert_eq!(marker.input.original_reference(), "s3://bucket/prefix/");
    assert_eq!(
        marker.input.s3_version.as_deref().map(String::as_str),
        Some("version-one")
    );
    assert_eq!(marker.version(), VersionEvidence::StrongEtag);
    assert!(!format!("{marker:?} {inventory:?}").contains("private-dir"));
    inventory.retain(|file| file.id() == &id);
    assert_eq!(inventory.into_files().next().expect("file only").id(), &id);
    let mut inventory = Inventory::discovered(super::Discovered {
        files: vec![file("one")],
        directories: vec![directory("private-dir")],
    })
    .expect("complete scope");
    assert_eq!(inventory.metadata_bytes(), accounted);
    inventory.retain(|_| false);
    assert_eq!(inventory.files().len(), 0);
    assert_eq!(inventory.directories().len(), 1);
    assert_eq!(inventory.metadata_bytes(), accounted);
    assert!(matches!(
        inventory.select_one(|_| true),
        Err(SelectionError::NoMatch)
    ));
}

#[test]
fn files_and_directory_metadata_share_one_count_and_text_ceiling() {
    let directories = (0..MAX_FILES).map(|_| directory("dir")).collect();
    assert!(matches!(
        Inventory::discovered(super::Discovered {
            files: vec![file("one")],
            directories,
        }),
        Err(InventoryError::Limit)
    ));
    let mut marker = directory("dir");
    marker.description.source_path = Zeroizing::new("x".repeat(MAX_METADATA_BYTES));
    assert!(matches!(
        Inventory::discovered(super::Discovered {
            files: Vec::new(),
            directories: vec![marker],
        }),
        Err(InventoryError::Limit)
    ));
    let inventory = Inventory::discovered(super::Discovered {
        files: Vec::new(),
        directories: vec![directory("dir")],
    })
    .expect("inspection-only scope");
    assert_eq!(inventory.files().len(), 0);
    assert_eq!(inventory.directories().len(), 1);
    assert_eq!(inventory.into_files().count(), 0);
}

#[test]
fn complete_empty_metadata_is_inspectable_but_selection_is_no_match() {
    let inventory = Inventory::complete(Vec::new()).expect("complete empty metadata fixture");
    assert_eq!(inventory.files().len(), 0);
    assert_eq!(inventory.metadata_bytes(), 0);
    assert!(matches!(
        inventory.select_one(|_| true),
        Err(SelectionError::NoMatch)
    ));
}

#[test]
fn zero_one_and_many_selection_matches_are_distinct() {
    let inventory = Inventory::complete(vec![file("one")]).expect("one");
    assert_eq!(inventory.files().len(), 1);
    let selected = inventory
        .select_one(|file| file.source_path() == "original/one")
        .expect("one match");
    assert_eq!(selected.name_hint(), Some("one"));
    assert_eq!(selected.version(), VersionEvidence::Unpinned);

    let inventory =
        Inventory::complete(vec![file("one"), file("two"), file("three")]).expect("many");
    assert_eq!(inventory.files().len(), 3);
    assert!(matches!(
        inventory.select_one(|_| true),
        Err(SelectionError::MultipleMatches)
    ));

    let inventory = Inventory::complete(vec![file("one"), file("two")]).expect("many");
    assert!(matches!(
        inventory.select_one(|_| false),
        Err(SelectionError::NoMatch)
    ));

    let inventory = Inventory::complete(vec![file("one"), file("two")]).expect("many");
    let selected = inventory
        .select_one(|file| file.name_hint() == Some("two"))
        .expect("one of many");
    assert_eq!(selected.source_path(), "original/two");
}

#[test]
fn file_iteration_retains_namespaces_and_occurrence_local_identity() {
    let first = file("same");
    let first_id = FileId::clone(first.id());
    let second = file("same");
    assert_ne!(first.id(), second.id());
    let inventory =
        Inventory::complete(vec![first, second]).expect("duplicate names are inventory metadata");
    let mut files = inventory.into_files();
    assert_eq!(files.len(), 2);
    let first = files.next().expect("first");
    assert_eq!(first.id(), &first_id);
    assert_eq!(
        first.namespace().expect("namespace").as_str(),
        "local/group"
    );
    let input = Input::from(first);
    assert_eq!(
        input.namespace().expect("namespace").as_str(),
        "local/group"
    );
    assert_eq!(
        input.original_reference(),
        "https://example.test/private-parent/member?token=private-token"
    );
    let selected = input.selected.expect("frozen selection context");
    assert_eq!(selected.id, first_id);
    assert_eq!(selected.description.source_path.as_str(), "original/same");
}

#[test]
fn metadata_limits_never_return_a_successful_prefix() {
    let files = (0..=MAX_FILES).map(|_| file("name")).collect();
    assert!(matches!(
        Inventory::complete(files),
        Err(InventoryError::Limit)
    ));
    let mut oversized = file("name");
    oversized.selected.description.source_path = Zeroizing::new("x".repeat(MAX_METADATA_BYTES));
    assert!(matches!(
        Inventory::complete(vec![oversized]),
        Err(InventoryError::Limit)
    ));
}

#[test]
fn default_inventory_file_and_id_debug_do_not_expose_reference_or_name_derivatives() {
    let file = file("private-name");
    for rendering in [
        format!("{file:?}"),
        format!("{file:#?}"),
        format!("{:?}", file.id()),
    ] {
        for secret in [
            "private-parent",
            "private-token",
            "private-name",
            "local/group",
            "example.test",
        ] {
            assert!(!rendering.contains(secret));
        }
    }
    let inventory = Inventory::complete(vec![file]).expect("one");
    assert!(!format!("{inventory:?}").contains("private-"));
}
