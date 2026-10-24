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
