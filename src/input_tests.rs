#![allow(clippy::expect_used)]

use core::error::Error as _;

use super::{Input, InputError, InputKind, InputScope, NamespaceError};

const SHARE: &str = "https://www.swisstransfer.com/dl/aaaaaaaa-1111-2222-3333-444444444444";

#[test]
fn an_empty_reference_is_not_an_empty_inventory() {
    assert!(matches!(
        Input::parse(""),
        Err(InputError::InvalidReference)
    ));
}

#[test]
fn one_exact_reference_has_no_implicit_group_or_tree_scope() {
    let input: Input = "https://example.test/archive.zip"
        .parse()
        .expect("exact reference");
    assert_eq!(input.kind(), InputKind::Http);
    assert_eq!(input.scope(), InputScope::Exact);
    assert!(input.namespace().is_none());
}

#[test]
fn known_reference_shapes_keep_their_resolution_intent() {
    for (reference, kind, scope) in [
        (
            "http://example.test/object",
            InputKind::Http,
            InputScope::Exact,
        ),
        (
            "https://example.test/tree/",
            InputKind::Http,
            InputScope::Exact,
        ),
        ("s3://bucket", InputKind::S3, InputScope::Exact),
        ("s3://bucket/key/", InputKind::S3, InputScope::Exact),
        (
            "ipfs://bafyexample/path",
            InputKind::Ipfs,
            InputScope::Exact,
        ),
        (SHARE, InputKind::SwissTransfer, InputScope::Collection),
        (
            "https://swisstransfer.com/dl/AAAAAAAA-1111-2222-3333-444444444444/",
            InputKind::SwissTransfer,
            InputScope::Collection,
        ),
        (
            "https://github.com/owner/project/releases",
            InputKind::GithubRelease,
            InputScope::Catalogue,
        ),
        (
            "https://github.com/owner/project/releases/",
            InputKind::GithubRelease,
            InputScope::Catalogue,
        ),
        (
            "https://github.com/owner/project/releases/latest",
            InputKind::GithubRelease,
            InputScope::Catalogue,
        ),
        (
            "https://github.com/owner/project/releases/tag/v1",
            InputKind::GithubRelease,
            InputScope::Catalogue,
        ),
        (
            "https://github.com/owner/project/releases/tag/series/v1",
            InputKind::GithubRelease,
            InputScope::Catalogue,
        ),
        (
            "https://github.com/owner/project/releases/download/v1/file.zip",
            InputKind::Http,
            InputScope::Exact,
        ),
        (
            "https://github.com/owner/project",
            InputKind::Http,
            InputScope::Exact,
        ),
        (
            "https://swisstransfer.com/about",
            InputKind::Http,
            InputScope::Exact,
        ),
        (
            "https://www.swisstransfer.com.attacker.test/dl/aaaaaaaa-1111-2222-3333-444444444444",
            InputKind::Http,
            InputScope::Exact,
        ),
        (
            "https://github.com.attacker.test/owner/project/releases",
            InputKind::Http,
            InputScope::Exact,
        ),
    ] {
        let input = Input::parse(reference).expect("recognized reference");
        assert_eq!(input.kind(), kind);
        assert_eq!(input.scope(), scope);
        assert_eq!(input.original_reference(), reference);
    }
}

#[test]
fn tree_scope_is_explicit_and_does_not_rewrite_the_literal_prefix() {
    for reference in [
        "https://example.test/dav/collection/",
        "http://example.test/dav/collection",
        "s3://bucket",
        "s3://bucket/literal%20prefix//",
        "s3://bucket/key?versionId=private-version",
    ] {
        let exact = Input::parse(reference).expect("exact intent");
        let tree = Input::tree(reference).expect("explicit tree intent");
        assert_eq!(exact.scope(), InputScope::Exact);
        assert_eq!(tree.scope(), InputScope::Tree);
        assert_eq!(tree.original_reference(), exact.original_reference());
        assert_eq!(tree.parsed_reference(), exact.parsed_reference());
    }
}

#[test]
fn unsupported_tree_combinations_are_typed_instead_of_silently_becoming_exact() {
    for (reference, kind) in [
        ("ipfs://bafyexample/directory", InputKind::Ipfs),
        (SHARE, InputKind::SwissTransfer),
        (
            "https://github.com/owner/project/releases/latest",
            InputKind::GithubRelease,
        ),
    ] {
        assert!(matches!(
            Input::tree(reference),
            Err(InputError::UnsupportedTree { kind: actual }) if actual == kind
        ));
    }
}

#[test]
fn owned_reference_preserves_original_spelling_credentials_and_queries() {
    let original = String::from(
        "https://bearer-user:bearer-password@EXAMPLE.test/private-path?token=private-query&x=1&x=2",
    );
    let input = Input::parse(&original).expect("credential-bearing reference");
    drop(original);
    let parsed = input.parsed_reference();
    assert_eq!(parsed.username(), "bearer-user");
    assert_eq!(parsed.password(), Some("bearer-password"));
    assert_eq!(parsed.path(), "/private-path");
    assert_eq!(parsed.query(), Some("token=private-query&x=1&x=2"));
    assert!(input.original_reference().contains("EXAMPLE.test"));
    assert_eq!(parsed.host_str(), Some("example.test"));
    for rendering in [
        format!("{input}"),
        format!("{input:?}"),
        format!("{input:#?}"),
    ] {
        for secret in [
            "bearer-user",
            "bearer-password",
            "private-path",
            "private-query",
            "example.test",
            "EXAMPLE.test",
        ] {
            assert!(!rendering.contains(secret));
        }
    }
}

#[test]
fn provider_queries_and_userinfo_are_not_stripped_to_manufacture_an_identity() {
    assert!(matches!(
        Input::parse(
            "https://user:password@swisstransfer.com/dl/aaaaaaaa-1111-2222-3333-444444444444?token=secret"
        ),
        Err(InputError::UnsupportedProviderReference)
    ));
    for reference in [
        "https://user:password@github.com/owner/project/releases/tag/v1?token=secret",
        "s3://user:password@bucket/key?versionId=secret",
        "ipfs://user:password@bafyexample/path?token=secret",
    ] {
        let input = Input::parse(reference).expect("preserved provider reference");
        assert_eq!(input.original_reference(), reference);
        assert_eq!(input.parsed_reference().username(), "user");
        assert_eq!(input.parsed_reference().password(), Some("password"));
        assert!(input.parsed_reference().query().is_some());
    }
}

#[test]
fn every_fragment_is_rejected_before_it_can_expand_member_selection() {
    for reference in [
        SHARE,
        "https://example.test/object",
        "s3://bucket/key",
        "ipfs://bafyexample",
    ] {
        for fragment in [
            "",
            "xget-file=private-member",
            "download",
            "unknown-selector",
        ] {
            let value = format!("{reference}#{fragment}");
            assert!(matches!(
                Input::parse(&value),
                Err(InputError::UnsupportedFragment)
            ));
            assert!(matches!(
                Input::tree(&value),
                Err(InputError::UnsupportedFragment)
            ));
        }
    }
}

#[test]
fn malformed_and_unsupported_references_have_typed_failures() {
    for reference in [
        " https://example.test",
        "https://example.test ",
        "https://example.test/a\nb",
        "https://example.test/a\\b",
    ] {
        assert!(matches!(
            Input::parse(reference),
            Err(InputError::InvalidReference)
        ));
    }
    assert!(matches!(
        Input::parse("relative/path"),
        Err(InputError::InvalidUrl(_))
    ));
    assert!(matches!(
        Input::parse("https://example.test:bad/path"),
        Err(InputError::InvalidUrl(_))
    ));
    assert!(matches!(
        Input::parse("file:///private/file"),
        Err(InputError::MissingAuthority)
    ));
    assert!(matches!(
        Input::parse("ftp://example.test/object"),
        Err(InputError::UnsupportedScheme)
    ));
    for reference in [
        "https://swisstransfer.com/dl",
        "https://swisstransfer.com/dl/",
        "https://swisstransfer.com/dl/not-a-share",
        "https://swisstransfer.com/dl/aaaaaaaa-1111-2222-3333-444444444444/extra",
    ] {
        assert!(matches!(
            Input::parse(reference),
            Err(InputError::InvalidShare)
        ));
    }
    for reference in [
        "http://swisstransfer.com/dl/aaaaaaaa-1111-2222-3333-444444444444",
        "https://swisstransfer.com:8443/dl/aaaaaaaa-1111-2222-3333-444444444444",
        "http://github.com/owner/project/releases",
        "https://github.com:8443/owner/project/releases/latest",
    ] {
        assert!(matches!(
            Input::parse(reference),
            Err(InputError::UnsupportedProviderReference)
        ));
    }
}

#[test]
fn object_dot_paths_are_rejected_instead_of_silently_changing_keys_or_prefixes() {
    for scheme in ["s3", "ipfs"] {
        for segment in [".", "..", "%2e", "%2E%2e", ".%2e", "%2E."] {
            let reference = format!("{scheme}://bucket/first/{segment}/last?token=secret");
            assert!(matches!(
                Input::parse(&reference),
                Err(InputError::UnsupportedObjectPath)
            ));
            assert!(matches!(
                Input::tree(&reference),
                Err(InputError::UnsupportedObjectPath)
            ));
        }
    }
    let input =
        Input::parse("s3://bucket/literal..name/%252e?token=../secret").expect("literal dots");
    assert_eq!(input.parsed_reference().path(), "/literal..name/%252e");
    assert_eq!(input.parsed_reference().query(), Some("token=../secret"));
    for reference in [
        "https:example.test",
        "https:/example.test",
        "https:///example.test",
    ] {
        assert!(matches!(
            Input::parse(reference),
            Err(InputError::InvalidReference)
        ));
    }
}

#[test]
fn namespaces_accept_one_or_many_literal_components_without_changing_remote_intent() {
    for namespace in [
        "first",
        "one/two/three",
        "資料/β",
        "percent%2Fname/space name",
    ] {
        let input = Input::parse(SHARE)
            .expect("share")
            .under(namespace)
            .expect("namespace");
        assert_eq!(
            input.namespace().expect("requested namespace").as_str(),
            namespace
        );
        assert_eq!(input.kind(), InputKind::SwissTransfer);
        assert_eq!(input.scope(), InputScope::Collection);
        assert_eq!(input.original_reference(), SHARE);
    }
    let input = Input::tree("s3://bucket/prefix/")
        .expect("tree")
        .under("first")
        .expect("first namespace")
        .under("second/nested")
        .expect("replacement namespace");
    assert_eq!(
        input.namespace().expect("namespace").as_str(),
        "second/nested"
    );
    assert_eq!(input.scope(), InputScope::Tree);
    assert_eq!(input.original_reference(), "s3://bucket/prefix/");
}

#[test]
fn namespaces_reject_unsafe_components_without_normalization() {
    for (namespace, expected) in [
        ("", NamespaceError::Empty),
        ("/absolute", NamespaceError::Absolute),
        ("//server/share", NamespaceError::Absolute),
        ("one/", NamespaceError::EmptyComponent),
        ("one//two", NamespaceError::EmptyComponent),
        (".", NamespaceError::Traversal),
        ("..", NamespaceError::Traversal),
        ("one/./two", NamespaceError::Traversal),
        ("one/../two", NamespaceError::Traversal),
        ("one\\two", NamespaceError::InvalidCharacter),
        ("\\\\server\\share", NamespaceError::InvalidCharacter),
        ("C:/absolute", NamespaceError::InvalidCharacter),
        ("C:relative", NamespaceError::InvalidCharacter),
        ("one/C:relative", NamespaceError::InvalidCharacter),
        ("one\0two", NamespaceError::InvalidCharacter),
        ("one/\ntwo", NamespaceError::InvalidCharacter),
        ("one/\u{7f}two", NamespaceError::InvalidCharacter),
        ("one/\u{85}two", NamespaceError::InvalidCharacter),
    ] {
        let result = Input::parse(SHARE).expect("share").under(namespace);
        assert!(matches!(result, Err(actual) if actual == expected));
    }
}

#[test]
fn managed_roles_are_reserved_at_every_depth_and_case() {
    for namespace in [
        ".xget",
        ".xget-native.lock",
        ".xget-native-run",
        ".xget-state",
        ".XGET-NATIVE.LOCK",
        "one/.xget-native-run/two",
        "one/payload.xget",
        "one/PAYLOAD.XGET",
    ] {
        assert!(matches!(
            Input::parse(SHARE).expect("share").under(namespace),
            Err(NamespaceError::ReservedRole)
        ));
    }
}

#[test]
fn parser_errors_keep_a_safe_typed_source_without_retaining_reference_secrets() {
    let error = Input::parse(
        "https://secret-user:secret-password@example.test:bad/secret-path?secret-query",
    )
    .expect_err("bad port");
    let InputError::InvalidUrl(cause) = &error else {
        panic!("expected typed parser cause")
    };
    assert!(matches!(cause, url::ParseError::InvalidPort));
    assert!(error.source().is_some());
    let mut current: Option<&(dyn core::error::Error + 'static)> = Some(&error);
    while let Some(error) = current {
        for rendering in [format!("{error}"), format!("{error:?}")] {
            for secret in [
                "secret-user",
                "secret-password",
                "secret-path",
                "secret-query",
                "example.test",
            ] {
                assert!(!rendering.contains(secret));
            }
        }
        current = error.source();
    }
}

#[test]
fn semantic_and_namespace_errors_do_not_echo_secrets_in_any_rendering() {
    for reference in [
        "https://secret-user:secret-password@example.test/secret-path?secret-query#selector",
        "https://secret-user:secret-password@swisstransfer.com/dl/secret-path?secret-query",
        "ftp://secret-user:secret-password@example.test/secret-path?secret-query",
    ] {
        let error = Input::parse(reference).expect_err("unsupported reference");
        assert!(error.source().is_none());
        for rendering in [
            format!("{error}"),
            format!("{error:?}"),
            format!("{error:#?}"),
        ] {
            for secret in [
                "secret-user",
                "secret-password",
                "secret-path",
                "secret-query",
                "example.test",
            ] {
                assert!(!rendering.contains(secret));
            }
        }
    }
    let error = Input::parse(SHARE)
        .expect("share")
        .under("secret-namespace/../private")
        .expect_err("traversal");
    assert!(error.source().is_none());
    assert!(!format!("{error}").contains("secret-namespace"));
    assert!(!format!("{error:?}").contains("secret-namespace"));
    let input = Input::parse(SHARE)
        .expect("share")
        .under("secret-namespace")
        .expect("namespace");
    assert!(!format!("{input:?}").contains("secret-namespace"));
    assert!(!format!("{:?}", input.namespace()).contains("secret-namespace"));
}
