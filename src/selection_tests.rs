#![allow(clippy::unwrap_used)]

use super::{PatternError as Error, Selection};

#[test]
fn no_patterns_include_every_valid_path_without_configured_intent() {
    let selection = Selection::parse(&[], &[]).unwrap();
    assert!(!selection.is_configured());
    assert_eq!(selection.includes().count(), 0);
    assert_eq!(selection.excludes().count(), 0);
    for path in ["file", "dir/file", "café/資料.txt"] {
        assert!(selection.matches(path));
    }
    assert!(Selection::default().matches("file"));
}

#[test]
fn each_positive_pattern_requires_a_match_before_exclusion_subtraction() {
    let selection = Selection::parse(&["a/*.bin", "b/*.bin"], &["b/**"]).unwrap();
    assert!(selection.matches("a/one.bin"));
    assert!(!selection.all_includes_match(["a/one.bin"]));
    assert!(selection.all_includes_match(["a/one.bin", "b/two.bin"]));
    assert!(!selection.matches("b/two.bin"));
    assert!(!selection.all_includes_match(core::iter::empty()));

    let repeated = Selection::parse(&["a/*", "a/*"], &[]).unwrap();
    assert!(repeated.all_includes_match(["a/one.bin"]));
    let excluded_only = Selection::parse(&[], &["**/*.tmp"]).unwrap();
    assert!(excluded_only.all_includes_match(core::iter::empty()));
}

#[test]
fn include_union_is_followed_by_exclusions() {
    let selection =
        Selection::parse(&["docs/**", "*.txt"], &["docs/private/**", "secret.txt"]).unwrap();
    assert!(selection.is_configured());
    for path in ["docs/readme", "docs/a/b", "notes.txt"] {
        assert!(selection.matches(path), "{path}");
    }
    for path in [
        "image.png",
        "docs/private/key",
        "secret.txt",
        "other/notes.txt",
    ] {
        assert!(!selection.matches(path), "{path}");
    }
    let exclusion_only = Selection::parse(&[], &["*.tmp", "cache/**"]).unwrap();
    assert!(exclusion_only.matches("file.txt"));
    assert!(!exclusion_only.matches("file.tmp"));
    assert!(!exclusion_only.matches("cache/file.txt"));
}

#[test]
fn literal_matching_is_complete_case_sensitive_and_not_normalized() {
    let selection = Selection::parse(&["café.txt"], &[]).unwrap();
    assert!(selection.matches("café.txt"));
    for path in [
        "Café.txt",
        "cafe\u{301}.txt",
        "dir/café.txt",
        "café.txt.bak",
    ] {
        assert!(!selection.matches(path), "{path}");
    }
}

#[test]
fn original_spellings_preserve_order_duplicates_and_list_roles() {
    let selection = Selection::parse(&["a/**/**/b", "é?", "é?"], &["x/*", "x/*"]).unwrap();
    assert_eq!(
        selection.includes().collect::<Vec<_>>(),
        ["a/**/**/b", "é?", "é?"]
    );
    assert_eq!(selection.excludes().collect::<Vec<_>>(), ["x/*", "x/*"]);
}

#[test]
fn recursive_components_match_zero_one_or_many_components() {
    let selection = Selection::parse(&["a/**/b"], &[]).unwrap();
    for path in ["a/b", "a/x/b", "a/x/y/b"] {
        assert!(selection.matches(path), "{path}");
    }
    for path in ["b", "a/b/c", "a/xb", "x/a/b"] {
        assert!(!selection.matches(path), "{path}");
    }
    let leading = Selection::parse(&["**/b"], &[]).unwrap();
    assert!(leading.matches("b"));
    assert!(leading.matches("a/x/b"));
    let trailing = Selection::parse(&["a/**"], &[]).unwrap();
    assert!(trailing.matches("a"));
    assert!(trailing.matches("a/x/y"));
    let repeated = Selection::parse(&["**/**/b/**/**"], &[]).unwrap();
    assert!(repeated.matches("b"));
    assert!(repeated.matches("a/b/c"));
    assert!(!repeated.matches("a/c"));
    let all = Selection::parse(&["**"], &[]).unwrap();
    assert!(all.matches("b"));
    assert!(all.matches("a/b/c"));
}

#[test]
fn local_wildcards_consume_scalars_but_never_separators() {
    let one = Selection::parse(&["?.txt"], &[]).unwrap();
    for path in ["a.txt", "é.txt", "資.txt", "\u{1f980}.txt"] {
        assert!(one.matches(path), "{path}");
    }
    for path in [".txt", "ab.txt", "e\u{301}.txt", "a/b.txt"] {
        assert!(!one.matches(path), "{path}");
    }
    let many = Selection::parse(&["a*b"], &[]).unwrap();
    for path in ["ab", "axb", "aé資料b"] {
        assert!(many.matches(path), "{path}");
    }
    assert!(!many.matches("a/x/b"));
    let component = Selection::parse(&["*"], &[]).unwrap();
    assert!(component.matches("file"));
    assert!(!component.matches("dir/file"));
}

#[test]
fn unsupported_or_nonrelative_pattern_syntax_is_rejected() {
    for pattern in [
        "", "/a", "a/", "a//b", ".", "..", "a/./b", "a/../b", "a\\b", "C:/a", "//host/a", "a\0b",
        "a\nb", "a\u{7f}b", "a\u{85}b", "[ab]", "{a,b}", "@(a|b)", "!(a)", "+(a)", "?(a)", "*(a)",
        "a**", "**a", "***", "a/***/b",
    ] {
        let result = Selection::parse(&[pattern], &[]);
        assert!(
            matches!(result, Err(Error::Syntax)),
            "{pattern:?}: {result:?}"
        );
        let result = Selection::parse(&[], &[pattern]);
        assert!(
            matches!(result, Err(Error::Syntax)),
            "{pattern:?}: {result:?}"
        );
    }
}

#[test]
fn all_pattern_budgets_accept_the_boundary_and_reject_the_next_unit() {
    let count_boundary = ["a"; 64];
    assert!(Selection::parse(&count_boundary[..32], &count_boundary[32..]).is_ok());
    assert!(matches!(
        Selection::parse(&count_boundary, &["b"]),
        Err(Error::Count)
    ));

    let length_boundary = "a".repeat(1024);
    assert!(Selection::parse(&[&length_boundary], &[]).is_ok());
    let too_long = "a".repeat(1025);
    assert!(matches!(
        Selection::parse(&[&too_long], &[]),
        Err(Error::Length)
    ));
    let unicode_boundary = "é".repeat(512);
    assert!(Selection::parse(&[&unicode_boundary], &[]).is_ok());
    let unicode_too_long = "é".repeat(513);
    assert!(matches!(
        Selection::parse(&[&unicode_too_long], &[]),
        Err(Error::Length)
    ));

    let depth_boundary = ["a"; 64].join("/");
    assert!(Selection::parse(&[&depth_boundary], &[]).is_ok());
    let too_deep = ["a"; 65].join("/");
    assert!(matches!(
        Selection::parse(&[&too_deep], &[]),
        Err(Error::Depth)
    ));

    let retained_boundary = [length_boundary.as_str(); 16];
    assert!(Selection::parse(&retained_boundary[..8], &retained_boundary[8..]).is_ok());
    assert!(matches!(
        Selection::parse(&retained_boundary, &["b"]),
        Err(Error::RetainedBytes)
    ));
}

#[test]
fn ambiguous_wildcard_paths_use_bounded_iterative_state() {
    // Many ways to distribute the same scalars among stars, ending in an impossible literal.
    let local_pattern = format!("{}z", "*a".repeat(500));
    let selection = Selection::parse(&[&local_pattern], &[]).unwrap();
    assert!(!selection.matches(&"a".repeat(255)));
    let matching_pattern = format!("{}*", "*a".repeat(100));
    let selection = Selection::parse(&[&matching_pattern], &[]).unwrap();
    assert!(selection.matches(&"a".repeat(255)));

    let recursive_pattern = format!("{}/z", ["**"; 63].join("/"));
    let selection = Selection::parse(&[&recursive_pattern], &[]).unwrap();
    assert!(!selection.matches(&["a"; 64].join("/")));
    let path = (0..16)
        .map(|_| "a".repeat(255))
        .collect::<Vec<_>>()
        .join("/");
    let selection = Selection::parse(&["**/a*"], &[]).unwrap();
    assert!(selection.matches(&path));
    assert!(!selection.matches(&"a".repeat(4097)));
    assert!(!selection.matches(&["a"; 65].join("/")));
}

#[test]
fn bounded_dynamic_matching_agrees_with_an_exhaustive_small_recursive_oracle() {
    fn spellings(alphabet: &[&str], separator: &str, depth: usize) -> Vec<String> {
        let mut all = Vec::new();
        let mut level = vec![String::new()];
        for _ in 0..depth {
            level = level
                .iter()
                .flat_map(|prefix| {
                    alphabet.iter().map(move |part| {
                        if prefix.is_empty() {
                            (*part).to_owned()
                        } else {
                            format!("{prefix}{separator}{part}")
                        }
                    })
                })
                .collect();
            all.extend(level.iter().map(String::clone));
        }
        all
    }
    // Exhaustive inputs are deliberately tiny; production never uses this recursive oracle.
    fn scalar(pattern: &[char], text: &[char]) -> bool {
        match pattern.split_first() {
            None => text.is_empty(),
            Some(('*', rest)) => {
                scalar(rest, text) || (!text.is_empty() && scalar(pattern, &text[1..]))
            }
            Some((token, rest)) => {
                !text.is_empty() && (*token == '?' || *token == text[0]) && scalar(rest, &text[1..])
            }
        }
    }
    fn path(pattern: &[&str], text: &[&str]) -> bool {
        match pattern.split_first() {
            None => text.is_empty(),
            Some((&"**", rest)) => {
                path(rest, text) || (!text.is_empty() && path(pattern, &text[1..]))
            }
            Some((part, rest)) => {
                !text.is_empty()
                    && scalar(
                        &part.chars().collect::<Vec<_>>(),
                        &text[0].chars().collect::<Vec<_>>(),
                    )
                    && path(rest, &text[1..])
            }
        }
    }
    let mut patterns = spellings(&["a", "b", "?", "*"], "", 4);
    patterns.extend(spellings(&["a", "b", "*", "?", "**"], "/", 3));
    let mut paths = spellings(&["a", "b"], "", 4);
    paths.extend(spellings(&["a", "b", "aa", "ab"], "/", 3));
    let mut comparisons = 0;
    for pattern in patterns {
        let Ok(selection) = Selection::parse(&[&pattern], &[]) else {
            continue;
        };
        for text in &paths {
            let expected = path(
                &pattern.split('/').collect::<Vec<_>>(),
                &text.split('/').collect::<Vec<_>>(),
            );
            assert_eq!(
                selection.matches(text),
                expected,
                "{pattern:?} against {text:?}"
            );
            comparisons += 1;
        }
    }
    assert!(comparisons > 10_000);
}
