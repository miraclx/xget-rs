use xprogress::{Bar, DrawTarget};

use super::{Color, Decoration, Footprint, Glyphs, RibbonStyle, Row, Theme};

fn sample_row() -> Row {
    Row {
        count: "received 1 KiB / 2 KiB".to_owned(),
        detailed: "50% received 1 KiB/2 KiB".to_owned(),
        rate: Some("5 KiB/s".to_owned()),
        eta: Some("ETA 1s".to_owned()),
        percent: Some("50.0%".to_owned()),
    }
}

fn theme(preset: RibbonStyle, glyphs: Glyphs) -> Theme {
    Theme {
        preset,
        glyphs,
        color: Color::Disabled,
    }
}

#[test]
fn track_geometry_depends_on_width_profile_not_on_growing_numbers() {
    let mut row = sample_row();
    for (columns, expected) in [(200, 48), (80, 48), (40, 28), (20, 8), (80, 48)] {
        for current in ["1 B", "999.99 KiB", "18446744073709551615 B"] {
            row.count = format!("received {current} / 18446744073709551615 B");
            let layout = row.layout(columns, Decoration::Bar);
            if layout.updating {
                assert_eq!(layout.bar_cells, expected);
                assert!(
                    layout
                        .text
                        .lines()
                        .all(|line| line.len() < usize::from(columns))
                );
                assert!(usize::from(layout.bar_cells) + 10 < usize::from(columns));
            } else {
                assert_eq!(layout.bar_cells, 0);
                assert_eq!(layout.text, row.plain());
            }
        }
    }
    let row = sample_row();
    for (columns, expected) in [(80, 48), (40, 28), (20, 8), (19, 0)] {
        let layout = row.layout(columns, Decoration::Bar);
        assert!(layout.updating);
        assert_eq!(layout.bar_cells, expected);
        assert!(
            layout
                .text
                .lines()
                .all(|line| line.len() < usize::from(columns))
        );
    }
}

#[test]
fn wide_metrics_reserve_independent_count_rate_and_eta_fields() {
    let mut row = sample_row();
    let first = row.layout(80, Decoration::Bar);
    row.count = "received 1.99 MiB / 2 MiB".to_owned();
    row.rate = Some("999.99 MiB/s".to_owned());
    row.eta = Some("ETA 123456s".to_owned());
    let grown = row.layout(80, Decoration::Bar);
    let end_of = |text: &str, field: &str| text.find(field).unwrap() + field.len();
    assert_eq!(
        end_of(&first.text, "5 KiB/s"),
        end_of(&grown.text, "999.99 MiB/s")
    );
    assert_eq!(
        end_of(&first.text, "ETA 1s"),
        end_of(&grown.text, "ETA 123456s")
    );
    assert_eq!(first.bar_cells, grown.bar_cells);
    row.rate = None;
    row.eta = None;
    assert_eq!(row.layout(80, Decoration::Bar).bar_cells, first.bar_cells);
}

#[test]
fn narrow_profiles_wrap_required_counts_and_drop_optional_eta_before_rate() {
    let row = sample_row();
    let regular = row.layout(40, Decoration::Bar);
    assert!(regular.text.contains("ETA 1s"));
    assert!(regular.text.contains("5 KiB/s"));
    let narrow = row.layout(20, Decoration::Bar);
    assert!(narrow.updating);
    assert!(!narrow.text.contains("ETA"));
    assert!(narrow.text.contains("5 KiB/s"));
    assert_eq!(narrow.text, "  received 1 KiB /\n  2 KiB\n  5 KiB/s");
    let huge = Row {
        count: "received 18446744073709551615 B / 18446744073709551615 B; awaiting completion"
            .to_owned(),
        detailed:
            "100% received 18446744073709551615 B/18446744073709551615 B; awaiting completion"
                .to_owned(),
        rate: None,
        eta: None,
        percent: Some("100.0%".to_owned()),
    };
    let layout = huge.layout(20, Decoration::Bar);
    assert!(!layout.updating);
    assert_eq!(layout.text, huge.plain());
    assert_eq!(layout.bar_cells, 0);
}

#[test]
fn unknown_and_plain_rows_have_no_determinate_track() {
    let mut row = sample_row();
    assert_eq!(row.layout(80, Decoration::Plain).bar_cells, 0);
    row.percent = None;
    assert_eq!(row.layout(80, Decoration::Bar).bar_cells, 0);
    assert_eq!(row.plain(), "50% received 1 KiB/2 KiB  5 KiB/s  ETA 1s");
}

#[test]
fn classic_has_a_heavy_frontier_and_a_visibly_lighter_empty_track_without_color() {
    let theme = theme(RibbonStyle::Classic, Glyphs::Unicode);
    let style = theme.style();
    assert_eq!(style.filler(), '━');
    assert_eq!(style.blank(), '─');
    assert_eq!(style.header(), Some('╸'));
    let mut bar = Bar::new([256_u64])
        .with_target(DrawTarget::hidden())
        .with_style(style);
    bar.advance(0, 160_u64);
    let text = bar.render_aggregate(32);
    assert_eq!(text, format!("{}╸{}", "━".repeat(19), "─".repeat(12)));
    assert!(!text.contains('#'));
    assert!(!text.contains('\x1b'));
}

#[test]
fn palettes_keep_single_cell_geometry_and_ascii_changes_ornaments_not_names() {
    for preset in [RibbonStyle::Classic, RibbonStyle::Fine, RibbonStyle::Solid] {
        for glyphs in [Glyphs::Unicode, Glyphs::Ascii] {
            let theme = theme(preset, glyphs);
            let mut bar = Bar::new([10_u64])
                .with_target(DrawTarget::hidden())
                .with_style(theme.style());
            for received in [0_u64, 1, 5, 10] {
                bar.lead_to(0, received);
                let rendered = bar.render_aggregate(28);
                assert_eq!(rendered.chars().count(), 28);
                assert!(!rendered.contains('\x1b'));
                if matches!(glyphs, Glyphs::Ascii) {
                    assert!(rendered.is_ascii());
                }
            }
            assert_eq!(
                theme.heading("[2/3] 映画.mkv  2 MiB"),
                "[2/3] 映画.mkv  2 MiB"
            );
            assert_eq!(
                theme.receipt("saved 映画.mkv  2 MiB\n  measured sha256:full-value"),
                "saved 映画.mkv  2 MiB\n  measured sha256:full-value"
            );
        }
    }
}

#[test]
fn resize_cleanup_counts_reflowed_rows_without_counting_our_sgr_bytes() {
    let block = format!(
        "\x1b[1marchive.bin  8 KiB\x1b[0m\n  \x1b[36m{}\x1b[0m  25.0%\n{}",
        "━".repeat(48),
        "x".repeat(66)
    );
    let mut footprint = Footprint::default();
    footprint.record(&block);
    assert_eq!(footprint.rows_at(80), 3);
    assert_eq!(footprint.rows_at(40), 5);
    assert_eq!(footprint.rows_at(20), 8);
    assert_eq!(footprint.rows_at(80), 3);
    assert_eq!(footprint.take_rows_at(20), 8);
    assert_eq!(footprint.take_rows_at(20), 0);
    footprint.record("next frame");
    assert_eq!(footprint.rows_at(80), 1);
    assert_eq!(Footprint::default().rows_at(20), 0);
}

#[test]
fn locale_detection_accepts_utf8_spellings_without_relabeling_ascii_locales() {
    for locale in ["en_US.UTF-8", "C.utf8", "UTF8"] {
        assert!(Theme::utf8(locale));
    }
    for locale in ["C", "POSIX", "", "en_US.ISO-8859-1"] {
        assert!(!Theme::utf8(locale));
    }
}
