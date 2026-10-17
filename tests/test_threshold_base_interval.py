"""The interval a bound is written against comes from the threshold row.

A ThresholdType that names an interval states what one such interval may hold.
``5-minute`` is a bound per five minutes; the same row written ``1-hour`` would
be a bound per hour. Reading the interval from the label means the threshold row
is the only place it is recorded.

It used to be recorded twice: as the constant ``THRESHOLD_BASE_MINUTES = 5``,
and as the literal string ``"5-minute"`` that gated it in three places, with
nothing making the two agree.
"""

import pandas as pd
import pytest

from etdmap.data_model import load_thresholds, scale_bounds, thresholds_dict
from etdmap.flags import scale_bound_to_cadence, threshold_base_minutes


class TestReadingTheIntervalFromTheLabel:
    @pytest.mark.parametrize(
        "label,minutes",
        [
            ("5-minute", 5.0),
            ("15-minute", 15.0),
            ("1-hour", 60.0),
            ("30-second", 0.5),
            ("5-minutes", 5.0),
        ],
    )
    def test_a_label_naming_an_interval_yields_its_minutes(self, label, minutes):
        assert threshold_base_minutes(label) == minutes

    @pytest.mark.parametrize("label", ["momentaan", "cumulatief", "algemeen"])
    def test_a_type_naming_no_interval_yields_na(self, label):
        """An instantaneous reading does not accumulate over a longer interval,
        a meter total is a year, and a percentage is dimensionless."""
        assert pd.isna(threshold_base_minutes(label))

    @pytest.mark.parametrize(
        "label", [None, pd.NA, "", "nonsense", "0-minute", "minute", "-5-minute"]
    )
    def test_anything_unreadable_yields_na(self, label):
        assert pd.isna(threshold_base_minutes(label))

    def test_whitespace_does_not_change_the_answer(self):
        assert threshold_base_minutes("  5-minute  ") == 5.0


class TestEveryTypeInTheTableIsAccountedFor:
    """Each ThresholdType the packaged table actually uses either names an
    interval or is deliberately one of the three that do not."""

    NO_INTERVAL = {"momentaan", "cumulatief", "algemeen"}

    def test_no_type_is_silently_unrecognised(self):
        used = set(load_thresholds()["ThresholdType"].dropna().astype(str))
        for label in used:
            resolved = threshold_base_minutes(label)
            assert pd.notna(resolved) or label in self.NO_INTERVAL, (
                f"ThresholdType '{label}' neither names an interval nor is one "
                f"of the types that deliberately do not scale. A bound of an "
                f"unrecognised type is applied unscaled."
            )


class TestScalingReadsTheRowNotAConstant:
    def test_the_five_minute_rows_scale_as_before(self):
        """Behaviour is unchanged for the label the table uses today."""
        assert scale_bound_to_cadence(100.0, "5-minute", 60) == 1200.0
        assert scale_bound_to_cadence(100.0, "5-minute", 15) == 300.0
        assert scale_bound_to_cadence(100.0, "5-minute", 5) == 100.0

    @pytest.mark.parametrize("kind", ["momentaan", "cumulatief", "algemeen"])
    def test_a_non_interval_bound_is_returned_unchanged(self, kind):
        assert scale_bound_to_cadence(100.0, kind, 60) == 100.0

    def test_an_unset_bound_stays_unset(self):
        assert pd.isna(scale_bound_to_cadence(pd.NA, "5-minute", 60))

    def test_an_unknown_cadence_leaves_the_bound_alone(self):
        """scale_bound_to_cadence stays lenient. The caller that judges real
        readings resolves the cadence first -- see resolve_bounds."""
        assert scale_bound_to_cadence(100.0, "5-minute", pd.NA) == 100.0

    def test_a_label_the_table_does_not_use_yet_scales_correctly(self):
        """The point of reading the row: a threshold rewritten as an hourly
        bound scales from one hour, with no code change."""
        assert scale_bound_to_cadence(100.0, "1-hour", 60) == 100.0
        assert scale_bound_to_cadence(100.0, "1-hour", 120) == 200.0
        assert scale_bound_to_cadence(100.0, "15-minute", 60) == 400.0

    def test_scale_bounds_reads_the_row_too(self):
        """The upper bound takes its declared shape, so it is not a plain
        multiple -- but the interval it scales from is still the row's own."""
        declared = {"Min": 0.0, "Max": 100.0, "ThresholdType": "15-minute"}
        _, hourly = scale_bounds(declared, 60)
        # Four intervals of fifteen minutes, with the default sqrt shape.
        assert float(hourly) == pytest.approx(100.0 * 4 ** 0.5)

    def test_the_same_figure_at_its_own_interval_is_unchanged(self):
        for label, minutes in [("5-minute", 5), ("15-minute", 15), ("1-hour", 60)]:
            declared = {"Min": 0.0, "Max": 100.0, "ThresholdType": label}
            assert float(scale_bounds(declared, minutes)[1]) == pytest.approx(100.0)


class TestTheConstantIsGone:
    def test_threshold_base_minutes_is_not_a_module_constant(self):
        """The duplication this replaced: a bare 5 in the module and the string
        '5-minute' in the table, with nothing keeping them in step."""
        import etdmap.flags as flags
        assert not hasattr(flags, "THRESHOLD_BASE_MINUTES")

    def test_a_real_threshold_row_resolves_its_own_interval(self):
        five_minute = [
            col for col, entry in thresholds_dict.items()
            if entry.get("ThresholdType") == "5-minute"
        ]
        assert five_minute, "the packaged table carries no 5-minute rows"
        for col in five_minute:
            assert threshold_base_minutes(
                thresholds_dict[col]["ThresholdType"]
            ) == 5.0
