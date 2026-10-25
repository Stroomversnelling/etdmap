"""A bound is declared against an interval, so it only means something once
the column's own reporting interval is known.

A column delivering every fifteen minutes carries three five-minute intervals
of consumption in one reading. Judged against the five-minute bound it loses
every normal reading it produces; judged against three times that bound it
keeps them and still loses an implausible one.
"""

import pandas as pd
import pytest

from etdmap.data_model import complement_lookup_names, scale_bounds, thresholds_dict
from etdmap.flags import MappingFlag, flag_column
from etdmap.mapping_helpers import apply_thresholds_to_df, cadence_minutes_by_column

# A 5-minute interval column with a declared Max, so scaling is expected to bite.
COL = "ElektriciteitNetgebruikDiff"
CUM = "ElektriciteitNetgebruik"
GRID_MAX = thresholds_dict[COL]["Max"]


def _cadence(column, minutes):
    return pd.DataFrame({
        "HuisIdBSV": pd.array([1], dtype="Int64"),
        "column": pd.array([column], dtype="string"),
        "cadence_minutes": pd.array([minutes], dtype="Int64"),
    })


def _frame(values):
    return pd.DataFrame({
        "ReadingDate": pd.date_range("2026-01-01", periods=len(values), freq="5min"),
        COL: pd.array(values, dtype="Float64"),
    })


class TestScaleBounds:
    def test_without_a_cadence_the_declared_bound_is_used(self):
        assert scale_bounds(thresholds_dict[COL])[1] == GRID_MAX

    def test_a_fifteen_minute_column_takes_the_declared_shape(self):
        """Three intervals of accumulation, but a peak cannot be held across
        all three, so the bound grows by the declared exponent rather than in
        proportion: B(3) = Max * 3 ** 0.5 at the default shape."""
        assert scale_bounds(thresholds_dict[COL], 15)[1] == pytest.approx(
            float(GRID_MAX) * 3 ** 0.5
        )

    def test_the_base_interval_scales_to_itself(self):
        assert scale_bounds(thresholds_dict[COL], 5)[1] == pytest.approx(float(GRID_MAX))

    def test_a_momentaan_bound_does_not_scale(self):
        """An instantaneous reading does not accumulate, so reporting slowly
        does not make a larger value plausible."""
        momentaan = next(
            c for c, t in thresholds_dict.items()
            if t.get("ThresholdType") == "momentaan" and pd.notna(t.get("Max"))
        )
        assert scale_bounds(thresholds_dict[momentaan], 60)[1] == thresholds_dict[momentaan]["Max"]


class TestCadenceLookup:
    def test_a_column_is_found_under_its_own_name(self):
        assert COL in complement_lookup_names(COL)

    def test_a_derived_interval_column_is_found_under_its_cumulative(self):
        """Cadence is measured on the columns as delivered, so the interval
        column is not in the table under its own name."""
        assert CUM in complement_lookup_names(COL)

    def test_a_cadence_row_on_the_cumulative_resolves_to_the_interval_column(self):
        resolved = cadence_minutes_by_column(_cadence(CUM, 15))
        assert resolved[COL] == 15

    def test_no_cadence_table_resolves_nothing(self):
        """Empty means unresolved, not unscaled: the caller then falls back to
        the dataset's own interval, or raises."""
        assert cadence_minutes_by_column(None) == {}

    def test_a_columns_own_row_beats_the_one_it_is_paired_with(self):
        """A variable delivered as intervals only still has a row for its absent
        register, carrying the grid as a fallback. Letting that reach the
        delivered interval column would judge an hourly reading against a
        one-slot bound."""
        cadence = pd.DataFrame({
            "HuisIdBSV": pd.array([1, 1], dtype="Int64"),
            "column": pd.array([CUM, COL], dtype="string"),
            "cadence_minutes": pd.array([5, 60], dtype="Int64"),
        })
        assert cadence_minutes_by_column(cadence)[COL] == 60

    def test_the_winner_does_not_depend_on_row_order(self):
        cadence = pd.DataFrame({
            "HuisIdBSV": pd.array([1, 1], dtype="Int64"),
            "column": pd.array([COL, CUM], dtype="string"),
            "cadence_minutes": pd.array([60, 5], dtype="Int64"),
        })
        resolved = cadence_minutes_by_column(cadence)
        assert resolved[COL] == 60
        assert resolved[CUM] == 5
        assert cadence_minutes_by_column(pd.DataFrame()) == {}


class TestCoarseColumnOutOfBounds:
    """The case S2 records: a coarse column compared against a bound meant for
    one grid interval."""

    def test_a_normal_coarse_reading_survives(self):
        """Over the five-minute bound, inside the shaped fifteen-minute one."""
        value = float(GRID_MAX) * 1.5
        df, stats = apply_thresholds_to_df(
            _frame([value]), cadence=_cadence(CUM, 15), return_stats=True
        )
        assert df[COL].notna().all()
        assert COL not in stats

    def test_an_implausible_coarse_reading_is_still_caught(self):
        """Past three intervals' worth, so implausible at any reading rate."""
        value = float(GRID_MAX) * 4
        df, stats = apply_thresholds_to_df(
            _frame([value]), cadence=_cadence(CUM, 15), return_stats=True
        )
        assert df[COL].isna().all()
        assert stats[COL]["n_above"] == 1
        assert stats[COL]["upper_bound"] == pytest.approx(
            float(GRID_MAX) * 3 ** 0.5
        )
        assert stats[COL]["cadence_minutes"] == 15

    def test_without_a_cadence_the_reading_is_not_judged_at_all(self):
        """The fix for the bug S2 records. Unscaled, a normal coarse reading
        was destroyed, because applying the declared bound multiplies it by one
        -- the same arithmetic as deciding the column reports at the interval
        the bound was written against. That decision is not this function's to
        make, so it raises instead of making it."""
        value = float(GRID_MAX) * 2
        with pytest.raises(ValueError, match="no cadence was resolved"):
            apply_thresholds_to_df(_frame([value]), return_stats=True)

    def test_the_removal_is_flagged_not_just_deleted(self):
        """A destroyed value must stay distinguishable from one that never
        arrived. Both are pd.NA; only the flag tells them apart."""
        value = float(GRID_MAX) * 4
        df, stats = apply_thresholds_to_df(
            _frame([value]), cadence=_cadence(CUM, 15), return_stats=True
        )
        assert df[COL].isna().all()
        flags = df[flag_column(COL)]
        assert (flags.iloc[0] & MappingFlag.OUT_OF_BOUNDS_HIGH.value) != 0
        assert (flags.iloc[0] & MappingFlag.OUT_OF_BOUNDS_LOW.value) == 0
        assert stats[COL]["n_above"] == 1

    def test_a_value_below_the_lower_bound_is_flagged_low(self):
        lower = thresholds_dict[COL]["Min"]
        df, _ = apply_thresholds_to_df(
            _frame([float(lower) - 1.0]), cadence=_cadence(CUM, 15), return_stats=True
        )
        flags = df[flag_column(COL)]
        assert (flags.iloc[0] & MappingFlag.OUT_OF_BOUNDS_LOW.value) != 0

    def test_an_in_bounds_value_gets_no_flag(self):
        df, _ = apply_thresholds_to_df(
            _frame([float(GRID_MAX)]), cadence=_cadence(CUM, 15), return_stats=True
        )
        assert df[COL].notna().all()
        name = flag_column(COL)
        assert name not in df.columns or int(df[name].fillna(0).iloc[0]) == 0

    def test_a_grid_cadence_column_is_judged_against_the_declared_bound(self):
        """A column reporting at the interval its bound was written against
        scales by one, so the declared bound is what it is judged by. This is
        the case the old unscaled behaviour happened to get right."""
        value = float(GRID_MAX) * 2
        df, stats = apply_thresholds_to_df(
            _frame([value]), cadence=_cadence(CUM, 5), return_stats=True
        )
        assert df[COL].isna().all()
        assert stats[COL]["n_above"] == 1
        assert stats[COL]["upper_bound"] == pytest.approx(float(GRID_MAX))
