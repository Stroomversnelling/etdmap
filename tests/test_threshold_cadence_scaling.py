"""A bound is declared against an interval, so it only means something once
the column's own reporting interval is known.

A column delivering every fifteen minutes carries three five-minute intervals
of consumption in one reading. Judged against the five-minute bound it loses
every normal reading it produces; judged against three times that bound it
keeps them and still loses an implausible one.

The model is declared here, so the bound and the cumulative-to-interval pairing
the scaling is read through are stated by the test rather than taken from
whatever the metadata currently says about a real variable.
"""

import pandas as pd
import pytest

from etdmap.data_model import complement_lookup_names, scale_bounds
from etdmap.flags import MappingFlag, flag_column
from etdmap.mapping_helpers import apply_thresholds_to_df, cadence_minutes_by_column
from etdmap.testing import (
    PAIRS_EMPTY,
    cadence_sidecar,
    declare_synthetic_model,
    household_batch_frame,
    synthetic_model,
)

#: The interval the frames here are recorded on, and the one the declared
#: ``5-minute`` bound is written against. A column reporting less often than
#: this is what the file contrasts.
GRID = 5

#: The bounds this file declares. Any numbers work; stating them is what keeps
#: every scaling assertion off the shipped metadata.
GRID_MAX = 1.5
GRID_MIN = 0.0


@pytest.fixture
def model():
    with declare_synthetic_model(
        synthetic_model(maximum=GRID_MAX, minimum=GRID_MIN)
    ) as declared:
        yield declared


@pytest.fixture
def column(model):
    """The declared interval column, whose bound is written per interval."""
    return model.device_complement


@pytest.fixture
def cumulative(model):
    """The cumulative variable the interval column is derived from.

    Cadence is measured on the columns as delivered, so this is the name a
    cadence table records and the interval column is resolved through.
    """
    return model.device


@pytest.fixture
def declared(model, column):
    """The threshold entry the declared model carries for the interval column.

    Read through the module rather than imported at the top of this file: the
    declaration is installed per test, and a module-level import would bind the
    shipped thresholds before it.
    """
    from etdmap.data_model import thresholds_dict

    return thresholds_dict[column]


def _frame(values, column):
    """One household batch's rows as a supplier delivered them, holding *values*.

    ``PAIRS_EMPTY``: the interval column stands alone as the subject, so its
    cumulative counterpart is present -- a frame holding half a pair is not a
    household artifact -- but empty, and the threshold pass skips a column with
    no values rather than judging a second one.
    """
    return household_batch_frame(
        n_rows=len(values),
        start="2026-01-01",
        freq=f"{GRID}min",
        tz=None,
        project=None,
        pairs=PAIRS_EMPTY,
        **{column: values},
    )


class TestScaleBounds:
    def test_without_a_cadence_the_declared_bound_is_used(self, declared):
        assert scale_bounds(declared)[1] == GRID_MAX

    def test_a_fifteen_minute_column_takes_the_declared_shape(self, declared):
        """Three intervals of accumulation, but a peak cannot be held across
        all three, so the bound grows by the declared exponent rather than in
        proportion: B(3) = Max * 3 ** 0.5 at the default shape."""
        assert scale_bounds(declared, 15)[1] == pytest.approx(
            float(GRID_MAX) * 3 ** 0.5
        )

    def test_the_base_interval_scales_to_itself(self, declared):
        assert scale_bounds(declared, GRID)[1] == pytest.approx(float(GRID_MAX))

    def test_a_momentaan_bound_does_not_scale(self):
        """An instantaneous reading does not accumulate, so reporting slowly
        does not make a larger value plausible.

        Names no variable: the subject is the threshold type, so the entry is
        taken from whichever shipped column carries one.
        """
        from etdmap.data_model import thresholds_dict

        momentaan = next(
            c for c, t in thresholds_dict.items()
            if t.get("ThresholdType") == "momentaan" and pd.notna(t.get("Max"))
        )
        assert scale_bounds(thresholds_dict[momentaan], 60)[1] == thresholds_dict[momentaan]["Max"]


class TestCadenceLookup:
    def test_a_column_is_found_under_its_own_name(self, column):
        assert column in complement_lookup_names(column)

    def test_a_derived_interval_column_is_found_under_its_cumulative(
        self, column, cumulative
    ):
        """Cadence is measured on the columns as delivered, so the interval
        column is not in the table under its own name."""
        assert cumulative in complement_lookup_names(column)

    def test_a_cadence_row_on_the_cumulative_resolves_to_the_interval_column(
        self, column, cumulative
    ):
        resolved = cadence_minutes_by_column(
            cadence_sidecar({cumulative: 15}, grid_minutes=GRID)
        )
        assert resolved[column] == 15

    def test_no_cadence_table_resolves_nothing(self):
        """Empty means unresolved, not unscaled: the caller then falls back to
        the household batch's own interval, or raises."""
        assert cadence_minutes_by_column(None) == {}

    def test_a_columns_own_row_beats_the_one_it_is_paired_with(
        self, column, cumulative
    ):
        """A variable delivered as intervals only still has a row for its absent
        cumulative counterpart, carrying the grid as a fallback. Letting that
        reach the delivered interval column would judge an hourly reading
        against a one-slot bound."""
        cadence = cadence_sidecar(
            {cumulative: GRID, column: 60}, grid_minutes=GRID
        )
        assert cadence_minutes_by_column(cadence)[column] == 60

    def test_the_winner_does_not_depend_on_row_order(self, column, cumulative):
        cadence = cadence_sidecar(
            {column: 60, cumulative: GRID}, grid_minutes=GRID
        )
        resolved = cadence_minutes_by_column(cadence)
        assert resolved[column] == 60
        assert resolved[cumulative] == GRID
        assert cadence_minutes_by_column(pd.DataFrame()) == {}


class TestCoarseColumnOutOfBounds:
    """A coarse column compared against a bound meant for one grid interval."""

    def test_a_normal_coarse_reading_survives(self, column, cumulative):
        """Over the five-minute bound, inside the shaped fifteen-minute one."""
        value = float(GRID_MAX) * 1.5
        df, stats = apply_thresholds_to_df(
            _frame([value], column),
            cadence=cadence_sidecar({cumulative: 15}, grid_minutes=GRID),
            return_stats=True,
        )
        assert df[column].notna().all()
        assert column not in stats

    def test_an_implausible_coarse_reading_is_still_caught(
        self, column, cumulative
    ):
        """Past three intervals' worth, so implausible at any reading rate."""
        value = float(GRID_MAX) * 4
        df, stats = apply_thresholds_to_df(
            _frame([value], column),
            cadence=cadence_sidecar({cumulative: 15}, grid_minutes=GRID),
            return_stats=True,
        )
        assert df[column].isna().all()
        assert stats[column]["n_above"] == 1
        assert stats[column]["upper_bound"] == pytest.approx(
            float(GRID_MAX) * 3 ** 0.5
        )
        assert stats[column]["cadence_minutes"] == 15

    def test_without_a_cadence_the_reading_is_not_judged_at_all(self, column):
        """Applying the declared bound unscaled multiplies it by one, which is
        the same arithmetic as deciding the column reports at the interval the
        bound was written against. That decision is not this function's to
        make, so it raises instead of making it."""
        value = float(GRID_MAX) * 2
        with pytest.raises(ValueError, match="no cadence was resolved"):
            apply_thresholds_to_df(_frame([value], column), return_stats=True)

    def test_the_removal_is_flagged_not_just_deleted(self, column, cumulative):
        """A destroyed value must stay distinguishable from one that never
        arrived. Both are pd.NA; only the flag tells them apart."""
        value = float(GRID_MAX) * 4
        df, stats = apply_thresholds_to_df(
            _frame([value], column),
            cadence=cadence_sidecar({cumulative: 15}, grid_minutes=GRID),
            return_stats=True,
        )
        assert df[column].isna().all()
        flags = df[flag_column(column)]
        assert (flags.iloc[0] & MappingFlag.OUT_OF_BOUNDS_HIGH.value) != 0
        assert (flags.iloc[0] & MappingFlag.OUT_OF_BOUNDS_LOW.value) == 0
        assert stats[column]["n_above"] == 1

    def test_a_value_below_the_lower_bound_is_flagged_low(
        self, column, cumulative, declared
    ):
        lower = declared["Min"]
        df, _ = apply_thresholds_to_df(
            _frame([float(lower) - 1.0], column),
            cadence=cadence_sidecar({cumulative: 15}, grid_minutes=GRID),
            return_stats=True,
        )
        flags = df[flag_column(column)]
        assert (flags.iloc[0] & MappingFlag.OUT_OF_BOUNDS_LOW.value) != 0

    def test_an_in_bounds_value_gets_no_flag(self, column, cumulative):
        df, _ = apply_thresholds_to_df(
            _frame([float(GRID_MAX)], column),
            cadence=cadence_sidecar({cumulative: 15}, grid_minutes=GRID),
            return_stats=True,
        )
        assert df[column].notna().all()
        name = flag_column(column)
        assert name not in df.columns or int(df[name].fillna(0).iloc[0]) == 0

    def test_a_grid_cadence_column_is_judged_against_the_declared_bound(
        self, column, cumulative
    ):
        """A column reporting at the interval its bound was written against
        scales by one, so the declared bound is what it is judged by."""
        value = float(GRID_MAX) * 2
        df, stats = apply_thresholds_to_df(
            _frame([value], column),
            cadence=cadence_sidecar({cumulative: GRID}, grid_minutes=GRID),
            return_stats=True,
        )
        assert df[column].isna().all()
        assert stats[column]["n_above"] == 1
        assert stats[column]["upper_bound"] == pytest.approx(float(GRID_MAX))
