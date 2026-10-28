"""No stage invents a grid.

The interval a household is recorded on is declared per batch as
``Gegevensfrequentie``. A stage that places readings on a grid, pads one, fills
onto one, or judges a value against one needs that interval, and a stage handed
nothing raises rather than choosing a number.

A five-minute default is not a conservative guess. It silently rescales every
threshold for a household recorded on anything else, and nothing in the output
records that it happened -- an unknown grid and a declared five minutes produce
the same numbers and the same silence.
"""

import pandas as pd
import pytest

import etdmap
from etdmap.data_model import resolve_bounds
from etdmap.index_helpers import grid_minutes_for_household
from etdmap.mapping_helpers import (
    apply_thresholds_to_df,
    detect_column_cadence,
    ensure_intervals,
    fill_down_infrequent_devices,
    run_standard_pipeline,
    snap_readings_to_grid,
)
from etdmap.testing import (
    PAIRS_EMPTY,
    batch_table_frame,
    dataset_frame,
    household_batch_of,
)

GRID = 5
COL = "ElektriciteitsgebruikBoilervat"

#: The dataset each household here is described through. The grid is declared
#: per dataset, so each one is derived from its household rather than stated
#: beside it.
_DATASETS = {
    household_id: household_batch_of(household_id)
    for household_id in (1, 2)
}


def _frame(n=12, freq="5min"):
    """A frame as a supplier delivered it: tz-naive, carrying no ids and no
    project. COL is a cumulative variable, so there is no interval counterpart
    to complete."""
    return dataset_frame(
        n_rows=n, freq=freq, tz=None, project=None,
        **{COL: [float(i) for i in range(n)]},
    )


class TestTheGridIsRequired:
    """Each stage names the argument it was not given, and where the value is
    declared. No stage says what to do about it: that is the operator's call."""

    def test_fill_down_refuses_without_a_grid(self):
        with pytest.raises(ValueError, match="grid_minutes was not given"):
            fill_down_infrequent_devices(_frame(), columns=[COL])

    def test_snap_refuses_without_a_grid(self):
        with pytest.raises(ValueError, match="freq_minutes was not given"):
            snap_readings_to_grid(_frame())

    def test_ensure_intervals_refuses_without_a_grid(self):
        with pytest.raises(ValueError, match="freq was not given"):
            ensure_intervals(_frame())

    def test_detect_column_cadence_refuses_without_a_grid(self):
        with pytest.raises(ValueError, match="neither grid_minutes nor household_id"):
            detect_column_cadence(_frame())

    @pytest.mark.parametrize(
        "message",
        ["Gegevensfrequentie"],
    )
    def test_every_message_points_at_the_declaration(self, message):
        """The message states where the interval comes from, so the operator
        knows which record to look at."""
        for call in (
            lambda: fill_down_infrequent_devices(_frame(), columns=[COL]),
            lambda: snap_readings_to_grid(_frame()),
            lambda: ensure_intervals(_frame()),
            lambda: detect_column_cadence(_frame()),
        ):
            with pytest.raises(ValueError, match=message):
                call()


class TestTheGridCanComeFromMetadata:
    """detect_column_cadence reads the grid itself rather than asking, because
    it is the function that decides a column's cadence."""

    @pytest.fixture
    def batch_table(self, tmp_path):
        """Two datasets, the second declaring no grid at all."""
        path = tmp_path / "huisbatch.csv"
        batch_table_frame(
            [_DATASETS[1], _DATASETS[2]],
            Gegevensfrequentie=["15min", pd.NA],
        ).to_csv(path, index=False)
        previous = etdmap.options.household_batch_csv_path
        etdmap.options.household_batch_csv_path = str(path)
        yield path
        etdmap.options.household_batch_csv_path = previous

    def test_a_household_id_resolves_the_grid(self, batch_table):
        out = detect_column_cadence(
            _frame(n=4, freq="15min"), household_id=1
        ).set_index("column")
        assert out.loc[COL, "cadence_minutes"] == 15

    def test_an_undeclared_grid_raises_naming_the_household(self, batch_table):
        with pytest.raises(ValueError, match="Household 2"):
            detect_column_cadence(_frame(), household_id=2)

    def test_an_explicit_grid_wins_over_the_lookup(self, batch_table):
        """A caller that already resolved the grid is not made to resolve it
        twice."""
        out = detect_column_cadence(
            _frame(), grid_minutes=GRID, household_id=1
        ).set_index("column")
        assert out.loc["ReadingDate", "cadence_minutes"] == GRID


class TestAnUnresolvedCadenceIsNotAnUnscaledBound:
    """Returning the declared bound when the cadence is unknown multiplies it
    by one, which is the same arithmetic as deciding the column reports at the
    interval the bound was written against."""

    def test_resolve_bounds_raises_for_a_per_interval_bound(self):
        declared = {"Min": 0.0, "Max": 1.5, "ThresholdType": "5-minute"}
        with pytest.raises(ValueError, match="written per interval"):
            resolve_bounds(declared, None, "SomeColumn")

    def test_the_message_names_the_column(self):
        declared = {"Min": 0.0, "Max": 1.5, "ThresholdType": "5-minute"}
        with pytest.raises(ValueError, match="SomeColumn"):
            resolve_bounds(declared, None, "SomeColumn")

    @pytest.mark.parametrize("kind", ["momentaan", "cumulatief", "algemeen"])
    def test_a_bound_that_names_no_interval_needs_no_cadence(self, kind):
        """An instantaneous reading does not accumulate, a meter total is a
        year, and a percentage is dimensionless. None of the three scales with
        a reporting interval, so none of them needs one."""
        declared = {"Min": 0.0, "Max": 20.0, "ThresholdType": kind}
        assert resolve_bounds(declared, None, "SomeColumn") == (0.0, 20.0)


class TestAResolvedCadenceScalesTheBound:
    """The other half of the class above, which covers the refusal and the
    pass-through: what a resolved cadence actually produces. Pinned at 15 and 60
    rather than only at the base interval, because at the base interval the
    factor is one and a function that ignored the cadence would pass.
    """

    DECLARED = {"Min": 0.0, "Max": 1.5, "ThresholdType": "5-minute"}

    def test_the_base_interval_leaves_it_alone(self):
        assert resolve_bounds(self.DECLARED, 5, "SomeColumn") == (0.0, 1.5)

    @pytest.mark.parametrize("minutes,factor", [(15, 3.0), (60, 12.0)])
    def test_a_coarser_cadence_widens_it_by_the_root_of_the_ratio(
        self, minutes, factor
    ):
        """A reading covering twelve intervals is not twelve times one
        interval's plausible maximum -- consumption over an hour averages out,
        so the bound grows as the square root of the ratio."""
        low, high = resolve_bounds(self.DECLARED, minutes, "SomeColumn")
        assert low == 0.0
        assert high == pytest.approx(1.5 * factor**0.5)

    def test_a_finer_cadence_tightens_it(self):
        low, high = resolve_bounds(self.DECLARED, 1, "SomeColumn")
        assert low == 0.0
        assert high == pytest.approx(1.5 * (1 / 5) ** 0.5)
        assert high < 1.5

    def test_the_lower_bound_is_not_scaled_away_from_zero(self):
        """Zero consumption is possible over any interval, so a Min of zero
        stays zero however long the reading covers."""
        for minutes in (5, 15, 60, 1440):
            assert resolve_bounds(self.DECLARED, minutes, "C")[0] == 0.0

    def test_the_pipeline_refuses_without_a_cadence(self, tmp_path):
        with pytest.raises(ValueError, match="no cadence was given"):
            run_standard_pipeline(
                _frame(), household_id=1, household_batch_id=_DATASETS[1],
                household_id_supplier="H1",
                mapped_folder_path=tmp_path, grid_minutes=GRID,
            )

    def test_the_threshold_pass_refuses_without_a_cadence(self):
        # PAIRS_EMPTY: the bound looked up is the interval column's, so the
        # cumulative counterpart is present but states no total of its own.
        df = dataset_frame(
            n_rows=1, tz=None, project=None, pairs=PAIRS_EMPTY,
            ElektriciteitNetgebruikHoogDiff=1.0,
        )
        with pytest.raises(ValueError, match="no cadence was resolved"):
            apply_thresholds_to_df(df)


class TestGridMinutesForHousehold:
    """The one place that already got this right, kept honest."""

    def test_it_raises_rather_than_defaulting(self):
        previous = etdmap.options.household_batch_csv_path
        etdmap.options.household_batch_csv_path = None
        try:
            with pytest.raises(ValueError, match="no household batch table"):
                grid_minutes_for_household(1)
        finally:
            etdmap.options.household_batch_csv_path = previous


class TestAnIntervalTheTableCannotRecord:
    """``cadence_minutes`` is Int64, so the sidecar holds whole minutes. A
    sub-minute interval has no representation in it.

    Reachable from metadata: ``frequency_to_minutes`` parses '30s' as 0.5, so a
    batch or a Meetfrequentie row written that way reaches here. The refusal is
    explicit because the Int64 cast alone raises a TypeError naming neither
    column nor value, and rounding makes it a zero-minute cadence.
    """

    def _frame(self):
        """The same supplier-shaped frame on a sub-minute grid."""
        return dataset_frame(
            n_rows=10, freq="30s", tz=None, project=None, **{COL: 1.0}
        )

    def test_a_sub_minute_grid_is_refused(self):
        with pytest.raises(ValueError, match="records whole minutes"):
            detect_column_cadence(self._frame(), grid_minutes=0.5)

    def test_a_sub_minute_declaration_is_refused(self):
        with pytest.raises(ValueError, match="records whole minutes"):
            detect_column_cadence(
                self._frame(), declared={COL: 0.5}, grid_minutes=GRID
            )

    def test_the_message_names_the_interval_in_its_own_unit(self):
        with pytest.raises(ValueError, match="30s"):
            detect_column_cadence(self._frame(), grid_minutes=0.5)

    def test_the_message_names_the_column(self):
        with pytest.raises(ValueError, match=COL):
            detect_column_cadence(
                self._frame(), declared={COL: 0.5}, grid_minutes=GRID
            )

    def test_a_whole_minute_grid_is_fine(self):
        out = detect_column_cadence(
            self._frame(), grid_minutes=GRID
        ).set_index("column")
        assert out.loc[COL, "cadence_minutes"] == GRID
