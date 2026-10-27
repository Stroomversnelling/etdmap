"""The record validators and the threshold pass agree about the same values.

Both judge a value against the bound its threshold row declares. A bound written
per interval states what ONE such interval may hold, so a column delivering
hourly carries twelve five-minute intervals of consumption in one reading and is
judged against twelve times the declared figure.

Step 8 of the pipeline writes ``validate_<col>`` and step 9 removes what is out
of bounds, so the two must scale the same way or one calls a reading invalid
that the other keeps. These run at a five-minute, a fifteen-minute and an hourly
grid.
"""

import pandas as pd
import pytest

from etdmap.data_model import thresholds_dict
from etdmap.flags import MappingFlag, flag_column
from etdmap.mapping_helpers import (
    apply_thresholds_to_df,
    detect_column_cadence,
    run_standard_pipeline,
)
from etdmap.record_validators import (
    condition_func_threshold,
    validate_thresholds_combined,
)
from etdmap.storage import mapped_household_paths

#: A cumulative variable whose interval column carries a '5-minute' bound.
CUM = "ElektriciteitNetgebruikHoog"
COL = f"{CUM}Diff"
DECLARED_MAX = float(thresholds_dict[COL]["Max"])


def _cadence(column, minutes, grid):
    """The shape detect_column_cadence produces, including the dataset row."""
    return pd.DataFrame({
        "column": pd.array(["ReadingDate", column], dtype="string"),
        "cadence_minutes": pd.array([grid, minutes], dtype="Int64"),
    })


def _frame(values, column=COL, freq="5min"):
    return pd.DataFrame({
        "ReadingDate": pd.date_range("2024-01-01", periods=len(values), freq=freq),
        column: pd.array(values, dtype="Float64"),
    })


class TestTheTwoLayersAgree:
    """What the threshold pass keeps, the record validator calls valid."""

    @pytest.mark.parametrize("minutes", [5, 15, 60])
    def test_a_normal_reading_for_the_cadence_passes_both(self, minutes):
        """Scaled by the shape, a reading plausible for one interval of this
        length is accepted by both layers."""
        value = DECLARED_MAX * (minutes / 5) ** 0.5 * 0.9
        cadence = _cadence(CUM, minutes, grid=5)

        verdict = condition_func_threshold(COL, cadence)(_frame([value]))
        kept, stats = apply_thresholds_to_df(
            _frame([value]), cadence=cadence, return_stats=True
        )

        assert bool(verdict.iloc[0]) is True
        assert kept[COL].notna().all()
        assert COL not in stats

    @pytest.mark.parametrize("minutes", [5, 15, 60])
    def test_an_implausible_reading_fails_both(self, minutes):
        value = DECLARED_MAX * (minutes / 5) ** 0.5 * 10
        cadence = _cadence(CUM, minutes, grid=5)

        verdict = condition_func_threshold(COL, cadence)(_frame([value]))
        removed, stats = apply_thresholds_to_df(
            _frame([value]), cadence=cadence, return_stats=True
        )

        assert bool(verdict.iloc[0]) is False
        assert removed[COL].isna().all()
        assert stats[COL]["n_above"] == 1

    @pytest.mark.parametrize("minutes", [15, 60])
    def test_the_bug_itself_a_coarse_reading_is_not_called_invalid(self, minutes):
        """The reading the unscaled check destroyed: over the five-minute
        bound, inside the scaled one for the interval it actually covers."""
        value = DECLARED_MAX * 1.2
        cadence = _cadence(CUM, minutes, grid=5)

        verdict = condition_func_threshold(COL, cadence)(_frame([value]))
        combined = validate_thresholds_combined(_frame([value]), cadence=cadence)
        kept, _ = apply_thresholds_to_df(
            _frame([value]), cadence=cadence, return_stats=True
        )

        assert bool(verdict.iloc[0]) is True
        assert bool(combined.iloc[0]) is True
        assert kept[COL].notna().all()

    def test_the_same_value_is_invalid_at_the_base_interval(self):
        """The contrast that makes the test above mean something: the value is
        genuinely out of bounds for a five-minute reading."""
        value = DECLARED_MAX * 1.2
        cadence = _cadence(CUM, 5, grid=5)
        verdict = condition_func_threshold(COL, cadence)(_frame([value]))
        assert bool(verdict.iloc[0]) is False


def _run_pipeline(tmp_path, grid, household_id, steps, household_batch_id=None):
    """One household through ``run_standard_pipeline``, read back.

    *steps* are the per-reading increments of the cumulative column, so a
    caller states what each interval holds and the accumulation is built from
    them. Read through the storage reader, so whichever layout the mapping is
    configured to write is the one resolved.
    """
    cumulative = pd.Series([0.0, *steps], dtype="Float64").cumsum()
    df = pd.DataFrame({
        "ReadingDate": pd.date_range(
            "2024-01-01", periods=len(cumulative), freq=f"{grid}min"
        ),
        CUM: cumulative,
    })
    cadence = detect_column_cadence(df, grid_minutes=grid)
    run_standard_pipeline(
        df, household_id=household_id,
        household_batch_id=household_batch_id or household_id + 1000,
        household_id_supplier=f"H{household_id}",
        mapped_folder_path=tmp_path, grid_minutes=grid, cadence=cadence,
    )
    paths = mapped_household_paths(tmp_path)
    return pd.read_parquet(paths[household_id], dtype_backend="numpy_nullable")


class TestThroughTheWholePipeline:
    """End to end at a coarse grid, which no pipeline test did before."""

    def _run(self, tmp_path, grid, household_id):
        # Increments of 1.2x the five-minute bound, which is inside the bound
        # scaled to either of these grids.
        return _run_pipeline(
            tmp_path, grid, household_id, [DECLARED_MAX * 1.2] * 23
        )

    @pytest.mark.parametrize("grid", [15, 60])
    def test_the_validator_flag_matches_what_survived(self, tmp_path, grid):
        """Step 8 writes validate_<col>; step 9 nulls what is out of bounds.
        A row the threshold pass kept must not be flagged invalid by step 8."""
        out = self._run(tmp_path, grid, household_id=grid)

        flag = f"validate_{COL}"
        assert flag in out.columns

        survived = out[COL].notna()
        called_invalid = out[flag] == False  # noqa: E712
        assert not (survived & called_invalid).any(), (
            f"at a {grid}min grid, {int((survived & called_invalid).sum())} row(s) "
            f"survived the threshold pass but were flagged invalid by the "
            f"record validators -- the two layers disagree"
        )

    @pytest.mark.parametrize("grid", [15, 60])
    def test_a_plausible_coarse_reading_is_not_nulled(self, tmp_path, grid):
        out = self._run(tmp_path, grid, household_id=100 + grid)
        # The per-interval increments are 1.2x the five-minute bound, which is
        # inside the scaled bound at both of these intervals.
        assert out[COL].notna().any()
        name = flag_column(COL)
        if name in out.columns:
            high = MappingFlag.OUT_OF_BOUNDS_HIGH.value
            assert int((out[name].fillna(0) & high).sum()) == 0


class TestAReadingOverItsBoundIsRefusedByAPipelineRun:
    """The other direction, and the step before the bound is applied.

    ``test_bound_flags.py`` covers what a bound does once resolved and
    ``test_declared_threshold_is_applied.py`` that the model's declaration
    becomes that bound. Neither runs the mapping, and the class above states
    only what the pipeline does with a plausible reading.

    Generated data is drawn inside its bounds, so a fixture cannot show this.
    """

    #: Three times the bound for one interval, which is over it at any grid.
    OVER = DECLARED_MAX * 3

    #: The reading that is over, among readings that are not.
    BAD_ROW = 3

    @pytest.fixture(scope="class")
    def mapped(self, tmp_path_factory):
        steps = [DECLARED_MAX * 0.5] * 8
        steps[self.BAD_ROW - 1] = self.OVER
        return _run_pipeline(
            tmp_path_factory.mktemp("over_bound"), 5, 1, steps
        )

    def test_the_reading_over_the_bound_is_flagged_high(self, mapped):
        flags = mapped[flag_column(COL)].fillna(0)
        high = (flags & MappingFlag.OUT_OF_BOUNDS_HIGH.value) != 0

        assert bool(high.iloc[self.BAD_ROW]), (
            f"the reading at row {self.BAD_ROW} holds {self.OVER}, which is "
            f"over the declared bound of {DECLARED_MAX} for one interval"
        )

    def test_no_other_reading_is_flagged(self, mapped):
        """Each other interval holds half the bound, so a run that flagged them
        too would be applying something other than the declared threshold."""
        flags = mapped[flag_column(COL)].fillna(0)
        high = (flags & MappingFlag.OUT_OF_BOUNDS_HIGH.value) != 0

        assert list(high[high].index) == [self.BAD_ROW]

    def test_the_value_does_not_survive(self, mapped):
        assert pd.isna(mapped[COL].iloc[self.BAD_ROW]), (
            "a reading outside its bound is not a measurement, so the "
            "threshold pass removes it"
        )

    def test_the_removal_is_recorded_as_an_action(self, mapped):
        """The cell reads out-of-bounds AND nulled: the signal and what was
        done about it, neither replacing the other."""
        flags = int(mapped[flag_column(COL)].fillna(0).iloc[self.BAD_ROW])

        assert flags & MappingFlag.NULLED.value

    def test_the_readings_inside_the_bound_survive(self, mapped):
        kept = mapped[COL].drop(index=self.BAD_ROW).dropna()

        assert len(kept), "the threshold pass removed everything"
