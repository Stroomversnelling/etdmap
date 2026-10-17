"""The record validators and the threshold pass agree about the same values.

Both judge a value against the bound its threshold row declares. A bound written
per interval states what ONE such interval may hold, so a column delivering
hourly carries twelve five-minute intervals of consumption in one reading and is
judged against twelve times the declared figure.

The record validators used to skip that scaling entirely. Inside one
``run_standard_pipeline`` call that produced a frame where step 8 wrote
``validate_<col> == False`` on rows that step 9 deliberately kept -- the two
layers disagreeing about the same data, on every household not recorded on five
minutes.

No test could see it, because every pipeline test pinned ``grid_minutes=5`` and
every fixture household declared ``5min``. These run at 15 minutes and an hour.
"""

import pandas as pd
import pytest

import etdmap
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


class TestThroughTheWholePipeline:
    """End to end at a coarse grid, which no pipeline test did before."""

    @pytest.fixture(autouse=True)
    def _flat_output(self):
        previous = etdmap.options.mapped_output_format
        etdmap.options.mapped_output_format = "flat"
        yield
        etdmap.options.mapped_output_format = previous

    def _run(self, tmp_path, grid, household_id):
        df = pd.DataFrame({
            "ReadingDate": pd.date_range(
                "2024-01-01", periods=24, freq=f"{grid}min"
            ),
            CUM: pd.array(
                [float(i) * DECLARED_MAX * 1.2 for i in range(24)], dtype="Float64"
            ),
        })
        cadence = detect_column_cadence(df, grid_minutes=grid)
        run_standard_pipeline(
            df, household_id=household_id, household_id_supplier=f"H{household_id}",
            mapped_folder_path=tmp_path, grid_minutes=grid, cadence=cadence,
        )
        return pd.read_parquet(
            tmp_path / f"household_{household_id}_table.parquet",
            dtype_backend="numpy_nullable",
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
