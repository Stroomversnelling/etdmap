"""Two things the data model decides for the mapping pipeline.

Negative-difference correction blanks readings. It is only correct where the
variable cannot decrease, which the data model declares per column with
Monotoon. A column that is allowed to fall carries its decreases as readings,
so neither the classification nor the blanking may run on it.

The threshold pass reports what it removed. Counts alone cannot say how much
left the household batch: one large removal and one small one read as two. The report
carries signed volumes so the size and direction of the loss is recoverable.
"""

import pandas as pd
import pytest

from etdmap import ids
from etdmap.data_model import cumulative_columns, monotonic_columns
from etdmap.flags import MappingFlag, flag_column
from etdmap.mapping_helpers import (
    add_diff_columns,
    apply_threshold_to_col,
    threshold_stats_to_rows,
    write_threshold_removals,
)
from etdmap.testing import (
    PAIRS_EMPTY,
    household_batches_frame,
    one_to_one_test_household_batch_of,
)

COL = "ElektriciteitNetgebruik"
DIFF = "ElektriciteitNetgebruikDiff"

#: The household batch every frame here describes. A difference is taken along one
#: household batch's series, so the household batch is what the correction is scoped to.
HOUSEHOLD_BATCH_ID = one_to_one_test_household_batch_of(1)


def _flags(df, column):
    name = flag_column(column)
    if name not in df.columns:
        return pd.Series(0, index=df.index, dtype="Int64")
    return df[name].fillna(0).astype("Int64")


def _frame(values, column=COL):
    """One household batch's readings of *column*, as the pipeline holds them: tz-naive
    and carrying no project.

    PAIRS_EMPTY: where *column* is the interval one, its cumulative counterpart
    is present but states no total of its own, so the interval values stand as
    given.
    """
    return household_batches_frame(
        {HOUSEHOLD_BATCH_ID: {column: list(values)}},
        start="2026-01-01", tz=None, project=None, pairs=PAIRS_EMPTY,
    )


class TestTheDeclarationDrivesTheCorrection:
    def test_the_declared_set_is_read_from_the_data_model(self):
        """Monotonicity is a property of the variable, so it is declared once
        in the data model rather than restated in the pipeline."""
        assert monotonic_columns
        assert set(monotonic_columns) <= set(cumulative_columns)

    def test_a_declared_column_is_corrected(self):
        """A meter reading that falls is a fault in a column that cannot fall.
        The reading is condemned and the difference recomputed without it."""
        assert COL in monotonic_columns
        out = add_diff_columns(
            _frame([10.0, 10.5, 9.0, 11.0]), id_column=ids.HOUSEHOLD_BATCH
        )
        assert not (out[DIFF] < 0).any()

    def test_an_undeclared_column_keeps_its_decrease(self, monkeypatch):
        """Where the variable may fall, the decrease is the measurement."""
        import etdmap.mapping_helpers as helpers

        monkeypatch.setattr(
            helpers, "monotonic_columns", [c for c in monotonic_columns if c != COL]
        )
        out = add_diff_columns(
            _frame([10.0, 10.5, 9.0, 11.0]), id_column=ids.HOUSEHOLD_BATCH
        )
        assert float(out.loc[2, DIFF]) == pytest.approx(-1.5)

    def test_an_undeclared_column_keeps_its_readings(self, monkeypatch):
        """The correction blanks the cumulative reading itself, so skipping it
        has to leave the source column untouched as well as the difference."""
        import etdmap.mapping_helpers as helpers

        monkeypatch.setattr(
            helpers, "monotonic_columns", [c for c in monotonic_columns if c != COL]
        )
        out = add_diff_columns(
            _frame([10.0, 10.5, 9.0, 11.0]), id_column=ids.HOUSEHOLD_BATCH
        )
        assert out[COL].astype(float).tolist() == pytest.approx(
            [10.0, 10.5, 9.0, 11.0]
        )

    def test_findings_still_reach_the_interval_column(self, monkeypatch):
        """Skipping the correction must not skip carrying what was found on the
        cumulative column across to its interval column."""
        import etdmap.mapping_helpers as helpers

        monkeypatch.setattr(
            helpers, "monotonic_columns", [c for c in monotonic_columns if c != COL]
        )
        df = _frame([10.0, 10.5, 11.0, 11.5])
        df[flag_column(COL)] = pd.array(
            [0, MappingFlag.NULLED.value, 0, 0], dtype="Int64"
        )
        out = add_diff_columns(df, id_column=ids.HOUSEHOLD_BATCH)
        assert _flags(out, DIFF)[1] & MappingFlag.NULLED.value


class TestTheRemovalReportCarriesVolumes:
    def _removed(self, values, lower, upper):
        df = _frame(values, column=DIFF)
        _, stats = apply_threshold_to_col(df, DIFF, lower, upper)
        return stats

    def test_a_positive_removal_is_reported_as_positive(self):
        stats = self._removed([0.5, 99.0, 0.5], 0.0, 1.5)
        assert stats["sum_removed_positive"] == pytest.approx(99.0)
        assert stats["sum_removed_negative"] == pytest.approx(0.0)

    def test_a_negative_removal_is_reported_as_negative(self):
        stats = self._removed([0.5, -99.0, 0.5], 0.0, 1.5)
        assert stats["sum_removed_negative"] == pytest.approx(-99.0)
        assert stats["sum_removed_positive"] == pytest.approx(0.0)

    def test_the_balance_is_their_sum(self):
        stats = self._removed([-99.0, 99.0, 0.5], 0.0, 1.5)
        assert stats["balance_removed"] == pytest.approx(0.0)
        assert stats["sum_removed_negative"] == pytest.approx(-99.0)
        assert stats["sum_removed_positive"] == pytest.approx(99.0)

    def test_the_balance_does_not_stand_in_for_the_volume(self):
        """Two removals that cancel are not the same as no removal, which is
        the whole reason the directions are reported separately."""
        cancelling = self._removed([-99.0, 99.0, 0.5], 0.0, 1.5)
        nothing_large = self._removed([-0.5, 2.0, 0.5], 0.0, 1.5)
        assert cancelling["balance_removed"] == pytest.approx(0.0)
        assert cancelling["sum_removed_positive"] > nothing_large[
            "sum_removed_positive"
        ]

    def test_counts_and_volumes_agree_on_direction(self):
        stats = self._removed([-99.0, 99.0, 0.5], 0.0, 1.5)
        assert stats["n_below"] == 1
        assert stats["n_above"] == 1

    def test_a_column_that_lost_nothing_reports_nothing(self):
        df = _frame([0.5, 0.5, 0.5], column=DIFF)
        _, stats = apply_threshold_to_col(df, DIFF, 0.0, 1.5)
        assert stats is None


class TestTheVolumesReachTheSidecar:
    """The report is written next to the mapped data, so the columns have to
    survive flattening and the typed write, not just exist in the dict."""

    def _rows(self):
        df = _frame([-99.0, 99.0, 0.5], column=DIFF)
        _, stats = apply_threshold_to_col(df, DIFF, 0.0, 1.5)
        return threshold_stats_to_rows(HOUSEHOLD_BATCH_ID, {DIFF: stats})

    @pytest.mark.parametrize("key", [
        "sum_removed_negative", "sum_removed_positive", "balance_removed",
    ])
    def test_the_row_carries_the_volume(self, key):
        assert self._rows()[0][key] is not None

    def test_the_written_file_carries_the_volume(self, tmp_path):
        write_threshold_removals(self._rows(), str(tmp_path))
        written = pd.read_parquet(
            tmp_path / "mapping_threshold_removals.parquet",
            dtype_backend="numpy_nullable",
        )
        assert float(written["sum_removed_negative"].iloc[0]) == pytest.approx(-99.0)
        assert float(written["sum_removed_positive"].iloc[0]) == pytest.approx(99.0)
        assert float(written["balance_removed"].iloc[0]) == pytest.approx(0.0)

    def test_the_volumes_are_nullable_floats(self, tmp_path):
        write_threshold_removals(self._rows(), str(tmp_path))
        written = pd.read_parquet(
            tmp_path / "mapping_threshold_removals.parquet",
            dtype_backend="numpy_nullable",
        )
        for key in ("sum_removed_negative", "sum_removed_positive",
                    "balance_removed"):
            assert written[key].dtype == "Float64"

    def test_an_empty_report_still_declares_the_columns(self, tmp_path):
        write_threshold_removals([], str(tmp_path))
        written = pd.read_parquet(
            tmp_path / "mapping_threshold_removals.parquet",
            dtype_backend="numpy_nullable",
        )
        for key in ("sum_removed_negative", "sum_removed_positive",
                    "balance_removed"):
            assert key in written.columns
