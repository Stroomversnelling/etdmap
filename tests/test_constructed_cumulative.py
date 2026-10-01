"""Building the cumulative column from a delivered interval column.

The cleaning pipeline works on cumulative variables: frozen spans, catch-ups
and gap jumps are all read from a running total. A supplier that delivers only
the interval form leaves nothing for those to work on, so the cumulative is
constructed from it before any of them run.

Separate from the reconstruction in etdtransform, which rebuilds a cumulative
after imputation from values the pipeline has already cleaned. This one runs
first, on data as delivered, and assumes nothing about quality.
"""

import pandas as pd
import pytest

from etdmap.flags import MappingFlag, flag_column
from etdmap.mapping_helpers import construct_cumulative_from_interval

CUM = "ElektriciteitNetgebruik"
DIFF = "ElektriciteitNetgebruikDiff"


def _flags(df, column):
    name = flag_column(column)
    if name not in df.columns:
        return pd.Series(0, index=df.index, dtype="Int64")
    return df[name].fillna(0).astype("Int64")


def _frame(intervals, cumulative=None):
    n = len(intervals)
    data = {
        "ReadingDate": pd.date_range("2026-01-01", periods=n, freq="5min"),
        DIFF: pd.array(intervals, dtype="Float64"),
    }
    if cumulative is not None:
        data[CUM] = pd.array(cumulative, dtype="Float64")
    return pd.DataFrame(data)


class TestItBuildsWhereOnlyIntervalsArrive:
    def test_the_cumulative_is_created(self):
        df = _frame([0.5, 0.5, 0.5, 0.5])
        construct_cumulative_from_interval(df)
        assert CUM in df.columns

    def test_it_is_the_running_total(self):
        df = _frame([0.5, 1.0, 0.25, 2.0])
        construct_cumulative_from_interval(df)
        assert df[CUM].astype(float).tolist() == pytest.approx([0.5, 1.5, 1.75, 3.75])

    def test_it_is_flagged_as_constructed(self):
        df = _frame([0.5, 0.5])
        construct_cumulative_from_interval(df)
        assert (_flags(df, CUM) & MappingFlag.CUMULATIVE_CONSTRUCTED.value).all()

    def test_a_gap_stays_a_gap(self):
        """A missing interval leaves the total undefined at that row, and the
        total resumes from what was accumulated before it."""
        df = _frame([0.5, pd.NA, 0.5])
        construct_cumulative_from_interval(df)
        values = df[CUM]
        assert float(values.iloc[0]) == pytest.approx(0.5)
        assert pd.isna(values.iloc[1])
        assert float(values.iloc[2]) == pytest.approx(1.0)

    def test_it_accepts_a_non_monotonic_interval_column(self):
        """Monotonicity is not required: this runs before any correction, on
        data as delivered, so refusing a dip would discard the case the
        cleaning stages exist to handle."""
        df = _frame([1.0, -0.5, 1.0])
        construct_cumulative_from_interval(df)
        assert df[CUM].astype(float).tolist() == pytest.approx([1.0, 0.5, 1.5])


class TestItLeavesDeliveredCumulativesAlone:
    def test_a_delivered_cumulative_is_untouched(self):
        df = _frame([0.5, 0.5], cumulative=[100.0, 100.5])
        construct_cumulative_from_interval(df)
        assert df[CUM].astype(float).tolist() == pytest.approx([100.0, 100.5])

    def test_it_is_not_flagged_as_constructed(self):
        df = _frame([0.5, 0.5], cumulative=[100.0, 100.5])
        construct_cumulative_from_interval(df)
        assert not (_flags(df, CUM) & MappingFlag.CUMULATIVE_CONSTRUCTED.value).any()

    def test_a_sparse_cumulative_still_counts_as_delivered(self):
        """One real reading is a reading. Overwriting the column because most
        of it is missing would discard what the meter did report."""
        df = _frame([0.5, 0.5, 0.5], cumulative=[pd.NA, 100.5, pd.NA])
        construct_cumulative_from_interval(df)
        assert pd.isna(df[CUM].iloc[0])
        assert float(df[CUM].iloc[1]) == pytest.approx(100.5)
        assert not (_flags(df, CUM) & MappingFlag.CUMULATIVE_CONSTRUCTED.value).any()

    def test_an_all_missing_cumulative_is_built(self):
        df = _frame([0.5, 0.5], cumulative=[pd.NA, pd.NA])
        construct_cumulative_from_interval(df)
        assert df[CUM].astype(float).tolist() == pytest.approx([0.5, 1.0])
        assert (_flags(df, CUM) & MappingFlag.CUMULATIVE_CONSTRUCTED.value).all()


class TestAnInventedCumulativeDoesNotBlockConstruction:
    """Only delivered values block construction. A cumulative holding nothing
    but invented zeros -- written for a device the household was assumed not
    to have -- reports nothing the meter said, and left in place its zeros
    would later be diffed over the delivered intervals."""

    def test_a_zero_filled_cumulative_is_replaced_by_the_construction(self):
        from etdmap.mapping_helpers import fill_zeros_for_device_not_installed

        df = _frame([0.5, 1.0, 0.25, 2.0])
        fill_zeros_for_device_not_installed(df, columns=[CUM])
        construct_cumulative_from_interval(df)
        assert df[CUM].astype(float).tolist() == pytest.approx([0.5, 1.5, 1.75, 3.75])
        assert (_flags(df, CUM) & MappingFlag.CUMULATIVE_CONSTRUCTED.value).all()

    def test_the_delivered_intervals_survive_the_whole_chain(self):
        """The collision the guard exists to prevent: zeros invented before
        the pipeline, then add_diff_columns replacing the delivered intervals
        with diffs of those zeros."""
        from etdmap.mapping_helpers import (
            add_diff_columns,
            fill_zeros_for_device_not_installed,
        )

        df = _frame([0.5, 1.0, 0.25, 2.0])
        fill_zeros_for_device_not_installed(df, columns=[CUM])
        construct_cumulative_from_interval(df)
        before = df[DIFF].astype(float).tolist()
        out = add_diff_columns(df, context="constructed over invented zeros")
        assert out[DIFF].astype(float).tolist() == pytest.approx(before)

    def test_a_delivered_value_with_a_finding_still_blocks(self):
        """A signal is an observation about a delivered value, not an
        invention: the value is still what the meter reported."""
        from etdmap.flags import set_flags

        df = _frame([0.5, 0.5], cumulative=[100.0, 100.5])
        set_flags(
            df, CUM,
            pd.Series([True, False], index=df.index),
            MappingFlag.OUT_OF_BOUNDS_HIGH,
        )
        construct_cumulative_from_interval(df)
        assert df[CUM].astype(float).tolist() == pytest.approx([100.0, 100.5])
        assert not (_flags(df, CUM) & MappingFlag.CUMULATIVE_CONSTRUCTED.value).any()

    def test_one_delivered_reading_among_invented_zeros_still_blocks(self):
        from etdmap.flags import set_flags

        df = _frame([0.5, 0.5, 0.5], cumulative=[0.0, 100.5, 0.0])
        set_flags(
            df, CUM,
            pd.Series([True, False, True], index=df.index),
            MappingFlag.FILLED_ZERO,
        )
        construct_cumulative_from_interval(df)
        assert float(df[CUM].iloc[1]) == pytest.approx(100.5)
        assert not (_flags(df, CUM) & MappingFlag.CUMULATIVE_CONSTRUCTED.value).any()

    def test_values_with_no_findings_column_still_block(self):
        """Nothing recorded against the values means every one counts as
        delivered, which is exactly the old guard."""
        df = _frame([0.5, 0.5], cumulative=[0.0, 0.0])
        construct_cumulative_from_interval(df)
        assert df[CUM].astype(float).tolist() == pytest.approx([0.0, 0.0])
        assert not (_flags(df, CUM) & MappingFlag.CUMULATIVE_CONSTRUCTED.value).any()


class TestItNeedsSomethingToBuildFrom:
    def test_an_absent_interval_column_builds_nothing(self):
        df = pd.DataFrame({
            "ReadingDate": pd.date_range("2026-01-01", periods=2, freq="5min"),
        })
        construct_cumulative_from_interval(df)
        assert CUM not in df.columns

    def test_an_empty_interval_column_builds_nothing(self):
        df = _frame([pd.NA, pd.NA])
        construct_cumulative_from_interval(df)
        assert CUM not in df.columns or df[CUM].isna().all()
        assert not (_flags(df, CUM) & MappingFlag.CUMULATIVE_CONSTRUCTED.value).any()


class TestTheDiffIsNotRederivedFromWhatItBuilt:
    """add_diff_columns would otherwise overwrite the delivered interval column
    with a difference of the total it was just built from -- a round trip that
    can only lose precision, and that discards the delivered values."""

    def test_the_delivered_intervals_survive_add_diff_columns(self):
        from etdmap.mapping_helpers import add_diff_columns

        df = _frame([0.5, 1.0, 0.25, 2.0])
        construct_cumulative_from_interval(df)
        before = df[DIFF].astype(float).tolist()
        out = add_diff_columns(df, context="constructed")
        assert out[DIFF].astype(float).tolist() == pytest.approx(before)


class TestFrozenSpanNullingIsBounded:
    """Treating a span blanks the interval column across it. The values that
    belong to the span are the zeros the frozen meter reported and the catch-up
    they arrive in -- anything else in that range is a delivered reading the
    span has no claim on."""

    def _frame_with_span(self, span_values):
        n = len(span_values)
        return pd.DataFrame({
            "HuisIdBSV": pd.array([1] * n, dtype="Int64"),
            "ReadingDate": pd.date_range("2026-01-01", periods=n, freq="5min"),
            CUM: pd.array([100.0 + i for i in range(n)], dtype="Float64"),
            DIFF: pd.array(span_values, dtype="Float64"),
        })

    def _treat(self, df, start, catchup):
        from etdmap.mapping_helpers import _treat_frozen_spans

        _treat_frozen_spans(
            df, [{"column": DIFF, "start": start, "catchup_position": catchup}]
        )
        return df

    def test_the_zeros_of_the_span_are_blanked(self):
        df = self._frame_with_span([1.0, 0.0, 0.0, 0.0, 9.0, 1.0])
        self._treat(df, start=1, catchup=4)
        assert df[DIFF].iloc[1:4].isna().all()

    def test_the_catch_up_is_blanked(self):
        df = self._frame_with_span([1.0, 0.0, 0.0, 0.0, 9.0, 1.0])
        self._treat(df, start=1, catchup=4)
        assert pd.isna(df[DIFF].iloc[4])

    def test_readings_outside_the_span_survive(self):
        df = self._frame_with_span([1.0, 0.0, 0.0, 0.0, 9.0, 1.0])
        self._treat(df, start=1, catchup=4)
        assert float(df[DIFF].iloc[0]) == pytest.approx(1.0)
        assert float(df[DIFF].iloc[5]) == pytest.approx(1.0)

    def test_a_non_zero_reading_inside_the_span_survives(self):
        """The detector found a flat run, so a value that is neither zero nor
        the catch-up is a delivered reading -- blanking it would destroy data
        the span does not account for."""
        df = self._frame_with_span([1.0, 0.0, 0.7, 0.0, 9.0, 1.0])
        self._treat(df, start=1, catchup=4)
        assert float(df[DIFF].iloc[2]) == pytest.approx(0.7)

    def test_the_blanking_is_flagged(self):
        df = self._frame_with_span([1.0, 0.0, 0.0, 0.0, 9.0, 1.0])
        self._treat(df, start=1, catchup=4)
        flags = _flags(df, DIFF)
        assert (flags.iloc[1:5] & MappingFlag.NULLED.value).all()
