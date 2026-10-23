"""Frozen spans on a column holding the supplier's own interval amounts.

`add_diff_columns` replaces a delivered interval with the difference of the
delivered cumulative column wherever that cumulative column carries data. Only
where it does not do the delivered amounts survive. The two are not the same
measurement and cannot be read with the same semantics:

- an absent interval. Derived from a cumulative column, `pd.NA` means no advance
  was recorded and belongs to the span. Delivered, it is unknown, and only `0.0`
  is a reported zero.
- treatment. The derived side blanks the cumulative column across the span,
  turning it into a gap whose end-to-start jump is the consumption, which
  imputation then spreads. Delivered, there is no cumulative column to give that
  energy back to, so blanking destroys it outright.

Which form a column holds is read from
`MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE`, set per cell by
`add_diff_columns`. The branch is per household per column, so these frames are
built through `add_diff_columns` rather than hand-rolling a `.diff()` -- the flag
is what the code reads, so a fixture that skips it tests the other side.

Synthetic frames only -- no pipeline, no fixtures.
"""

import pandas as pd
import pytest

from etdmap.flags import MappingFlag, flag_column

#: The grid these frames are built on and declared to sit on.
GRID = 5

CUM = "WatergebruikWarmTapwater"
DIFF = "WatergebruikWarmTapwaterDiff"


def _measured_cadence(df):
    """The cadence table a mapper measures before the pipeline runs."""
    from etdmap.mapping_helpers import detect_column_cadence
    return detect_column_cadence(df, grid_minutes=GRID)


def _delivered_frame(values, col=DIFF):
    """A frame where only the interval column was delivered.

    No cumulative column carries data, so `add_diff_columns` keeps these values
    and sets no derivation flag.
    """
    from etdmap.mapping_helpers import add_diff_columns, rearrange_model_columns
    df = pd.DataFrame({
        "ReadingDate": pd.date_range(
            "2026-01-01", periods=len(values), freq="5min"),
        col: pd.array(values, dtype="Float64"),
    })
    df = rearrange_model_columns(df, add_columns=True, context="t")
    return add_diff_columns(df, context="t")


def _detect(df):
    from etdmap.mapping_helpers import _detect_frozen_spans_for_household
    return _detect_frozen_spans_for_household(
        df, 1, cadence=_measured_cadence(df), context="t",
    )


def _treat(df, rows):
    from etdmap.mapping_helpers import _treat_frozen_spans
    return _treat_frozen_spans(df, rows, context="t")


#: A reported-zero run reaching a catch-up above the column's 5-minute bound
#: of 100. Span is positions 1-4, catch-up at 5, so the share covers 5 cells.
A_SPAN = [2.0, 0.0, 0.0, 0.0, 0.0, 150.0, 2.0, 2.0]


class TestDeliveredIntervalsNeedReportedZeros:
    """Reading an absence as "did not advance" is the cumulative assumption. On
    this side it invents spans that never happened."""

    def test_a_run_of_reported_zeros_fires(self):
        rows = _detect(_delivered_frame(A_SPAN))
        assert len(rows) == 1
        assert rows[0]["span_kind"] == "delivered_interval"
        assert rows[0]["start"] == 1
        assert rows[0]["catchup_position"] == 5

    def test_a_run_of_absences_does_not_fire(self):
        """The defect this closes: nothing was reported, so nothing says the
        column stopped advancing."""
        rows = _detect(_delivered_frame(
            [2.0, pd.NA, pd.NA, pd.NA, pd.NA, 150.0, 2.0, 2.0]))
        assert rows == []

    def test_one_absence_before_the_catch_up_ends_the_run(self):
        rows = _detect(_delivered_frame(
            [2.0, 0.0, 0.0, 0.0, pd.NA, 150.0, 2.0, 2.0]))
        assert rows == []

    def test_a_non_zero_value_breaks_the_run(self):
        """The span is only the stretch of reported zeros reaching the
        catch-up, so a real reading inside it shortens the span."""
        rows = _detect(_delivered_frame(
            [2.0, 0.0, 0.0, 1.0, 0.0, 150.0, 2.0, 2.0]))
        assert len(rows) == 1
        assert rows[0]["start"] == 4
        assert rows[0]["catchup_position"] == 5


class TestSpreadingConservesVolume:
    """The catch-up holds everything accumulated since the column last
    advanced, covering the frozen slots and its own."""

    def _treated(self, values=None):
        df = _delivered_frame(A_SPAN if values is None else values)
        before = float(df[DIFF].sum())
        rows = _detect(df)
        _treat(df, rows)
        return df, rows, before

    def test_the_columns_total_is_unchanged(self):
        df, rows, before = self._treated()
        assert len(rows) == 1
        assert float(df[DIFF].sum()) == pytest.approx(before)

    def test_each_written_cell_holds_an_equal_share(self):
        df, _, _ = self._treated()
        share = 150.0 / (5 - 1 + 1)
        for position in range(1, 6):
            assert float(df[DIFF].iloc[position]) == pytest.approx(share)

    def test_nothing_is_blanked(self):
        """The derived treatment blanks; this one rewrites. A NULLED here would
        mean the volume had been destroyed after all."""
        df, _, _ = self._treated()
        flags = df[flag_column(DIFF)].fillna(0).astype("Int64")
        assert not (flags & MappingFlag.NULLED.value).any()
        assert int(df[DIFF].isna().sum()) == 0

    def test_the_rewritten_window_is_flagged(self):
        df, _, _ = self._treated()
        flags = df[flag_column(DIFF)].fillna(0).astype("Int64")
        spread = (
            flags & MappingFlag.FROZEN_DELIVERED_INTERVAL_SPREAD.value
        ) != 0
        assert spread.tolist() == [
            False, True, True, True, True, True, False, False,
        ]

    def test_the_condition_and_the_arrival_are_flagged(self):
        df, _, _ = self._treated()
        flags = df[flag_column(DIFF)].fillna(0).astype("Int64")
        assert ((flags & MappingFlag.FROZEN_SPAN.value) != 0).tolist() == [
            False, True, True, True, True, False, False, False,
        ]
        assert ((flags & MappingFlag.CATCHUP.value) != 0).tolist() == [
            False, False, False, False, False, True, False, False,
        ]

    def test_the_undelivered_cumulative_column_is_untouched(self):
        """rearrange_model_columns adds it all-NA. Stamping FROZEN_SPAN there
        would put a finding on a column the supplier never sent."""
        df, _, _ = self._treated()
        assert df[CUM].isna().all()
        assert flag_column(CUM) not in df.columns

    def test_the_spread_values_survive_the_threshold_pass(self):
        """What the share-over-bound guard is for: a spread value must be
        inside the bound, or step 9 nulls it and the volume is lost twice."""
        from etdmap.mapping_helpers import apply_thresholds_to_df
        df, _, before = self._treated()
        out = apply_thresholds_to_df(df, cadence=_measured_cadence(df))
        assert float(out[DIFF].sum()) == pytest.approx(before)


class TestAnUnexplainableCatchUpIsNotSpread:
    """A catch-up too large to be one interval even spread out is not a
    deferred accumulation. Writing it anyway would only have the threshold pass
    null the spread values, losing the volume a second time and more quietly."""

    VALUES = [2.0, 0.0, 0.0, 20000.0, 2.0, 2.0]

    def test_the_delivered_values_are_left_in_place(self):
        df = _delivered_frame(self.VALUES)
        rows = _detect(df)
        assert len(rows) == 1
        _treat(df, rows)
        assert df[DIFF].tolist() == self.VALUES

    def test_the_span_is_still_recorded(self):
        df = _delivered_frame(self.VALUES)
        _treat(df, _detect(df))
        flags = df[flag_column(DIFF)].fillna(0).astype("Int64")
        assert (flags.iloc[1] & MappingFlag.FROZEN_SPAN.value) != 0
        assert (flags.iloc[3] & MappingFlag.CATCHUP.value) != 0

    def test_nothing_is_marked_as_spread(self):
        df = _delivered_frame(self.VALUES)
        _treat(df, _detect(df))
        flags = df[flag_column(DIFF)].fillna(0).astype("Int64")
        assert not (
            flags & MappingFlag.FROZEN_DELIVERED_INTERVAL_SPREAD.value
        ).any()


class TestPartnerLessIntervalColumnsAreScanned:
    """An interval column declaring no complement resolves
    cumulative_column() to None, so it never reaches the derived side and was
    never scanned at all before the fork."""

    def test_a_delivered_netuitwisseling_reports_a_span(self):
        rows = _detect(_delivered_frame(
            [1.0, 0.0, 0.0, 0.0, 0.0, 9.0, 1.0, 1.0], col="Netuitwisseling"))
        assert [r["column"] for r in rows] == ["Netuitwisseling"]
        assert rows[0]["span_kind"] == "delivered_interval"

    def test_a_dimensionless_column_is_not_scanned(self):
        """ZelfgebruikPercentage is algemeen: it does not accumulate, so a run
        of zeros is not a stalled sensor and its Max is not a per-interval
        bound to compare a catch-up against."""
        rows = _detect(_delivered_frame(
            [1.0, 0.0, 0.0, 0.0, 0.0, 500.0, 1.0, 1.0],
            col="ZelfgebruikPercentage"))
        assert rows == []


class TestTheFlagDecidesTheReading:
    """One underlying truth, three delivery shapes. The flag is what routes
    them, so each is built through add_diff_columns as production builds it."""

    #: A cumulative column that stalls for four intervals then catches up 150.
    CUM_VALUES = [10.0, 12.0, 12.0, 12.0, 12.0, 12.0, 162.0, 164.0]

    def _frame(self, cum=None, diff=None):
        from etdmap.mapping_helpers import (
            add_diff_columns,
            rearrange_model_columns,
        )
        n = len(cum) if cum is not None else len(diff)
        data = {
            "ReadingDate": pd.date_range(
                "2026-01-01", periods=n, freq="5min"),
        }
        if cum is not None:
            data[CUM] = pd.array(cum, dtype="Float64")
        if diff is not None:
            data[DIFF] = pd.array(diff, dtype="Float64")
        df = rearrange_model_columns(pd.DataFrame(data), add_columns=True,
                                     context="t")
        return add_diff_columns(df, context="t")

    def test_cumulative_only_is_treated_as_derived(self):
        df = self._frame(cum=self.CUM_VALUES)
        rows = _detect(df)
        assert [r["span_kind"] for r in rows] == ["derived_interval"]
        _treat(df, rows)
        # The derived treatment blanks the cumulative column across the span
        # and keeps the reading before it as the gap's anchor.
        assert float(df[CUM].iloc[1]) == pytest.approx(12.0)
        assert df[CUM].iloc[2:6].isna().all()

    def test_both_delivered_is_treated_as_derived(self):
        """add_diff_columns prefers the cumulative column wherever it carries
        data, so a delivered interval beside it is replaced and the column is
        derived."""
        df = self._frame(
            cum=self.CUM_VALUES, diff=[0.0] * len(self.CUM_VALUES))
        rows = _detect(df)
        assert [r["span_kind"] for r in rows] == ["derived_interval"]

    def test_interval_only_is_treated_as_delivered(self):
        df = self._frame(diff=A_SPAN)
        rows = _detect(df)
        assert [r["span_kind"] for r in rows] == ["delivered_interval"]

    def test_the_two_sides_are_distinguished_by_the_flag_alone(self):
        """The same values, routed differently because one frame records the
        derivation and the other does not."""
        derived = self._frame(cum=self.CUM_VALUES)
        delivered = self._frame(diff=A_SPAN)
        bit = MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE.value
        assert (
            (derived[flag_column(DIFF)].fillna(0).astype("Int64") & bit) != 0
        ).any()
        findings = delivered.get(flag_column(DIFF))
        if findings is not None:
            assert not (
                (findings.fillna(0).astype("Int64") & bit) != 0
            ).any()
