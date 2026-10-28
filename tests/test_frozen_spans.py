"""Frozen-span detection: a cumulative variable that stops advancing and then delivers its
accumulation in one impossible jump.

A flat stretch on its own is not a fault -- a device that is not running does not
consume. It becomes a fault only when the stretch ENDS in a reading too large to
have happened in one interval, because that reading is the accumulation arriving
late. Detection is therefore anchored on the jump and walks backwards, never
forwards from the flat stretch.

Synthetic frames only -- no pipeline, no fixtures.
"""

import pandas as pd
import pytest

from etdmap.flags import (
    MappingFlag,
    contiguous_runs,
    flag_column,
    frozen_span_flags,
    set_flags,
)
from etdmap.testing import (
    PAIRS_EMPTY,
    cadence_sidecar,
    household_batch_frame,
    household_batch_of,
    household_batches_frame,
)

#: The grid these frames are built on and declared to sit on. Passed
#: explicitly: there is no default grid.
GRID = 5

#: A plausible 5-minute bound for a tap water variable, in litres.
MAX_PER_INTERVAL = 100.0

#: The household batch these frames describe. It is the key every sidecar
#: row is recorded under, and the household it belongs to is derived from it.
HOUSEHOLD_BATCH_ID = household_batch_of(1)


def _measured_cadence(df):
    """The cadence table a mapper measures before the pipeline runs."""
    from etdmap.mapping_helpers import detect_column_cadence
    return detect_column_cadence(df, grid_minutes=GRID)


def _intervals(values):
    """The per-interval amounts frozen_span_flags reads, which it takes alone:
    no frame, so the values are the whole input."""
    return pd.Series(pd.array(values, dtype="Float64"))


def _flagged(values, upper_bound=MAX_PER_INTERVAL):
    flags = frozen_span_flags(_intervals(values), upper_bound=upper_bound)
    return {
        "frozen": [(flags & MappingFlag.FROZEN_SPAN.value != 0).tolist()],
        "catchup": (flags & MappingFlag.CATCHUP.value != 0).tolist(),
        "any": bool((flags != MappingFlag.NONE.value).any()),
    }


class TestContiguousRuns:
    def test_one_run(self):
        mask = pd.Series([False, True, True, True, False])
        assert contiguous_runs(mask) == [(1, 4)]

    def test_several_runs(self):
        mask = pd.Series([True, True, False, True, False, False, True])
        assert contiguous_runs(mask) == [(0, 2), (3, 4), (6, 7)]

    def test_run_touching_both_ends(self):
        mask = pd.Series([True, True, True])
        assert contiguous_runs(mask) == [(0, 3)]

    def test_no_runs(self):
        assert contiguous_runs(pd.Series([False, False])) == []

    def test_empty(self):
        assert contiguous_runs(pd.Series([], dtype="boolean")) == []

    def test_na_in_the_mask_is_not_a_run(self):
        # A mask carrying pd.NA would otherwise silently read as True.
        mask = pd.Series(pd.array([True, pd.NA, True], dtype="boolean"))
        assert contiguous_runs(mask) == [(0, 1), (2, 3)]


class TestFiresOnARealFrozenSpan:
    def test_a_flat_run_ending_in_an_impossible_jump(self):
        # Eight intervals of nothing, then 400 litres in one five-minute slot.
        values = [2.0, 2.0] + [0.0] * 8 + [400.0] + [2.0, 2.0]
        out = _flagged(values)
        assert out["any"]
        assert out["catchup"] == [False] * 10 + [True] + [False, False]
        frozen = out["frozen"][0]
        assert frozen[2:10] == [True] * 8, "the flat stretch is the span"
        assert frozen[10] is False, "the jump itself is the catch-up, not frozen"
        assert frozen[:2] == [False, False]

    def test_the_span_reaches_back_over_the_whole_flat_run(self):
        values = [1.0] + [0.0] * 50 + [900.0]
        frozen = _flagged(values)["frozen"][0]
        assert sum(frozen) == 50

    def test_two_separate_spans_both_fire(self):
        values = [1.0] + [0.0] * 5 + [300.0] + [1.0] + [0.0] * 5 + [300.0]
        out = _flagged(values)
        assert sum(out["catchup"]) == 2
        assert sum(out["frozen"][0]) == 10


class TestTheSeededFirstIntervalIsNotAFrozenSpan:
    """``add_diff_columns`` seeds the first interval of a derived column with
    zero, because differencing has no predecessor there. That zero is
    structural, not a measurement of a cumulative variable standing still, so
    it must not start a span.

    Counted as one, any column whose FIRST real interval exceeds the bound
    reads as a one-row frozen span with that interval as its catch-up, and the
    treatment then blanks row 0 of the cumulative variable, which the supplier
    delivered.
    """

    def test_the_seed_alone_is_not_a_span(self):
        # Row 0 seeded, row 1 over the bound: nothing stood still.
        out = _flagged([0.0, MAX_PER_INTERVAL + 1, 1.0, 1.0])
        assert not out["any"]

    def test_a_real_span_starting_after_the_seed_still_fires(self):
        """The seed is excluded; everything after it is not."""
        out = _flagged([0.0, 1.0, 0.0, 0.0, MAX_PER_INTERVAL + 1])
        assert out["frozen"][0] == [False, False, True, True, False]
        assert out["catchup"] == [False, False, False, False, True]

    def test_a_delivered_interval_column_may_report_zero_at_row_zero(self):
        """The other reading of the same cell. On a column holding the
        supplier's OWN interval amounts there is no seed -- row 0 is a reported
        value, and a reported zero is a real non-advance that may begin a
        span."""
        flags = frozen_span_flags(
            _intervals([0.0, 0.0, MAX_PER_INTERVAL + 1]),
            upper_bound=MAX_PER_INTERVAL,
            missing_counts_as_frozen=False,
        )
        frozen = (flags & MappingFlag.FROZEN_SPAN.value != 0).tolist()
        assert frozen == [True, True, False]


class TestDoesNotFireWithoutACatchUp:
    """A device that is not running is not a fault."""

    def test_a_summer_of_zero_space_heating(self):
        # Months of nothing, then normal winter use. No impossible jump.
        values = [3.0] * 5 + [0.0] * 2000 + [3.0] * 5
        assert not _flagged(values)["any"]

    def test_an_overnight_zero_run(self):
        values = [5.0, 5.0] + [0.0] * 96 + [5.0, 5.0]
        assert not _flagged(values)["any"]

    def test_an_absent_device_never_reports(self):
        assert not _flagged([0.0] * 500)["any"]

    def test_a_device_that_is_all_missing(self):
        assert not _flagged([pd.NA] * 500)["any"]

    def test_a_jump_with_no_flat_run_before_it_is_not_a_span(self):
        # A lone spike is a different fault -- out of bounds -- not a frozen span.
        values = [2.0, 3.0, 2.0, 400.0, 2.0]
        out = _flagged(values)
        assert sum(out["frozen"][0]) == 0
        assert sum(out["catchup"]) == 0


class TestMissingReadings:
    def test_a_missing_reading_inside_a_flat_run_does_not_split_it(self):
        # A gap in transmission still counts as not advancing.
        values = [1.0] + [0.0] * 4 + [pd.NA] + [0.0] * 4 + [500.0]
        frozen = _flagged(values)["frozen"][0]
        assert sum(frozen) == 9, "one span of nine, not two of four"
        assert frozen[1:10] == [True] * 9

    def test_a_missing_reading_immediately_before_the_catch_up(self):
        values = [1.0] + [0.0] * 6 + [pd.NA] + [500.0]
        out = _flagged(values)
        assert out["catchup"][-1] is True, "the jump must still register"
        assert sum(out["frozen"][0]) == 7


class TestBounds:
    def test_no_upper_bound_means_no_jump_is_impossible(self):
        # Consistent with bound_flags: a bound that is not set is not checked.
        values = [1.0] + [0.0] * 8 + [400.0]
        assert not _flagged(values, upper_bound=pd.NA)["any"]

    def test_a_value_exactly_on_the_bound_is_not_a_catch_up(self):
        values = [1.0] + [0.0] * 8 + [MAX_PER_INTERVAL]
        assert not _flagged(values)["any"]

    def test_flags_are_nullable_ints_and_never_na(self):
        values = [1.0, pd.NA] + [0.0] * 8 + [400.0]
        flags = frozen_span_flags(_intervals(values), upper_bound=MAX_PER_INTERVAL)
        assert str(flags.dtype) == "Int64"
        assert not flags.isna().any()


class TestNegativeIntervalsAreNotFrozen:
    def test_a_negative_interval_is_a_meter_reset_not_a_frozen_span(self):
        """A decreasing cumulative variable is CUMULATIVE_METER_RESET.

        Treating it as 'quiet' as well would have one fault wearing two names and
        would silently extend a span across a reset.
        """
        values = [1.0] + [0.0] * 3 + [-8.0] + [0.0] * 3 + [500.0]
        frozen = _flagged(values)["frozen"][0]
        assert frozen[4] is False, "the reset itself is not part of the span"
        assert sum(frozen) == 3, "only the flat run after the reset"


class TestScaleBoundToCadence:
    def test_a_five_minute_bound_scales_with_the_reporting_interval(self):
        from etdmap.flags import scale_bound_to_cadence
        assert scale_bound_to_cadence(100.0, "5-minute", 60) == 1200.0
        assert scale_bound_to_cadence(100.0, "5-minute", 15) == 300.0
        assert scale_bound_to_cadence(100.0, "5-minute", 5) == 100.0

    def test_an_instantaneous_bound_does_not_scale(self):
        from etdmap.flags import scale_bound_to_cadence
        # Power does not accumulate: reporting less often does not make a
        # larger instantaneous reading plausible.
        assert scale_bound_to_cadence(100.0, "momentaan", 60) == 100.0

    def test_a_yearly_bound_does_not_scale(self):
        from etdmap.flags import scale_bound_to_cadence
        assert scale_bound_to_cadence(100.0, "cumulatief", 60) == 100.0

    def test_an_unset_bound_stays_unset(self):
        from etdmap.flags import scale_bound_to_cadence
        assert pd.isna(scale_bound_to_cadence(pd.NA, "5-minute", 60))

    def test_an_unknown_cadence_leaves_the_bound_alone(self):
        from etdmap.flags import scale_bound_to_cadence
        assert scale_bound_to_cadence(100.0, "5-minute", pd.NA) == 100.0


class TestIntervalColumnsInheritTheirCumulativeCadence:
    """A Diff column is derived in the pipeline, so it is not in the cadence
    table -- that was measured on the columns as delivered. Without inheritance
    every interval bound would go unscaled."""

    def _frame(self):
        # 300 sits between the two bounds this class contrasts: over the
        # five-minute bound of 100, under the shaped hourly bound of 100 * 12
        # ** 0.5 = 346.4. A value outside both would make the test pass for the
        # wrong reason.
        #
        # PAIRS_EMPTY: the cumulative counterpart was not delivered, which is
        # why the cadence table carries a row for it and none for this column,
        # so it stands as mapping leaves it -- present and empty.
        n = 200
        return household_batch_frame(
            n_rows=n,
            start="2024-01-01",
            freq=f"{GRID}min",
            tz=None,
            project=None,
            pairs=PAIRS_EMPTY,
            WatergebruikWarmTapwaterDiff=(
                [0.5] * 20 + [0.0] * 60 + [300.0] + [0.5] * (n - 81)
            ),
        )

    def _spans(self, cadence_minutes):
        from etdmap.mapping_helpers import _detect_frozen_spans_for_household_batch
        return _detect_frozen_spans_for_household_batch(
            self._frame(),
            HOUSEHOLD_BATCH_ID,
            cadence=cadence_sidecar(
                {"WatergebruikWarmTapwater": cadence_minutes},
                grid_minutes=GRID,
            ),
        )

    def test_a_five_minute_cadence_makes_the_jump_implausible(self):
        rows = self._spans(5)
        assert len(rows) == 1
        assert rows[0]["upper_bound"] == 100.0
        assert rows[0]["n_intervals"] == 60

    def test_an_hourly_cadence_makes_the_same_jump_plausible(self):
        assert self._spans(60) == []


class TestTreatment:
    """Blanking a span turns it into an ordinary gap whose end-to-start jump is
    the consumption it was holding, which the existing redistribution machinery
    then spreads. No new spreading code."""

    def _frame(self, catchup=600.0, n=40, freeze=(10, 29), normal=3.0):
        first, last = freeze
        values, current = [], 1000.0 - first * normal
        for i in range(n):
            if i < first:
                current += normal
            elif i <= last:
                pass
            elif i == last + 1:
                current += catchup
            else:
                current += normal
            values.append(current)
        # Through add_diff_columns rather than a hand-rolled .diff(), so the
        # interval carries VALUE_DERIVED_FROM_CUMULATIVE as production
        # sets it. Detection and treatment read that bit to decide which form
        # the column holds, and a frame without it is read as the supplier's
        # own interval amounts -- a different and deliberately different
        # treatment.
        from etdmap.mapping_helpers import add_diff_columns
        return add_diff_columns(
            household_batch_frame(
                n_rows=n,
                start="2026-01-01",
                freq=f"{GRID}min",
                tz=None,
                project=None,
                WatergebruikWarmTapwater=values,
            ),
            context="frozen-span fixture",
        )

    def _treated(self, df):
        from etdmap.mapping_helpers import (
            _detect_frozen_spans_for_household_batch,
            _treat_frozen_spans,
        )
        rows = _detect_frozen_spans_for_household_batch(
            df, HOUSEHOLD_BATCH_ID, cadence=_measured_cadence(df)
        )
        _treat_frozen_spans(df, rows)
        return rows

    def test_the_cumulative_variable_reads_missing_across_the_span(self):
        df = self._frame()
        rows = self._treated(df)
        assert len(rows) == 1
        assert int(df["WatergebruikWarmTapwater"].isna().sum()) == 20

    def test_the_reading_before_the_span_is_kept_as_the_anchor(self):
        # Blanking it too leaves the gap without a starting cumulative value and
        # the redistributed volume no longer equals what was consumed.
        df = self._frame()
        self._treated(df)
        assert pd.notna(df["WatergebruikWarmTapwater"].iloc[9])

    def test_the_catch_up_is_blanked_with_the_span(self):
        # Every interval across the span is wrong: zeros that understate, then
        # one reading that overstates. Imputation rebuilds them from the cumulative variable.
        df = self._frame()
        self._treated(df)
        diff = df["WatergebruikWarmTapwaterDiff"]
        assert diff.iloc[10:31].isna().all()
        assert pd.notna(diff.iloc[9]) and pd.notna(diff.iloc[31])

    def test_nothing_is_treated_when_nothing_is_detected(self):
        # A flat run with no catch-up after it is an idle device, not a fault.
        df = self._frame(catchup=3.0)
        rows = self._treated(df)
        assert rows == []
        assert int(df["WatergebruikWarmTapwater"].isna().sum()) == 0


class TestOnlyAccumulatingVariables:
    """A frozen span is a cumulative variable holding consumption back. A momentaan reading
    is a sample at an instant: a flat run of voltage is the voltage being steady,
    and the jump after it is the voltage changing. There is nothing accumulated
    to redistribute, and treating it would blank real samples and invent values
    between them.

    32 of the 67 columns carrying a Max are momentaan, so this is not a corner.
    """

    def _frame(self, column):
        # The shape that fires for a cumulative variable: flat, then an implausible jump.
        n = 30
        df = household_batch_frame(
            n_rows=n,
            start="2026-01-01",
            freq=f"{GRID}min",
            tz=None,
            project=None,
            **{column: [230.0] * 8 + [230.0] * 10 + [9999.0] + [230.0] * (n - 19)},
        )
        # Differenced here rather than through add_diff_columns, which stamps
        # VALUE_DERIVED_FROM_CUMULATIVE: without that finding the interval
        # column is read as the supplier's own amounts, which is the form this
        # class judges.
        df[column + "Diff"] = df[column].diff().round(10)
        df.loc[0, column + "Diff"] = 0
        return df

    def test_a_momentaan_column_is_never_treated_as_a_frozen_span(self):
        from etdmap.mapping_helpers import _detect_frozen_spans_for_household_batch
        df = self._frame("SlimmemeterVoltageL1")
        assert _detect_frozen_spans_for_household_batch(
            df, HOUSEHOLD_BATCH_ID, cadence=_measured_cadence(df)
        ) == []

    def test_a_cumulative_column_with_the_same_shape_is(self):
        from etdmap.mapping_helpers import _detect_frozen_spans_for_household_batch
        df = self._frame("WatergebruikWarmTapwater")
        assert len(_detect_frozen_spans_for_household_batch(
            df, HOUSEHOLD_BATCH_ID, cadence=_measured_cadence(df)
        )) == 1

    def test_an_interval_column_follows_its_cumulative_variable(self):
        from etdmap.mapping_helpers import _accumulates
        assert _accumulates("WatergebruikWarmTapwaterDiff")
        assert not _accumulates("ElektriciteitVermogen")


class TestARegisterIsNotScanned:
    """frozen_spans reads its argument as per-interval amounts. On a cumulative
    variable -- a running total -- a run of zeros means absent rather than idle,
    and a value over the bound is a reading outside its declared range, which
    the threshold pass already reports.

    Scanned anyway, cumulative_column of a cumulative variable is None, so
    _treat_frozen_spans takes its interval branch on it and blanks that
    reading, stamping FROZEN_SPAN and CATCHUP for a span that never happened.

    13 of the 30 columns carrying a Max and accumulating are cumulative, so
    this is not a corner either.
    """

    # An absence in the cumulative variable, then a reading past its declared
    # Max. The shape that reaches the scan: fillna(0) == 0 counts NA as "did
    # not advance". The counterpart is completed as a usable total, which is
    # what an interval column derived from it is derived from.
    def _register_frame(self, column, over_bound):
        n = 12
        return household_batch_frame(
            n_rows=n,
            start="2026-01-01",
            freq=f"{GRID}min",
            tz=None,
            project=None,
            **{column: [100.0, 200.0, 300.0] + [pd.NA] * 5 + [over_bound] * 4},
        )

    def test_a_register_reports_no_span(self):
        from etdmap.mapping_helpers import _detect_frozen_spans_for_household_batch
        df = self._register_frame("ElektriciteitNetgebruik", 109967.957)
        assert _detect_frozen_spans_for_household_batch(
            df, HOUSEHOLD_BATCH_ID, cadence=_measured_cadence(df)
        ) == []

    def test_the_same_shape_on_its_interval_column_still_does(self):
        """An interval column DERIVED from a cumulative variable carries
        per-interval amounts and is read, and there an absence counts as not
        advancing -- so this frame's NA run is a span.

        The derivation is asserted by the flag, because that is what detection
        reads. Without it the same frame is the supplier's own amounts, where
        pd.NA is unknown rather than idle and no span is reported.
        """
        from etdmap.mapping_helpers import _detect_frozen_spans_for_household_batch
        col = "ElektriciteitNetgebruikDiff"
        df = self._register_frame(col, 9.0)
        set_flags(
            df, col, df[col].notna(),
            MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE,
        )
        rows = _detect_frozen_spans_for_household_batch(
            df, HOUSEHOLD_BATCH_ID, cadence=_measured_cadence(df)
        )
        assert len(rows) == 1
        assert rows[0]["column"] == col
        assert rows[0]["span_kind"] == "derived_interval"

    def test_a_registers_over_bound_reading_survives_treatment(self):
        """The reading is out of bounds and the threshold pass is what removes
        it; span treatment must not, because blanking it here also claims the
        span that justified it."""
        from etdmap.mapping_helpers import (
            _detect_frozen_spans_for_household_batch,
            _treat_frozen_spans,
        )
        col = "ElektriciteitNetgebruik"
        df = self._register_frame(col, 109967.957)
        rows = _detect_frozen_spans_for_household_batch(
            df, HOUSEHOLD_BATCH_ID, cadence=_measured_cadence(df)
        )
        _treat_frozen_spans(df, rows)
        assert float(df[col].iloc[8]) == pytest.approx(109967.957)
        flags = df.get(flag_column(col))
        if flags is not None:
            carried = MappingFlag(int(flags.iloc[8]))
            assert not (carried & MappingFlag.FROZEN_SPAN)
            assert not (carried & MappingFlag.CATCHUP)


class TestFlagsSurviveTheColumnFilter:
    """`rearrange_model_columns` keeps only data model columns, and a findings
    column is not one. Dropping it destroys every flag the mapper set: the
    fills run before the pipeline, so that record of which values were invented
    is the only thing distinguishing them from measurements.

    The column's presence is not evidence on its own -- span treatment
    recreates it carrying only its own bits -- so the bits are asserted too.
    """

    def _filled_frame(self, n=40):
        from etdmap.mapping_helpers import fill_down_infrequent_devices
        col = "ElektriciteitsgebruikBoilervat"
        df = household_batch_frame(
            n_rows=n,
            start="2026-01-01",
            freq=f"{GRID}min",
            tz=None,
            project=None,
            **{col: [float(i) if i % 12 == 0 else pd.NA for i in range(n)]},
        )
        return col, fill_down_infrequent_devices(df, columns=[col], grid_minutes=GRID)

    def test_a_findings_column_survives_with_the_column_it_describes(self):
        from etdmap.flags import flag_column
        from etdmap.mapping_helpers import rearrange_model_columns
        col, df = self._filled_frame()
        before = int((df[flag_column(col)] != MappingFlag.NONE.value).sum())
        assert before > 0

        out = rearrange_model_columns(df, add_columns=False)
        assert flag_column(col) in out.columns
        assert int((out[flag_column(col)] != MappingFlag.NONE.value).sum()) == before

    def test_the_bits_survive_not_just_the_column(self):
        from etdmap.flags import flag_column
        from etdmap.mapping_helpers import rearrange_model_columns
        col, df = self._filled_frame()
        out = rearrange_model_columns(df, add_columns=False)
        filled_down = (out[flag_column(col)] & MappingFlag.FILLED_DOWN.value) != 0
        assert int(filled_down.sum()) == 36

    def test_a_findings_column_for_an_unmapped_column_is_still_dropped(self):
        from etdmap.mapping_helpers import rearrange_model_columns
        # Built here: the subject is a column the data model does not have,
        # which no builder produces, carrying an Int64 findings column.
        df = pd.DataFrame({
            "ReadingDate": pd.date_range("2026-01-01", periods=3, freq="5min"),
            "nan [nan]": pd.array([1.0, 2.0, 3.0], dtype="Float64"),
            "nan [nan]_flags": pd.array([1, 1, 1], dtype="Int64"),
        })
        out = rearrange_model_columns(df, add_columns=False)
        assert "nan [nan]" not in out.columns
        assert "nan [nan]_flags" not in out.columns


class TestNullingIsBoundedToTheSpan:
    """Treating a span blanks the interval column across it. The values that
    belong to the span are the zeros the frozen meter reported and the catch-up
    they arrive in -- anything else in that range is a delivered reading the
    span has no claim on."""

    CUM = "ElektriciteitNetgebruik"
    DIFF = "ElektriciteitNetgebruikDiff"

    def _frame_with_span(self, span_values):
        # Both halves of the pair are stated, so nothing is completed: the
        # cumulative column is what the derived treatment hands the span's
        # energy back to, and the interval column is what it blanks.
        n = len(span_values)
        return household_batches_frame(
            {HOUSEHOLD_BATCH_ID: {
                self.CUM: [100.0 + i for i in range(n)],
                self.DIFF: list(span_values),
            }},
            start="2026-01-01",
            freq=f"{GRID}min",
            tz=None,
        )

    def _treat(self, df, start, catchup):
        from etdmap.mapping_helpers import _treat_frozen_spans

        _treat_frozen_spans(
            df,
            [{"column": self.DIFF, "start": start, "catchup_position": catchup}],
        )
        return df

    def test_the_zeros_of_the_span_are_blanked(self):
        df = self._frame_with_span([1.0, 0.0, 0.0, 0.0, 9.0, 1.0])
        self._treat(df, start=1, catchup=4)
        assert df[self.DIFF].iloc[1:4].isna().all()

    def test_the_catch_up_is_blanked(self):
        df = self._frame_with_span([1.0, 0.0, 0.0, 0.0, 9.0, 1.0])
        self._treat(df, start=1, catchup=4)
        assert pd.isna(df[self.DIFF].iloc[4])

    def test_readings_outside_the_span_survive(self):
        df = self._frame_with_span([1.0, 0.0, 0.0, 0.0, 9.0, 1.0])
        self._treat(df, start=1, catchup=4)
        assert float(df[self.DIFF].iloc[0]) == pytest.approx(1.0)
        assert float(df[self.DIFF].iloc[5]) == pytest.approx(1.0)

    def test_a_non_zero_reading_inside_the_span_survives(self):
        """The detector found a flat run, so a value that is neither zero nor
        the catch-up is a delivered reading -- blanking it would destroy data
        the span does not account for."""
        df = self._frame_with_span([1.0, 0.0, 0.7, 0.0, 9.0, 1.0])
        self._treat(df, start=1, catchup=4)
        assert float(df[self.DIFF].iloc[2]) == pytest.approx(0.7)

    def test_the_blanking_is_flagged(self):
        df = self._frame_with_span([1.0, 0.0, 0.0, 0.0, 9.0, 1.0])
        self._treat(df, start=1, catchup=4)
        flags = df[flag_column(self.DIFF)].fillna(0).astype("Int64")
        assert (flags.iloc[1:5] & MappingFlag.NULLED.value).all()
