"""Frozen-span detection: a register that stops advancing and then delivers its
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
    frozen_span_flags,
)

# A plausible 5-minute bound for a tap water register, in litres.
MAX_PER_INTERVAL = 100.0


def _intervals(values):
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
        # A gap in transmission is still "the register did not advance".
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
    def test_a_negative_interval_is_a_meter_reset_not_a_frozen_register(self):
        """group D classifies a decreasing register as CUMULATIVE_METER_RESET.

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


class TestIntervalColumnsInheritTheirRegistersCadence:
    """A Diff column is derived in the pipeline, so it is not in the cadence
    table -- that was measured on the columns as delivered. Without inheritance
    every interval bound would go unscaled."""

    def _frame(self):
        n = 200
        df = pd.DataFrame({"ReadingDate": pd.date_range("2024-01-01", periods=n, freq="5min")})
        df["WatergebruikWarmTapwaterDiff"] = pd.array(
            [0.5] * 20 + [0.0] * 60 + [900.0] + [0.5] * (n - 81), dtype="Float64"
        )
        return df

    def _spans(self, cadence_minutes):
        from etdmap.mapping_helpers import _detect_frozen_spans_for_household
        cadence = pd.DataFrame({
            "column": ["WatergebruikWarmTapwater"],
            "cadence_minutes": pd.array([cadence_minutes], dtype="Int64"),
        })
        return _detect_frozen_spans_for_household(self._frame(), 1, cadence=cadence)

    def test_a_five_minute_register_makes_the_jump_impossible(self):
        rows = self._spans(5)
        assert len(rows) == 1
        assert rows[0]["upper_bound"] == 100.0
        assert rows[0]["n_intervals"] == 60

    def test_an_hourly_register_makes_the_same_jump_plausible(self):
        assert self._spans(60) == []
