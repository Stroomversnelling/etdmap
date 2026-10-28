"""The negative-diff correction of cumulative columns.

``add_diff_columns`` is pipeline step 7. What it does to a decreasing register
is asserted here as expected values per input shape, with the invariants that
hold across every shape and the flags each correction records.

The shapes cover: clean monotonic series, isolated zero dips, runs of zeros,
meter resets, non-zero negative dips, frozen (constant) runs, all-NA columns,
leading NA, and single-row frames.

**The correction removes, it does not rewrite.** Every surviving reading is the
one the supplier delivered; an offending reading is blanked, and the interval
across a blank is blank. The corrected register is the delivered register with
holes, which is what makes the flags beside it the record of what happened.
"""

import logging
import random

import pandas as pd
import pytest

from etdmap import ids
from etdmap.data_model import cumulative_columns
from etdmap.flags import MappingFlag, flag_column
from etdmap.mapping_helpers import (
    _detect_negative_diff_cases,
    add_diff_columns,
    validate_cumulative_variables,
)

COL = cumulative_columns[0]

# The four mutually exclusive classifications a decreasing reading can get.
CASE_FLAGS = (
    MappingFlag.CUMULATIVE_JUMP_BACK,
    MappingFlag.CUMULATIVE_REPEAT_NEGATIVE,
    MappingFlag.CUMULATIVE_METER_RESET,
    MappingFlag.CUMULATIVE_NO_RECOVERY,
)
DIFF = COL + "Diff"
COMPARED = ["ReadingDate", COL, DIFF]


@pytest.fixture(autouse=True)
def _quiet_logging():
    """The correction routines log per anomaly; these tests assert on data."""
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


def _frame(values, dates=None):
    """One-cumulative-column frame on a regular 5 minute grid."""
    if dates is None:
        dates = pd.date_range("2024-01-01", periods=len(values), freq="5min")
    return pd.DataFrame(
        {
            "ReadingDate": dates,
            COL: pd.array(values, dtype="Float64"),
        }
    )


def _corrected(df, **kwargs):
    """The frame step 7 produces, with the correction applied."""
    return add_diff_columns(df.copy(), context="correction", **kwargs)


def _values(series):
    """A list with pd.NA rendered as None, so an expectation reads plainly."""
    return [None if pd.isna(v) else float(v) for v in series]


def _assert_only_removes(before, after, label):
    """Every surviving reading is the delivered one, and nothing was invented."""
    for i, (was, now) in enumerate(zip(before, _values(after[COL]))):
        was = None if was is pd.NA or pd.isna(was) else float(was)
        if now is not None:
            assert was == now, (
                f"{label}: row {i} holds {now} but {was} was delivered -- "
                f"the correction rewrote a reading instead of removing it"
            )


def _assert_interval_follows_the_register(after, label):
    """The Diff is the step in the corrected register.

    The no-negative-interval half is **conditional on the variable being
    declared monotonic**, which is a metadata fact and not a property of
    cumulative columns in general: ``add_diff_columns`` skips the negative-diff
    correction entirely for a cumulative variable that is not in
    ``monotonic_columns``, because there a decrease is simply the reading.
    ``test_the_column_under_test_is_declared_monotonic`` asserts the premise.
    """
    cum = _values(after[COL])
    diff = _values(after[DIFF])
    assert diff[0] == 0.0 or diff[0] is None, (
        f"{label}: row 0 of a differenced column has no predecessor, so it is "
        f"seeded, not computed"
    )
    for i in range(1, len(cum)):
        if cum[i - 1] is not None and cum[i] is not None and diff[i] is not None:
            assert diff[i] == pytest.approx(cum[i] - cum[i - 1]), (
                f"{label}: Diff at row {i} is not the register's step"
            )
    for i, value in enumerate(diff):
        if value is not None:
            assert value >= 0, (
                f"{label}: a negative interval survived at row {i}, on a "
                f"variable declared monotonic"
            )


def test_the_column_under_test_is_declared_monotonic():
    """The premise of every no-negative-interval assertion in this file.

    Monotonicity is declared per variable in the data model, and the correction
    is applied only where it is declared. Every cumulative column in the model
    declares it, so the branch that leaves a decrease alone has no column to
    exercise it.
    """
    from etdmap.data_model import monotonic_columns

    assert COL in set(monotonic_columns)


# ---------------------------------------------------------------------------
# Named shapes -- one scenario per case the correction routine classifies
# ---------------------------------------------------------------------------

NAMED_CASES = {
    # No correction path at all.
    "clean_monotonic": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0],
    "clean_with_flat_runs": [10.0, 10.0, 10.0, 11.0, 11.0, 12.0],
    "single_row": [7.0],
    "two_rows": [7.0, 8.0],
    "all_na": [pd.NA, pd.NA, pd.NA, pd.NA],
    "leading_na": [pd.NA, pd.NA, 3.0, 4.0, 5.0],
    "trailing_na": [3.0, 4.0, 5.0, pd.NA, pd.NA],
    # Case 1 -- meter dips then jumps back to (or above) the pre-dip value.
    "isolated_zero_dip": [10.0, 11.0, 0.0, 12.0, 13.0],
    "zero_run": [10.0, 11.0, 0.0, 0.0, 0.0, 12.0, 13.0],
    "zero_run_at_end_recovering": [10.0, 11.0, 0.0, 0.0, 14.0],
    "non_zero_negative_dip": [10.0, 11.0, 9.0, 12.0, 13.0],
    "two_separate_dips": [10.0, 11.0, 0.0, 12.0, 13.0, 0.0, 15.0],
    "dip_recovers_exactly": [10.0, 11.0, 9.0, 11.0, 12.0],
    # Case 2 -- two negative diffs in a row.
    "consecutive_negatives": [10.0, 11.0, 9.0, 7.0, 12.0, 13.0],
    "three_consecutive_negatives": [20.0, 19.0, 18.0, 17.0, 25.0],
    # Case 3 -- meter reset: recovery never reaches the pre-dip level.
    "meter_reset_to_zero": [10.0, 11.0, 12.0, 0.0, 1.0, 2.0, 3.0],
    "meter_reset_to_nonzero": [100.0, 101.0, 102.0, 5.0, 6.0, 7.0],
    "meter_reset_with_na_before_dip": [100.0, 101.0, pd.NA, 5.0, 6.0, 7.0],
    # Case "no next" -- negative dip with no subsequent increase.
    "dip_then_frozen": [10.0, 11.0, 12.0, 5.0, 5.0, 5.0],
    "dip_at_last_row": [10.0, 11.0, 12.0, 5.0],
    "zero_run_to_end": [10.0, 11.0, 12.0, 0.0, 0.0, 0.0],
    # Mixed
    "reset_then_dip": [50.0, 51.0, 10.0, 11.0, 0.0, 13.0],
    "na_between_dips": [50.0, 51.0, 0.0, pd.NA, 52.0, 10.0, 11.0],
    "dip_immediately_after_leading_na": [pd.NA, 50.0, 0.0, 51.0, 52.0],
}


#: What the correction produces for each named shape: the corrected register
#: and the interval column, with None for pd.NA. An offending reading is
#: blanked, and the interval across a blank is blank.
EXPECTED = {
    "clean_monotonic": (
        [10.0, 11.0, 12.0, 13.0, 14.0, 15.0],
        [0.0, 1.0, 1.0, 1.0, 1.0, 1.0],
    ),
    "clean_with_flat_runs": (
        [10.0, 10.0, 10.0, 11.0, 11.0, 12.0],
        [0.0, 0.0, 0.0, 1.0, 0.0, 1.0],
    ),
    "single_row": ([7.0], [0.0]),
    "two_rows": ([7.0, 8.0], [0.0, 1.0]),
    "all_na": ([None] * 4, [0.0, None, None, None]),
    "leading_na": (
        [None, None, 3.0, 4.0, 5.0], [0.0, None, None, 1.0, 1.0],
    ),
    "trailing_na": (
        [3.0, 4.0, 5.0, None, None], [0.0, 1.0, 1.0, None, None],
    ),
    "isolated_zero_dip": (
        [10.0, 11.0, None, 12.0, 13.0], [0.0, 1.0, None, None, 1.0],
    ),
    "zero_run": (
        [10.0, 11.0, None, None, None, 12.0, 13.0],
        [0.0, 1.0, None, None, None, None, 1.0],
    ),
    "zero_run_at_end_recovering": (
        [10.0, 11.0, None, None, 14.0], [0.0, 1.0, None, None, None],
    ),
    "non_zero_negative_dip": (
        [10.0, 11.0, None, 12.0, 13.0], [0.0, 1.0, None, None, 1.0],
    ),
    "two_separate_dips": (
        [10.0, 11.0, None, 12.0, 13.0, None, 15.0],
        [0.0, 1.0, None, None, 1.0, None, None],
    ),
    "dip_recovers_exactly": (
        [10.0, 11.0, None, 11.0, 12.0], [0.0, 1.0, None, None, 1.0],
    ),
    "consecutive_negatives": (
        [10.0, 11.0, None, None, 12.0, 13.0],
        [0.0, 1.0, None, None, None, 1.0],
    ),
    "three_consecutive_negatives": (
        [20.0, None, None, None, 25.0], [0.0, None, None, None, None],
    ),
    "meter_reset_to_zero": (
        [10.0, 11.0, 12.0, None, 1.0, 2.0, 3.0],
        [0.0, 1.0, 1.0, None, None, 1.0, 1.0],
    ),
    "meter_reset_to_nonzero": (
        [100.0, 101.0, 102.0, None, 6.0, 7.0],
        [0.0, 1.0, 1.0, None, None, 1.0],
    ),
    "meter_reset_with_na_before_dip": (
        [100.0, 101.0, None, 5.0, 6.0, 7.0],
        [0.0, 1.0, None, None, 1.0, 1.0],
    ),
    "dip_then_frozen": (
        [10.0, 11.0, 12.0, None, None, None],
        [0.0, 1.0, 1.0, None, None, None],
    ),
    "dip_at_last_row": (
        [10.0, 11.0, 12.0, None], [0.0, 1.0, 1.0, None],
    ),
    "zero_run_to_end": (
        [10.0, 11.0, 12.0, None, None, None],
        [0.0, 1.0, 1.0, None, None, None],
    ),
    "reset_then_dip": (
        [50.0, 51.0, None, 11.0, None, 13.0],
        [0.0, 1.0, None, None, None, None],
    ),
    "na_between_dips": (
        [50.0, 51.0, None, None, 52.0, None, 11.0],
        [0.0, 1.0, None, None, None, None, None],
    ),
    "dip_immediately_after_leading_na": (
        [None, 50.0, None, 51.0, 52.0], [0.0, None, None, None, 1.0],
    ),
}


def test_every_named_shape_has_an_expectation():
    """So a shape added to the corpus cannot quietly go unasserted."""
    assert sorted(EXPECTED) == sorted(NAMED_CASES)


@pytest.mark.parametrize("name", sorted(NAMED_CASES))
def test_the_named_shape_produces_the_expected_register_and_interval(name):
    out = _corrected(_frame(NAMED_CASES[name]))
    expected_cum, expected_diff = EXPECTED[name]
    assert _values(out[COL]) == expected_cum, f"{name}: corrected register"
    assert _values(out[DIFF]) == expected_diff, f"{name}: interval column"


@pytest.mark.parametrize("name", sorted(NAMED_CASES))
def test_the_named_shape_only_loses_readings(name):
    values = NAMED_CASES[name]
    out = _corrected(_frame(values))
    _assert_only_removes(values, out, name)
    _assert_interval_follows_the_register(out, name)


class TestAMeterResetIsNotMadeMonotonic:
    """A corrected register is not non-decreasing. Where the meter restarted,
    the readings after the reset are lower and are kept as delivered; only the
    transition is blanked."""

    def test_the_readings_after_a_reset_are_kept_as_delivered(self):
        out = _corrected(_frame(NAMED_CASES["meter_reset_to_zero"]))
        assert _values(out[COL])[4:] == [1.0, 2.0, 3.0]

    def test_the_register_decreases_across_the_reset(self):
        out = _corrected(_frame(NAMED_CASES["meter_reset_to_zero"]))
        present = [v for v in _values(out[COL]) if v is not None]
        assert any(b < a for a, b in zip(present, present[1:]))

    def test_and_the_interval_across_it_is_blank_rather_than_negative(self):
        out = _corrected(_frame(NAMED_CASES["meter_reset_to_zero"]))
        assert _values(out[DIFF])[3:5] == [None, None]


# ---------------------------------------------------------------------------
# Generated shapes
# ---------------------------------------------------------------------------


def _inject(rng, values):
    """Apply one randomly chosen anomaly in place."""
    n = len(values)
    i = rng.randrange(n)
    kind = rng.choice(
        [
            "zero_dip",
            "zero_run",
            "reset",
            "small_negative",
            "na_run",
            "freeze",
        ]
    )
    if kind == "zero_dip":
        values[i] = 0.0
    elif kind == "zero_run":
        for j in range(i, min(i + rng.randint(2, 5), n)):
            values[j] = 0.0
    elif kind == "reset":
        offset = values[i]
        if offset is pd.NA:
            return
        for j in range(i, n):
            if values[j] is not pd.NA:
                values[j] = round(values[j] - offset, 6)
    elif kind == "small_negative":
        if values[i] is pd.NA:
            return
        values[i] = round(max(0.0, values[i] - rng.uniform(0.1, 5.0)), 6)
    elif kind == "na_run":
        for j in range(i, min(i + rng.randint(1, 4), n)):
            values[j] = pd.NA
    elif kind == "freeze":
        held = values[i]
        for j in range(i, min(i + rng.randint(2, 6), n)):
            values[j] = held
    return


def _generate(rng, n):
    """A cumulative reading series with zero to three injected anomalies."""
    current = rng.uniform(0.0, 500.0)
    values = []
    for _ in range(n):
        current += rng.choice([0.0, 0.0, 0.0, 0.001, 0.05, 0.4, 2.5])
        values.append(round(current, 6))
    for _ in range(rng.randint(0, 3)):
        _inject(rng, values)
    return values


@pytest.mark.parametrize("seed", range(120))
def test_a_generated_shape_only_loses_readings(seed):
    """The invariants over 120 generated series: the correction removes rather
    than rewrites, and the interval it leaves is the step in the register it
    left."""
    rng = random.Random(seed)
    values = _generate(rng, rng.randint(1, 30))
    label = f"seed={seed} values={values}"
    out = _corrected(_frame(values))
    _assert_only_removes(values, out, label)
    _assert_interval_follows_the_register(out, label)


# ---------------------------------------------------------------------------
# Grouped input -- the id_column path must agree too
# ---------------------------------------------------------------------------


def _corpus():
    """Every frame this file asserts against, named."""
    for name in sorted(NAMED_CASES):
        yield name, _frame(NAMED_CASES[name])
    for seed in range(120):
        rng = random.Random(seed)
        yield f"seed={seed}", _frame(_generate(rng, rng.randint(1, 30)))


class TestDetector:
    """``_detect_negative_diff_cases`` is what makes unconditional detection
    safe: it must read the column and write nothing."""

    def test_detection_does_not_touch_the_frame(self):
        for name, df in _corpus():
            before = df.copy(deep=True)
            _detect_negative_diff_cases(df, COL)
            pd.testing.assert_frame_equal(df, before, obj=f"{name}: frame mutated")

    def test_clean_column_yields_no_cases(self):
        df = _frame(NAMED_CASES["clean_monotonic"])
        assert _detect_negative_diff_cases(df, COL) is None

    def test_cases_are_mutually_exclusive_and_cover_every_negative_diff(self):
        for name, df in _corpus():
            cases = _detect_negative_diff_cases(df, COL)
            if cases is None:
                continue
            set_bits = sum(
                ((cases.flags & flag.value) != 0).astype("Int64") for flag in CASE_FLAGS
            )
            assert (set_bits <= 1).all(), f"{name}: cases overlap"
            # The first reading has no predecessor, so it is not a decrease.
            is_negative = (cases.readings[COL + "Diff_no_gap"] < 0).fillna(False)
            # Masked to the four cases: CUMULATIVE_ZERO_TOTAL is independent of
            # them and can sit on a reading that does not decrease.
            assert (set_bits == 1).equals(is_negative), (
                f"{name}: cases do not cover all negative diffs"
            )

    def test_corpus_exercises_all_four_cases(self):
        """Guards the corpus against becoming vacuous: if the generator stops
        producing one of the shapes, the classifications it covers are hollow."""
        totals = {flag.name: 0 for flag in CASE_FLAGS}
        for _, df in _corpus():
            cases = _detect_negative_diff_cases(df, COL)
            if cases is None:
                continue
            for flag in CASE_FLAGS:
                totals[flag.name] += int(((cases.flags & flag.value) != 0).sum())
        for name, count in totals.items():
            assert count > 0, f"corpus never produced case '{name}': {totals}"

    def test_zero_total_flags_zeros_from_the_first_decrease_onward(self):
        # Leading zeros are a meter that has not started counting, not a
        # fault. Only the zero after the decrease is flagged.
        df = _frame([0.0, 10.0, 11.0, 0.0, 12.0, 13.0])
        cases = _detect_negative_diff_cases(df, COL)
        zero_total = (cases.flags & MappingFlag.CUMULATIVE_ZERO_TOTAL.value) != 0
        assert zero_total.tolist() == [False, False, False, True, False, False]

    def test_zero_total_is_independent_of_the_four_cases(self):
        # A decrease to a non-zero value: classified, but no zero total.
        df = _frame([100.0, 101.0, 102.0, 5.0, 6.0, 7.0])
        cases = _detect_negative_diff_cases(df, COL)
        assert ((cases.flags & MappingFlag.CUMULATIVE_ZERO_TOTAL.value) != 0).sum() == 0
        assert (cases.flags != MappingFlag.NONE.value).sum() == 1

    def test_leading_zeros_are_not_a_fault(self):
        """A meter that has not started counting is not faulty. What follows
        the zeros may be -- a large first reading is accumulated catch-up --
        but that is a different fault on a different reading."""
        df = _frame([0.0, 0.0, 93.255304, 89.184582, 93.656304])
        cases = _detect_negative_diff_cases(df, COL)
        zero_total = (cases.flags & MappingFlag.CUMULATIVE_ZERO_TOTAL.value) != 0
        assert zero_total.sum() == 0
        assert validate_cumulative_variables(df)['no_unexpected_zero'] is True

    def test_zero_after_a_recorded_total_is_a_fault(self):
        df = _frame([10.0, 11.0, 0.0, 12.0, 13.0])
        cases = _detect_negative_diff_cases(df, COL)
        zero_total = (cases.flags & MappingFlag.CUMULATIVE_ZERO_TOTAL.value) != 0
        assert zero_total.tolist() == [False, False, True, False, False]
        assert validate_cumulative_variables(df)['no_unexpected_zero'] is False

    def test_zero_total_agrees_with_the_validator(self):
        """Detector and validator compute this independently, so they must not
        disagree on any frame in the corpus."""
        for name, df in _corpus():
            cases = _detect_negative_diff_cases(df, COL)
            flagged = (
                cases is not None
                and ((cases.flags & MappingFlag.CUMULATIVE_ZERO_TOTAL.value) != 0).any()
            )
            expected = not validate_cumulative_variables(df)['no_unexpected_zero']
            assert bool(flagged) == expected, (
                f"{name}: detector and validator disagree on the zero total"
            )

    def test_the_shared_scope_is_exactly_its_two_members(self):
        """CUMULATIVE_DECREASE_RANGE exists so the applier states the shared
        deletion scope once instead of re-ORing its members."""
        assert MappingFlag.CUMULATIVE_DECREASE_RANGE == (
            MappingFlag.CUMULATIVE_JUMP_BACK | MappingFlag.CUMULATIVE_REPEAT_NEGATIVE
        )


# ---------------------------------------------------------------------------
# Known divergences on degenerate ReadingDate input
#
# The two implementations agree on every frame whose ReadingDate is a strictly
# increasing series of valid timestamps. They disagree when it is not. Neither
# side is clearly right, so these tests characterise what each one currently
# does rather than asserting that they match. If a change makes them agree,
# these tests fail and should be replaced by an agreement test.
# ---------------------------------------------------------------------------


class TestTheInputContractIsEnforcedAtTheBoundary:
    """One reading per instant, and every ReadingDate present.

    An interval is the step between consecutive readings, and the correction
    locates the reading a dip recovers at by position in the sorted series.
    Both properties need a frame where every row can be placed in time and no
    two rows claim the same instant.

    The pipeline refuses a frame that does not have them, so the correction is
    never handed one. There are no tests here of what the arithmetic does to
    such a frame: asserting an answer for an input the pipeline rejects would
    make that answer the contract.
    """

    def _pipeline(self, dates, values=(10.0, 11.0, 0.0, 12.0, 13.0)):
        from etdmap.mapping_helpers import run_standard_pipeline

        df = _frame(list(values), dates=dates)
        return run_standard_pipeline(
            df, household_id=1, household_batch_id=1001, household_id_supplier="H1",
            mapped_folder_path=".", grid_minutes=5,
        )

    def test_an_unparseable_timestamp_is_refused(self):
        dates = pd.array(
            ["2024-01-01 00:00", "not a date", "2024-01-01 00:10"],
            dtype="string",
        )
        with pytest.raises(ValueError, match="missing or could not be parsed"):
            self._pipeline(dates, values=(10.0, 11.0, 12.0))

    def test_a_missing_timestamp_in_a_datetime_column_is_refused(self):
        """A column already typed datetime64 can still carry NaT, and the check
        covers that as well as a value that needed coercing."""
        dates = list(pd.date_range("2024-01-01", periods=5, freq="5min"))
        dates[4] = pd.NaT
        with pytest.raises(ValueError, match="missing or could not be parsed"):
            self._pipeline(pd.to_datetime(pd.Series(dates)))

    def test_the_refusal_says_why_a_timestamp_is_needed(self):
        dates = list(pd.date_range("2024-01-01", periods=5, freq="5min"))
        dates[4] = pd.NaT
        with pytest.raises(ValueError, match="cannot be differenced"):
            self._pipeline(pd.to_datetime(pd.Series(dates)))

    def test_a_duplicate_timestamp_is_refused(self):
        """Two readings at one instant have no well-defined step between them,
        and the correction cannot locate a recovery by position when two rows
        share a position in time."""
        dates = list(pd.date_range("2024-01-01", periods=5, freq="5min"))
        dates[3] = dates[2]
        with pytest.raises(ValueError, match="duplicate ReadingDate"):
            self._pipeline(pd.to_datetime(pd.Series(dates)))

    def test_the_refusal_names_the_instant_and_the_remedy(self):
        dates = list(pd.date_range("2024-01-01", periods=5, freq="5min"))
        dates[3] = dates[2]
        with pytest.raises(ValueError, match="snap_readings_to_grid"):
            self._pipeline(pd.to_datetime(pd.Series(dates)))

    def test_a_well_formed_frame_is_not_refused(self, tmp_path):
        """So the two guards cannot be satisfied by refusing everything.

        Run with a measured cadence and a real output folder, because the
        pipeline has its own further preconditions and this test is about
        getting PAST the two timestamp guards, not about tripping the next one.
        """
        from etdmap.mapping_helpers import (
            detect_column_cadence,
            run_standard_pipeline,
        )

        values = [10.0, 11.0, 0.0, 12.0, 13.0]
        df = _frame(values, dates=pd.date_range(
            "2024-01-01", periods=len(values), freq="5min"
        ))
        entry = run_standard_pipeline(
            df, household_id=1, household_batch_id=1001, household_id_supplier="H1",
            mapped_folder_path=str(tmp_path), grid_minutes=5,
            cadence=detect_column_cadence(df, grid_minutes=5),
        )
        assert entry[ids.HOUSEHOLD] == 1

        # ...and the correction still did its work on the dip, so getting past
        # the guards is not the same as skipping step 7. Read through the
        # artifact's own reader, which resolves whichever layout was written.
        from etdmap.storage import read_mapped_households

        written = read_mapped_households(
            [1], mapped_folder_path=str(tmp_path)
        ).sort_values("ReadingDate").reset_index(drop=True)
        assert pd.isna(written[COL].iloc[2]), "the zero dip was not corrected"
        assert float(written[COL].iloc[1]) == 11.0


# ---------------------------------------------------------------------------
# Well-formed but awkward ReadingDate shapes -- these must agree
# ---------------------------------------------------------------------------


class TestTheTimestampShapeDoesNotChangeTheCorrection:
    """Well-formed but awkward ReadingDate shapes. The dip is at row 2 in each,
    so the correction has to reach the same verdict however the timestamps are
    spelled -- and the structural property of each shape has to survive."""

    DIP = [10.0, 11.0, 0.0, 12.0, 13.0]
    #: What the plain five-minute layout produces for DIP, from EXPECTED.
    CORRECTED = [10.0, 11.0, None, 12.0, 13.0]
    INTERVALS = [0.0, 1.0, None, None, 1.0]

    def test_a_timezone_aware_index_reaches_the_same_verdict(self):
        dates = pd.date_range(
            "2024-01-01", periods=5, freq="5min", tz="Europe/Amsterdam"
        )
        out = _corrected(_frame(self.DIP, dates=dates))
        assert _values(out[COL]) == self.CORRECTED
        assert _values(out[DIFF]) == self.INTERVALS
        assert str(out["ReadingDate"].dt.tz) == "Europe/Amsterdam", (
            "the timezone is the supplier's fact and is not to be dropped"
        )

    def test_non_default_index_labels_are_preserved(self):
        df = _frame(self.DIP)
        df.index = [100, 7, 42, 3, 99]
        out = _corrected(df)
        assert list(out.index) == [100, 7, 42, 3, 99], (
            "the correction indexes by position; it must not reset the labels"
        )
        assert _values(out[COL]) == self.CORRECTED
        assert _values(out[DIFF]) == self.INTERVALS

    def test_irregular_spacing_reaches_the_same_verdict(self):
        """The correction is about the sequence of readings, not their spacing:
        a six-hour gap between two of them changes nothing about which reading
        is impossible."""
        dates = pd.to_datetime([
            "2024-01-01 00:00", "2024-01-01 00:05", "2024-01-01 06:00",
            "2024-01-01 06:05", "2024-01-02 00:00",
        ])
        out = _corrected(_frame(self.DIP, dates=dates))
        assert _values(out[COL]) == self.CORRECTED
        assert _values(out[DIFF]) == self.INTERVALS


def test_a_second_clean_cumulative_column_is_untouched():
    """A negative diff in one column must not change any other column. The
    correction is per column: a fault in one is not a fault in another."""
    other = "Gasgebruik"
    clean = [1.0, 2.0, 3.0, 4.0, 5.0]
    df = _frame([10.0, 11.0, 0.0, 12.0, 13.0])
    df[other] = pd.array(clean, dtype="Float64")

    out = _corrected(df)

    assert _values(out[other]) == clean, "the clean column lost a reading"
    assert _values(out[f"{other}Diff"]) == [0.0, 1.0, 1.0, 1.0, 1.0]
    # ...while the dipping column was corrected as it is on its own.
    assert _values(out[COL]) == [10.0, 11.0, None, 12.0, 13.0]


class TestValidatorScanIsNotRedundant:
    """The negative-diff block in ``validate_cumulative_variables`` looks like a
    second scan of what ``_detect_negative_diff_cases`` already finds, but it
    also produces ``no_unexpected_zero``, which the detector has no counterpart
    for and which feeds the ``all(valid_result.values())`` drop decision in
    ``add_diff_columns``. Removing the scan would silently drop that flag."""

    def _validate(self, values):
        return validate_cumulative_variables(_frame(values))

    def test_zero_dip_sets_both_flags(self):
        result = self._validate([10.0, 11.0, 0.0, 12.0, 13.0])
        assert result["no_negative_diff"] is False
        assert result["no_unexpected_zero"] is False

    def test_negative_dip_without_zero_leaves_zero_flag_set(self):
        """The two flags are independent: a decrease that never reaches zero
        trips only the negative-diff flag."""
        result = self._validate([10.0, 11.0, 9.0, 12.0, 13.0])
        assert result["no_negative_diff"] is False
        assert result["no_unexpected_zero"] is True

    def test_clean_column_trips_neither_flag(self):
        result = self._validate([10.0, 11.0, 12.0, 13.0])
        assert result["no_negative_diff"] is True
        assert result["no_unexpected_zero"] is True


class TestGroupedInputIsCorrectedPerHousehold:
    """``id_column`` is the path the mappers use. Each household's series is its
    own: one household's dip must not reach another's readings, and each gets
    its own seeded first interval."""

    def _grouped(self):
        frames = []
        for household, seed in enumerate(range(20, 26)):
            rng = random.Random(seed)
            frame = _frame(_generate(rng, 24))
            frame[ids.HOUSEHOLD] = household
            frames.append(frame)
        return pd.concat(frames, ignore_index=True)

    def test_each_household_only_loses_its_own_readings(self):
        df = self._grouped()
        out = _corrected(df, id_column=ids.HOUSEHOLD)
        for household, delivered in df.groupby(ids.HOUSEHOLD)[COL]:
            own = out[out[ids.HOUSEHOLD] == household].sort_values("ReadingDate")
            _assert_only_removes(
                list(delivered), own.reset_index(drop=True),
                f"household {household}",
            )

    def test_each_household_gets_its_own_seeded_first_interval(self):
        """Row 0 of a differenced series has no predecessor, and "row 0" is per
        household -- not per frame."""
        out = _corrected(self._grouped(), id_column=ids.HOUSEHOLD)
        for household, group in out.groupby(ids.HOUSEHOLD):
            first = group.sort_values("ReadingDate")[DIFF].iloc[0]
            assert float(first) == 0.0, household

    def test_the_interval_follows_each_households_own_register(self):
        out = _corrected(self._grouped(), id_column=ids.HOUSEHOLD)
        for household, group in out.groupby(ids.HOUSEHOLD):
            _assert_interval_follows_the_register(
                group.sort_values("ReadingDate").reset_index(drop=True),
                f"household {household}",
            )

    def test_one_households_dip_does_not_reach_another(self):
        clean = [100.0, 101.0, 102.0, 103.0]
        dipping = [10.0, 11.0, 0.0, 12.0]
        frames = []
        for household, values in ((1, dipping), (2, clean)):
            frame = _frame(values)
            frame[ids.HOUSEHOLD] = household
            frames.append(frame)
        out = _corrected(pd.concat(frames, ignore_index=True),
                         id_column=ids.HOUSEHOLD)

        second = out[out[ids.HOUSEHOLD] == 2].sort_values("ReadingDate")
        assert _values(second[COL]) == clean
        first = out[out[ids.HOUSEHOLD] == 1].sort_values("ReadingDate")
        assert _values(first[COL]) == [10.0, 11.0, None, 12.0]


# ---------------------------------------------------------------------------
# What the correction records
# ---------------------------------------------------------------------------
# A correction that is not recorded is indistinguishable from data. A removal
# pairs a signal (which fault was seen) with an action (what was done), and the
# interval column records that it was derived at all.

CUM_FLAGS = flag_column(COL)
DIFF_FLAGS = flag_column(DIFF)


def _bits(frame, column):
    """The flag integer per row, 0 where the column does not exist."""
    if column not in frame.columns:
        return [0] * len(frame)
    return [0 if pd.isna(v) else int(v) for v in frame[column]]


def _has(value, flag):
    return bool(value & flag.value)


def _names(value):
    """The single-bit flag names set on one cell, as a set of strings."""
    return {
        m.name for m in MappingFlag
        if m.value and bin(m.value).count("1") == 1 and value & m.value
    }


def _lost(delivered, corrected):
    """Row positions where a delivered reading is gone from the output."""
    return [
        i for i, (was, now) in enumerate(zip(delivered, corrected))
        if was is not None and now is None
    ]


class TestEveryRemovalIsRecorded:
    """The signal/action pairing, over every shape in the corpus."""

    @pytest.mark.parametrize("name", sorted(NAMED_CASES))
    def test_a_removed_reading_carries_nulled(self, name):
        values = NAMED_CASES[name]
        out = _corrected(_frame(values))
        delivered = [None if pd.isna(v) else float(v) for v in values]
        flags = _bits(out, CUM_FLAGS)

        for i in _lost(delivered, _values(out[COL])):
            assert _has(flags[i], MappingFlag.NULLED), (
                f"{name}: row {i} lost a delivered reading with no NULLED -- "
                f"the removal is not recorded"
            )

    @pytest.mark.parametrize("name", sorted(NAMED_CASES))
    def test_a_removal_never_happens_without_a_fault_being_named(self, name):
        """The implication runs action -> signal, and only that way: something
        was removed, so something must say why. For the converse see
        ``TestASignalDoesNotImplyAnAction``.
        """
        out = _corrected(_frame(NAMED_CASES[name]))
        flags = _bits(out, CUM_FLAGS)
        nulled = [i for i, v in enumerate(flags) if _has(v, MappingFlag.NULLED)]
        if not nulled:
            return
        classified = [
            i for i, v in enumerate(flags)
            if any(_has(v, flag) for flag in CASE_FLAGS)
        ]
        assert classified, (
            f"{name}: rows {nulled} were removed and no row names a fault"
        )


class TestASignalDoesNotImplyAnAction:
    """A signal says what was seen, an action says what was done, and the two
    are independent. A fault can be recorded on a reading that is kept.
    """

    def test_a_meter_reset_is_flagged_on_a_reading_that_is_kept(self):
        """The register restarted; the reading after the restart is correct and
        is kept. The signal records that the series stepped down here."""
        out = _corrected(_frame(NAMED_CASES["meter_reset_with_na_before_dip"]))
        flags = _bits(out, CUM_FLAGS)

        assert _has(flags[3], MappingFlag.CUMULATIVE_METER_RESET)
        assert not _has(flags[3], MappingFlag.NULLED)
        assert float(out[COL].iloc[3]) == 5.0, "the reading was kept"

    def test_the_same_reset_reading_is_removed_when_it_reads_zero(self):
        """A reset whose first reading is zero loses that reading: a zero total
        after accumulation is its own fault."""
        out = _corrected(_frame(NAMED_CASES["meter_reset_to_zero"]))
        flags = _bits(out, CUM_FLAGS)

        assert _has(flags[3], MappingFlag.CUMULATIVE_METER_RESET)
        assert _has(flags[3], MappingFlag.CUMULATIVE_ZERO_TOTAL)
        assert _has(flags[3], MappingFlag.NULLED)
        assert pd.isna(out[COL].iloc[3])

    @pytest.mark.parametrize("seed", range(40))
    def test_a_generated_shape_records_each_removal(self, seed):
        rng = random.Random(seed)
        values = _generate(rng, rng.randint(1, 30))
        out = _corrected(_frame(values))
        delivered = [None if pd.isna(v) else float(v) for v in values]
        flags = _bits(out, CUM_FLAGS)
        for i in _lost(delivered, _values(out[COL])):
            assert _has(flags[i], MappingFlag.NULLED), f"seed={seed} row {i}"


class TestWhichFaultEachShapeIsRecordedAs:
    """The classification reaches the frame's flag column, not just the
    detector's own return value. One shape per case."""

    def _signals_at(self, name, row):
        out = _corrected(_frame(NAMED_CASES[name]))
        value = _bits(out, CUM_FLAGS)[row]
        return {flag.name for flag in CASE_FLAGS if _has(value, flag)}

    def test_a_dip_that_jumps_back_is_a_jump_back(self):
        assert self._signals_at("isolated_zero_dip", 2) == {
            "CUMULATIVE_JUMP_BACK"
        }

    def test_a_dip_that_does_not_reach_zero_is_still_a_jump_back(self):
        assert self._signals_at("non_zero_negative_dip", 2) == {
            "CUMULATIVE_JUMP_BACK"
        }

    def test_the_first_of_two_decreases_in_a_row_is_a_repeat_negative(self):
        assert self._signals_at("consecutive_negatives", 2) == {
            "CUMULATIVE_REPEAT_NEGATIVE"
        }

    def test_a_recovery_that_never_returns_to_level_is_a_meter_reset(self):
        assert self._signals_at("meter_reset_to_zero", 3) == {
            "CUMULATIVE_METER_RESET"
        }

    def test_a_decrease_with_no_recovery_at_all_is_no_recovery(self):
        assert self._signals_at("dip_then_frozen", 3) == {
            "CUMULATIVE_NO_RECOVERY"
        }
        assert self._signals_at("dip_at_last_row", 3) == {
            "CUMULATIVE_NO_RECOVERY"
        }

    def test_exactly_one_classification_lands_on_the_offending_reading(self):
        """The four are mutually exclusive, which is what makes a count of them
        a count of faults rather than of bits."""
        for name, row in (
            ("isolated_zero_dip", 2), ("non_zero_negative_dip", 2),
            ("consecutive_negatives", 2), ("meter_reset_to_zero", 3),
            ("dip_then_frozen", 3), ("dip_at_last_row", 3),
        ):
            assert len(self._signals_at(name, row)) == 1, name

    def test_a_dip_to_zero_also_records_the_zero_total(self):
        """A separate signal from the four, and independent of them: the
        reading was zero, which is its own observation."""
        out = _corrected(_frame(NAMED_CASES["isolated_zero_dip"]))
        assert _has(_bits(out, CUM_FLAGS)[2], MappingFlag.CUMULATIVE_ZERO_TOTAL)

    def test_a_dip_that_does_not_reach_zero_does_not(self):
        out = _corrected(_frame(NAMED_CASES["non_zero_negative_dip"]))
        assert not _has(
            _bits(out, CUM_FLAGS)[2], MappingFlag.CUMULATIVE_ZERO_TOTAL
        )

    def test_a_continuation_of_the_same_span_is_nulled_without_a_new_fault(self):
        """The fault is the reading that broke the series. The rows removed
        with it are consequences, and labelling each one a fault would count
        one anomaly several times."""
        out = _corrected(_frame(NAMED_CASES["dip_then_frozen"]))
        flags = _bits(out, CUM_FLAGS)
        for row in (4, 5):
            assert _has(flags[row], MappingFlag.NULLED)
            assert not any(_has(flags[row], flag) for flag in CASE_FLAGS)


class TestTheIntervalColumnRecordsItsOwnProvenance:
    """An interval differenced from a register and one the supplier delivered
    are indistinguishable per cell without this."""

    def test_every_derived_interval_says_so(self):
        out = _corrected(_frame(NAMED_CASES["clean_monotonic"]))
        flags = _bits(out, DIFF_FLAGS)
        for i in range(1, len(flags)):
            assert _has(
                flags[i], MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE
            ), i

    def test_row_zero_is_seeded_not_derived(self):
        """It has no predecessor to difference, so it was invented -- which is
        what FIRST_ZERO says, and why the derivation flag must not be there."""
        out = _corrected(_frame(NAMED_CASES["clean_monotonic"]))
        first = _bits(out, DIFF_FLAGS)[0]
        assert _has(first, MappingFlag.FIRST_ZERO)
        assert not _has(
            first, MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE
        )

    def test_a_blanked_interval_keeps_the_history_of_being_derived(self):
        """It was differenced and then removed. Both happened, so both are on
        the cell: a flag records history and bits only ever accumulate."""
        out = _corrected(_frame(NAMED_CASES["isolated_zero_dip"]))
        flags = _bits(out, DIFF_FLAGS)
        for row in (2, 3):
            assert pd.isna(out[DIFF].iloc[row])
            assert _has(flags[row], MappingFlag.NULLED)
            assert _has(
                flags[row], MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE
            )

    @pytest.mark.parametrize("name", sorted(NAMED_CASES))
    def test_every_blanked_interval_is_recorded_as_removed(self, name):
        """An interval that *could* have been differenced and is nonetheless
        empty was removed, and says so.

        Both register values have to be present for that to apply: an interval
        whose predecessor is absent was never computable, so its emptiness is
        not a removal and carries no action -- which is why ``leading_na`` has
        an empty interval at row 2 with no ``NULLED`` on it.
        """
        out = _corrected(_frame(NAMED_CASES[name]))
        flags = _bits(out, DIFF_FLAGS)
        for i in range(1, len(out)):
            computable = (
                pd.notna(out[COL].iloc[i]) and pd.notna(out[COL].iloc[i - 1])
            )
            if computable and pd.isna(out[DIFF].iloc[i]):
                assert _has(flags[i], MappingFlag.NULLED), f"{name} row {i}"


class TestAnUntouchedColumnIsNotFlagged:
    def test_a_clean_register_gets_no_flag_column_at_all(self):
        """``set_flags`` creates the column on first use, so the absence of one
        is itself the statement that nothing happened to the register."""
        out = _corrected(_frame(NAMED_CASES["clean_monotonic"]))
        assert CUM_FLAGS not in out.columns

    def test_but_its_interval_column_does_get_one(self):
        """Because deriving the interval IS something that happened."""
        out = _corrected(_frame(NAMED_CASES["clean_monotonic"]))
        assert DIFF_FLAGS in out.columns

    def test_a_corrected_register_gets_one(self):
        out = _corrected(_frame(NAMED_CASES["isolated_zero_dip"]))
        assert CUM_FLAGS in out.columns


# ---------------------------------------------------------------------------
# The vocabulary, read off the data that produces it
# ---------------------------------------------------------------------------
# This table is meant to be read, not only run. Each row is one reading:
# what the supplier delivered, what the register and interval hold afterwards,
# and the signals and actions recorded on each. Together they say what every
# flag in this part of the vocabulary MEANS, in terms of data that caused it.
#
# How to read a row:
#   delivered      the reading as it arrived
#   register       what the cumulative column holds after step 7 (None = pd.NA)
#   register flags signals and actions on the cumulative column
#   interval       what the Diff column holds (None = pd.NA)
#   interval flags signals and actions on the Diff column
#
# Four things the table makes visible that no single assertion does:
#   * A fault is named on the reading that caused it, and the readings removed
#     with it carry the action alone -- one anomaly, one classification.
#   * The row AFTER a removal keeps its register reading but loses its
#     interval: there is nothing to difference it from. That is a removal and
#     carries NULLED.
#   * An interval whose predecessor never arrived carries no flags at all. It
#     was not removed; it was never computable.
#   * CUMULATIVE_METER_RESET appears on a reading that is KEPT. A signal is an
#     observation, not a verdict on the value.

FLAG_STORY = {
    "clean_monotonic": [
        # delivered  register  register flags  interval  interval flags
        (10.0, 10.0, (), 0.0, ("FIRST_ZERO", "NULL_DERIVED_FROM_CUMULATIVE")),
        (11.0, 11.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
        (12.0, 12.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
        (13.0, 13.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
    ],
    # The meter dips to zero and comes back above where it was. The zero is
    # impossible, so it goes; the recovery reading is real and stays.
    "isolated_zero_dip": [
        (10.0, 10.0, (), 0.0, ("FIRST_ZERO", "NULL_DERIVED_FROM_CUMULATIVE")),
        (11.0, 11.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
        (0.0, None,
         ("CUMULATIVE_JUMP_BACK", "CUMULATIVE_ZERO_TOTAL", "NULLED"),
         None, ("NULLED", "VALUE_DERIVED_FROM_CUMULATIVE",
          "NULL_DERIVED_FROM_CUMULATIVE")),
        (12.0, 12.0, (), None,
         ("NULLED", "VALUE_DERIVED_FROM_CUMULATIVE",
          "NULL_DERIVED_FROM_CUMULATIVE")),
        (13.0, 13.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
    ],
    # The same shape without reaching zero: one fewer signal, same action.
    "non_zero_negative_dip": [
        (10.0, 10.0, (), 0.0, ("FIRST_ZERO", "NULL_DERIVED_FROM_CUMULATIVE")),
        (11.0, 11.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
        (9.0, None, ("CUMULATIVE_JUMP_BACK", "NULLED"),
         None, ("NULLED", "VALUE_DERIVED_FROM_CUMULATIVE",
          "NULL_DERIVED_FROM_CUMULATIVE")),
        (12.0, 12.0, (), None,
         ("NULLED", "VALUE_DERIVED_FROM_CUMULATIVE",
          "NULL_DERIVED_FROM_CUMULATIVE")),
        (13.0, 13.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
    ],
    # Two decreases running: the first is a repeat, the last is classified by
    # how the series recovers from it.
    "consecutive_negatives": [
        (10.0, 10.0, (), 0.0, ("FIRST_ZERO", "NULL_DERIVED_FROM_CUMULATIVE")),
        (11.0, 11.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
        (9.0, None, ("CUMULATIVE_REPEAT_NEGATIVE", "NULLED"),
         None, ("NULLED", "VALUE_DERIVED_FROM_CUMULATIVE",
          "NULL_DERIVED_FROM_CUMULATIVE")),
        (7.0, None, ("CUMULATIVE_JUMP_BACK", "NULLED"),
         None, ("NULLED", "VALUE_DERIVED_FROM_CUMULATIVE",
          "NULL_DERIVED_FROM_CUMULATIVE")),
        (12.0, 12.0, (), None,
         ("NULLED", "VALUE_DERIVED_FROM_CUMULATIVE",
          "NULL_DERIVED_FROM_CUMULATIVE")),
        (13.0, 13.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
    ],
    "three_consecutive_negatives": [
        (20.0, 20.0, (), 0.0, ("FIRST_ZERO", "NULL_DERIVED_FROM_CUMULATIVE")),
        (19.0, None, ("CUMULATIVE_REPEAT_NEGATIVE", "NULLED"),
         None, ("NULLED", "VALUE_DERIVED_FROM_CUMULATIVE",
          "NULL_DERIVED_FROM_CUMULATIVE")),
        (18.0, None, ("CUMULATIVE_REPEAT_NEGATIVE", "NULLED"),
         None, ("NULLED", "VALUE_DERIVED_FROM_CUMULATIVE",
          "NULL_DERIVED_FROM_CUMULATIVE")),
        (17.0, None, ("CUMULATIVE_JUMP_BACK", "NULLED"),
         None, ("NULLED", "VALUE_DERIVED_FROM_CUMULATIVE",
          "NULL_DERIVED_FROM_CUMULATIVE")),
        (25.0, 25.0, (), None,
         ("NULLED", "VALUE_DERIVED_FROM_CUMULATIVE",
          "NULL_DERIVED_FROM_CUMULATIVE")),
    ],
    # The register restarted from zero. The zero itself is condemned; the
    # count-up after it is kept and is a new baseline.
    "meter_reset_to_zero": [
        (10.0, 10.0, (), 0.0, ("FIRST_ZERO", "NULL_DERIVED_FROM_CUMULATIVE")),
        (11.0, 11.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
        (12.0, 12.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
        (0.0, None,
         ("CUMULATIVE_METER_RESET", "CUMULATIVE_ZERO_TOTAL", "NULLED"),
         None, ("NULLED", "VALUE_DERIVED_FROM_CUMULATIVE",
          "NULL_DERIVED_FROM_CUMULATIVE")),
        (1.0, 1.0, (), None,
         ("NULLED", "VALUE_DERIVED_FROM_CUMULATIVE",
          "NULL_DERIVED_FROM_CUMULATIVE")),
        (2.0, 2.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
        (3.0, 3.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
    ],
    # A reset whose first reading is NOT zero: nothing is impossible here, so
    # the reading is KEPT and only the observation is recorded. This is the row
    # that shows a signal is not a verdict. Its interval carries no flags --
    # the reading before it never arrived, so there was nothing to difference.
    "meter_reset_with_na_before_dip": [
        (100.0, 100.0, (), 0.0, ("FIRST_ZERO", "NULL_DERIVED_FROM_CUMULATIVE")),
        (101.0, 101.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
        (None, None, (), None, ("NULL_DERIVED_FROM_CUMULATIVE",)),
        (5.0, 5.0, ("CUMULATIVE_METER_RESET",), None, ("NULL_DERIVED_FROM_CUMULATIVE",)),
        (6.0, 6.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
        (7.0, 7.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
    ],
    # The series drops and never comes back. The drop is the fault; the frozen
    # readings after it are removed as part of the same span and carry the
    # action without a second classification.
    "dip_then_frozen": [
        (10.0, 10.0, (), 0.0, ("FIRST_ZERO", "NULL_DERIVED_FROM_CUMULATIVE")),
        (11.0, 11.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
        (12.0, 12.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
        (5.0, None, ("CUMULATIVE_NO_RECOVERY", "NULLED"),
         None, ("NULLED", "VALUE_DERIVED_FROM_CUMULATIVE",
          "NULL_DERIVED_FROM_CUMULATIVE")),
        (5.0, None, ("NULLED",),
         None, ("NULLED", "VALUE_DERIVED_FROM_CUMULATIVE",
          "NULL_DERIVED_FROM_CUMULATIVE")),
        (5.0, None, ("NULLED",),
         None, ("NULLED", "VALUE_DERIVED_FROM_CUMULATIVE",
          "NULL_DERIVED_FROM_CUMULATIVE")),
    ],
    # Nothing is wrong here, so nothing is flagged on the register. Row 0 still
    # gets its seeded zero -- FIRST_ZERO is unconditional, even where no
    # reading arrived -- and rows whose predecessor is absent get no interval.
    # They are not unflagged, though: the differencing reached them and
    # produced nothing, which NULL_DERIVED_FROM_CUMULATIVE records. An empty
    # cell in a derived column is not an untouched cell.
    "leading_na": [
        (None, None, (), 0.0, ("FIRST_ZERO", "NULL_DERIVED_FROM_CUMULATIVE")),
        (None, None, (), None, ("NULL_DERIVED_FROM_CUMULATIVE",)),
        (3.0, 3.0, (), None, ("NULL_DERIVED_FROM_CUMULATIVE",)),
        (4.0, 4.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
        (5.0, 5.0, (), 1.0, ("VALUE_DERIVED_FROM_CUMULATIVE",)),
    ],
}


class TestTheFlagStory:
    """Walk the table above against the real function, row by row."""

    @pytest.mark.parametrize("shape", sorted(FLAG_STORY))
    def test_every_row_holds_what_the_table_says(self, shape):
        rows = FLAG_STORY[shape]
        delivered = [row[0] if row[0] is not None else pd.NA for row in rows]
        out = _corrected(_frame(delivered))

        register = _values(out[COL])
        interval = _values(out[DIFF])
        register_flags = _bits(out, CUM_FLAGS)
        interval_flags = _bits(out, DIFF_FLAGS)

        assert len(out) == len(rows), shape
        for i, (_in, want_reg, want_rf, want_iv, want_if) in enumerate(rows):
            where = f"{shape} row {i}"
            assert register[i] == want_reg, f"{where}: register"
            assert interval[i] == want_iv, f"{where}: interval"
            assert _names(register_flags[i]) == set(want_rf), (
                f"{where}: register flags"
            )
            assert _names(interval_flags[i]) == set(want_if), (
                f"{where}: interval flags"
            )

    @pytest.mark.parametrize("shape", sorted(FLAG_STORY))
    def test_the_table_names_real_members(self, shape):
        """So a renamed or removed flag cannot sit in the table unnoticed."""
        for row in FLAG_STORY[shape]:
            for name in row[2] + row[4]:
                assert name in MappingFlag.__members__, name

    def test_the_shapes_in_the_table_are_shapes_the_corpus_covers(self):
        """The table documents the vocabulary; the corpus tests the values. A
        shape in one and not the other means they have drifted apart."""
        assert set(FLAG_STORY) <= set(NAMED_CASES) | {"clean_monotonic"}
