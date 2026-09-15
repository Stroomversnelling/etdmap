"""Oracle tests for the negative-diff correction of cumulative columns.

``add_diff_columns_legacy`` is the original loop-based implementation and is used
here as a behaviour oracle for the vectorised ``add_diff_columns``. Both must
produce the same corrected cumulative column and the same Diff column for every
input shape the pipeline can see.

The generated shapes cover: clean monotonic series, isolated zero dips, runs of
zeros, meter resets, non-zero negative dips, frozen (constant) runs, all-NA
columns, leading NA, and single-row frames.
"""

import logging
import random

import pandas as pd
import pytest

from etdmap.data_model import cumulative_columns
from etdmap.flags import MappingFlag
from etdmap.mapping_helpers import (
    _detect_negative_diff_cases,
    add_diff_columns,
    add_diff_columns_legacy,
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
    """The correction routines log per anomaly; the oracle only cares about data."""
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


def _run_both(df):
    legacy = add_diff_columns_legacy(df.copy(), context="oracle")
    vector = add_diff_columns(df.copy(), context="oracle")
    return legacy, vector


def _assert_agree(df, label):
    legacy, vector = _run_both(df)
    pd.testing.assert_frame_equal(
        vector[COMPARED].reset_index(drop=True),
        legacy[COMPARED].reset_index(drop=True),
        obj=f"{label}: vectorised vs legacy",
    )


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


@pytest.mark.parametrize("name", sorted(NAMED_CASES))
def test_named_shapes_agree_with_legacy(name):
    _assert_agree(_frame(NAMED_CASES[name]), name)


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
def test_generated_shapes_agree_with_legacy(seed):
    rng = random.Random(seed)
    values = _generate(rng, rng.randint(1, 30))
    _assert_agree(_frame(values), f"seed={seed} values={values}")


# ---------------------------------------------------------------------------
# Grouped input -- the id_column path must agree too
# ---------------------------------------------------------------------------


def _corpus():
    """Every frame the oracle tests above compare, named."""
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
        """Guards the oracle against becoming vacuous: if the generator stops
        producing one of the shapes, the agreement it reports is hollow."""
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
        # Leading zeros are a register that has not started counting, not a
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


class TestDegenerateReadingDates:
    def test_duplicate_reading_dates_diverge(self):
        """Legacy finds the reading after the dip by date ('ReadingDate > rd'),
        which skips the duplicate, classifies the dip as a meter reset, and then
        blanks every row sharing the dip's date. The vectorised version finds it
        by position, so the recovery it sees is the duplicate itself: the range
        it blanks is empty and the negative diff survives into the output."""
        dates = list(pd.date_range("2024-01-01", periods=5, freq="5min"))
        dates[3] = dates[2]
        df = _frame([10.0, 11.0, 0.0, 12.0, 13.0], dates=dates)
        legacy = add_diff_columns_legacy(df.copy())
        vector = add_diff_columns(df.copy())

        assert legacy[COL].isna().tolist() == [False, False, True, True, False]
        assert legacy[DIFF].isna().tolist() == [False, False, True, True, True]

        assert vector[COL].isna().tolist() == [False] * 5
        assert float(vector[DIFF].iloc[2]) == -11.0

    def test_nat_reading_date_diverges(self):
        """sort_values puts NaT last, but astype('int64') maps NaT to INT64_MIN,
        so the epoch-nanosecond series the vectorised version hands to
        searchsorted is not sorted and the row range it derives runs to the end
        of the frame. Legacy compares timestamps directly and is unaffected."""
        dates = list(pd.date_range("2024-01-01", periods=5, freq="5min"))
        dates[4] = pd.NaT
        df = _frame([10.0, 11.0, 0.0, 12.0, 13.0], dates=dates)
        legacy = add_diff_columns_legacy(df.copy())
        vector = add_diff_columns(df.copy())

        assert legacy[COL].isna().tolist() == [False, False, True, False, False]
        assert vector[COL].isna().tolist() == [False, False, True, True, True]


# ---------------------------------------------------------------------------
# Well-formed but awkward ReadingDate shapes -- these must agree
# ---------------------------------------------------------------------------


def test_tz_aware_reading_dates_agree():
    dates = pd.date_range("2024-01-01", periods=7, freq="5min", tz="Europe/Amsterdam")
    _assert_agree(_frame([10.0, 11.0, 0.0, 0.0, 12.0, 13.0, 14.0], dates=dates), "tz aware")


def test_non_default_index_labels_agree():
    df = _frame([10.0, 11.0, 0.0, 12.0, 13.0])
    df.index = [100, 7, 42, 3, 99]
    _assert_agree(df, "odd index labels")


def test_irregular_spacing_agrees():
    dates = pd.to_datetime([
        "2024-01-01 00:00", "2024-01-01 00:05", "2024-01-01 06:00",
        "2024-01-01 06:05", "2024-01-02 00:00",
    ])
    _assert_agree(_frame([10.0, 11.0, 0.0, 12.0, 13.0], dates=dates), "irregular spacing")


def test_second_clean_cumulative_column_agrees():
    """A negative diff in one column must not change any other column. The
    correction used to run for every cumulative column whenever any one of them
    failed validation."""
    other = "Gasgebruik"
    df = _frame([10.0, 11.0, 0.0, 12.0, 13.0])
    df[other] = pd.array([1.0, 2.0, 3.0, 4.0, 5.0], dtype="Float64")
    legacy = add_diff_columns_legacy(df.copy())
    vector = add_diff_columns(df.copy())
    cols = [*COMPARED, other, other + "Diff"]
    pd.testing.assert_frame_equal(
        vector[cols].reset_index(drop=True),
        legacy[cols].reset_index(drop=True),
        obj="two columns: vectorised vs legacy",
    )
    assert vector[other].tolist() == [1.0, 2.0, 3.0, 4.0, 5.0]


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


def test_grouped_input_agrees_with_legacy():
    frames = []
    for household, seed in enumerate(range(20, 26)):
        rng = random.Random(seed)
        df = _frame(_generate(rng, 24))
        df["HuisIdBSV"] = household
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    legacy = add_diff_columns_legacy(df.copy(), id_column="HuisIdBSV")
    vector = add_diff_columns(df.copy(), id_column="HuisIdBSV")
    sort_key = ["HuisIdBSV", "ReadingDate"]
    pd.testing.assert_frame_equal(
        vector.sort_values(sort_key)[COMPARED].reset_index(drop=True),
        legacy.sort_values(sort_key)[COMPARED].reset_index(drop=True),
        obj="grouped: vectorised vs legacy",
    )
