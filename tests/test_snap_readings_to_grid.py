"""
Tests for snap_readings_to_grid in etdmap.mapping_helpers.

All test data is hardcoded — no fixture files required.
The suite covers:
 - Basic snapping (on-grid, uniform offset, single row)
 - Output row count equals unique slot count
 - Two-stream merging with non-overlapping columns
 - K-pass modal selection when both streams have values for the same column
 - Three-stream merging (K=3 passes)
 - Irregular / staggered per-column timing, with and without value ambiguity
 - Edge cases: all-NA column, non-numeric column, custom column name,
   custom freq_minutes
 - Cumulative column stream consistency (the primary motivating use case)
"""

import pandas as pd
import pytest

from etdmap.mapping_helpers import snap_readings_to_grid

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

T0 = pd.Timestamp("2023-01-01 00:00:00")


def slot(n: int) -> pd.Timestamp:
    """Return the n-th 5-minute grid slot from T0."""
    return T0 + pd.Timedelta(minutes=5 * n)


def at(n: int, seconds: int = 0) -> pd.Timestamp:
    """Return a timestamp at slot n, offset by *seconds*."""
    return slot(n) + pd.Timedelta(seconds=seconds)


# ---------------------------------------------------------------------------
# Basic snapping
# ---------------------------------------------------------------------------


def test_already_on_grid_is_unchanged():
    """Timestamps already on grid boundaries pass through unmodified."""
    df = pd.DataFrame({
        "ReadingDate": [slot(0), slot(1), slot(2)],
        "val": [1.0, 2.0, 3.0],
    })
    result = snap_readings_to_grid(df)

    assert list(result["ReadingDate"]) == [slot(0), slot(1), slot(2)]
    assert list(result["val"]) == [1.0, 2.0, 3.0]


def test_uniform_offset_snaps_to_grid():
    """All rows with the same constant offset snap correctly to their nearest slot."""
    df = pd.DataFrame({
        "ReadingDate": [at(0, 22), at(1, 22), at(2, 22)],
        "val": [1.0, 2.0, 3.0],
    })
    result = snap_readings_to_grid(df)

    assert list(result["ReadingDate"]) == [slot(0), slot(1), slot(2)]
    assert list(result["val"]) == [1.0, 2.0, 3.0]


def test_single_row():
    """Single-row input returns exactly one row with the snapped timestamp."""
    df = pd.DataFrame({
        "ReadingDate": [at(0, 22)],
        "val": [42.0],
    })
    result = snap_readings_to_grid(df)

    assert len(result) == 1
    assert result["ReadingDate"].iloc[0] == slot(0)
    assert result["val"].iloc[0] == 42.0


def test_output_row_count_equals_unique_slots():
    """Output contains exactly one row per unique snapped slot."""
    # 6 input rows → 3 slots (2 rows per slot)
    df = pd.DataFrame({
        "ReadingDate": [
            slot(0), at(0, 22),
            slot(1), at(1, 22),
            slot(2), at(2, 22),
        ],
        "val": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
    })
    result = snap_readings_to_grid(df)

    assert len(result) == 3
    assert list(result["ReadingDate"]) == [slot(0), slot(1), slot(2)]


# ---------------------------------------------------------------------------
# Two-stream merging
# ---------------------------------------------------------------------------


def test_two_stream_merge_non_overlapping_columns():
    """
    Two sensor streams at different offsets share the same 5-min slots.
    Each stream exclusively owns its columns; the result merges them into
    a single row per slot with all column values populated.
    """
    n = 4
    df = pd.DataFrame({
        "ReadingDate": [at(i, 22) for i in range(n)] + [slot(i) for i in range(n)],
        "col_a": [float(i) for i in range(n)] + [None] * n,        # +22s stream only
        "col_b": [None] * n + [float(i * 10) for i in range(n)],   # +0s stream only
    })
    result = snap_readings_to_grid(df)

    assert len(result) == n
    assert list(result["ReadingDate"]) == [slot(i) for i in range(n)]
    assert list(result["col_a"]) == [float(i) for i in range(n)]
    assert list(result["col_b"]) == [float(i * 10) for i in range(n)]


def test_two_stream_kpass_modal_picks_correct_row():
    """
    When both streams supply non-NA values for the same column, the K-pass
    uses each column's characteristic offset (modal) to select the correct
    stream.

    Design:
      - n_regular "clean" slots where each column is non-null in its own stream
        only.  These establish an unambiguous modal for each column.
      - n_contested slots where both streams are non-null.  The modal (now
        clearly determined) must choose the correct row in each contested slot.

    col_a: modal = +22s (4 clean +22s rows vs 2 contested +0s rows → 4:2)
    col_b: modal = +0s  (4 clean +0s rows vs 2 contested +22s rows → 4:2)
    Sentinel 99.0 is placed in the wrong stream for each contested slot
    and must never appear in the output.
    """
    n_regular = 4
    n_contested = 2

    # Clean slots: col_a exclusively from +22s, col_b exclusively from +0s
    clean_22 = [at(i, 22) for i in range(n_regular)]
    clean_0  = [slot(i)   for i in range(n_regular)]

    # Contested slots: both streams supply non-null values
    base = n_regular
    cont_22 = [at(base + i, 22) for i in range(n_contested)]
    cont_0  = [slot(base + i)   for i in range(n_contested)]

    df = pd.DataFrame({
        "ReadingDate": clean_22 + clean_0 + cont_22 + cont_0,
        # col_a: correct in +22s stream, sentinel 99.0 in +0s contested rows
        "col_a": (
            [float(i + 1) for i in range(n_regular)]   # clean +22s: correct
            + [None] * n_regular                         # clean +0s:  NA
            + [10.0, 20.0]                               # contested +22s: correct
            + [99.0] * n_contested                       # contested +0s: sentinel
        ),
        # col_b: correct in +0s stream, sentinel 99.0 in +22s contested rows
        "col_b": (
            [None] * n_regular                                       # clean +22s: NA
            + [float((i + 1) * 10) for i in range(n_regular)]       # clean +0s: correct
            + [99.0] * n_contested                                   # contested +22s: sentinel
            + [100.0, 200.0]                                         # contested +0s: correct
        ),
    })
    result = snap_readings_to_grid(df)

    total = n_regular + n_contested
    assert len(result) == total

    # Clean slots: unambiguous — each col comes from its own stream
    assert list(result["col_a"].iloc[:n_regular]) == [float(i + 1) for i in range(n_regular)]
    assert list(result["col_b"].iloc[:n_regular]) == [float((i + 1) * 10) for i in range(n_regular)]

    # Contested slots: modal wins — sentinel 99.0 must not appear
    assert result["col_a"].iloc[n_regular]     == 10.0
    assert result["col_a"].iloc[n_regular + 1] == 20.0
    assert result["col_b"].iloc[n_regular]     == 100.0
    assert result["col_b"].iloc[n_regular + 1] == 200.0
    assert 99.0 not in result["col_a"].tolist()
    assert 99.0 not in result["col_b"].tolist()


def test_three_stream_merge_k_equals_3():
    """
    Three sensor streams at +0s, +22s, and +45s each own one column.
    The K-pass runs three times (K=3) and populates each column from the
    correct stream.
    """
    n = 4
    df = pd.DataFrame({
        "ReadingDate": (
            [slot(i) for i in range(n)]
            + [at(i, 22) for i in range(n)]
            + [at(i, 45) for i in range(n)]
        ),
        "col_0":  [float(i) for i in range(n)] + [None] * (n * 2),
        "col_22": [None] * n + [float(i * 10) for i in range(n)] + [None] * n,
        "col_45": [None] * (n * 2) + [float(i * 100) for i in range(n)],
    })
    result = snap_readings_to_grid(df)

    assert len(result) == n
    assert list(result["col_0"])  == [float(i) for i in range(n)]
    assert list(result["col_22"]) == [float(i * 10) for i in range(n)]
    assert list(result["col_45"]) == [float(i * 100) for i in range(n)]


# ---------------------------------------------------------------------------
# Irregular / staggered timing
# ---------------------------------------------------------------------------


def test_irregular_timing_single_value_per_column_per_slot():
    """
    Rows arrive at unpredictable offsets within each slot.
    Each column is non-null in exactly one row per slot, so the correct
    value is unambiguous regardless of modal determination.

    Offsets used (seconds from T0):
      10, 60, 120 → all round to slot(0)
      310, 360, 430 → all round to slot(1)
    """
    offsets_s = [10, 60, 120, 310, 360, 430]
    df = pd.DataFrame({
        "ReadingDate": [T0 + pd.Timedelta(seconds=s) for s in offsets_s],
        "col_a": [1.0, None, None, 2.0, None, None],
        "col_b": [None, 10.0, None, None, 20.0, None],
        "col_c": [None, None, 100.0, None, None, 200.0],
    })
    result = snap_readings_to_grid(df)

    assert len(result) == 2
    assert list(result["col_a"]) == [1.0, 2.0]
    assert list(result["col_b"]) == [10.0, 20.0]
    assert list(result["col_c"]) == [100.0, 200.0]


def test_irregular_timing_modal_wins_with_competing_values():
    """
    A column's characteristic offset is established from many regular rows.
    In a contested slot where two competing rows both have non-NA values,
    the row whose timestamp is closest to the column's modal wins.

    Setup:
      - 6 regular slots where col_a arrives at +22s → modal = +22s
      - 1 contested slot: +22s row has val=10.0, +30s row has val=99.0
        Distance from modal: +22s→0s, +30s→8s → 10.0 should be chosen.
    """
    n_regular = 6
    regular_times = [at(i, 22) for i in range(n_regular)]
    regular_vals  = [float(i) for i in range(n_regular)]

    contested = n_regular
    df = pd.DataFrame({
        "ReadingDate": regular_times + [at(contested, 22), at(contested, 30)],
        "col_a": regular_vals + [10.0, 99.0],
    })
    result = snap_readings_to_grid(df)

    assert result["col_a"].iloc[contested] == 10.0
    assert 99.0 not in result["col_a"].tolist()


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_all_na_column_does_not_crash():
    """
    An entirely-NA column is assigned modal 0.0 (fallback) and returned
    as all-NA without raising an error.
    """
    df = pd.DataFrame({
        "ReadingDate": [slot(0), slot(1), slot(2)],
        "good_col":  [1.0, 2.0, 3.0],
        "empty_col": [None, None, None],
    })
    result = snap_readings_to_grid(df)

    assert len(result) == 3
    assert list(result["good_col"]) == [1.0, 2.0, 3.0]
    assert result["empty_col"].isna().all()


def test_non_numeric_column_passes_through():
    """
    Non-numeric (string) columns are handled correctly.
    groupby.first() is dtype-agnostic and skips NA regardless of column type.
    """
    df = pd.DataFrame({
        "ReadingDate": [slot(0), at(0, 22), slot(1), at(1, 22)],
        "label": pd.array(["A", pd.NA, "B", pd.NA], dtype="string"),
        "num":   [None, 1.0, None, 2.0],
    })
    result = snap_readings_to_grid(df)

    assert len(result) == 2
    assert list(result["label"]) == ["A", "B"]
    assert list(result["num"]) == [1.0, 2.0]


def test_custom_date_column_name():
    """The function respects a non-default date column name."""
    df = pd.DataFrame({
        "Timestamp": [slot(0), slot(1)],
        "val": [1.0, 2.0],
    })
    result = snap_readings_to_grid(df, date_column="Timestamp")

    assert "Timestamp" in result.columns
    assert "ReadingDate" not in result.columns
    assert len(result) == 2
    assert list(result["val"]) == [1.0, 2.0]


def test_freq_minutes_parameter():
    """A non-default freq_minutes groups readings into wider grid slots."""
    # Three readings at 0, 5, and 7 minutes all fall within the first
    # 15-minute window and snap to the same slot (00:00:00).
    df = pd.DataFrame({
        "ReadingDate": [
            T0,
            T0 + pd.Timedelta(minutes=5),
            T0 + pd.Timedelta(minutes=7),
        ],
        "val": [1.0, 2.0, 3.0],
    })
    result = snap_readings_to_grid(df, freq_minutes=15)

    assert len(result) == 1
    assert result["ReadingDate"].iloc[0] == T0


# ---------------------------------------------------------------------------
# Motivating use case: cumulative column stream consistency
# ---------------------------------------------------------------------------


def test_cumulative_column_stream_consistency():
    """
    A cumulative column arriving at +22s must never draw a value from the
    +0s stream.  Without per-column modal snapping, adjacent slots would
    alternately use the +22s and +0s streams, producing diffs that span
    either ~278s or ~322s instead of 300s (±7.3% error per slot).

    With per-column modal snapping every slot draws from the same stream,
    so all diffs equal exactly one physical measurement interval.
    """
    n = 6
    cum_values = [1000.0 + i * 5.5 for i in range(n)]  # strictly increasing at +5.5 each slot

    df = pd.DataFrame({
        "ReadingDate": [at(i, 22) for i in range(n)] + [slot(i) for i in range(n)],
        "ElektriciteitCum": cum_values + [None] * n,    # present only in +22s stream
        "OtherCol":         [None] * n + [float(i * 2) for i in range(n)],
    })
    result = snap_readings_to_grid(df)

    assert len(result) == n
    assert list(result["ElektriciteitCum"]) == cum_values

    # All consecutive diffs must equal exactly 5.5 — proof of single-stream consistency
    diffs = result["ElektriciteitCum"].diff().dropna()
    assert (abs(diffs - 5.5) < 1e-9).all()


# ---------------------------------------------------------------------------
# Edge cases NOT yet covered — review eventually
# ---------------------------------------------------------------------------
#
# 1. MODAL TIE-BREAKING (non-deterministic, hard to fix without API change)
#    When a column has equal counts of two different offsets (e.g. 3 rows at
#    +22s and 3 rows at +0s), pandas value_counts() does not guarantee a stable
#    tie-breaking order.  The modal could be either offset, making group
#    assignment for that column undefined.
#    Real-world risk: a column that appears equally in two streams — rare in
#    typical supplier data but possible for infrequent shared metrics.
#    Mitigation options to consider:
#      - Accept the first-seen offset (stable sort in pandas 2+)
#      - Require the caller to pass explicit per-column overrides
#      - After processing, compare diffs and warn if they vary unexpectedly
#
# def test_modal_tie_breaking_is_stable():
#     # TODO: Determine whether pandas value_counts() is actually stable (
#     # insertion-order) for ties in pandas 2.2+ and whether that guarantee
#     # is documented / reliable.  If yes, write a test that asserts the
#     # first-encountered offset wins.  If no, add a deterministic tie-breaker
#     # (e.g. sort offsets numerically after value_counts) and test that.
#     pass
#
#
# 2. EMPTY DATAFRAME
#    An empty df (zero rows) is not tested.  Visual inspection suggests the
#    function returns an empty df with just the date column, but this should
#    be confirmed — particularly the `max_dev = raw_offsets.abs().max()`
#    which returns NaN on an empty Series and may produce surprising f-string
#    output (formatted as "nan").
#
# def test_empty_dataframe():
#     # TODO: Pass an empty df and assert len(result) == 0 with no exception.
#     pass
#
#
# 3. MIDPOINT BOUNDARY ROUNDING (pandas banker's rounding)
#    dt.round('5min') uses "round half to even" (IEEE 754 banker's rounding).
#    A timestamp exactly 2 min 30 s after a slot boundary (the midpoint) will
#    round to whichever neighbouring minute is even — NOT necessarily the later
#    slot.  This is unlikely in typical supplier data (offsets are +22s and +0s, both
#    far from the ±150 s boundary) but could matter for other suppliers.
#
# def test_midpoint_boundary_rounds_to_even():
#     # TODO: Construct a timestamp at exactly 2:30 from a slot and verify the
#     # rounding direction matches the pandas documentation for the given
#     # pandas version in use.  Add a comment to the function docstring if the
#     # behaviour is surprising.
#     pass
#
#
# 4. NEGATIVE SIGNED OFFSETS
#    Readings that arrive slightly BEFORE their snapped slot (e.g. 23:59:59
#    rounding to 00:00:00 the next day) produce a negative raw_offset.
#    The K-pass dist = (raw_offsets - modal_val).abs() handles this correctly,
#    and value_counts() correctly identifies a negative modal.  Not tested.
#
# def test_negative_offset_column():
#     # TODO: Build a df where all readings for one column arrive 1 s before
#     # the slot boundary (offset = -1s) and verify snapping and modal = -1.
#     pass
#
#
# 5. INTRA-STREAM DUPLICATES (two readings from the same stream in one slot)
#    If the same sensor emits two readings within the same 5-min window (both
#    at +22s), both land in the same slot and have identical dist from modal.
#    The sort is stable in pandas, so groupby.first() takes the row that
#    appeared first in the input after sorting — effectively arbitrary.
#    Not a correctness issue (either value is equally valid) but worth noting
#    for cumulative columns where one of the two values may be a duplicate
#    transmission vs. a true new reading.
#
# def test_intra_stream_duplicate_readings():
#     # TODO: Add two rows at at(0, 22) and at(0, 22) for the same column.
#     # Assert that exactly one row is returned for slot(0), with either value
#     # (not both, not NA).  Consider logging a warning for this case in the
#     # implementation if it occurs frequently in real supplier data.
#     pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
