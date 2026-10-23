"""``SLOT_NOT_DELIVERED`` is a fact about the slot, not about the column.

The grid is given -- it is the household batch's declared frequency -- so when
padding completes it, a slot the source delivered no row into is a finding in
its own right: the slot exists and nothing arrived. That is true of every column
on the frame at once, because no row arrived at all.

What the bit recorded instead was whether some earlier step happened to touch
the column. ``mark_undelivered_slots`` wrote it only where a flag cell already
held ``pd.NA``, and a flag cell can only hold ``pd.NA`` if the flag column
existed before the merge added rows. A column whose first finding comes later
gets its flag column created as ``NONE`` by ``set_flags`` and never receives the
bit -- for the identical physical situation. Two households with the same gap
then read differently depending on which columns an earlier step reached.

So the mask of created slots is the input, and the bit is written from it.
``flags.py``'s own end-of-pipeline guard records the symptom this fixes: on the
2026-10-21 estate run "the damaged set was exactly the columns flagged before
the grid padding".

Frames here are built in the test rather than loaded: the shape that matters is
a frame needing padding with one column flagged beforehand and one not, which no
committed fixture has.
"""

import pandas as pd
import pytest

from etdmap.flags import (
    MappingFlag,
    assert_flag_history_complete,
    flag_column,
    mark_undelivered_slots,
    set_flags,
)
from etdmap.mapping_helpers import ensure_intervals

GRID = "5min"
FLAGGED = "ElektriciteitsgebruikBoilervat"
UNFLAGGED = "ElektriciteitsgebruikWTW"

# Slots 2 and 3 of a five-slot grid are the ones no row is delivered into.
DELIVERED_POSITIONS = (0, 1, 4)
CREATED_POSITIONS = (2, 3)


def _delivered_frame():
    """Three readings on a five-slot 5-minute grid, so padding creates two.

    The gap is in the middle rather than at an edge so the created slots cannot
    be confused with the range ends that ``ensure_intervals`` derives the grid
    from.
    """
    slots = pd.date_range("2024-01-01 00:00", periods=5, freq=GRID)
    delivered = [slots[i] for i in DELIVERED_POSITIONS]
    return pd.DataFrame({
        "ReadingDate": delivered,
        FLAGGED: pd.array([1.0, 2.0, 5.0], dtype="Float64"),
        UNFLAGGED: pd.array([10.0, 20.0, 50.0], dtype="Float64"),
    })


def _with_one_column_flagged():
    """The asymmetry the defect turned on: one column already has a flag
    column before the grid is completed, the other has none."""
    frame = _delivered_frame()
    set_flags(
        frame,
        FLAGGED,
        pd.Series([True, False, False], index=frame.index),
        MappingFlag.SLOT_COLLISION,
    )
    assert flag_column(FLAGGED) in frame.columns
    assert flag_column(UNFLAGGED) not in frame.columns
    return frame


def _held(frame, column, position):
    return MappingFlag(int(frame[flag_column(column)].iloc[position]))


class TestEveryColumnRecordsTheUndeliveredSlot:
    def test_the_column_flagged_before_padding_records_it(self):
        padded = ensure_intervals(_with_one_column_flagged(), freq=GRID)
        for position in CREATED_POSITIONS:
            assert MappingFlag.SLOT_NOT_DELIVERED in _held(
                padded, FLAGGED, position
            )

    def test_the_column_not_flagged_before_padding_records_it_too(self):
        """The defect. Identical slot, identical absence, and before the fix
        this column has no flag column at all -- so the finding that the grid
        held a slot nothing arrived in was recorded for one column and not the
        other, decided by an earlier step neither column controls."""
        padded = ensure_intervals(_with_one_column_flagged(), freq=GRID)
        assert flag_column(UNFLAGGED) in padded.columns
        for position in CREATED_POSITIONS:
            assert MappingFlag.SLOT_NOT_DELIVERED in _held(
                padded, UNFLAGGED, position
            )

    def test_the_two_columns_agree_on_which_slots_were_undelivered(self):
        """Stated as the invariant rather than per column, because that is the
        property: the bit describes the row."""
        padded = ensure_intervals(_with_one_column_flagged(), freq=GRID)
        bit = MappingFlag.SLOT_NOT_DELIVERED.value
        marked = {
            column: set(
                padded.index[
                    (padded[flag_column(column)].fillna(0).astype("Int64") & bit) != 0
                ]
            )
            for column in (FLAGGED, UNFLAGGED)
        }
        assert marked[FLAGGED] == marked[UNFLAGGED]
        assert marked[FLAGGED] == set(CREATED_POSITIONS)

    def test_delivered_slots_are_not_marked(self):
        padded = ensure_intervals(_with_one_column_flagged(), freq=GRID)
        for position in DELIVERED_POSITIONS:
            for column in (FLAGGED, UNFLAGGED):
                assert MappingFlag.SLOT_NOT_DELIVERED not in _held(
                    padded, column, position
                )

    def test_a_finding_written_before_the_padding_survives_it(self):
        padded = ensure_intervals(_with_one_column_flagged(), freq=GRID)
        assert MappingFlag.SLOT_COLLISION in _held(padded, FLAGGED, 0)

    def test_the_padded_frame_carries_a_complete_flag_history(self):
        padded = ensure_intervals(_with_one_column_flagged(), freq=GRID)
        assert_flag_history_complete(padded)


class TestNothingIsMarkedWhereNothingWasCreated:
    def test_a_frame_already_on_its_grid_gains_no_findings(self):
        """``ensure_intervals`` returns early when the row count already
        matches, so no slot was created and there is nothing to record. The bit
        must not appear, and a flag column must not be invented for a column
        that has none."""
        slots = pd.date_range("2024-01-01 00:00", periods=5, freq=GRID)
        frame = pd.DataFrame({
            "ReadingDate": slots,
            FLAGGED: pd.array([1.0, 2.0, 3.0, 4.0, 5.0], dtype="Float64"),
            UNFLAGGED: pd.array([10.0, 20.0, 30.0, 40.0, 50.0], dtype="Float64"),
        })
        result = ensure_intervals(frame, freq=GRID)
        assert flag_column(UNFLAGGED) not in result.columns
        assert flag_column(FLAGGED) not in result.columns


class TestTheCreatedMaskIsTheInput:
    """``mark_undelivered_slots`` takes the mask rather than inferring it.

    Inferring from ``pd.NA`` is what made the bit depend on flag-column
    creation order. The mask comes from the merge, which knows exactly which
    slots it added.
    """

    def test_the_mask_decides_which_rows_are_marked(self):
        frame = _delivered_frame()
        created = pd.Series([False, True, False], index=frame.index)
        mark_undelivered_slots(frame, created=created)
        assert MappingFlag.SLOT_NOT_DELIVERED in _held(frame, FLAGGED, 1)
        assert MappingFlag.SLOT_NOT_DELIVERED not in _held(frame, FLAGGED, 0)

    def test_every_data_column_is_marked_from_one_mask(self):
        frame = _delivered_frame()
        created = pd.Series([False, True, False], index=frame.index)
        mark_undelivered_slots(frame, created=created)
        for column in (FLAGGED, UNFLAGGED):
            assert MappingFlag.SLOT_NOT_DELIVERED in _held(frame, column, 1)

    def test_an_empty_mask_writes_nothing(self):
        frame = _delivered_frame()
        created = pd.Series([False, False, False], index=frame.index)
        mark_undelivered_slots(frame, created=created)
        assert flag_column(FLAGGED) not in frame.columns

    def test_without_a_mask_it_still_repairs_a_missing_history(self):
        """The fallback path, kept because a frame can be assembled outside
        ``ensure_intervals`` and arrive with pd.NA in a flag column. There the
        damage is all that is known, so it is all that can be recorded."""
        frame = _with_one_column_flagged()
        extra = pd.DataFrame({
            "ReadingDate": [pd.Timestamp("2024-01-01 00:25")],
            FLAGGED: pd.array([6.0], dtype="Float64"),
            UNFLAGGED: pd.array([60.0], dtype="Float64"),
        })
        joined = pd.concat([frame, extra], ignore_index=True)
        assert joined[flag_column(FLAGGED)].isna().any()
        mark_undelivered_slots(joined)
        assert MappingFlag.SLOT_NOT_DELIVERED in _held(joined, FLAGGED, 3)
        assert_flag_history_complete(joined)
