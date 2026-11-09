"""What the supplier delivered into a cell has three answers, and exactly one
applies.

| answer | condition |
|---|---|
| ``RAW_DELIVERED_VALUE`` | a row arrived at this slot and carried a value |
| ``DELIVERED_NA`` | a row arrived and carried nothing for this column |
| ``SLOT_NOT_DELIVERED`` | no row arrived at this slot |

They are not three findings that happen to be related; they are one question
partitioned. The partition is the contract, and a cell carrying two of them or
none of them is a defect.

Two of the answers are written on the frame as delivered, by ``mark_delivered``.
The third is written at padding, by ``mark_undelivered_slots``, because its cells
do not exist before the padding creates them -- a physical necessity, not a split
in the question.

Why this matters: before it, a cell that the supplier simply left empty was
indistinguishable from a slot no row arrived in, and from a column no step ever
reached. Delivery completeness was measured from a flag that answered a cadence
question instead, and the one flag that did speak about delivery was written at
step 6b, after ``fill_down_infrequent_devices`` had already put values into the
gaps -- which is why it needed to subtract ``INVENTED`` to undo its own
lateness.

Frames are built in the test. The shape that matters -- a delivered value, a
delivered-but-empty cell and a padded slot in one frame -- is not in any
committed fixture.
"""

import pandas as pd

from etdmap.flags import (
    MappingFlag,
    assert_flag_history_complete,
    flag_column,
    set_flags,
)
from etdmap.mapping_helpers import ensure_intervals, mark_delivered

GRID = "5min"
FULL = "ElektriciteitsgebruikBoilervat"
PARTIAL = "ElektriciteitsgebruikWTW"

DELIVERED_POSITIONS = (0, 1, 4)
CREATED_POSITIONS = (2, 3)

#: The three answers, as one set, so a test can ask how many of them a cell
#: holds without listing them again.
DELIVERY_ANSWERS = (
    MappingFlag.RAW_DELIVERED_VALUE,
    MappingFlag.DELIVERED_NA,
    MappingFlag.SLOT_NOT_DELIVERED,
)


def _delivered_frame():
    """Three readings on a five-slot grid. ``PARTIAL`` is empty on the second
    of them, which is the delivered-but-empty case."""
    slots = pd.date_range("2024-01-01 00:00", periods=5, freq=GRID)
    return pd.DataFrame({
        "ReadingDate": [slots[i] for i in DELIVERED_POSITIONS],
        FULL: pd.array([1.0, 2.0, 5.0], dtype="Float64"),
        PARTIAL: pd.array([10.0, pd.NA, 50.0], dtype="Float64"),
    })


def _marked_and_padded():
    frame = _delivered_frame()
    mark_delivered(frame)
    return ensure_intervals(frame, freq=GRID)


def _answers(frame, column, position):
    held = MappingFlag(int(frame[flag_column(column)].iloc[position]))
    return [answer for answer in DELIVERY_ANSWERS if answer in held]


class TestTheThreeAnswersPartitionEveryCell:
    def test_every_cell_holds_exactly_one_answer(self):
        """The contract, stated as the invariant rather than case by case."""
        padded = _marked_and_padded()
        for column in (FULL, PARTIAL):
            for position in range(len(padded)):
                assert len(_answers(padded, column, position)) == 1, (
                    f"{column} row {position} holds "
                    f"{_answers(padded, column, position)}"
                )

    def test_a_delivered_value_reads_as_delivered(self):
        padded = _marked_and_padded()
        assert _answers(padded, FULL, 0) == [MappingFlag.RAW_DELIVERED_VALUE]

    def test_a_delivered_row_with_an_empty_cell_reads_as_delivered_na(self):
        """The case nothing expressed before: the row arrived, and the supplier
        carried no value for this column in it."""
        padded = _marked_and_padded()
        assert _answers(padded, PARTIAL, 1) == [MappingFlag.DELIVERED_NA]

    def test_a_slot_no_row_arrived_in_reads_as_not_delivered(self):
        padded = _marked_and_padded()
        for position in CREATED_POSITIONS:
            for column in (FULL, PARTIAL):
                assert _answers(padded, column, position) == [
                    MappingFlag.SLOT_NOT_DELIVERED
                ]

    def test_the_two_absences_are_distinguishable(self):
        """The whole point. Both cells are empty; they are empty for different
        reasons, and the reasons no longer read alike."""
        padded = _marked_and_padded()
        assert padded[PARTIAL].iloc[1] is pd.NA
        assert padded[PARTIAL].iloc[2] is pd.NA
        assert _answers(padded, PARTIAL, 1) != _answers(padded, PARTIAL, 2)

    def test_the_padded_frame_carries_a_complete_flag_history(self):
        assert_flag_history_complete(_marked_and_padded())


class TestMarkingTheDeliveredFrame:
    def test_it_creates_a_flag_column_for_every_markable_column(self):
        frame = _delivered_frame()
        mark_delivered(frame)
        for column in (FULL, PARTIAL):
            assert flag_column(column) in frame.columns

    def test_it_does_not_flag_the_date_column(self):
        frame = _delivered_frame()
        mark_delivered(frame)
        assert flag_column("ReadingDate") not in frame.columns

    def test_a_column_that_delivered_nothing_is_still_answered(self):
        """It reads DELIVERED_NA throughout rather than carrying no finding at
        all. An empty findings column used to be the only record of a column
        the supplier never populated, which is indistinguishable from a column
        no step reached."""
        frame = _delivered_frame()
        frame["Gasgebruik"] = pd.array([pd.NA] * 3, dtype="Float64")
        mark_delivered(frame)
        for position in range(3):
            assert _answers(frame, "Gasgebruik", position) == [
                MappingFlag.DELIVERED_NA
            ]

    def test_findings_already_on_the_frame_survive(self):
        frame = _delivered_frame()
        set_flags(
            frame,
            FULL,
            pd.Series([True, False, False], index=frame.index),
            MappingFlag.SLOT_COLLISION,
        )
        mark_delivered(frame)
        held = MappingFlag(int(frame[flag_column(FULL)].iloc[0]))
        assert MappingFlag.SLOT_COLLISION in held
        assert MappingFlag.RAW_DELIVERED_VALUE in held

    def test_a_combined_tariff_root_is_not_delivered(self):
        """The supplier sent no root value, so the delivery answer stays
        DELIVERED_NA and TARIFF_COMBINED is what says a value was written there
        from the registers that were sent. Two facts, both true, neither
        standing in for the other.

        RAW_DELIVERED_VALUE used to land on these cells, because delivery was
        marked at pipeline step 6b -- by which time the combine had already
        populated the root. Marking on the frame as delivered separates them.
        """
        from etdmap.mapping_helpers import combine_tariff_registers

        slots = pd.date_range("2024-01-01 00:00", periods=3, freq=GRID)
        frame = pd.DataFrame({
            "ReadingDate": slots,
            "ElektriciteitNetgebruikHoog": pd.array(
                [1.0, 2.0, 3.0], dtype="Float64"
            ),
            "ElektriciteitNetgebruikLaag": pd.array(
                [4.0, 5.0, 6.0], dtype="Float64"
            ),
        })
        mark_delivered(frame)
        combine_tariff_registers(frame)

        root = "ElektriciteitNetgebruik"
        held = MappingFlag(int(frame[flag_column(root)].iloc[0]))
        assert MappingFlag.TARIFF_COMBINED in held
        assert MappingFlag.DELIVERED_NA in held
        assert MappingFlag.RAW_DELIVERED_VALUE not in held
        # Not delivered is not the same as not measured: the value carries what
        # the registers reported, so it stays in a project average.
        assert not (held & MappingFlag.NOT_A_MEASUREMENT)
        # And the registers themselves were delivered.
        for register in ("ElektriciteitNetgebruikHoog",
                         "ElektriciteitNetgebruikLaag"):
            assert _answers(frame, register, 0) == [
                MappingFlag.RAW_DELIVERED_VALUE
            ]

    def test_it_initialises_a_missing_history_rather_than_refusing_it(self):
        """A multi-sheet merge adds rows to a frame that already carries flag
        columns, leaving pd.NA in them. ``set_flags`` refuses that by design, so
        marking delivery has to initialise first -- and the row it initialises
        is a row that arrived with this column empty, which is exactly
        DELIVERED_NA. That subsumes the repair the merge used to need."""
        frame = _delivered_frame()
        set_flags(
            frame,
            FULL,
            pd.Series([True, False, False], index=frame.index),
            MappingFlag.SLOT_COLLISION,
        )
        extra = pd.DataFrame({
            "ReadingDate": [pd.Timestamp("2024-01-01 00:25")],
            FULL: pd.array([pd.NA], dtype="Float64"),
            PARTIAL: pd.array([60.0], dtype="Float64"),
        })
        joined = pd.concat([frame, extra], ignore_index=True)
        assert joined[flag_column(FULL)].isna().any()

        mark_delivered(joined)

        assert _answers(joined, FULL, 3) == [MappingFlag.DELIVERED_NA]
        assert _answers(joined, PARTIAL, 3) == [MappingFlag.RAW_DELIVERED_VALUE]
        assert_flag_history_complete(joined)
