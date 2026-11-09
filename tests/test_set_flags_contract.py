"""The contract of ``set_flags`` itself, which nothing was testing.

``set_flags`` is the only writer of a ``<column>_flags`` column, so every
finding in the mapped artifact passes through it. It appears in seven test
files and eight call sites, every one of them using it to ARRANGE a fixture
before testing something else -- so its own behaviour was asserted nowhere.

That gap had a cost. ``set_flags`` records a finding with
``df.loc[mask, name] = df.loc[mask, name] | flag.value``, and in pandas
nullable arithmetic ``pd.NA | 2048`` is ``pd.NA``. On any cell whose flag
column already held pd.NA the write was therefore **silently swallowed** -- no
error, no record, the bit simply absent. Measured on the 2026-10-21 estate
mapping: 19,552,205 flag cells held pd.NA across 52 of 88 flag columns, and on
772,643 of them the cell carried a real value whose ``FILLED_DOWN`` bit was
lost, so an invented zero read as a delivered measurement and entered its
project average.

A flag column is the pipeline describing its own actions, not a measurement, so
there is no state of genuine ignorance for pd.NA to represent: the pipeline
always knows what it did. pd.NA in a flag column is therefore not a value to
interpret but a broken invariant -- some step added rows to a frame already
carrying the column without recording anything on them -- and the safe response
at a write is to refuse.

Where the grid is COMPLETED, the answer is not ``NONE`` either. The grid is
given -- it is the household batch's declared frequency, not something a stage
invents -- so what happened is that a slot exists and the source delivered
nothing into it. That says why the value is absent, and belongs in the cell's
history, so ``mark_undelivered_slots`` records ``SLOT_NOT_DELIVERED``. ``NONE``
would claim nothing happened here. Setting a bit also replaces the pd.NA, so
recording the signal is what makes every later finding on those cells
recordable at all.

The test that matters most here is the INTERACTION one: write a flag, add rows,
write again. No existing test has that shape, because the suite is organised
per step and a unit test builds one frame and flags it. The defect lives
between two steps.
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

COLUMN = "ElektriciteitsgebruikBoilervat"
FLAGS = flag_column(COLUMN)


def _frame(values=(1.0, 2.0, 3.0)):
    return pd.DataFrame({
        "ReadingDate": pd.date_range("2024-01-01", periods=len(values), freq="5min"),
        COLUMN: pd.array(values, dtype="Float64"),
    })


def _mask(frame, *positions):
    return pd.Series(
        [i in positions for i in range(len(frame))], index=frame.index
    )


class TestTheColumnIsCreatedEmpty:
    def test_it_is_created_on_first_use(self):
        frame = _frame()
        assert FLAGS not in frame.columns
        set_flags(frame, COLUMN, _mask(frame, 0), MappingFlag.NULLED)
        assert FLAGS in frame.columns

    def test_unflagged_cells_read_none_not_missing(self):
        # The distinction this whole file exists for: a cell nothing was
        # recorded about holds 0, never pd.NA.
        frame = _frame()
        set_flags(frame, COLUMN, _mask(frame, 0), MappingFlag.NULLED)
        assert frame[FLAGS].notna().all()
        assert int(frame[FLAGS].iloc[1]) == MappingFlag.NONE.value

    def test_the_column_is_nullable_integer(self):
        frame = _frame()
        set_flags(frame, COLUMN, _mask(frame, 0), MappingFlag.NULLED)
        assert frame[FLAGS].dtype == "Int64"


class TestFindingsAccumulate:
    def test_a_second_flag_is_ored_in_not_assigned_over(self):
        frame = _frame()
        set_flags(frame, COLUMN, _mask(frame, 0), MappingFlag.OUT_OF_BOUNDS_LOW)
        set_flags(frame, COLUMN, _mask(frame, 0), MappingFlag.NULLED)
        held = MappingFlag(int(frame[FLAGS].iloc[0]))
        assert MappingFlag.OUT_OF_BOUNDS_LOW in held
        assert MappingFlag.NULLED in held

    def test_a_flag_is_never_cleared(self):
        # A flag column is the cell's history; bits only ever OR in. Where a
        # later step undoes an earlier one the answer is to ADD the action
        # recording it, never to remove the earlier bit.
        frame = _frame()
        set_flags(frame, COLUMN, _mask(frame, 0), MappingFlag.FILLED_DOWN)
        set_flags(frame, COLUMN, _mask(frame, 0), MappingFlag.NULLED)
        assert MappingFlag.FILLED_DOWN in MappingFlag(int(frame[FLAGS].iloc[0]))

    def test_an_unmasked_cell_is_untouched(self):
        frame = _frame()
        set_flags(frame, COLUMN, _mask(frame, 0), MappingFlag.NULLED)
        assert int(frame[FLAGS].iloc[1]) == MappingFlag.NONE.value
        assert int(frame[FLAGS].iloc[2]) == MappingFlag.NONE.value

    def test_a_missing_value_in_the_mask_selects_nothing(self):
        # The mask comes from the caller and may be the result of a comparison
        # against pd.NA, so it is the one NA set_flags already tolerated.
        frame = _frame((1.0, pd.NA, 3.0))
        mask = frame[COLUMN] > 2.0
        set_flags(frame, COLUMN, mask, MappingFlag.OUT_OF_BOUNDS_HIGH)
        assert int(frame[FLAGS].iloc[0]) == MappingFlag.NONE.value
        assert int(frame[FLAGS].iloc[1]) == MappingFlag.NONE.value
        assert MappingFlag.OUT_OF_BOUNDS_HIGH in MappingFlag(int(frame[FLAGS].iloc[2]))


class TestAWriteOntoAMissingHistoryIsRefused:
    """pd.NA in the flag column means a step added rows without initialising
    them, and the write that follows would be swallowed in silence.

    Reproduces the estate defect exactly: the row-adding step here is the same
    kind of left merge onto a complete grid that ``ensure_intervals`` performs,
    which is where the pd.NA came from in production.
    """

    @staticmethod
    def _flagged_then_padded():
        """A frame flagged BEFORE rows were added, as the mappers produce it.

        Snapping sets SLOT_COLLISION and so creates the flag column; the grid
        padding that follows merges new rows in, and every column already on
        the frame -- the flag column included -- is pd.NA on them.
        """
        frame = _frame()
        set_flags(frame, COLUMN, _mask(frame, 0), MappingFlag.SLOT_COLLISION)
        grid = pd.DataFrame({
            "ReadingDate": pd.date_range(
                "2024-01-01", periods=len(frame) + 2, freq="5min"
            )
        })
        padded = pd.merge(grid, frame, on="ReadingDate", how="left")
        assert padded[FLAGS].isna().any(), "the premise: padding leaves pd.NA"
        return padded

    def test_the_padding_really_does_leave_a_missing_history(self):
        # Guards the premise rather than the behaviour: if a future pandas or a
        # changed merge stops producing pd.NA here, the tests below would pass
        # for the wrong reason.
        padded = self._flagged_then_padded()
        assert int(padded[FLAGS].isna().sum()) == 2

    def test_writing_onto_a_missing_history_raises(self):
        padded = self._flagged_then_padded()
        # fill_down_infrequent_devices writes a value into a padded row and
        # records FILLED_DOWN on it. That is the write that vanished.
        with pytest.raises(ValueError, match=r"(?i)flag"):
            set_flags(
                padded, COLUMN, _mask(padded, 3, 4), MappingFlag.FILLED_DOWN
            )

    def test_the_message_names_the_column_and_the_count(self):
        padded = self._flagged_then_padded()
        with pytest.raises(ValueError) as raised:
            set_flags(
                padded, COLUMN, _mask(padded, 3, 4), MappingFlag.FILLED_DOWN
            )
        message = str(raised.value)
        assert FLAGS in message
        assert "2" in message

    def test_a_write_clear_of_the_missing_rows_is_allowed(self):
        """The division of labour between the two guards.

        ``set_flags`` checks only the rows it is writing, so its cost is
        proportional to the mask rather than the frame -- it runs on every flag
        write. A write that lands clear of the damage therefore succeeds, and
        the damage elsewhere is caught by
        :func:`assert_flag_history_complete`, which scans every column once at
        the end. Nothing escapes; it is detected at the end rather than here.
        """
        padded = self._flagged_then_padded()
        set_flags(padded, COLUMN, _mask(padded, 0), MappingFlag.NULLED)
        assert MappingFlag.NULLED in MappingFlag(int(padded[FLAGS].iloc[0]))
        with pytest.raises(ValueError, match=FLAGS):
            assert_flag_history_complete(padded)

    def test_recording_the_absent_delivery_accepts_the_write(self):
        # The companion direction, and the shape of the fix: once the completed
        # grid records SLOT_NOT_DELIVERED on the bare cells, the write lands --
        # and the cell then carries both facts, the absent delivery and the
        # fill that followed it.
        padded = self._flagged_then_padded()
        mark_undelivered_slots(padded)
        set_flags(padded, COLUMN, _mask(padded, 3, 4), MappingFlag.FILLED_DOWN)
        held = MappingFlag(int(padded[FLAGS].iloc[3]))
        assert MappingFlag.FILLED_DOWN in held
        assert MappingFlag.SLOT_NOT_DELIVERED in held
        # And the flag written before the padding survived it.
        assert MappingFlag.SLOT_COLLISION in MappingFlag(int(padded[FLAGS].iloc[0]))
        assert_flag_history_complete(padded)


class TestTheEndOfPipelineScan:
    """One scan over every flag column, so a column no later write touches
    cannot carry a missing history to disk unnoticed.

    This is the completeness half. On the 2026-10-21 estate run, 52 flag
    columns held pd.NA and only 4 of them ever saw a later write -- so a guard
    that only fires on write would have reported 4 and let 48 through.
    """

    def test_a_clean_frame_passes(self):
        frame = _frame()
        set_flags(frame, COLUMN, _mask(frame, 0), MappingFlag.NULLED)
        assert_flag_history_complete(frame)

    def test_a_frame_with_no_flag_columns_passes(self):
        # Nothing to check is not a failure: a frame before any finding is
        # recorded is legitimate.
        assert_flag_history_complete(_frame())

    def test_a_data_column_may_hold_missing_values(self):
        # The check is about flag columns only. Missingness in a DATA column is
        # a first-class value and must not be flagged as damage.
        frame = _frame((1.0, pd.NA, 3.0))
        set_flags(frame, COLUMN, _mask(frame, 0), MappingFlag.NULLED)
        assert_flag_history_complete(frame)

    def test_it_names_every_damaged_column_and_its_count(self):
        frame = _frame()
        other = "WatergebruikWarmTapwater"
        set_flags(frame, COLUMN, _mask(frame, 0), MappingFlag.SLOT_COLLISION)
        set_flags(frame, other, _mask(frame, 0), MappingFlag.SLOT_COLLISION)
        grid = pd.DataFrame({
            "ReadingDate": pd.date_range(
                "2024-01-01", periods=len(frame) + 2, freq="5min"
            )
        })
        padded = pd.merge(grid, frame, on="ReadingDate", how="left")

        with pytest.raises(ValueError) as raised:
            assert_flag_history_complete(padded)
        message = str(raised.value)
        # Per column, because which columns are damaged is the finding: it
        # identifies the step, and an aggregate count does not.
        assert FLAGS in message
        assert flag_column(other) in message
        assert "2" in message

    def test_padding_the_grid_initialises_the_rows_it_adds(self):
        """The cause, fixed where it happens.

        ``ensure_intervals`` pads a household onto its complete grid with a
        merge, and every column already on the frame is pd.NA on the rows it
        adds -- flag columns included. That is where the 19,552,205 missing
        histories on the 2026-10-21 estate run came from, and the reason the
        damaged set was exactly the columns flagged before the padding.

        A row the padding creates genuinely has an empty history, and NONE is
        how an empty history is written. So this is initialisation, not an
        interpretation of pd.NA.
        """
        from etdmap.mapping_helpers import ensure_intervals

        # Readings at 0, 5 and 15 minutes: the slot at 10 is missing, so the
        # padding has a row to add.
        frame = pd.DataFrame({
            "ReadingDate": pd.to_datetime([
                "2024-01-01 00:00", "2024-01-01 00:05", "2024-01-01 00:15",
            ]),
            COLUMN: pd.array([1.0, 2.0, 3.0], dtype="Float64"),
        })
        set_flags(frame, COLUMN, _mask(frame, 0), MappingFlag.SLOT_COLLISION)

        padded = ensure_intervals(frame, freq="5min")

        assert len(padded.index) == 4, "the premise: a row was added"
        assert_flag_history_complete(padded)
        # The slot says why it is empty: the source delivered no row into it.
        # Not NONE, which would claim nothing happened here.
        added = padded[padded[COLUMN].isna()]
        assert len(added.index) == 1
        assert MappingFlag.SLOT_NOT_DELIVERED in MappingFlag(int(added[FLAGS].iloc[0]))
        # A signal, so it must not reach the composite a project average
        # filters on -- the absence describes the delivery, not a value.
        assert not (MappingFlag.SLOT_NOT_DELIVERED & MappingFlag.NOT_A_MEASUREMENT)
        # And a slot the source DID deliver into carries no such claim, with
        # the finding recorded before the padding intact.
        held = MappingFlag(int(padded[FLAGS].iloc[0]))
        assert MappingFlag.SLOT_COLLISION in held
        assert MappingFlag.SLOT_NOT_DELIVERED not in held

    def test_the_context_is_carried_into_the_message(self):
        # The scan runs per household, so the message has to say which one.
        frame = _frame()
        set_flags(frame, COLUMN, _mask(frame, 0), MappingFlag.SLOT_COLLISION)
        frame.loc[1, FLAGS] = pd.NA
        with pytest.raises(ValueError, match="HuisIdBSV 42"):
            assert_flag_history_complete(frame, context="HuisIdBSV 42")
