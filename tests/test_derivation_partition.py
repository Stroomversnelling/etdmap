"""Deriving an interval has two outcomes, and every cell it wrote records one.

``add_diff_columns`` assigns the interval column in a single statement, so every
cell of it was written by the derivation. Two things can come out:

====================================== ======================================
``VALUE_DERIVED_FROM_CUMULATIVE``      the difference was computable
``NULL_DERIVED_FROM_CUMULATIVE``       the difference came out ``pd.NA``
====================================== ======================================

Exactly one holds of every cell the derivation touched. Before the split only
the value case was recorded, so an empty cell in a derived column was
indistinguishable from an empty cell in a column no step ever reached -- and on
a tariff split interval, which no supplier delivers, that was *every* cell: the
column read as untouched when in fact it had been computed and had produced
nothing.

Row 0 is the same gap by a different route. ``.diff()`` has no predecessor
there, so the derivation produced ``pd.NA`` and the seed then wrote ``0`` over
it. Both happened; the cell records both.

The name drops "DELIVERED" deliberately. The derivation also runs on an all-NA
cumulative column, where claiming a delivery would be false --
``RAW_DELIVERED_VALUE`` on the cumulative is what says what the supplier sent.

Synthetic frames only.
"""

import pandas as pd

from etdmap.flags import MappingFlag, flag_column
from etdmap.mapping_helpers import add_diff_columns

GRID = 5
CUM = "ElektriciteitsgebruikBoilervat"
DIFF = f"{CUM}Diff"

#: A register with a hole in it, so the differencing produces both outcomes:
#: rows 1 and 4 are computable, rows 2 and 3 are not (one operand is absent),
#: and row 0 has no predecessor at all.
VALUES = [10.0, 11.0, pd.NA, 13.0, 14.0]
EXPECT_VALUE_AT = (1, 4)
EXPECT_NULL_AT = (0, 2, 3)

DERIVATION_ANSWERS = (
    MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE,
    MappingFlag.NULL_DERIVED_FROM_CUMULATIVE,
)


def _derived():
    frame = pd.DataFrame({
        "ReadingDate": pd.date_range("2026-01-01", periods=len(VALUES), freq="5min"),
        CUM: pd.array(VALUES, dtype="Float64"),
    })
    return add_diff_columns(frame, context="derivation partition")


def _answers(out, position):
    held = MappingFlag(int(out[flag_column(DIFF)].iloc[position]))
    return [answer for answer in DERIVATION_ANSWERS if answer in held]


class TestTheTwoOutcomesPartitionEveryWrittenCell:
    def test_every_cell_holds_exactly_one_outcome(self):
        """The contract. No cell of a column the derivation wrote is silent."""
        out = _derived()
        for position in range(len(VALUES)):
            assert len(_answers(out, position)) == 1, (
                f"row {position} holds {_answers(out, position)}"
            )

    def test_a_computable_difference_records_the_value_outcome(self):
        out = _derived()
        for position in EXPECT_VALUE_AT:
            assert _answers(out, position) == [
                MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE
            ]

    def test_an_absent_operand_records_the_null_outcome(self):
        """Rows 2 and 3: the register is absent on one side of the difference,
        so nothing could be computed. The cell is empty because the derivation
        produced nothing, which is what it now says."""
        out = _derived()
        for position in (2, 3):
            assert _answers(out, position) == [
                MappingFlag.NULL_DERIVED_FROM_CUMULATIVE
            ]
            assert out[DIFF].iloc[position] is pd.NA


class TestTheSeededFirstCell:
    def test_it_records_both_the_null_derivation_and_the_seed(self):
        """Two things happened to row 0 and both are true of it: the
        differencing reached it and produced nothing, and the seed then wrote a
        zero. Recording only the seed left the derivation's visit unrecorded."""
        out = _derived()
        held = MappingFlag(int(out[flag_column(DIFF)].iloc[0]))
        assert MappingFlag.NULL_DERIVED_FROM_CUMULATIVE in held
        assert MappingFlag.FIRST_ZERO in held

    def test_the_seeded_value_is_still_written(self):
        out = _derived()
        assert float(out[DIFF].iloc[0]) == 0.0

    def test_it_does_not_claim_a_value_was_derived(self):
        out = _derived()
        held = MappingFlag(int(out[flag_column(DIFF)].iloc[0]))
        assert MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE not in held

    def test_the_seed_is_still_excluded_from_measurement(self):
        """FIRST_ZERO carries that, and the null outcome must not quietly
        change it: the cell holds an invented zero whatever else is recorded."""
        out = _derived()
        held = MappingFlag(int(out[flag_column(DIFF)].iloc[0]))
        assert held & MappingFlag.NOT_A_MEASUREMENT


class TestTheCompositeIdentifiesADerivedColumn:
    """A caller asking "was this column derived?" must not have to name one
    outcome. Testing the value member alone would read a column that produced
    only nulls as never derived, which is the case the split exists for."""

    def test_the_composite_is_the_two_outcomes(self):
        assert MappingFlag.INTERVAL_DERIVED_FROM_CUMULATIVE == (
            MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE
            | MappingFlag.NULL_DERIVED_FROM_CUMULATIVE
        )

    def test_every_written_cell_matches_the_composite(self):
        out = _derived()
        bits = out[flag_column(DIFF)].fillna(0).astype("Int64")
        assert (
            (bits & MappingFlag.INTERVAL_DERIVED_FROM_CUMULATIVE.value) != 0
        ).all()

    def test_a_column_that_derived_nothing_but_nulls_still_reads_as_derived(
        self,
    ):
        """An all-NA register: the derivation runs, writes nothing but nulls,
        and the column is still a derived column. Under the old single member
        it carried no finding at all and read as untouched."""
        frame = pd.DataFrame({
            "ReadingDate": pd.date_range("2026-01-01", periods=4, freq="5min"),
            CUM: pd.array([pd.NA] * 4, dtype="Float64"),
        })
        out = add_diff_columns(frame, context="all-NA register")
        bits = out[flag_column(DIFF)].fillna(0).astype("Int64")
        assert (
            (bits & MappingFlag.INTERVAL_DERIVED_FROM_CUMULATIVE.value) != 0
        ).all()
        assert not (
            (bits & MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE.value) != 0
        ).any()
