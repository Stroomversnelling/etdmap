"""NOT_CHANGED: a cell that holds a value and carries no recorded action.

Recorded so an empty findings column cannot be read two ways -- as a value
mapping left alone, or as a column no step reached.

It is set over the same population ``mark_delivery_and_cadence`` marks: every
data column of the model. An empty cell is never marked, because there is no
value in it to leave alone; which kind of absence it is comes from
ON_CADENCE_NA and OFF_CADENCE_NA instead.
"""

import pandas as pd

from etdmap.flags import MappingFlag, flag_column
from etdmap.mapping_helpers import mark_unchanged_values

COL = "ZonopwekTotaal"


def _frame(values=None):
    return pd.DataFrame({
        "ReadingDate": pd.date_range("2026-01-01", periods=4, freq="5min"),
        COL: pd.array([pd.NA] * 4 if values is None else values, dtype="Float64"),
    })


class TestNotChanged:
    def test_values_no_action_touched_are_marked(self):
        df = _frame([1.0, 2.0, 3.0, 4.0])
        df[flag_column(COL)] = pd.Series(
            [MappingFlag.FILLED_DOWN.value, MappingFlag.NONE.value,
             MappingFlag.OUT_OF_BOUNDS_HIGH.value, MappingFlag.NONE.value],
            dtype="Int64",
        )
        mark_unchanged_values(df)
        flags = df[flag_column(COL)]
        marked = (flags & MappingFlag.NOT_CHANGED.value) != 0

        # FILLED_DOWN is an action taken; OUT_OF_BOUNDS_HIGH is a signal, and a
        # signal without an action means the value itself was left alone.
        assert marked.tolist() == [False, True, True, True]
        assert (flags[0] & MappingFlag.FILLED_DOWN.value) != 0, (
            "marking must add to the findings, not replace them"
        )

    def test_an_empty_cell_is_never_marked(self):
        """ensure_intervals pads every grid slot the supplier did not report
        in, and a column reporting more slowly than the grid is empty between
        its ticks. Marking those would claim an inspection of nothing."""
        df = _frame([1.0, pd.NA, 3.0, pd.NA])
        mark_unchanged_values(df)
        marked = (df[flag_column(COL)] & MappingFlag.NOT_CHANGED.value) != 0
        assert marked.tolist() == [True, False, True, False]

    def test_a_column_with_no_findings_column_still_gets_one(self):
        """The case the flag exists for. A column no step made a finding about
        is exactly the one whose empty findings column was ambiguous, so it
        must not be the one the marking skips."""
        df = _frame([1.0, 2.0, 3.0, 4.0])
        assert flag_column(COL) not in df.columns
        mark_unchanged_values(df)
        assert flag_column(COL) in df.columns
        marked = (df[flag_column(COL)] & MappingFlag.NOT_CHANGED.value) != 0
        assert marked.all()

    def test_a_column_outside_the_data_model_is_not_marked(self):
        """The marking spans the model, so a stray column carries no claim
        about it. rearrange_model_columns has already dropped these by the
        time the pipeline marks."""
        df = _frame([1.0, 2.0, 3.0, 4.0])
        df["NotAModelColumn"] = pd.array([1.0] * 4, dtype="Float64")
        mark_unchanged_values(df)
        assert flag_column("NotAModelColumn") not in df.columns
