"""The two mapping actions on values mapping did not compute.

FILLED_ZERO: a whole column written as zero because the device is not
installed. NOT_CHANGED: a value examined and left as delivered, recorded so an
empty findings column cannot be read two ways.
"""

import pandas as pd

from etdmap.flags import MappingFlag, flag_column
from etdmap.mapping_helpers import (
    fill_zeros_for_device_not_installed,
    mark_unchanged_values,
)

COL = "Zon-opwekTotaal"


def _frame(values=None):
    return pd.DataFrame({
        "ReadingDate": pd.date_range("2026-01-01", periods=4, freq="5min"),
        COL: pd.array([pd.NA] * 4 if values is None else values, dtype="Float64"),
    })


class TestFilledZero:
    def test_a_missing_device_column_is_zeroed_and_flagged(self):
        df = fill_zeros_for_device_not_installed(_frame(), columns=[COL])
        assert df[COL].astype(float).tolist() == [0.0] * 4
        flags = df[flag_column(COL)]
        assert ((flags & MappingFlag.FILLED_ZERO.value) != 0).all(), (
            "an invented zero is indistinguishable from a reported one "
            "without the flag"
        )

    def test_a_delivered_column_is_left_alone(self):
        df = fill_zeros_for_device_not_installed(
            _frame([1.0, 2.0, 3.0, 4.0]), columns=[COL],
        )
        assert df[COL].astype(float).tolist() == [1.0, 2.0, 3.0, 4.0]
        assert flag_column(COL) not in df.columns


class TestAPairThatDeliversBlocksTheZeroFill:
    """"Not installed" is an assertion the data can falsify: a delivered
    value in the column's linked pair is the device reporting, so the column
    is left for the pipeline to construct rather than zeroed."""

    CUM = "ElektriciteitNetgebruik"
    DIFF = "ElektriciteitNetgebruikDiff"

    def _frame(self, **cols):
        n = 4
        data = {
            "ReadingDate": pd.date_range("2026-01-01", periods=n, freq="5min"),
        }
        for name, values in cols.items():
            data[name] = pd.array(values, dtype="Float64")
        return pd.DataFrame(data)

    def test_a_delivered_interval_keeps_its_absent_cumulative_open(self):
        df = fill_zeros_for_device_not_installed(
            self._frame(**{self.DIFF: [0.5, 0.5, pd.NA, 0.5]}),
            columns=[self.CUM],
        )
        assert self.CUM not in df.columns
        assert flag_column(self.CUM) not in df.columns

    def test_a_delivered_interval_keeps_an_all_na_cumulative_open(self):
        df = fill_zeros_for_device_not_installed(
            self._frame(**{
                self.CUM: [pd.NA] * 4,
                self.DIFF: [0.5, 0.5, 0.5, 0.5],
            }),
            columns=[self.CUM],
        )
        assert df[self.CUM].isna().all()
        assert flag_column(self.CUM) not in df.columns

    def test_the_mirror_direction_holds_too(self):
        # An interval column listed for zeros while its cumulative delivers.
        df = fill_zeros_for_device_not_installed(
            self._frame(**{self.CUM: [10.0, 10.5, 11.0, 11.5]}),
            columns=[self.DIFF],
        )
        assert self.DIFF not in df.columns
        assert flag_column(self.DIFF) not in df.columns

    def test_an_empty_pair_does_not_block_the_fill(self):
        df = fill_zeros_for_device_not_installed(
            self._frame(**{self.DIFF: [pd.NA] * 4}),
            columns=[self.CUM],
        )
        assert df[self.CUM].astype(float).tolist() == [0.0] * 4
        flags = df[flag_column(self.CUM)]
        assert ((flags & MappingFlag.FILLED_ZERO.value) != 0).all()


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
