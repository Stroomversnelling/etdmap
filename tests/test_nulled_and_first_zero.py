"""Blanking a value is an action and is recorded as one.

Before this, a condemned value carried only the signal that condemned it --
OUT_OF_BOUNDS_HIGH, CUMULATIVE_METER_RESET and so on -- and nothing said the
value had been removed. A reader could not tell a value the pipeline blanked
from one the supplier never sent.
"""

import pandas as pd
import pytest

from etdmap.flags import MappingFlag, flag_column
from etdmap.mapping_helpers import apply_threshold_to_col

COL = "ElektriciteitNetgebruik"
DIFF = "ElektriciteitNetgebruikDiff"


def _flags(df, column):
    name = flag_column(column)
    if name not in df.columns:
        return pd.Series(0, index=df.index, dtype="Int64")
    return df[name].fillna(0).astype("Int64")


class TestThresholdRemovalRecordsTheBlanking:
    def _frame(self):
        return pd.DataFrame({
            "HuisIdBSV": pd.array([1] * 4, dtype="Int64"),
            "ReadingDate": pd.date_range("2026-01-01", periods=4, freq="5min"),
            DIFF: pd.array([0.5, 99.0, 0.5, 0.5], dtype="Float64"),
        })

    def test_the_value_is_blanked(self):
        df, _ = apply_threshold_to_col(self._frame(), DIFF, 0.0, 1.5)
        assert pd.isna(df.loc[1, DIFF])

    def test_the_condition_is_recorded(self):
        df, _ = apply_threshold_to_col(self._frame(), DIFF, 0.0, 1.5)
        assert _flags(df, DIFF)[1] & MappingFlag.OUT_OF_BOUNDS_HIGH.value

    def test_the_blanking_is_recorded(self):
        df, _ = apply_threshold_to_col(self._frame(), DIFF, 0.0, 1.5)
        assert _flags(df, DIFF)[1] & MappingFlag.NULLED.value, (
            "the value was removed and nothing says so"
        )

    def test_untouched_rows_carry_neither(self):
        df, _ = apply_threshold_to_col(self._frame(), DIFF, 0.0, 1.5)
        flags = _flags(df, DIFF)
        for row in (0, 2, 3):
            assert not (flags[row] & MappingFlag.NULLED.value)
            assert not (flags[row] & MappingFlag.OUT_OF_BOUNDS_HIGH.value)

    def test_a_value_already_missing_is_not_marked_as_blanked(self):
        """NULLED means this pass removed it, not that it is absent."""
        frame = self._frame()
        frame.loc[2, DIFF] = pd.NA
        df, _ = apply_threshold_to_col(frame, DIFF, 0.0, 1.5)
        assert not (_flags(df, DIFF)[2] & MappingFlag.NULLED.value)


class TestNulledIsAnAction:
    def test_it_reaches_invented(self):
        assert MappingFlag.NULLED & MappingFlag.INVENTED

    def test_it_reaches_not_a_measurement(self):
        assert MappingFlag.NULLED & MappingFlag.NOT_A_MEASUREMENT


class TestFirstZeroMarksTheSeed:
    """add_diff_columns writes 0 at each household's first interval because
    differencing has no predecessor there. That zero is structural."""

    def test_the_seed_carries_the_flag(self):
        from etdmap.mapping_helpers import add_diff_columns

        df = pd.DataFrame({
            "HuisIdBSV": pd.array([1] * 4, dtype="Int64"),
            "ReadingDate": pd.date_range("2026-01-01", periods=4, freq="5min"),
            COL: pd.array([10.0, 10.5, 11.0, 11.5], dtype="Float64"),
        })
        out = add_diff_columns(df, id_column="HuisIdBSV")
        assert float(out.loc[0, DIFF]) == 0.0
        assert _flags(out, DIFF)[0] & MappingFlag.FIRST_ZERO.value, (
            "the seeded zero is not marked, so it reads as a measurement"
        )

    def test_later_rows_are_not_marked(self):
        from etdmap.mapping_helpers import add_diff_columns

        df = pd.DataFrame({
            "HuisIdBSV": pd.array([1] * 4, dtype="Int64"),
            "ReadingDate": pd.date_range("2026-01-01", periods=4, freq="5min"),
            COL: pd.array([10.0, 10.5, 11.0, 11.5], dtype="Float64"),
        })
        out = add_diff_columns(df, id_column="HuisIdBSV")
        flags = _flags(out, DIFF)
        for row in (1, 2, 3):
            assert not (flags[row] & MappingFlag.FIRST_ZERO.value)

    def test_each_household_gets_its_own_seed(self):
        from etdmap.mapping_helpers import add_diff_columns

        df = pd.DataFrame({
            "HuisIdBSV": pd.array([1, 1, 2, 2], dtype="Int64"),
            "ReadingDate": list(pd.date_range("2026-01-01", periods=2, freq="5min")) * 2,
            COL: pd.array([10.0, 10.5, 20.0, 20.5], dtype="Float64"),
        })
        out = add_diff_columns(df, id_column="HuisIdBSV")
        flags = _flags(out, DIFF)
        seeded = [i for i in out.index if flags[i] & MappingFlag.FIRST_ZERO.value]
        assert len(seeded) == 2, f"expected one seed per household, got {seeded}"

    def test_the_seed_is_excluded_from_the_average(self):
        assert MappingFlag.FIRST_ZERO & MappingFlag.NOT_A_MEASUREMENT
