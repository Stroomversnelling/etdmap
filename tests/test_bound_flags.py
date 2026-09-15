"""Bound semantics: thresholds may be one-sided, and a bound that is not set
is no limit on that side."""

import pandas as pd
import pytest

from etdmap.flags import MappingFlag, bound_flags
from etdmap.mapping_helpers import apply_threshold_to_col

LOW = MappingFlag.OUT_OF_BOUNDS_LOW
HIGH = MappingFlag.OUT_OF_BOUNDS_HIGH
NONE = MappingFlag.NONE

VALUES = pd.array([-5.0, 0.0, 7.0, 20.0, pd.NA], dtype="Float64")


@pytest.fixture
def series():
    return pd.Series(VALUES)


class TestBoundFlags:
    def test_both_bounds_set(self, series):
        assert bound_flags(series, 0.0, 10.0).tolist() == [LOW, NONE, NONE, HIGH, NONE]

    def test_unset_upper_bound_is_no_upper_limit(self, series):
        assert bound_flags(series, 0.0, pd.NA).tolist() == [LOW, NONE, NONE, NONE, NONE]

    def test_unset_lower_bound_is_no_lower_limit(self, series):
        assert bound_flags(series, pd.NA, 10.0).tolist() == [NONE, NONE, NONE, HIGH, NONE]

    def test_no_bounds_set_is_the_same_as_no_threshold_row(self, series):
        assert bound_flags(series).tolist() == [NONE] * 5

    def test_a_value_can_carry_both_bits(self):
        # Min above Max is a broken threshold row, but the flags compose.
        flags = bound_flags(pd.Series(pd.array([5.0], dtype="Float64")), 10.0, 1.0)
        assert flags.tolist() == [LOW | HIGH]

    def test_flags_are_nullable_ints(self, series):
        assert str(bound_flags(series, 0.0, 10.0).dtype) == "Int64"


class TestApplyThresholdToColOneSided:
    def _frame(self):
        return pd.DataFrame({"x": VALUES})

    def test_unset_upper_bound_removes_only_low_values(self):
        out, stats = apply_threshold_to_col(self._frame(), "x", 0.0, pd.NA)
        assert out["x"].isna().tolist() == [True, False, False, False, True]
        assert stats["n_below"] == 1
        assert stats["n_above"] == 0

    def test_stats_record_which_bounds_were_checked(self):
        _, stats = apply_threshold_to_col(self._frame(), "x", 0.0, pd.NA)
        assert stats["lower_bound"] == 0.0
        # n_above is 0 because nothing was checked, not because nothing was found.
        assert pd.isna(stats["upper_bound"])

    def test_no_bounds_set_removes_nothing(self):
        out, stats = apply_threshold_to_col(self._frame(), "x", pd.NA, pd.NA)
        assert out["x"].tolist()[:4] == [-5.0, 0.0, 7.0, 20.0]
        assert stats is None
