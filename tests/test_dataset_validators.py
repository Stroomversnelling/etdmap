import pandas as pd
import pytest

from etdmap.data_model import cumulative_columns
from etdmap.dataset_validators import (
    dataset_flag_conditions,
    validate_columns,
    validate_cumm_thesholds,
)
from etdmap.index_helpers import read_metadata


def test_validate_columns(valid_metadata_file):
    """Basic test for validate_columns with a valid condition function."""
    def condition_func(df):
        return df["num_column"] > 0

    test_df = read_metadata(valid_metadata_file)
    result = validate_columns(test_df, ["num_column"], condition_func)
    assert result is True

def test_dataset_flag_conditions():
    """Test keys and values in dataset_flag_conditions.

    Tests:
    1. Whether testsfuncs for all cumulative columns are present
    2. Whether special checks are present:
        - "validate_monitoring_data_counts"
        - "validate_energiegebruik_warmteopwekker"
        - "validate_approximately_one_year_of_records"
        - "validate_columns_exist"
        - "validate_no_readingdate_gap"
    3. Wheter all values are functions.
    """
    assert all("validate_" + col in dataset_flag_conditions for col in cumulative_columns)
    assert all("validate_" + col + "Diff" in dataset_flag_conditions for col in cumulative_columns)

    special_checks = (
        "validate_monitoring_data_counts",
        "validate_energiegebruik_warmteopwekker", 
        "validate_approximately_one_year_of_records",
        "validate_columns_exist",
        "validate_no_readingdate_gap"
    )
    assert all(check in dataset_flag_conditions for check in special_checks)

    # check if each value in dict is a function
    assert all(callable(value) for value in dataset_flag_conditions.values())


# ---------------------------------------------------------------------------
# validate_cumm_thesholds: per-interval diff must fall within [Min, Max].
# Fail-path coverage with crafted input independent of thresholds.csv.
# ---------------------------------------------------------------------------

class TestValidateCummThresholds:
    _TH = {"X": {"Min": 0.0, "Max": 1.0}}

    def test_within_bounds_true(self):
        # cumulative -> diffs 0.5, 0.5, 0.5 all within [0, 1]
        df = pd.DataFrame({"X": pd.array([0.0, 0.5, 1.0, 1.5], dtype="Float64")})
        assert validate_cumm_thesholds(df, "X", self._TH) is True

    def test_out_of_bounds_false(self):
        # diffs of 2.0 exceed Max=1.0
        df = pd.DataFrame({"X": pd.array([0.0, 2.0, 4.0], dtype="Float64")})
        assert validate_cumm_thesholds(df, "X", self._TH) is False

    def test_all_na_returns_na(self):
        df = pd.DataFrame({"X": pd.array([pd.NA, pd.NA, pd.NA], dtype="Float64")})
        assert validate_cumm_thesholds(df, "X", self._TH) is pd.NA


if __name__ == "__main__":
    # Run pytest for debugging the testing
    pytest.main(["-v"])
