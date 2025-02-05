import pytest

from etdmap.data_model import cumulative_columns
from etdmap.dataset_validators import dataset_flag_conditions, validate_columns
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


if __name__ == "__main__":
    # Run pytest for debugging the testing
    pytest.main(["-v"])
