import pytest

from etdmap.data_model import cumulative_columns
from etdmap.index_helpers import read_metadata
from etdmap.record_validators import (
    columns_5min_momentaan,
    record_flag_conditions,
)


def test_record_flag_conditions():
    """Test keys and values in record_flag_conditions.

    Tests:
    1. Whether testsfuncs for all datananalysis columns are present
    2. CHeck whether Diff columns are there for all cumulative columns
    3. Whether special checks are present:
        - "validate_reading_date_uniek"
        - "validate_300sec"
        - "validate_elektriciteitgebruik"
        - "validate_warmteproductie"
        - "validate_thresholds_combined"
    4. Wheter all values of dict are functions.
    """
    # check all cumulative column-checks are in keys
    assert all(col + "Diff" in record_flag_conditions for col in cumulative_columns)
    assert all('validate_' + col + 'Diff_outliers' in record_flag_conditions for col in cumulative_columns)

    # check all momentaal column-checks are in record-validator keys
    assert all('validate_' + col in record_flag_conditions for col in columns_5min_momentaan)
    special_checks = (
        "validate_reading_date_uniek",
        "validate_300sec",
        "validate_elektriciteitgebruik",
        "validate_warmteproductie",
        "validate_thresholds_combined",
    )
    # check if the extra checks are in record_flag_conditions
    assert all(check in record_flag_conditions for check in special_checks)

    # check if each value in dict is a function
    assert all(callable(value) for value in record_flag_conditions.values())

if __name__ == "__main__":
    # Run pytest for debugging the testing
    pytest.main(["-v"])
