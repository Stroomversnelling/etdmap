import pandas as pd
import pytest

import etdmap.record_validators as record_validators
from etdmap.data_model import cumulative_columns, interval_column
from etdmap.index_helpers import read_metadata
from etdmap.record_validators import (
    columns_with_value_bounds,
    record_flag_conditions,
    validate_thresholds_combined,
)


def test_record_flag_conditions():
    """Test keys and values in record_flag_conditions.

    Tests:
    1. Whether testsfuncs for all datananalysis columns are present
    2. CHeck whether Diff columns are there for all cumulative columns
    3. Whether special checks are present:
        - "validate_reading_date_unique"
        - "validate_observed_spacing"
        - "validate_elektriciteitgebruik"
        - "validate_warmteproductie"
        - "validate_thresholds_combined"
    4. Wheter all values of dict are functions.
    """
    # check all cumulative column-checks are in keys
    # The interval column is the one the data model links to the cumulative
    # variable, not its name plus 'Diff'.
    assert all(
        'validate_' + interval_column(col) + '_outliers' in record_flag_conditions
        for col in cumulative_columns
    )

    # check all momentaal column-checks are in record-validator keys
    assert all('validate_' + col in record_flag_conditions for col in columns_with_value_bounds)
    special_checks = (
        "validate_reading_date_unique",
        "validate_observed_spacing",
        "validate_elektriciteitgebruik",
        "validate_warmteproductie",
        "validate_thresholds_combined",
    )
    # check if the extra checks are in record_flag_conditions
    assert all(check in record_flag_conditions for check in special_checks)

    # check if each value in dict is a function
    assert all(callable(value) for value in record_flag_conditions.values())


# ---------------------------------------------------------------------------
# Which ThresholdTypes get a per-column bound check. Pins the rule rather than
# today's thresholds.csv, so it holds as types are added to the data model.
# ---------------------------------------------------------------------------

class TestWhichThresholdsGetAPerColumnCheck:
    CHECKED_PER_READING = {"5-minute", "momentaan", "algemeen"}

    def test_selection_matches_the_rule_exactly(self):
        thresholds = record_validators.thresholds_df
        selected = set(columns_with_value_bounds)
        for _, row in thresholds.iterrows():
            expected = row["ThresholdType"] in self.CHECKED_PER_READING
            assert (row["Variabele"] in selected) is expected, row["Variabele"]

    def test_a_cumulative_total_is_not_checked_per_reading(self):
        """A meter total is checked by the cumulative-diff category instead."""
        thresholds = record_validators.thresholds_df
        cumulative = thresholds[thresholds["ThresholdType"] == "cumulatief"]["Variabele"]
        assert not set(cumulative) & set(columns_with_value_bounds)

    def test_every_selected_column_got_a_validator(self):
        assert all(
            "validate_" + col in record_flag_conditions
            for col in columns_with_value_bounds
        )


# ---------------------------------------------------------------------------
# validate_thresholds_combined: per-row, flags whether all values are within
# their [Min, Max]. Reads the module-level `thresholds_dict` captured at
# import, so monkeypatch that attribute (not thresholds.csv). Fail-path
# coverage with crafted out-of-bound input.
# ---------------------------------------------------------------------------

class TestValidateThresholdsCombined:
    def test_flags_out_of_bounds_row(self, monkeypatch):
        monkeypatch.setattr(record_validators, "thresholds_dict", {"X": {"Min": 0.0, "Max": 1.0}})
        df = pd.DataFrame({"X": pd.array([0.5, 5.0, 0.2], dtype="Float64")})
        result = validate_thresholds_combined(df)
        # True = within bounds, False = out of bounds
        assert result.tolist() == [True, False, True]

    def test_all_na_row_is_na(self, monkeypatch):
        monkeypatch.setattr(record_validators, "thresholds_dict", {"X": {"Min": 0.0, "Max": 1.0}})
        df = pd.DataFrame({"X": pd.array([pd.NA, 0.5], dtype="Float64")})
        result = validate_thresholds_combined(df)
        assert pd.isna(result.iloc[0])
        assert bool(result.iloc[1]) is True


if __name__ == "__main__":
    # Run pytest for debugging the testing
    pytest.main(["-v"])
