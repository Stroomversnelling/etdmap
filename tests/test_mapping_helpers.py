"""
Tests for etdmap.mapping_helpers utility functions.

Covers the functions that are most likely to be called by new supplier scripts:
  - rearrange_model_columns: column ordering, missing-column injection, extra columns
  - ensure_intervals: gap filling, already-correct input, excess rows
  - fill_down_infrequent_devices: forward/backward fill for known columns
  - run_standard_pipeline: ReadingDate validation, sorts, saves parquet

Functions that exercise heavy domain logic (add_diff_columns, validate_cumulative_variables,
apply_thresholds_to_df, snap_readings_to_grid) have dedicated test files or are covered
indirectly by run_standard_pipeline.
"""

import os
import pandas as pd
import pytest

from etdmap.mapping_helpers import (
    ensure_intervals,
    fill_down_infrequent_devices,
    rearrange_model_columns,
    run_standard_pipeline,
)
from etdmap.data_model import model_column_order, model_column_type


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _minimal_pipeline_df(n=3, freq="5min"):
    """Return a tiny DataFrame with ReadingDate and one cumulative column."""
    from etdmap.data_model import cumulative_columns
    col = cumulative_columns[0]
    dates = pd.date_range("2023-01-01", periods=n, freq=freq)
    return pd.DataFrame({"ReadingDate": dates, col: range(n)})


# ---------------------------------------------------------------------------
# rearrange_model_columns
# ---------------------------------------------------------------------------


class TestRearrangeModelColumns:
    def test_adds_missing_model_columns_when_add_is_true(self):
        df = pd.DataFrame({"ReadingDate": pd.date_range("2023-01-01", periods=2, freq="5min")})
        result = rearrange_model_columns(df, add_columns=True)
        for col in model_column_order:
            assert col in result.columns, f"Expected model column '{col}' to be added"

    def test_missing_columns_filled_with_na(self):
        df = pd.DataFrame({"ReadingDate": pd.date_range("2023-01-01", periods=2, freq="5min")})
        result = rearrange_model_columns(df, add_columns=True)
        model_cols_except_reading_date = [c for c in model_column_order if c != "ReadingDate"]
        for col in model_cols_except_reading_date:
            assert result[col].isna().all(), f"Newly added column '{col}' should be all NA"

    def test_extra_columns_appended_at_end(self):
        df = pd.DataFrame({
            "ReadingDate": pd.date_range("2023-01-01", periods=2, freq="5min"),
            "CustomSupplierCol": [1, 2],
        })
        result = rearrange_model_columns(df, add_columns=True)
        assert "CustomSupplierCol" in result.columns
        # Extra column should appear after all model columns
        model_end = max(result.columns.tolist().index(c) for c in model_column_order if c in result.columns)
        extra_idx = result.columns.tolist().index("CustomSupplierCol")
        assert extra_idx > model_end

    def test_add_columns_false_keeps_only_present_model_cols(self):
        df = pd.DataFrame({"ReadingDate": pd.date_range("2023-01-01", periods=2, freq="5min")})
        result = rearrange_model_columns(df, add_columns=False)
        # Only ReadingDate (a model column) should remain from model_column_order
        assert "ReadingDate" in result.columns
        # No other model columns that weren't in original df should appear
        non_reading_model = [c for c in model_column_order if c != "ReadingDate"]
        for col in non_reading_model:
            assert col not in result.columns

    def test_model_columns_in_correct_order(self):
        # Give it a scrambled set of model columns
        present = [c for c in model_column_order[:5]]
        df = pd.DataFrame({c: pd.array([None], dtype=model_column_type[c]) for c in reversed(present)})
        result = rearrange_model_columns(df, add_columns=False)
        result_order = [c for c in result.columns if c in model_column_order]
        expected_order = [c for c in model_column_order if c in present]
        assert result_order == expected_order


# ---------------------------------------------------------------------------
# ensure_intervals
# ---------------------------------------------------------------------------


class TestEnsureIntervals:
    def test_already_correct_returns_unchanged(self):
        dates = pd.date_range("2023-01-01", periods=5, freq="5min")
        df = pd.DataFrame({"ReadingDate": dates, "Value": range(5)})
        result = ensure_intervals(df)
        assert len(result) == 5

    def test_fills_missing_intervals(self):
        # Create a df with a gap: skip one 5-min slot
        dates = pd.date_range("2023-01-01", periods=5, freq="5min").tolist()
        dates.pop(2)  # remove one slot
        df = pd.DataFrame({"ReadingDate": dates, "Value": range(4)})
        result = ensure_intervals(df)
        # Should now have 5 rows (the missing slot added)
        assert len(result) == 5

    def test_filled_row_has_na_values(self):
        dates = pd.date_range("2023-01-01", periods=5, freq="5min").tolist()
        dates.pop(2)
        df = pd.DataFrame({"ReadingDate": dates, "Value": [1.0, 2.0, 4.0, 5.0]})
        result = ensure_intervals(df)
        gap_row = result[result["ReadingDate"] == pd.Timestamp("2023-01-01 00:10:00")]
        assert len(gap_row) == 1
        assert pd.isna(gap_row["Value"].iloc[0])

    def test_single_row_unchanged(self):
        df = pd.DataFrame({"ReadingDate": [pd.Timestamp("2023-01-01")], "V": [1.0]})
        result = ensure_intervals(df)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# fill_down_infrequent_devices
# ---------------------------------------------------------------------------


class TestFillDownInfrequentDevices:
    _FILL_COLS = (
        "ElektriciteitsgebruikBoilervat",
        "ElektriciteitsgebruikRadiator",
        "ElektriciteitsgebruikBooster",
    )

    def _make_df(self):
        col = self._FILL_COLS[0]
        return pd.DataFrame({
            col: [1.0, None, None, 2.0, None],
        })

    def test_forward_fills_gaps(self):
        col = self._FILL_COLS[0]
        df = self._make_df()
        result = fill_down_infrequent_devices(df, columns=self._FILL_COLS)
        assert result[col].iloc[1] == 1.0  # ffilled
        assert result[col].iloc[2] == 1.0  # ffilled

    def test_backward_fills_leading_na(self):
        col = self._FILL_COLS[0]
        df = pd.DataFrame({col: [None, None, 5.0]})
        result = fill_down_infrequent_devices(df, columns=self._FILL_COLS)
        assert result[col].iloc[0] == 5.0  # bfilled

    def test_all_na_replaced_with_zero(self):
        col = self._FILL_COLS[0]
        df = pd.DataFrame({col: [None, None, None]})
        result = fill_down_infrequent_devices(df, columns=self._FILL_COLS)
        assert (result[col] == 0.0).all()

    def test_ignores_unknown_columns(self):
        df = pd.DataFrame({"NotAFillCol": [None, None, 3.0]})
        result = fill_down_infrequent_devices(df, columns=self._FILL_COLS)
        # NotAFillCol is not in the fill list -- should remain unchanged
        assert pd.isna(result["NotAFillCol"].iloc[0])

    def test_custom_columns_parameter(self):
        df = pd.DataFrame({"MyCol": [None, None, 7.0]})
        result = fill_down_infrequent_devices(df, columns=("MyCol",))
        assert result["MyCol"].iloc[0] == 7.0


# ---------------------------------------------------------------------------
# run_standard_pipeline
# ---------------------------------------------------------------------------


class TestRunStandardPipeline:
    def test_raises_key_error_when_no_reading_date(self, tmp_path):
        df = pd.DataFrame({"SomeCol": [1, 2, 3]})
        with pytest.raises(KeyError, match="ReadingDate"):
            run_standard_pipeline(df, huis_code=1, huis_id="HuisA", mapped_folder_path=tmp_path)

    def test_raises_value_error_when_reading_date_unparseable(self, tmp_path):
        df = pd.DataFrame({"ReadingDate": ["bad", "also-bad"]})
        with pytest.raises((ValueError, Exception)):
            run_standard_pipeline(df, huis_code=1, huis_id="HuisA", mapped_folder_path=tmp_path)

    def test_saves_parquet_and_returns_entry(self, tmp_path):
        """End-to-end smoke test: pipeline writes a file and returns the index entry."""
        df = _minimal_pipeline_df(n=12)
        result = run_standard_pipeline(df, huis_code=42, huis_id="HuisX", mapped_folder_path=tmp_path)
        assert result == {"HuisIdLeverancier": "HuisX", "HuisIdBSV": 42}
        out_file = tmp_path / "household_42_table.parquet"
        assert out_file.exists()

    def test_output_sorted_by_reading_date(self, tmp_path):
        """Rows must be in ascending ReadingDate order in the saved file."""
        df = _minimal_pipeline_df(n=6)
        df = df.iloc[::-1].reset_index(drop=True)  # reverse the order
        run_standard_pipeline(df, huis_code=1, huis_id="H", mapped_folder_path=tmp_path)
        out = pd.read_parquet(tmp_path / "household_1_table.parquet")
        assert (out["ReadingDate"].diff().dropna() >= pd.Timedelta(0)).all()

    def test_output_has_model_columns(self, tmp_path):
        """Output parquet must contain all model columns."""
        df = _minimal_pipeline_df(n=6)
        run_standard_pipeline(df, huis_code=2, huis_id="H2", mapped_folder_path=tmp_path)
        out = pd.read_parquet(tmp_path / "household_2_table.parquet")
        for col in model_column_order:
            assert col in out.columns, f"Model column '{col}' missing from output"
