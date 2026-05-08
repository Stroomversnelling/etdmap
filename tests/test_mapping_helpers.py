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

import json
from unittest.mock import patch

from etdmap.data_model import load_unit_map, model_column_order, model_column_type
from etdmap.mapping_helpers import (
    _STATS_DTYPES,
    _cast_stats_dtypes,
    _seasonal_slices,
    collect_column_stats,
    collect_mapped_data_stats,
    ensure_intervals,
    expand_tz_columns,
    fill_down_infrequent_devices,
    fill_zeros_for_device_not_installed,
    rearrange_model_columns,
    run_standard_pipeline,
)


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
# fill_zeros_for_device_not_installed
# ---------------------------------------------------------------------------


class TestFillZerosForDeviceNotInstalled:
    _COLS = ("ElektriciteitsgebruikRadiator", "ElektriciteitsgebruikWTW")

    def test_adds_missing_column_as_zero(self):
        df = pd.DataFrame({"Other": pd.array([1.0, 2.0], dtype="Float64")})
        result = fill_zeros_for_device_not_installed(df, columns=self._COLS)
        for col in self._COLS:
            assert col in result.columns
            assert (result[col] == 0).all()
            assert result[col].dtype == "Float64"

    def test_fills_all_na_column_with_zero(self):
        df = pd.DataFrame({
            "ElektriciteitsgebruikRadiator": pd.array([pd.NA, pd.NA, pd.NA], dtype="Float64")
        })
        result = fill_zeros_for_device_not_installed(df, columns=("ElektriciteitsgebruikRadiator",))
        assert (result["ElektriciteitsgebruikRadiator"] == 0).all()

    def test_does_not_overwrite_existing_values(self):
        df = pd.DataFrame({
            "ElektriciteitsgebruikRadiator": pd.array([1.0, 2.0, 3.0], dtype="Float64")
        })
        result = fill_zeros_for_device_not_installed(df, columns=("ElektriciteitsgebruikRadiator",))
        assert list(result["ElektriciteitsgebruikRadiator"]) == [1.0, 2.0, 3.0]

    def test_does_not_overwrite_partial_data(self):
        df = pd.DataFrame({
            "ElektriciteitsgebruikRadiator": pd.array([pd.NA, 5.0, pd.NA], dtype="Float64")
        })
        result = fill_zeros_for_device_not_installed(df, columns=("ElektriciteitsgebruikRadiator",))
        assert result["ElektriciteitsgebruikRadiator"].iloc[1] == 5.0
        assert pd.isna(result["ElektriciteitsgebruikRadiator"].iloc[0])

    def test_output_dtype_is_float64_nullable(self):
        df = pd.DataFrame({"Other": [1]})
        result = fill_zeros_for_device_not_installed(df, columns=("ElektriciteitsgebruikWTW",))
        assert str(result["ElektriciteitsgebruikWTW"].dtype) == "Float64"


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
        result.pop("_validation_summary", None)
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


# ---------------------------------------------------------------------------
# collect_column_stats
# ---------------------------------------------------------------------------


class TestCollectColumnStats:
    """The stats collector must return one stable type per key, so the
    downstream cast in _cast_stats_dtypes never falls back to object."""

    def test_numeric_float64_populates_numeric_stats(self):
        s = pd.Series(pd.array([1.0, 2.0, 3.0, 4.0, pd.NA], dtype="Float64"), name="x")
        out = collect_column_stats("hh1", s)
        assert out["count"] == 4
        assert out["missing"] == 1
        assert out["min"] == 1.0
        assert out["max"] == 4.0
        assert out["mean"] == 2.5
        assert out["median"] == 2.5
        assert out["quantile_25"] == 1.75
        assert out["quantile_75"] == 3.25
        assert out["iqr"] == 1.5
        assert pd.isna(out["min_datetime"]) and pd.isna(out["max_datetime"])
        assert pd.isna(out["top5"])

    def test_bool_column_populates_min_max_as_zero_one(self):
        """The 'ever fired' filter target: bool max == 1 means the validator
        triggered at least once."""
        s = pd.Series(pd.array([True, False, False, True, pd.NA], dtype="boolean"), name="flag")
        out = collect_column_stats("hh1", s)
        assert out["min"] == 0.0
        assert out["max"] == 1.0
        assert out["mean"] == 0.5
        assert pd.isna(out["std"])

    def test_bool_all_false_max_is_zero(self):
        """The 'never fired' case: bool max == 0 means the validator
        never triggered. This is what users filter against to find
        'ever-failed' columns by inverting the predicate."""
        s = pd.Series(pd.array([False, False, False], dtype="boolean"), name="flag")
        out = collect_column_stats("hh1", s)
        assert out["min"] == 0.0
        assert out["max"] == 0.0

    def test_datetime_tz_aware_writes_to_min_max_datetime(self):
        s = pd.Series(
            pd.to_datetime(["2024-01-01 10:00", "2024-01-02 22:00"]).tz_localize("Europe/Amsterdam"),
            name="ts",
        )
        out = collect_column_stats("hh1", s)
        assert pd.isna(out["min"]) and pd.isna(out["max"])
        # Both Amsterdam timestamps converted to UTC-naive
        assert out["min_datetime"] == pd.Timestamp("2024-01-01 09:00")
        assert out["max_datetime"] == pd.Timestamp("2024-01-02 21:00")

    def test_datetime_naive_passthrough(self):
        s = pd.Series(pd.to_datetime(["2024-01-01", "2024-01-05"]), name="ts")
        out = collect_column_stats("hh1", s)
        assert out["min_datetime"] == pd.Timestamp("2024-01-01")
        assert out["max_datetime"] == pd.Timestamp("2024-01-05")

    def test_object_column_top5_is_json(self):
        s = pd.Series(["a", "a", "b", "c", "a", "b"], name="cat")
        out = collect_column_stats("hh1", s)
        assert isinstance(out["top5"], str)
        parsed = json.loads(out["top5"])
        assert parsed == {"a": 3, "b": 2, "c": 1}
        assert pd.isna(out["min"]) and pd.isna(out["max"])

    def test_all_na_returns_all_na_stats(self):
        s = pd.Series(pd.array([pd.NA, pd.NA, pd.NA], dtype="Float64"), name="x")
        out = collect_column_stats("hh1", s)
        assert out["count"] == 0
        assert out["missing"] == 3
        for k in ("min", "max", "mean", "std", "median", "iqr",
                  "quantile_25", "quantile_75", "top5"):
            assert pd.isna(out[k]), f"{k} should be NA, got {out[k]!r}"
        assert pd.isna(out["min_datetime"]) and pd.isna(out["max_datetime"])

    def test_type_field_is_string(self):
        """type must be the stringified dtype, never a dtype object,
        so .astype('string') downstream succeeds."""
        s = pd.Series(pd.array([1.0], dtype="Float64"), name="x")
        out = collect_column_stats("hh1", s)
        assert out["type"] == "Float64"
        assert isinstance(out["type"], str)


# ---------------------------------------------------------------------------
# _cast_stats_dtypes  (regression test for the object-dtype min/max bug)
# ---------------------------------------------------------------------------


class TestGetStatsDtypes:
    """The schema cast must produce nullable dtypes everywhere, even when
    different rows populate different stat keys (the bug get_data_stats
    was created to fix)."""

    def test_mixed_rows_become_typed_columns(self):
        # Row 1: numeric column. Row 2: datetime column. Row 3: object.
        rows = [
            collect_column_stats(
                "hh1",
                pd.Series(pd.array([1.0, 2.0, 3.0], dtype="Float64"), name="num"),
            ),
            collect_column_stats(
                "hh1",
                pd.Series(pd.to_datetime(["2024-01-01", "2024-01-02"]), name="ts"),
            ),
            collect_column_stats(
                "hh1",
                pd.Series(["a", "b", "a"], name="cat"),
            ),
        ]
        df = pd.DataFrame(rows)
        df = _cast_stats_dtypes(df)
        assert str(df["min"].dtype) == "Float64"
        assert str(df["max"].dtype) == "Float64"
        assert str(df["mean"].dtype) == "Float64"
        assert str(df["count"].dtype) == "Int64"
        assert str(df["missing"].dtype) == "Int64"
        assert str(df["type"].dtype) == "string"
        assert str(df["top5"].dtype) == "string"
        # min_datetime / max_datetime are proper datetime64[ns]
        assert df["min_datetime"].dtype.kind == "M"
        assert df["max_datetime"].dtype.kind == "M"

    def test_dtype_contract_keys_present(self):
        """Every key in _STATS_DTYPES (except those added by wrapping
        layers) is a column collect_column_stats produces. Catches drift
        between the two. 'season' is intentionally injected by
        collect_mapped_data_stats / process_raw_data_file, not by
        collect_column_stats itself."""
        out = collect_column_stats("hh1", pd.Series([1.0, 2.0], name="x"))
        externally_injected = {"season"}
        for key in _STATS_DTYPES.keys():
            if key in externally_injected:
                continue
            assert key in out, f"{key!r} declared in _STATS_DTYPES but absent from collect_column_stats output"

    def test_csv_round_trip_preserves_numeric_min_max(self, tmp_path):
        """End-to-end: a stats DataFrame written to CSV and read back
        must keep numeric dtypes. This is the bug we are fixing."""
        rows = [
            collect_column_stats(
                "hh1",
                pd.Series(pd.array([1.0, 2.0, 3.0], dtype="Float64"), name="x"),
            ),
        ]
        df = _cast_stats_dtypes(pd.DataFrame(rows))
        path = tmp_path / "stats.csv"
        df.to_csv(path, index=False)
        loaded = pd.read_csv(path)
        # The min / max columns must be numeric on round-trip, not object.
        assert loaded["min"].dtype.kind in "fi"
        assert loaded["max"].dtype.kind in "fi"


# ---------------------------------------------------------------------------
# expand_tz_columns
# ---------------------------------------------------------------------------


class TestExpandTzColumns:
    """Per-row default is the safe path; vectorized=True is the opt-in
    fast path that requires uniformity and refuses to guess."""

    def _mk_object_col_mixed_tz(self):
        # Per-row varying tzinfo: Amsterdam, UTC, naive(None)
        return pd.Series(
            [
                pd.Timestamp("2024-01-01 10:00", tz="Europe/Amsterdam"),
                pd.Timestamp("2024-01-01 09:00", tz="UTC"),
                None,
            ],
            dtype=object,
            name="rd",
        )

    def _mk_object_col_uniform_tz(self):
        return pd.Series(
            [
                pd.Timestamp("2024-01-01 10:00", tz="Europe/Amsterdam"),
                pd.Timestamp("2024-01-02 11:00", tz="Europe/Amsterdam"),
                None,
            ],
            dtype=object,
            name="rd",
        )

    def test_per_row_default_handles_mixed_tz(self):
        df = pd.DataFrame({"rd": self._mk_object_col_mixed_tz()})
        out = expand_tz_columns(df)
        assert "rd_TZ" in out.columns
        assert "rd_UTC_naive" in out.columns
        assert out["rd_TZ"].iloc[0] == "Europe/Amsterdam"
        assert out["rd_TZ"].iloc[1] == "UTC"
        assert pd.isna(out["rd_TZ"].iloc[2])
        # Both rows convert to the same UTC instant (09:00)
        assert out["rd_UTC_naive"].iloc[0] == pd.Timestamp("2024-01-01 09:00")
        assert out["rd_UTC_naive"].iloc[1] == pd.Timestamp("2024-01-01 09:00")
        assert pd.isna(out["rd_UTC_naive"].iloc[2])

    def test_datetime64_tz_dtype_passes_through_vectorised(self):
        s = pd.to_datetime(["2024-01-01 10:00", "2024-01-02 11:00"]).tz_localize("Europe/Amsterdam")
        df = pd.DataFrame({"rd": s})
        out = expand_tz_columns(df)
        assert "rd_TZ" in out.columns
        assert "rd_UTC_naive" in out.columns
        assert out["rd_TZ"].iloc[0] == "Europe/Amsterdam"
        assert out["rd_UTC_naive"].iloc[0] == pd.Timestamp("2024-01-01 09:00")

    def test_datetime64_naive_unchanged(self):
        s = pd.to_datetime(["2024-01-01", "2024-01-02"])
        df = pd.DataFrame({"rd": s})
        out = expand_tz_columns(df)
        # No expansion when the column has no tz to strip
        assert "rd_TZ" not in out.columns
        assert "rd_UTC_naive" not in out.columns
        assert (out["rd"] == df["rd"]).all()

    def test_vectorized_uniform_matches_per_row(self):
        df = pd.DataFrame({"rd": self._mk_object_col_uniform_tz()})
        per_row = expand_tz_columns(df, vectorized=False)
        vector = expand_tz_columns(df, vectorized=True)
        assert (per_row["rd_TZ"].fillna("__NA__") == vector["rd_TZ"].fillna("__NA__")).all()
        assert (per_row["rd_UTC_naive"].fillna(pd.Timestamp(0)) ==
                vector["rd_UTC_naive"].fillna(pd.Timestamp(0))).all()

    def test_vectorized_mixed_tz_raises(self):
        df = pd.DataFrame({"rd": self._mk_object_col_mixed_tz()})
        with pytest.raises(ValueError, match=r"rd.*uniform tzinfo"):
            expand_tz_columns(df, vectorized=True)

    def test_non_object_non_datetime_columns_passthrough(self):
        df = pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        out = expand_tz_columns(df)
        assert list(out.columns) == ["x", "y"]
        assert (out["x"] == df["x"]).all()


# ---------------------------------------------------------------------------
# load_unit_map
# ---------------------------------------------------------------------------


class TestLoadUnitMap:
    """The unit map is the project-wide single source of truth for column
    units. Plot helpers and reports rely on it."""

    def test_known_columns_have_expected_units(self):
        m = load_unit_map()
        assert m["ElektriciteitNetgebruikHoog"] == "kWh"
        assert m["WarmteproductieWarmtepomp"] == "GJ"
        assert m["Gasgebruik"] == "m3"

    def test_diff_columns_inherit_parent_unit(self):
        """Diff variants are listed explicitly in thresholds.csv and must
        carry the same unit as their parent cumulative column."""
        m = load_unit_map()
        assert m["ElektriciteitNetgebruikHoogDiff"] == m["ElektriciteitNetgebruikHoog"]

    def test_unknown_column_returns_none_via_get(self):
        m = load_unit_map()
        assert m.get("ThisColumnDoesNotExist") is None


# ---------------------------------------------------------------------------
# Seasonal split (collect_mapped_data_stats / get_data_stats)
# ---------------------------------------------------------------------------


def _year_of_data():
    """Synthetic household-style DataFrame: one row per hour for a full
    calendar year, with one numeric column, one bool column, and one object
    column. The numeric column's value equals the month so seasonal slicing
    is verifiable from the stats."""
    rd = pd.date_range("2024-01-01", "2024-12-31 23:00", freq="h")
    months = rd.month
    return pd.DataFrame({
        "ReadingDate": rd,
        "month_value": pd.array(months.astype(float), dtype="Float64"),
        "always_true": pd.array([True] * len(rd), dtype="boolean"),
        "label":       pd.array(["x"] * len(rd), dtype="string"),
    })


class TestSeasonalSlices:
    def test_seasonal_false_returns_only_annual(self):
        df = _year_of_data()
        out = _seasonal_slices(df, seasonal=False)
        assert [s for s, _ in out] == ["annual"]

    def test_seasonal_true_returns_three_slices(self):
        df = _year_of_data()
        out = _seasonal_slices(df, seasonal=True)
        names = [s for s, _ in out]
        assert names == ["annual", "cold", "warm"]
        cold_df = dict(out)["cold"]
        warm_df = dict(out)["warm"]
        # Cold = Oct-Apr (months 10,11,12,1,2,3,4) = 7 months out of 12.
        # 2024 is a leap year: (Jan 31 + Feb 29 + Mar 31 + Apr 30 + Oct 31
        # + Nov 30 + Dec 31) * 24 hours = 213 * 24 = 5112.
        assert len(cold_df) == 5112
        # Warm = May-Sep = (May 31 + Jun 30 + Jul 31 + Aug 31 + Sep 30) * 24
        # = 153 * 24 = 3672.
        assert len(warm_df) == 3672
        assert len(cold_df) + len(warm_df) == len(df)

    def test_seasonal_true_without_reading_date_falls_back(self):
        """No ReadingDate means seasonal slicing is impossible; the helper
        must degrade to annual-only rather than raising."""
        df = pd.DataFrame({"x": pd.array([1.0, 2.0], dtype="Float64")})
        out = _seasonal_slices(df, seasonal=True)
        assert [s for s, _ in out] == ["annual"]

    def test_custom_seasons_dict_overrides_default(self):
        """Callers can pass an arbitrary {season_name: month_set} mapping
        to replace the Netherlands cold/warm default."""
        df = _year_of_data()
        custom = {
            "Q1": {1, 2, 3}, "Q2": {4, 5, 6},
            "Q3": {7, 8, 9}, "Q4": {10, 11, 12},
        }
        out = _seasonal_slices(df, seasonal=True, seasons=custom)
        names = [s for s, _ in out]
        assert names == ["annual", "Q1", "Q2", "Q3", "Q4"]
        # Each quarter should have ~3 months of hourly data.
        sliced = dict(out)
        # 2024 leap year: Jan 31 + Feb 29 + Mar 31 = 91 days = 2184 hours
        assert len(sliced["Q1"]) == 91 * 24
        # Apr 30 + May 31 + Jun 30 = 91 days
        assert len(sliced["Q2"]) == 91 * 24

    def test_custom_seasons_annual_key_is_reserved(self):
        """If the caller's dict includes 'annual', that entry is ignored
        (the auto-emitted annual slice covers it)."""
        df = _year_of_data()
        custom = {"annual": {1, 2, 3}, "winter": {12, 1, 2}}
        out = _seasonal_slices(df, seasonal=True, seasons=custom)
        names = [s for s, _ in out]
        assert names == ["annual", "winter"]
        # The auto-annual slice still spans the whole year, not just the
        # caller's 'annual' month set.
        assert len(dict(out)["annual"]) == 8784  # leap-year hours


class TestCollectMappedDataStatsSeasonal:
    """The seasonal kwarg must produce the right row shapes:
    three rows per numeric / bool column, one row per non-numeric."""

    def _patched_get_mapped_data(self, df):
        return patch(
            "etdmap.mapping_helpers.get_mapped_data",
            return_value=df,
        )

    def test_default_is_single_annual_row_per_column(self):
        df = _year_of_data()
        with self._patched_get_mapped_data(df):
            rows = collect_mapped_data_stats("hh1")
        # One row per source column (4 columns: ReadingDate, month_value,
        # always_true, label).
        assert len(rows) == 4
        assert all(r["season"] == "annual" for r in rows)

    def test_seasonal_true_emits_three_rows_for_numeric_and_bool(self):
        df = _year_of_data()
        with self._patched_get_mapped_data(df):
            rows = collect_mapped_data_stats("hh1", seasonal=True)
        by_col = {}
        for r in rows:
            by_col.setdefault(r["column"], []).append(r["season"])
        # Numeric and bool columns: three seasons each.
        assert sorted(by_col["month_value"]) == ["annual", "cold", "warm"]
        assert sorted(by_col["always_true"]) == ["annual", "cold", "warm"]
        # Datetime / object columns: only annual.
        assert by_col["ReadingDate"] == ["annual"]
        assert by_col["label"] == ["annual"]

    def test_seasonal_annual_row_matches_unsplit_baseline(self):
        """The annual slice must produce stats identical to seasonal=False."""
        df = _year_of_data()
        with self._patched_get_mapped_data(df):
            baseline = collect_mapped_data_stats("hh1")
            seasonal = collect_mapped_data_stats("hh1", seasonal=True)
        baseline_by_col = {r["column"]: r for r in baseline}
        seasonal_annual_by_col = {
            r["column"]: r for r in seasonal if r["season"] == "annual"
        }
        for col in baseline_by_col:
            b = baseline_by_col[col]
            s = seasonal_annual_by_col[col]
            assert b["count"] == s["count"]
            # Numeric stats: compare with NA-tolerant equality.
            for k in ("min", "max", "mean", "median"):
                bv, sv = b[k], s[k]
                if pd.isna(bv) and pd.isna(sv):
                    continue
                assert bv == sv, f"{col}.{k}: baseline={bv} seasonal={sv}"

    def test_seasonal_cold_warm_row_counts(self):
        """Cold / warm slice stats must reflect the row counts of those
        months only. month_value column equals the month, so cold mean
        should average the month values of cold months, etc."""
        df = _year_of_data()
        with self._patched_get_mapped_data(df):
            rows = collect_mapped_data_stats("hh1", seasonal=True)
        cold = next(r for r in rows if r["column"] == "month_value" and r["season"] == "cold")
        warm = next(r for r in rows if r["column"] == "month_value" and r["season"] == "warm")
        assert cold["min"] == 1.0   # January
        assert cold["max"] == 12.0  # December
        assert warm["min"] == 5.0
        assert warm["max"] == 9.0
