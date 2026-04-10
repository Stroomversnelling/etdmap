"""
Tests for etdmap.timestamp_helpers.

derive_and_normalize_reading_date:
  - already-present ReadingDate returns df unchanged
  - tries candidate columns in order, uses first match
  - raises ValueError when no candidate is found
  - parses timestamps and returns UTC datetime64
  - drops NaT rows (unparseable values)
  - raises ValueError when ALL rows fail parsing
  - drops the source column, adds ReadingDate

normalize_to_utc_for_storage:
  - subtracts default 1-hour offset
  - respects custom from_offset_hours
  - raises KeyError when datetime_col is absent
  - returns a copy — original is not mutated
"""

import pandas as pd
import pytest

from etdmap.timestamp_helpers import (
    derive_and_normalize_reading_date,
    normalize_to_utc_for_storage,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _df_with_col(col_name, values):
    return pd.DataFrame({col_name: values})


# ---------------------------------------------------------------------------
# derive_and_normalize_reading_date
# ---------------------------------------------------------------------------


class TestDeriveAndNormalizeReadingDate:
    def test_already_present_returns_unchanged(self):
        """If 'ReadingDate' is already in the DataFrame, return it as-is."""
        ts = pd.Timestamp("2023-01-01 00:00:00", tz="UTC")
        df = pd.DataFrame({"ReadingDate": [ts], "other": [1]})
        result = derive_and_normalize_reading_date(df, ["Datum"])
        pd.testing.assert_frame_equal(result, df)

    def test_tries_candidates_in_order(self):
        """First candidate found in the DataFrame is used; readingDate is skipped."""
        df = pd.DataFrame({
            "readingDate": ["2023-01-01T00:00:00"],
            "Datum": ["2024-06-01T00:00:00"],
        })
        # candidates = ["Datum", "readingDate"]; "Datum" comes first so it is used (2024)
        result = derive_and_normalize_reading_date(df, ["Datum", "readingDate"])
        assert "ReadingDate" in result.columns
        assert result["ReadingDate"].iloc[0].year == 2024

    def test_uses_second_candidate_when_first_absent(self):
        df = pd.DataFrame({"readingDate": ["2023-03-15T10:00:00"]})
        result = derive_and_normalize_reading_date(df, ["Datum", "readingDate"])
        assert "ReadingDate" in result.columns
        assert result["ReadingDate"].iloc[0].month == 3

    def test_raises_value_error_when_no_candidate_found(self):
        df = pd.DataFrame({"SomeOtherCol": [1, 2, 3]})
        with pytest.raises(ValueError, match="No timestamp column found"):
            derive_and_normalize_reading_date(df, ["Datum", "readingDate"])

    def test_returns_utc_datetime(self):
        df = pd.DataFrame({"Datum": ["2023-06-01T12:00:00"]})
        result = derive_and_normalize_reading_date(df, ["Datum"])
        assert pd.api.types.is_datetime64_any_dtype(result["ReadingDate"])
        assert result["ReadingDate"].dt.tz is not None
        assert str(result["ReadingDate"].dt.tz) == "UTC"

    def test_drops_source_column(self):
        df = pd.DataFrame({"Datum": ["2023-01-01T00:00:00", "2023-01-01T00:05:00"]})
        result = derive_and_normalize_reading_date(df, ["Datum"])
        assert "Datum" not in result.columns
        assert "ReadingDate" in result.columns

    def test_drops_nat_rows(self):
        df = pd.DataFrame({
            "Datum": ["2023-01-01T00:00:00", "not-a-date", "2023-01-01T00:10:00"],
        })
        result = derive_and_normalize_reading_date(df, ["Datum"])
        assert len(result) == 2
        assert result["ReadingDate"].notna().all()

    def test_raises_when_all_rows_unparseable(self):
        df = pd.DataFrame({"Datum": ["bad", "also-bad"]})
        with pytest.raises(ValueError, match="All rows were dropped"):
            derive_and_normalize_reading_date(df, ["Datum"])

    def test_context_included_in_error(self):
        df = pd.DataFrame({"x": [1]})
        with pytest.raises(ValueError, match="myproject"):
            derive_and_normalize_reading_date(df, ["Datum"], context="myproject")

    def test_tz_aware_strings_parsed_correctly(self):
        """ISO 8601 strings with explicit UTC offset are parsed to UTC."""
        df = pd.DataFrame({"Datum": ["2023-06-15T10:00:00+02:00"]})
        result = derive_and_normalize_reading_date(df, ["Datum"])
        # +02:00 input → UTC is 08:00
        assert result["ReadingDate"].iloc[0].hour == 8
        assert str(result["ReadingDate"].dt.tz) == "UTC"

    def test_multiple_rows_parsed_correctly(self):
        dates = ["2023-01-01T00:00:00", "2023-01-01T00:05:00", "2023-01-01T00:10:00"]
        df = pd.DataFrame({"Datum": dates})
        result = derive_and_normalize_reading_date(df, ["Datum"])
        assert len(result) == 3
        assert (result["ReadingDate"].diff().dropna() == pd.Timedelta("5min")).all()


# ---------------------------------------------------------------------------
# normalize_to_utc_for_storage
# ---------------------------------------------------------------------------


class TestNormalizeToUtcForStorage:
    def _make_df(self, hours=12):
        ts = pd.Timestamp(f"2023-06-15 {hours:02d}:00:00")
        return pd.DataFrame({"ReadingDate": [ts]})

    def test_subtracts_one_hour_by_default(self):
        df = self._make_df(hours=13)
        result = normalize_to_utc_for_storage(df)
        assert result["ReadingDate"].iloc[0].hour == 12

    def test_custom_offset(self):
        df = self._make_df(hours=14)
        result = normalize_to_utc_for_storage(df, from_offset_hours=2.0)
        assert result["ReadingDate"].iloc[0].hour == 12

    def test_zero_offset_is_noop(self):
        df = self._make_df(hours=10)
        result = normalize_to_utc_for_storage(df, from_offset_hours=0.0)
        assert result["ReadingDate"].iloc[0].hour == 10

    def test_raises_key_error_when_column_missing(self):
        df = pd.DataFrame({"SomeOtherCol": [1]})
        with pytest.raises(KeyError, match="ReadingDate"):
            normalize_to_utc_for_storage(df)

    def test_custom_datetime_col(self):
        df = pd.DataFrame({"Timestamp": [pd.Timestamp("2023-01-01 13:00:00")]})
        result = normalize_to_utc_for_storage(df, datetime_col="Timestamp")
        assert result["Timestamp"].iloc[0].hour == 12

    def test_returns_copy_not_mutating_original(self):
        df = self._make_df(hours=13)
        original_hour = df["ReadingDate"].iloc[0].hour
        result = normalize_to_utc_for_storage(df)
        assert df["ReadingDate"].iloc[0].hour == original_hour  # unchanged
        assert result["ReadingDate"].iloc[0].hour == original_hour - 1

    def test_fractional_offset(self):
        df = pd.DataFrame({"ReadingDate": [pd.Timestamp("2023-01-01 12:30:00")]})
        result = normalize_to_utc_for_storage(df, from_offset_hours=0.5)
        assert result["ReadingDate"].iloc[0] == pd.Timestamp("2023-01-01 12:00:00")
