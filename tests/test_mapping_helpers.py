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
    NUMERIC_STATS_SCHEMA,
    apply_threshold_to_col,
    apply_thresholds_to_df,
    _STATS_DTYPES,
    _cast_stats_dtypes,
    _seasonal_slices,
    _synthesise_tariff_roots,
    collect_column_stats,
    collect_mapped_data_stats,
    combine_source_columns,
    compute_numeric_column_stats,
    ensure_intervals,
    expand_tz_columns,
    fill_down_infrequent_devices,
    rearrange_model_columns,
    run_standard_pipeline,
    threshold_stats_to_rows,
    write_threshold_removals,
)
from etdmap.data_stats import (
    annotate_mapped_bsv_variable,
    process_raw_data_file,
)

#: The grid these frames are built on and declared to sit on. Passed
#: explicitly: there is no default grid.
GRID = 5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pipeline_cadence(df, grid_minutes=GRID):
    """The cadence table a mapper measures before calling the pipeline."""
    from etdmap.mapping_helpers import detect_column_cadence
    return detect_column_cadence(df, grid_minutes=grid_minutes)


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

    def test_unmapped_supplier_columns_are_dropped(self):
        """Mapped output carries the data model, not whatever the supplier sent."""
        df = pd.DataFrame({
            "ReadingDate": pd.date_range("2023-01-01", periods=2, freq="5min"),
            "CustomSupplierCol": [1, 2],
            "alklimaHeatPump_firmware_version": [3, 4],
            "nan [nan]": [5, 6],
        })
        result = rearrange_model_columns(df, add_columns=True)
        assert set(result.columns) == set(model_column_order)

    def test_unmapped_dropped_when_add_columns_false(self):
        df = pd.DataFrame({
            "ReadingDate": pd.date_range("2023-01-01", periods=2, freq="5min"),
            "CustomSupplierCol": [1, 2],
        })
        result = rearrange_model_columns(df, add_columns=False)
        assert "CustomSupplierCol" not in result.columns
        assert list(result.columns) == ["ReadingDate"]

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


class TestUnparseableValuesAreRecorded:
    """Coercion uses errors='coerce', so a value the expected dtype cannot read
    is replaced by pd.NA. Unflagged, that is indistinguishable from a reading
    the supplier never sent, and the removal is unrecoverable from the artifact.
    """

    COL = "ElektriciteitNetgebruik"

    def _frame(self, values):
        return pd.DataFrame({
            "ReadingDate": pd.date_range("2026-01-01", periods=len(values), freq="5min"),
            self.COL: values,
        })

    def test_a_value_that_fails_coercion_is_flagged_nulled(self):
        from etdmap.flags import MappingFlag, flag_column
        out = rearrange_model_columns(
            self._frame(["1.5", "not-a-number", "3.5"]), add_columns=False,
        )
        assert out[self.COL].isna().tolist() == [False, True, False]
        bits = out[flag_column(self.COL)].fillna(0).astype("int64")
        assert ((bits & MappingFlag.NULLED.value) != 0).tolist() == [
            False, True, False,
        ]

    def test_a_value_already_absent_is_not_flagged(self):
        """Only a cell that held a value lost one. An empty cell was not
        delivered and nothing removed it."""
        from etdmap.flags import MappingFlag, flag_column
        out = rearrange_model_columns(
            self._frame(["1.5", None, "3.5"]), add_columns=False,
        )
        findings = flag_column(self.COL)
        if findings in out.columns:
            bits = out[findings].fillna(0).astype("int64")
            assert not ((bits & MappingFlag.NULLED.value) != 0).any()

    def test_a_clean_column_gains_no_findings_column(self):
        from etdmap.flags import flag_column
        out = rearrange_model_columns(
            self._frame(pd.array([1.5, 2.5], dtype="Float64")), add_columns=False,
        )
        assert flag_column(self.COL) not in out.columns


# ---------------------------------------------------------------------------
# ensure_intervals
# ---------------------------------------------------------------------------


class TestEnsureIntervals:
    def test_already_correct_returns_unchanged(self):
        dates = pd.date_range("2023-01-01", periods=5, freq="5min")
        df = pd.DataFrame({"ReadingDate": dates, "Value": range(5)})
        result = ensure_intervals(df, freq=f'{GRID}min')
        assert len(result) == 5

    def test_fills_missing_intervals(self):
        # Create a df with a gap: skip one 5-min slot
        dates = pd.date_range("2023-01-01", periods=5, freq="5min").tolist()
        dates.pop(2)  # remove one slot
        df = pd.DataFrame({"ReadingDate": dates, "Value": range(4)})
        result = ensure_intervals(df, freq=f'{GRID}min')
        # Should now have 5 rows (the missing slot added)
        assert len(result) == 5

    def test_filled_row_has_na_values(self):
        dates = pd.date_range("2023-01-01", periods=5, freq="5min").tolist()
        dates.pop(2)
        df = pd.DataFrame({"ReadingDate": dates, "Value": [1.0, 2.0, 4.0, 5.0]})
        result = ensure_intervals(df, freq=f'{GRID}min')
        gap_row = result[result["ReadingDate"] == pd.Timestamp("2023-01-01 00:10:00")]
        assert len(gap_row) == 1
        assert pd.isna(gap_row["Value"].iloc[0])

    def test_single_row_unchanged(self):
        df = pd.DataFrame({"ReadingDate": [pd.Timestamp("2023-01-01")], "V": [1.0]})
        result = ensure_intervals(df, freq=f'{GRID}min')
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
            col: pd.array([1.0, pd.NA, pd.NA, 2.0, pd.NA], dtype="Float64"),
        })

    def test_forward_fills_gaps(self):
        col = self._FILL_COLS[0]
        df = self._make_df()
        result = fill_down_infrequent_devices(
            df, columns=self._FILL_COLS, grid_minutes=GRID
        )
        assert result[col].iloc[1] == 1.0  # ffilled
        assert result[col].iloc[2] == 1.0  # ffilled

    def test_backward_fills_leading_na(self):
        col = self._FILL_COLS[0]
        df = pd.DataFrame({col: pd.array([pd.NA, pd.NA, 5.0], dtype="Float64")})
        result = fill_down_infrequent_devices(
            df, columns=self._FILL_COLS, grid_minutes=GRID
        )
        assert result[col].iloc[0] == 5.0  # bfilled

    def test_all_na_stays_all_na(self):
        """A never-seen device column is left untouched. The old remaining-NA
        -> 0.0 fallback was an implicit absence claim inferred from
        missingness; absence is now declared per household in the metadata
        (ApparaatOntbrekend), so this function may only densify a device
        that actually reports."""
        from etdmap.flags import flag_column
        col = self._FILL_COLS[0]
        df = pd.DataFrame({col: pd.array([pd.NA] * 3, dtype="Float64")})
        result = fill_down_infrequent_devices(
            df, columns=self._FILL_COLS, grid_minutes=GRID
        )
        assert result[col].isna().all()
        assert result[col].dtype == "Float64"  # no silent downcast on fill
        # No values were written, so no findings may be recorded either.
        findings = flag_column(col)
        if findings in result.columns:
            assert (result[findings].fillna(0) == 0).all()

    def test_ignores_unknown_columns(self):
        df = pd.DataFrame({
            "NotAFillCol": pd.array([pd.NA, pd.NA, 3.0], dtype="Float64"),
        })
        result = fill_down_infrequent_devices(
            df, columns=self._FILL_COLS, grid_minutes=GRID
        )
        # NotAFillCol is not in the fill list -- should remain unchanged
        assert pd.isna(result["NotAFillCol"].iloc[0])

    def test_custom_columns_parameter(self):
        df = pd.DataFrame({"MyCol": pd.array([pd.NA, pd.NA, 7.0], dtype="Float64")})
        result = fill_down_infrequent_devices(
            df, columns=("MyCol",), grid_minutes=GRID
        )
        assert result["MyCol"].iloc[0] == 7.0


# ---------------------------------------------------------------------------
# combine_source_columns
# ---------------------------------------------------------------------------


class TestCombineSourceColumns:
    """Tests for combine_source_columns.

    Resolves multiple raw source columns mapping to one standard target
    column. Used for heat-pump energy/power binnen+buiten splits
    and CO2 sensor combinations. Three states:
      1. target already present  -> drop source columns, target wins
      2. target absent + sources -> combine via sum or mean (NA-preserving)
      3. neither                  -> all-NA target column
    """

    @staticmethod
    def _df(**cols):
        """Build a DataFrame with Float64 nullable columns (ADR-005)."""
        return pd.DataFrame({k: pd.array(v, dtype="Float64") for k, v in cols.items()})

    # --- State 1: target already present ---------------------------------

    def test_target_present_no_sources_returns_unchanged(self):
        df = self._df(target=[1.0, 2.0, 3.0], other=[10.0, 20.0, 30.0])
        result = combine_source_columns(
            df, target="target", sources=("a", "b"),
            operator="sum", target_dtype="Float64",
        )
        assert list(result.columns) == ["target", "other"]
        assert list(result["target"]) == [1.0, 2.0, 3.0]

    def test_target_present_drops_both_sources(self):
        df = self._df(target=[1.0, 2.0], a=[100.0, 200.0], b=[300.0, 400.0])
        result = combine_source_columns(
            df, target="target", sources=("a", "b"),
            operator="sum", target_dtype="Float64",
        )
        assert "a" not in result.columns
        assert "b" not in result.columns
        # Target value is preserved unchanged -- the existing total wins.
        assert list(result["target"]) == [1.0, 2.0]

    def test_target_present_drops_only_present_source(self):
        df = self._df(target=[1.0], a=[5.0])  # b absent
        result = combine_source_columns(
            df, target="target", sources=("a", "b"),
            operator="sum", target_dtype="Float64",
        )
        assert "a" not in result.columns
        assert list(result["target"]) == [1.0]

    # --- State 2: target absent, operator=sum ----------------------------

    def test_sum_two_sources_added_per_row(self):
        df = self._df(a=[1.0, 2.0, 3.0], b=[10.0, 20.0, 30.0])
        result = combine_source_columns(
            df, target="t", sources=("a", "b"),
            operator="sum", target_dtype="Float64",
        )
        assert "a" not in result.columns and "b" not in result.columns
        assert list(result["t"]) == [11.0, 22.0, 33.0]

    def test_sum_preserves_na_when_either_source_na(self):
        # NA-preservation: a missing reading on either side at time t
        # produces NA at t (no silent zero-fill, no half-value).
        df = self._df(a=[1.0, pd.NA, 3.0], b=[10.0, 20.0, pd.NA])
        result = combine_source_columns(
            df, target="t", sources=("a", "b"),
            operator="sum", target_dtype="Float64",
        )
        out = list(result["t"])
        assert out[0] == 11.0
        assert pd.isna(out[1])
        assert pd.isna(out[2])

    def test_sum_one_source_present_uses_it_directly(self):
        df = self._df(a=[1.0, 2.0])  # b absent
        result = combine_source_columns(
            df, target="t", sources=("a", "b"),
            operator="sum", target_dtype="Float64",
        )
        assert "a" not in result.columns
        assert list(result["t"]) == [1.0, 2.0]

    # --- State 2: target absent, operator=mean ---------------------------

    def test_mean_two_sources_averaged_per_row(self):
        df = self._df(a=[2.0, 4.0, 6.0], b=[4.0, 8.0, 12.0])
        result = combine_source_columns(
            df, target="t", sources=("a", "b"),
            operator="mean", target_dtype="Float64",
        )
        assert list(result["t"]) == [3.0, 6.0, 9.0]

    def test_mean_preserves_na_when_either_source_na(self):
        df = self._df(a=[2.0, pd.NA, 6.0], b=[4.0, 8.0, pd.NA])
        result = combine_source_columns(
            df, target="t", sources=("a", "b"),
            operator="mean", target_dtype="Float64",
        )
        out = list(result["t"])
        assert out[0] == 3.0
        assert pd.isna(out[1])
        assert pd.isna(out[2])

    def test_mean_one_source_pass_through(self):
        # Mean over a single available source equals the source itself
        # -- no zero-fill of the missing one.
        df = self._df(a=[5.0, 10.0])  # b absent
        result = combine_source_columns(
            df, target="t", sources=("a", "b"),
            operator="mean", target_dtype="Float64",
        )
        assert list(result["t"]) == [5.0, 10.0]

    def test_mean_three_sources_averaged(self):
        df = self._df(a=[3.0], b=[6.0], c=[9.0])
        result = combine_source_columns(
            df, target="t", sources=("a", "b", "c"),
            operator="mean", target_dtype="Float64",
        )
        assert list(result["t"]) == [6.0]

    # --- State 3: neither target nor sources -----------------------------

    def test_no_sources_fills_na_using_target_dtype(self):
        df = self._df(other=[1.0, 2.0])
        result = combine_source_columns(
            df, target="t", sources=("a", "b"),
            operator="sum", target_dtype="Float64",
        )
        assert "t" in result.columns
        assert result["t"].isna().all()
        assert str(result["t"].dtype) == "Float64"

    def test_no_sources_falls_back_to_model_column_type(self):
        # CO2 is a real model column; helper should look up its dtype.
        df = self._df(other=[1.0])
        result = combine_source_columns(
            df, target="CO2", sources=("CO2_1", "CO2_2"), operator="mean",
        )
        assert "CO2" in result.columns
        assert str(result["CO2"].dtype) == model_column_type["CO2"]
        assert result["CO2"].isna().all()

    def test_no_sources_unknown_target_raises_key_error(self):
        df = self._df(other=[1.0])
        with pytest.raises(KeyError, match="No dtype known"):
            combine_source_columns(
                df, target="not_a_real_model_column",
                sources=("a", "b"), operator="sum",
            )

    def test_no_sources_required_target_logs_warning(self, caplog):
        # ElektriciteitsgebruikWarmtepomp is Vereist=ja in etdmodel.csv.
        # Missing all sources should be loud (WARNING) without halting.
        df = self._df(other=[1.0])
        with caplog.at_level("WARNING"):
            combine_source_columns(
                df, target="ElektriciteitsgebruikWarmtepomp",
                sources=("ElektriciteitsgebruikWarmtepomp_binnen",
                         "ElektriciteitsgebruikWarmtepomp_buiten"),
                operator="sum",
            )
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert len(warnings) == 1
        assert "Vereist=ja" in warnings[0].message
        assert "ElektriciteitsgebruikWarmtepomp" in warnings[0].message

    def test_no_sources_optional_target_is_silent(self, caplog):
        # CO2 is Vereist=nee. Missing all sources should be silent --
        # all-NA optional columns are normal in this pipeline.
        df = self._df(other=[1.0])
        with caplog.at_level("WARNING"):
            combine_source_columns(
                df, target="CO2", sources=("CO2_1", "CO2_2"), operator="mean",
            )
        warnings = [r for r in caplog.records if r.levelname in ("WARNING", "ERROR")]
        assert warnings == []

    # --- Errors and behavioural contract ---------------------------------

    def test_invalid_operator_raises_value_error(self):
        df = self._df(a=[1.0], b=[2.0])
        with pytest.raises(ValueError, match="must be 'sum' or 'mean'"):
            combine_source_columns(
                df, target="t", sources=("a", "b"),
                operator="median", target_dtype="Float64",
            )

    def test_modifies_df_in_place_and_returns_same_object(self):
        df = self._df(a=[1.0], b=[2.0])
        result = combine_source_columns(
            df, target="t", sources=("a", "b"),
            operator="sum", target_dtype="Float64",
        )
        # Same convention as fill_down_infrequent_devices etc.
        assert result is df

    def test_combined_target_keeps_float64_nullable_dtype(self):
        df = self._df(a=[1.0, 2.0], b=[3.0, 4.0])
        result = combine_source_columns(
            df, target="t", sources=("a", "b"),
            operator="sum", target_dtype="Float64",
        )
        assert str(result["t"].dtype) == "Float64"


class TestCombinedColumnsIsRecorded:
    """The sources are dropped after the combine, so an unflagged target is
    indistinguishable from one the supplier delivered whole. COMBINED_COLUMNS
    is the record, and it is an action without being INVENTED: the value
    carries what the delivered sources reported.

    The three production targets are ElektriciteitsgebruikWarmtepomp and
    ElektriciteitVermogenWarmtepomp (each a sum of a _binnen/_buiten pair) and
    CO2 (a mean of CO2_1 and CO2_2), all from the Watch-E mappers.
    """

    TARGET = "ElektriciteitsgebruikWarmtepomp"

    @staticmethod
    def _df(**cols):
        return pd.DataFrame(
            {k: pd.array(v, dtype="Float64") for k, v in cols.items()}
        )

    def _bits(self, df, column):
        from etdmap.flags import flag_column
        return df[flag_column(column)].fillna(0).astype("int64")

    def test_a_summed_target_is_flagged(self):
        from etdmap.flags import MappingFlag
        df = self._df(**{
            f"{self.TARGET}_binnen": [1.0, 2.0],
            f"{self.TARGET}_buiten": [10.0, 20.0],
        })
        combine_source_columns(
            df, target=self.TARGET,
            sources=(f"{self.TARGET}_binnen", f"{self.TARGET}_buiten"),
            operator="sum",
        )
        assert list(df[self.TARGET]) == [11.0, 22.0]
        bits = self._bits(df, self.TARGET)
        assert ((bits & MappingFlag.COMBINED_COLUMNS.value) != 0).all()

    def test_a_meaned_target_is_flagged(self):
        from etdmap.flags import MappingFlag
        df = self._df(CO2_1=[400.0, 500.0], CO2_2=[600.0, 700.0])
        combine_source_columns(
            df, target="CO2", sources=("CO2_1", "CO2_2"), operator="mean",
        )
        assert list(df["CO2"]) == [500.0, 600.0]
        bits = self._bits(df, "CO2")
        assert ((bits & MappingFlag.COMBINED_COLUMNS.value) != 0).all()

    def test_a_cell_no_source_covered_is_not_flagged(self):
        """The combine is NA-preserving, so a cell left empty is one nothing
        wrote -- flagging it would claim a value that is not there."""
        from etdmap.flags import MappingFlag
        df = self._df(**{
            f"{self.TARGET}_binnen": [1.0, pd.NA],
            f"{self.TARGET}_buiten": [10.0, 20.0],
        })
        combine_source_columns(
            df, target=self.TARGET,
            sources=(f"{self.TARGET}_binnen", f"{self.TARGET}_buiten"),
            operator="sum",
        )
        bits = self._bits(df, self.TARGET)
        assert (bits & MappingFlag.COMBINED_COLUMNS.value).tolist() == [
            MappingFlag.COMBINED_COLUMNS.value, 0,
        ]

    def test_a_delivered_target_is_not_flagged(self):
        """State 1: the delivered target wins and nothing is combined, so
        there is no action to record."""
        from etdmap.flags import flag_column
        df = self._df(**{
            self.TARGET: [5.0, 6.0],
            f"{self.TARGET}_binnen": [1.0, 2.0],
        })
        combine_source_columns(
            df, target=self.TARGET,
            sources=(f"{self.TARGET}_binnen", f"{self.TARGET}_buiten"),
            operator="sum",
        )
        assert list(df[self.TARGET]) == [5.0, 6.0]
        assert flag_column(self.TARGET) not in df.columns

    def test_an_all_na_target_is_not_flagged(self):
        """State 3: no source present, so the all-NA column records nothing."""
        from etdmap.flags import flag_column
        df = self._df(Other=[1.0, 2.0])
        combine_source_columns(
            df, target=self.TARGET,
            sources=(f"{self.TARGET}_binnen", f"{self.TARGET}_buiten"),
            operator="sum",
        )
        assert df[self.TARGET].isna().all()
        assert flag_column(self.TARGET) not in df.columns

    def test_it_is_an_action_that_is_still_a_measurement(self):
        from etdmap.flags import MappingFlag
        assert MappingFlag.COMBINED_COLUMNS & MappingFlag.ACTIONS
        assert MappingFlag.COMBINED_COLUMNS & MappingFlag.ACTIONS_TAKEN
        assert not (MappingFlag.COMBINED_COLUMNS & MappingFlag.INVENTED)
        assert not (
            MappingFlag.COMBINED_COLUMNS & MappingFlag.NOT_A_MEASUREMENT
        )


# ---------------------------------------------------------------------------
# run_standard_pipeline
# ---------------------------------------------------------------------------


class TestRunStandardPipeline:
    """Characterizes the flat (one file per household) mapping output. The
    default mapped_output_format is 'sharded', so flat is pinned explicitly
    here; sharded behaviour is covered in test_sharded_mapping.py."""

    @pytest.fixture(autouse=True)
    def _flat_output_format(self):
        import etdmap
        prev = etdmap.options.mapped_output_format
        etdmap.options.mapped_output_format = "flat"
        yield
        etdmap.options.mapped_output_format = prev

    def test_raises_key_error_when_no_reading_date(self, tmp_path):
        df = pd.DataFrame({"SomeCol": [1, 2, 3]})
        with pytest.raises(KeyError, match="ReadingDate"):
            run_standard_pipeline(
                df, household_id=1, household_batch_id=1001, household_id_supplier="HuisA",
                mapped_folder_path=tmp_path, grid_minutes=GRID,
            )

    def test_raises_value_error_when_reading_date_unparseable(self, tmp_path):
        df = pd.DataFrame({"ReadingDate": ["bad", "also-bad"]})
        with pytest.raises((ValueError, Exception)):
            run_standard_pipeline(
                df, household_id=1, household_batch_id=1001, household_id_supplier="HuisA",
                mapped_folder_path=tmp_path, grid_minutes=GRID,
            )

    def test_saves_parquet_and_returns_entry(self, tmp_path):
        """End-to-end smoke test: pipeline writes a file and returns the index entry."""
        df = _minimal_pipeline_df(n=12)
        result = run_standard_pipeline(
            df, household_id=42, household_batch_id=1042, household_id_supplier="HuisX",
            mapped_folder_path=tmp_path, grid_minutes=GRID,
            cadence=_pipeline_cadence(df),
        )
        # Underscore keys are payloads the mapper pops and routes elsewhere --
        # the summary to the report, the cadence to the sidecar. What is left is
        # the index entry, and nothing else may reach the index.
        result.pop("_validation_summary", None)
        result.pop("_cadence", None)
        assert result == {"HuisIdLeverancier": "HuisX", "HuisIdBSV": 42}
        out_file = tmp_path / "household_42_table.parquet"
        assert out_file.exists()

    def test_output_sorted_by_reading_date(self, tmp_path):
        """Rows must be in ascending ReadingDate order in the saved file."""
        df = _minimal_pipeline_df(n=6)
        df = df.iloc[::-1].reset_index(drop=True)  # reverse the order
        run_standard_pipeline(
            df, household_id=1, household_batch_id=1001, household_id_supplier="H",
            mapped_folder_path=tmp_path, grid_minutes=GRID,
            cadence=_pipeline_cadence(df),
        )
        out = pd.read_parquet(tmp_path / "household_1_table.parquet", dtype_backend="numpy_nullable")
        assert (out["ReadingDate"].diff().dropna() >= pd.Timedelta(0)).all()

    def test_output_has_model_columns(self, tmp_path):
        """Output parquet must contain all model columns."""
        df = _minimal_pipeline_df(n=6)
        run_standard_pipeline(
            df, household_id=2, household_batch_id=1002, household_id_supplier="H2",
            mapped_folder_path=tmp_path, grid_minutes=GRID,
            cadence=_pipeline_cadence(df),
        )
        out = pd.read_parquet(tmp_path / "household_2_table.parquet", dtype_backend="numpy_nullable")
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
        assert out["p25"] == 1.75
        assert out["p75"] == 3.25
        assert out["iqr"] == 1.5
        # p25 / p99 are interpolated; with [1, 2, 3, 4] the values are close
        # to min and max but not equal to them.
        assert out["p01"] is not pd.NA and float(out["p01"]) == pytest.approx(1.03)
        assert out["p99"] is not pd.NA and float(out["p99"]) == pytest.approx(3.97)
        assert pd.isna(out["min_datetime"]) and pd.isna(out["max_datetime"])
        assert pd.isna(out["top5"])

    def test_q1_q99_on_large_sample_match_percentile_definition(self):
        """p25 and p99 are the 1st and 99th percentiles. With 100+ points
        they should land within typical-tail ranges away from min/max,
        confirming they aren't picking up single-record extremes."""
        # Values 1..100. p25 ~ 1.99, p99 ~ 99.01 by linear interpolation.
        s = pd.Series(
            pd.array([float(i) for i in range(1, 101)], dtype="Float64"),
            name="x",
        )
        out = collect_column_stats("hh1", s)
        assert float(out["p01"]) == pytest.approx(1.99)
        assert float(out["p99"]) == pytest.approx(99.01)
        assert float(out["min"]) == 1.0
        assert float(out["max"]) == 100.0

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
                  "p25", "p75", "top5"):
            assert pd.isna(out[k]), f"{k} should be NA, got {out[k]!r}"
        assert pd.isna(out["min_datetime"]) and pd.isna(out["max_datetime"])

    def test_type_field_is_string(self):
        """type must be the stringified dtype, never a dtype object,
        so .astype('string') downstream succeeds."""
        s = pd.Series(pd.array([1.0], dtype="Float64"), name="x")
        out = collect_column_stats("hh1", s)
        assert out["type"] == "Float64"
        assert isinstance(out["type"], str)


class TestProcessRawDataFileTimestamp:
    """Raw parquets can store the timestamp as object strings with per-row DST
    offsets (e.g. a 'Datum' column). process_raw_data_file must derive a datetime
    'ReadingDate' via the canonical helper so min_datetime/max_datetime populate
    -- and fall back gracefully when no timestamp column is present."""

    def test_tz_aware_string_datum_yields_reading_date_min_max(self, tmp_path):
        df = pd.DataFrame({
            "Datum": ["2024-05-01 00:00:00+02:00",   # CEST -> 22:00 UTC prev day
                      "2025-01-15 12:00:00+01:00"],   # CET  -> 11:00 UTC
            "value": [1.0, 2.0],
        })
        df.to_parquet(tmp_path / "Address 1.parquet")
        rows = process_raw_data_file(("Address 1.parquet", str(tmp_path)))
        sdf = pd.DataFrame(rows)
        rd = sdf[sdf["column"] == "ReadingDate"]
        assert not rd.empty, "ReadingDate row should be present"
        assert rd.iloc[0]["min_datetime"] == pd.Timestamp("2024-04-30 22:00:00")
        assert rd.iloc[0]["max_datetime"] == pd.Timestamp("2025-01-15 11:00:00")
        # source timestamp column is consumed by the canonical helper
        assert "Datum" not in set(sdf["column"])

    def test_no_timestamp_column_falls_back_without_error(self, tmp_path):
        df = pd.DataFrame({"value": [1.0, 2.0], "Adres": ["Address 1", "Address 1"]})
        df.to_parquet(tmp_path / "Address 2.parquet")
        rows = process_raw_data_file(("Address 2.parquet", str(tmp_path)))
        sdf = pd.DataFrame(rows)
        # stats still emitted for the data columns; no ReadingDate row
        assert "value" in set(sdf["column"])
        assert "ReadingDate" not in set(sdf["column"])


# ---------------------------------------------------------------------------
# NUMERIC_STATS_SCHEMA parity (ADR-018)
# ---------------------------------------------------------------------------


class TestNumericStatsSchema:
    """The kernel and the per-column wrapper must both honour
    NUMERIC_STATS_SCHEMA. Drift here would silently break downstream
    consumers that read stats columns by name (parent ADR-018)."""

    def test_kernel_returns_exactly_the_schema_keys(self):
        """compute_numeric_column_stats's keys are NUMERIC_STATS_SCHEMA.
        No additions, no omissions, no renames."""
        s = pd.Series(pd.array([1.0, 2.0, 3.0, 4.0], dtype="Float64"))
        keys = set(compute_numeric_column_stats(s).keys())
        assert keys == set(NUMERIC_STATS_SCHEMA), (
            f"kernel keys diverged from NUMERIC_STATS_SCHEMA: "
            f"missing={set(NUMERIC_STATS_SCHEMA) - keys}, "
            f"unexpected={keys - set(NUMERIC_STATS_SCHEMA)}"
        )

    def test_collect_column_stats_includes_schema_for_numeric(self):
        """collect_column_stats's numeric branch must populate every key in
        NUMERIC_STATS_SCHEMA. Type-specific extras (top5, min_datetime)
        are allowed -- the contract is 'at least these'."""
        s = pd.Series(pd.array([1.0, 2.0, 3.0, 4.0], dtype="Float64"), name="x")
        stats = collect_column_stats("hh1", s)
        for key in NUMERIC_STATS_SCHEMA:
            assert key in stats, f"NUMERIC_STATS_SCHEMA key '{key}' missing"

    def test_kernel_and_wrapper_produce_identical_numeric_values(self):
        """The wrapper delegates to the kernel; values must match exactly
        for the keys both produce."""
        s = pd.Series(pd.array([1.0, 2.0, 3.0, 4.0, pd.NA], dtype="Float64"), name="x")
        kernel = compute_numeric_column_stats(s)
        wrapper = collect_column_stats("hh1", s)
        for key in NUMERIC_STATS_SCHEMA:
            kv, wv = kernel[key], wrapper[key]
            if pd.isna(kv) and pd.isna(wv):
                continue
            assert float(kv) == pytest.approx(float(wv)), (
                f"kernel/wrapper diverged on '{key}': kernel={kv}, wrapper={wv}"
            )


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

    def test_diff_columns_have_explicit_rows(self):
        """Diff variants are explicit rows in etdmodel.csv (Diff='ja'),
        not synthesised from a suffix convention. Each Diff row carries
        its own unit declaration; the unit happens to match its
        cumulative parent in normal cases but the model is authoritative
        (no suffix-based inheritance in ``load_unit_map``)."""
        m = load_unit_map()
        assert m["ElektriciteitNetgebruikHoogDiff"] == "kWh"
        assert m["ElektriciteitNetgebruikHoog"] == "kWh"

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
        # get_mapped_data was relocated alongside collect_mapped_data_stats
        # from etdmap.mapping_helpers to etdmap.data_stats; patch the new
        # location so the test intercepts the actual call site.
        return patch(
            "etdmap.data_stats.get_mapped_data",
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


# ---------------------------------------------------------------------------
# _synthesise_tariff_roots
# ---------------------------------------------------------------------------


class TestSynthesisedRootFromHoogLaag:
    """When a household reports only one tariff register, the helper
    must synthesise the root combined column so downstream stats can
    compare across projects on the root name."""

    HOOG = "ElektriciteitNetgebruikHoog"
    LAAG = "ElektriciteitNetgebruikLaag"
    ROOT = "ElektriciteitNetgebruik"

    def test_only_hoog_synthesises_root_equal_to_hoog(self):
        df = pd.DataFrame({
            self.HOOG: pd.array([1.0, 2.0, 3.0], dtype="Float64"),
        })
        out = _synthesise_tariff_roots(df)
        assert self.ROOT in out.columns
        # Laag treated as 0 -> root equals hoog
        assert out[self.ROOT].tolist() == [1.0, 2.0, 3.0]
        # Original input is not mutated
        assert self.ROOT not in df.columns

    def test_only_laag_synthesises_root_equal_to_laag(self):
        df = pd.DataFrame({
            self.LAAG: pd.array([4.0, 5.0, 6.0], dtype="Float64"),
        })
        out = _synthesise_tariff_roots(df)
        assert self.ROOT in out.columns
        assert out[self.ROOT].tolist() == [4.0, 5.0, 6.0]

    def test_both_hoog_and_laag_synthesises_sum(self):
        df = pd.DataFrame({
            self.HOOG: pd.array([1.0, 2.0, 3.0], dtype="Float64"),
            self.LAAG: pd.array([10.0, 20.0, 30.0], dtype="Float64"),
        })
        out = _synthesise_tariff_roots(df)
        assert out[self.ROOT].tolist() == [11.0, 22.0, 33.0]

    def test_neither_no_synthesis(self):
        df = pd.DataFrame({"unrelated": pd.array([1.0], dtype="Float64")})
        out = _synthesise_tariff_roots(df)
        assert self.ROOT not in out.columns

    def test_root_already_present_with_data_not_overwritten(self):
        """If the supplier already reports the combined root directly,
        the synthesiser must not clobber that value."""
        df = pd.DataFrame({
            self.ROOT: pd.array([100.0, 200.0, 300.0], dtype="Float64"),
            self.HOOG: pd.array([1.0, 2.0, 3.0], dtype="Float64"),
            self.LAAG: pd.array([10.0, 20.0, 30.0], dtype="Float64"),
        })
        out = _synthesise_tariff_roots(df)
        assert out[self.ROOT].tolist() == [100.0, 200.0, 300.0]

    def test_root_present_but_all_na_gets_synthesised(self):
        """When the root column exists in the schema but is all NA
        for this household, synthesise from the splits."""
        df = pd.DataFrame({
            self.ROOT: pd.array([pd.NA, pd.NA, pd.NA], dtype="Float64"),
            self.HOOG: pd.array([1.0, 2.0, 3.0], dtype="Float64"),
            self.LAAG: pd.array([10.0, 20.0, 30.0], dtype="Float64"),
        })
        out = _synthesise_tariff_roots(df)
        assert out[self.ROOT].tolist() == [11.0, 22.0, 33.0]

    def test_diff_pair_handled_too(self):
        """Diff variants (HoogDiff / LaagDiff -> Diff) follow the same rule."""
        df = pd.DataFrame({
            "ElektriciteitNetgebruikHoogDiff": pd.array([0.1, 0.2], dtype="Float64"),
            "ElektriciteitNetgebruikLaagDiff": pd.array([0.5, 0.5], dtype="Float64"),
        })
        out = _synthesise_tariff_roots(df)
        assert "ElektriciteitNetgebruikDiff" in out.columns
        assert out["ElektriciteitNetgebruikDiff"].tolist() == [0.6, 0.7]

    def test_both_na_rows_stay_na_in_synthesised_root(self):
        """When BOTH Hoog and Laag are NA at a given timestamp, the
        synthesised root must remain NA (not become 0). Otherwise the
        row count for the root would be inflated by "no data"
        timestamps treated as zero readings."""
        df = pd.DataFrame({
            self.HOOG: pd.array([1.0, pd.NA, pd.NA], dtype="Float64"),
            self.LAAG: pd.array([10.0, 5.0, pd.NA], dtype="Float64"),
        })
        out = _synthesise_tariff_roots(df)
        # Row 0: both have data -> 1 + 10 = 11
        # Row 1: only Laag has data -> 0 + 5 = 5
        # Row 2: BOTH NA -> root must be NA
        result = out[self.ROOT].tolist()
        assert result[0] == 11.0
        assert result[1] == 5.0
        assert pd.isna(result[2])


class TestThresholdStatsToRows:
    """threshold_stats_to_rows flattens the per-column threshold_stats dict
    (from apply_thresholds_to_df(return_stats=True)) into sidecar rows."""

    def test_empty_or_none_yields_no_rows(self):
        assert threshold_stats_to_rows(1, {}) == []
        assert threshold_stats_to_rows(1, None) == []

    def test_one_row_per_affected_column(self):
        stats = {
            "ElektriciteitVermogen": {
                "n_below": 2, "n_above": 1,
                "sum_removed_negative": -6.0, "sum_removed_positive": 25000.0,
                "balance_removed": 24994.0,
                "min_removed": -5.0, "max_removed": 25000.0,
                "lower_bound": 0.0, "upper_bound": 20000.0,
            },
            "TemperatuurWoonkamer": {
                "n_below": 0, "n_above": 3,
                "sum_removed_negative": 0.0, "sum_removed_positive": 250.0,
                "balance_removed": 250.0,
                "min_removed": 40.0, "max_removed": 99.0,
                "lower_bound": pd.NA, "upper_bound": 35.0,
            },
        }
        rows = threshold_stats_to_rows(42, stats)
        by_col = {r["column"]: r for r in rows}
        assert len(rows) == 2
        assert by_col["ElektriciteitVermogen"]["HuisBatchIdBSV"] == 42
        assert by_col["ElektriciteitVermogen"]["n_below"] == 2
        assert by_col["ElektriciteitVermogen"]["n_above"] == 1
        assert by_col["ElektriciteitVermogen"]["min_removed"] == -5.0
        assert by_col["TemperatuurWoonkamer"]["max_removed"] == 99.0
        # Signed volumes, so the report says how much left and which way. The
        # extremes cannot: two removals of 5 read the same as one of 10.
        assert by_col["ElektriciteitVermogen"]["sum_removed_negative"] == -6.0
        assert by_col["ElektriciteitVermogen"]["sum_removed_positive"] == 25000.0
        assert by_col["ElektriciteitVermogen"]["balance_removed"] == 24994.0
        # n_below of 0 on a one-sided threshold means "not checked".
        assert pd.isna(by_col["TemperatuurWoonkamer"]["lower_bound"])

    def test_the_cadence_the_bound_was_scaled_to_is_carried(self):
        """apply_thresholds_to_df records it on each column's stats, and it was
        being dropped here -- leaving the sidecar's cadence_minutes null on
        every row, so a removal could not be read against the interval its
        bound was scaled to."""
        stats = {
            "ElektriciteitNetgebruikDiff": {
                "n_below": 0, "n_above": 1,
                "sum_removed_negative": 0.0, "sum_removed_positive": 9.0,
                "balance_removed": 9.0,
                "min_removed": 9.0, "max_removed": 9.0,
                "lower_bound": 0.0, "upper_bound": 1.5,
                "cadence_minutes": 60,
            },
        }
        rows = threshold_stats_to_rows(7, stats)
        assert rows[0]["cadence_minutes"] == 60

    def test_a_column_with_no_measured_cadence_carries_na(self):
        """A column the pipeline built is absent from the cadence table, so
        there is no measured interval to report."""
        stats = {
            "ZonopwekBruto": {
                "n_below": 0, "n_above": 1,
                "sum_removed_negative": 0.0, "sum_removed_positive": 5.0,
                "balance_removed": 5.0,
                "min_removed": 5.0, "max_removed": 5.0,
                "lower_bound": 0.0, "upper_bound": 1.0,
                "cadence_minutes": pd.NA,
            },
        }
        rows = threshold_stats_to_rows(7, stats)
        assert pd.isna(rows[0]["cadence_minutes"])


class TestARemovalRecordsItsCondition:
    """A signal names an observed condition, an action names what the pipeline
    did. A removal needs both: the threshold pass, the negative-diff cases and
    frozen-span treatment all pair one with NULLED. Coercion did not, so a
    reader saw "removed" with no reason.
    """

    COL = "ElektriciteitNetgebruik"

    def _frame(self, values):
        return pd.DataFrame({
            "ReadingDate": pd.date_range(
                "2026-01-01", periods=len(values), freq="5min"),
            self.COL: values,
        })

    def test_an_unparseable_value_records_both_signal_and_action(self):
        from etdmap.flags import MappingFlag, flag_column
        out = rearrange_model_columns(
            self._frame(["1.5", "not-a-number", "3.5"]), add_columns=False,
        )
        bits = out[flag_column(self.COL)].fillna(0).astype("int64")
        assert ((bits & MappingFlag.UNPARSEABLE_VALUE.value) != 0).tolist() == [
            False, True, False,
        ]
        assert ((bits & MappingFlag.NULLED.value) != 0).tolist() == [
            False, True, False,
        ]

    def test_it_is_a_signal_that_makes_the_cell_unusable(self):
        from etdmap.flags import MappingFlag
        assert MappingFlag.UNPARSEABLE_VALUE & MappingFlag.SIGNALS
        assert not (MappingFlag.UNPARSEABLE_VALUE & MappingFlag.ACTIONS)
        assert MappingFlag.UNPARSEABLE_VALUE & MappingFlag.NOT_A_MEASUREMENT


class TestAPartialCombineRecordsTheLoss:
    """combine_source_columns preserves pd.NA, so a reading where only some
    sources delivered yields an empty target -- and the sources are dropped
    straight after, so a delivered value is destroyed. Without the pair the
    cell is indistinguishable from one no source ever covered.
    """

    TARGET = "ElektriciteitsgebruikWarmtepomp"

    @staticmethod
    def _df(**cols):
        return pd.DataFrame(
            {k: pd.array(v, dtype="Float64") for k, v in cols.items()}
        )

    def _combine(self, binnen, buiten):
        df = self._df(**{
            f"{self.TARGET}_binnen": binnen,
            f"{self.TARGET}_buiten": buiten,
        })
        combine_source_columns(
            df, target=self.TARGET,
            sources=(f"{self.TARGET}_binnen", f"{self.TARGET}_buiten"),
            operator="sum",
        )
        return df

    def test_a_partial_reading_records_both_signal_and_action(self):
        from etdmap.flags import MappingFlag, flag_column
        df = self._combine([1.0, 2.0], [10.0, pd.NA])
        assert df[self.TARGET].isna().tolist() == [False, True]
        bits = df[flag_column(self.TARGET)].fillna(0).astype("int64")
        assert ((bits & MappingFlag.SOURCES_INCOMPLETE.value) != 0).tolist() == [
            False, True,
        ]
        assert ((bits & MappingFlag.NULLED.value) != 0).tolist() == [
            False, True,
        ]

    def test_a_reading_no_source_covered_records_nothing(self):
        """Nothing was delivered and nothing was lost, so there is no removal
        to record -- only the absence, which the delivery marking classifies."""
        from etdmap.flags import MappingFlag, flag_column
        df = self._combine([pd.NA, 2.0], [pd.NA, 20.0])
        bits = df[flag_column(self.TARGET)].fillna(0).astype("int64")
        assert not ((bits & MappingFlag.SOURCES_INCOMPLETE.value) != 0).iloc[0]
        assert not ((bits & MappingFlag.NULLED.value) != 0).iloc[0]

    def test_a_fully_covered_reading_is_combined_not_lost(self):
        from etdmap.flags import MappingFlag, flag_column
        df = self._combine([1.0, 2.0], [10.0, 20.0])
        bits = df[flag_column(self.TARGET)].fillna(0).astype("int64")
        assert ((bits & MappingFlag.COMBINED_COLUMNS.value) != 0).all()
        assert not ((bits & MappingFlag.SOURCES_INCOMPLETE.value) != 0).any()

    def test_it_is_a_signal_that_makes_the_cell_unusable(self):
        from etdmap.flags import MappingFlag
        assert MappingFlag.SOURCES_INCOMPLETE & MappingFlag.SIGNALS
        assert MappingFlag.SOURCES_INCOMPLETE & MappingFlag.NOT_A_MEASUREMENT


class TestDerivingAnIntervalIsRecorded:
    """The interval column is written as a difference of the cumulative. Where
    the supplier delivered an interval too, that delivered value is replaced --
    and nothing said so, which left mark_unchanged_values stamping NOT_CHANGED
    on a column mapping had written in full.
    """

    CUM = "ElektriciteitNetgebruik"
    DIFF = "ElektriciteitNetgebruikDiff"

    def _frame(self, cum=None, diff=None, n=4):
        data = {
            "ReadingDate": pd.date_range("2026-01-01", periods=n, freq="5min"),
        }
        if cum is not None:
            data[self.CUM] = pd.array(cum, dtype="Float64")
        if diff is not None:
            data[self.DIFF] = pd.array(diff, dtype="Float64")
        return pd.DataFrame(data)

    def test_a_derived_interval_says_so(self):
        from etdmap.flags import MappingFlag, flag_column
        from etdmap.mapping_helpers import add_diff_columns
        out = add_diff_columns(
            self._frame(cum=[10.0, 11.0, 12.0, 13.0]), context="t",
        )
        bits = out[flag_column(self.DIFF)].fillna(0).astype("int64")
        derived = MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE.value
        # Row 0 is the seeded zero, not a difference.
        assert not (bits.iloc[0] & derived)
        assert bits.iloc[0] & MappingFlag.FIRST_ZERO.value
        assert ((bits.iloc[1:] & derived) != 0).all()

    def test_an_overwritten_delivered_interval_carries_both_marks(self):
        """The pairing that makes the replacement readable: the delivery mark
        says the supplier sent a value here, the action says it was replaced."""
        from etdmap.flags import MappingFlag, flag_column, set_flags
        df = self._frame(cum=[10.0, 11.0, 12.0, 13.0], diff=[9.9, 9.9, 9.9, 9.9])
        # mark_delivery_and_cadence runs before add_diff_columns and records
        # what the supplier sent, which is what survives the overwrite.
        set_flags(df, self.DIFF, df[self.DIFF].notna(),
                  MappingFlag.RAW_DELIVERED_VALUE)
        from etdmap.mapping_helpers import add_diff_columns
        out = add_diff_columns(df, context="t")

        # The delivered 9.9s are gone, replaced by the cumulative's difference.
        assert out[self.DIFF].iloc[1:].tolist() == [1.0, 1.0, 1.0]
        bits = out[flag_column(self.DIFF)].fillna(0).astype("int64")
        assert ((bits.iloc[1:] & MappingFlag.RAW_DELIVERED_VALUE.value) != 0).all()
        assert ((bits.iloc[1:]
                 & MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE.value) != 0).all()

    def test_a_kept_delivered_interval_is_not_marked_derived(self):
        """Interval-only delivery: the cumulative is empty, so nothing is
        differenced and the delivered values stand."""
        from etdmap.flags import MappingFlag, flag_column
        from etdmap.mapping_helpers import add_diff_columns
        out = add_diff_columns(
            self._frame(cum=[pd.NA] * 4, diff=[0.5, 0.5, 0.5, 0.5]), context="t",
        )
        assert out[self.DIFF].tolist() == [0.5, 0.5, 0.5, 0.5]
        findings = flag_column(self.DIFF)
        if findings in out.columns:
            bits = out[findings].fillna(0).astype("int64")
            assert not ((
                bits & MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE.value
            ) != 0).any()

    def test_it_is_an_action_that_is_still_a_measurement(self):
        from etdmap.flags import MappingFlag
        f = MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE
        assert f & MappingFlag.ACTIONS
        assert f & MappingFlag.ACTIONS_TAKEN
        assert not (f & MappingFlag.INVENTED)
        assert not (f & MappingFlag.NOT_A_MEASUREMENT)

    def test_not_changed_no_longer_claims_the_column_was_left_alone(self):
        """The defect this flag closes. NOT_CHANGED is set exactly where
        ACTIONS_TAKEN is absent, so before this action existed a derived
        interval read as examined-and-unchanged."""
        from etdmap.flags import MappingFlag, flag_column
        from etdmap.mapping_helpers import mark_unchanged_values
        from etdmap.mapping_helpers import add_diff_columns
        out = add_diff_columns(
            self._frame(cum=[10.0, 11.0, 12.0, 13.0]), context="t",
        )
        mark_unchanged_values(out)
        bits = out[flag_column(self.DIFF)].fillna(0).astype("int64")
        assert not ((bits & MappingFlag.NOT_CHANGED.value) != 0).any()


class TestANegativeIntervalIsRecorded:
    """A variable declared monotonic cannot decrease, so a negative interval
    means its cumulative went backwards -- whoever produced the value.

    OUT_OF_BOUNDS_LOW also catches it, but only where the interval column
    declares a Min, which is metadata rather than a code guarantee, and it
    states a bounds violation rather than a monotonicity one. Monotonicity is
    declared on the cumulative and derived for the interval paired with it, so
    this signal needs no threshold at all.
    """

    CUM = "ElektriciteitNetgebruik"
    DIFF = "ElektriciteitNetgebruikDiff"
    #: Momentaan: a decrease here is simply the reading.
    FREE = "ElektriciteitVermogen"

    def _frame(self, **cols):
        n = len(next(iter(cols.values())))
        data = {
            "ReadingDate": pd.date_range("2026-01-01", periods=n, freq="5min"),
        }
        for name, values in cols.items():
            data[name] = pd.array(values, dtype="Float64")
        return pd.DataFrame(data)

    def test_a_delivered_negative_interval_is_flagged(self):
        """Interval-only delivery: the cumulative is absent, so the supplier's
        values are kept and the negative is the supplier's own."""
        from etdmap.flags import MappingFlag, flag_column
        from etdmap.mapping_helpers import add_diff_columns
        out = add_diff_columns(
            self._frame(**{
                self.CUM: [pd.NA] * 5,
                self.DIFF: [0.5, 0.5, -0.3, 0.5, 0.5],
            }),
            context="t",
        )
        assert out[self.DIFF].iloc[2] == -0.3, "the delivered value is kept"
        bits = out[flag_column(self.DIFF)].fillna(0).astype("int64")
        assert ((bits & MappingFlag.NEGATIVE_INTERVAL.value) != 0).tolist() == [
            False, False, True, False, False,
        ]

    def test_it_is_set_before_the_threshold_pass_removes_the_value(self):
        """The signal does not depend on a Min being declared: it is recorded
        where the interval is produced, which is step 7, while the threshold
        pass runs at step 9."""
        from etdmap.flags import MappingFlag, flag_column
        from etdmap.mapping_helpers import add_diff_columns
        out = add_diff_columns(
            self._frame(**{
                self.CUM: [pd.NA] * 3,
                self.DIFF: [0.5, -0.3, 0.5],
            }),
            context="t",
        )
        # Still holding the negative value, already flagged.
        assert out[self.DIFF].iloc[1] == -0.3
        bits = out[flag_column(self.DIFF)].fillna(0).astype("int64")
        assert bits.iloc[1] & MappingFlag.NEGATIVE_INTERVAL.value

    def test_a_variable_that_may_decrease_is_never_flagged(self):
        """The scope. A momentaan reading falling is the reading, not a fault,
        and such a column leaves add_diff_columns before this site."""
        from etdmap.flags import MappingFlag, flag_column
        from etdmap.mapping_helpers import add_diff_columns
        out = add_diff_columns(
            self._frame(**{self.FREE: [5.0, -2.0, 3.0]}), context="t",
        )
        for column in out.columns:
            findings = flag_column(column)
            if findings in out.columns:
                bits = out[findings].fillna(0).astype("int64")
                assert not (
                    (bits & MappingFlag.NEGATIVE_INTERVAL.value) != 0
                ).any(), f"{column} was flagged and may legitimately decrease"

    def test_a_clean_monotonic_column_is_not_flagged(self):
        from etdmap.flags import MappingFlag, flag_column
        from etdmap.mapping_helpers import add_diff_columns
        out = add_diff_columns(
            self._frame(**{self.CUM: [10.0, 11.0, 12.0, 13.0]}), context="t",
        )
        bits = out[flag_column(self.DIFF)].fillna(0).astype("int64")
        assert not ((bits & MappingFlag.NEGATIVE_INTERVAL.value) != 0).any()

    def test_it_is_a_signal_that_makes_the_cell_unusable(self):
        from etdmap.flags import MappingFlag
        f = MappingFlag.NEGATIVE_INTERVAL
        assert f & MappingFlag.SIGNALS
        assert not (f & MappingFlag.ACTIONS)
        assert f & MappingFlag.NOT_A_MEASUREMENT


class TestGridCollisionsAreCounted:
    """Two readings in one grid slot: groupby.first() keeps the one closest to
    the column's characteristic offset and the other is dropped. The discarded
    reading is gone and cannot carry a flag, so a count is the only record
    available.
    """

    COL = "ElektriciteitNetgebruik"

    def _frame(self, stamps, values):
        return pd.DataFrame({
            "ReadingDate": pd.to_datetime(stamps),
            self.COL: pd.array(values, dtype="Float64"),
        })

    def test_a_discarded_value_is_counted(self):
        from etdmap.mapping_helpers import snap_readings_to_grid
        out, collisions = snap_readings_to_grid(
            self._frame(
                ["2026-01-01 00:00:10", "2026-01-01 00:00:50",
                 "2026-01-01 00:05:10"],
                [1.0, 2.0, 3.0],
            ),
            freq_minutes=5, context="t", return_stats=True,
        )
        assert len(out) == 2
        assert collisions[self.COL]["n_discarded"] == 1
        assert collisions[self.COL]["n_slots_contested"] == 1
        assert collisions[self.COL]["max_per_slot"] == 2

    def test_one_reading_per_slot_discards_nothing(self):
        from etdmap.mapping_helpers import snap_readings_to_grid
        _, collisions = snap_readings_to_grid(
            self._frame(
                ["2026-01-01 00:00:10", "2026-01-01 00:05:10"], [1.0, 2.0],
            ),
            freq_minutes=5, context="t", return_stats=True,
        )
        assert collisions == {}

    def test_a_second_row_holding_no_value_discards_nothing(self):
        """A slot with two rows is not a collision for a column only one of
        them reported in -- the merge loses nothing there."""
        from etdmap.mapping_helpers import snap_readings_to_grid
        _, collisions = snap_readings_to_grid(
            self._frame(
                ["2026-01-01 00:00:10", "2026-01-01 00:00:50"],
                [1.0, pd.NA],
            ),
            freq_minutes=5, context="t", return_stats=True,
        )
        assert collisions == {}

    def test_the_default_return_is_still_one_frame(self):
        from etdmap.mapping_helpers import snap_readings_to_grid
        out = snap_readings_to_grid(
            self._frame(
                ["2026-01-01 00:00:10", "2026-01-01 00:00:50"], [1.0, 2.0],
            ),
            freq_minutes=5, context="t",
        )
        assert isinstance(out, pd.DataFrame)

    def test_rows_carry_the_household_and_column(self):
        from etdmap.mapping_helpers import grid_collisions_to_rows
        rows = grid_collisions_to_rows(42, {
            self.COL: {
                "n_discarded": 3, "n_slots_contested": 2, "max_per_slot": 3,
            },
        })
        assert rows[0]["HuisBatchIdBSV"] == 42
        assert rows[0]["column"] == self.COL
        assert rows[0]["n_discarded"] == 3

    def test_empty_or_none_yields_no_rows(self):
        from etdmap.mapping_helpers import grid_collisions_to_rows
        assert grid_collisions_to_rows(1, {}) == []
        assert grid_collisions_to_rows(1, None) == []

    def test_the_sidecar_is_typed(self, tmp_path):
        from etdmap.mapping_helpers import write_grid_collisions
        path = write_grid_collisions(
            [{"HuisBatchIdBSV": 1, "column": self.COL, "n_discarded": 2,
              "n_slots_contested": 1, "max_per_slot": 3}],
            str(tmp_path),
        )
        out = pd.read_parquet(path, dtype_backend="numpy_nullable")
        assert len(out) == 1
        assert str(out["HuisBatchIdBSV"].dtype) == "Int64"
        assert str(out["column"].dtype) == "string"
        assert str(out["n_discarded"].dtype) == "Int64"

    def test_an_empty_sidecar_still_has_the_schema(self, tmp_path):
        from etdmap.mapping_helpers import write_grid_collisions
        path = write_grid_collisions([], str(tmp_path))
        out = pd.read_parquet(path, dtype_backend="numpy_nullable")
        assert out.empty
        assert "n_discarded" in out.columns

    def test_the_surviving_cell_says_it_was_one_of_several(self):
        """The count says how many readings a column lost; it cannot say which
        cells were contested. The survivor records that for itself."""
        from etdmap.flags import MappingFlag, flag_column
        from etdmap.mapping_helpers import snap_readings_to_grid
        out = snap_readings_to_grid(
            self._frame(
                ["2026-01-01 00:00:10", "2026-01-01 00:00:50",
                 "2026-01-01 00:05:10"],
                [1.0, 2.0, 3.0],
            ),
            freq_minutes=5, context="t",
        )
        bits = out[flag_column(self.COL)].fillna(0).astype("int64")
        assert ((bits & MappingFlag.SLOT_COLLISION.value) != 0).tolist() == [
            True, False,
        ]

    def test_an_uncontested_slot_is_not_marked(self):
        from etdmap.flags import MappingFlag, flag_column
        from etdmap.mapping_helpers import snap_readings_to_grid
        out = snap_readings_to_grid(
            self._frame(
                ["2026-01-01 00:00:10", "2026-01-01 00:05:10"], [1.0, 2.0],
            ),
            freq_minutes=5, context="t",
        )
        findings = flag_column(self.COL)
        if findings in out.columns:
            bits = out[findings].fillna(0).astype("int64")
            assert not ((bits & MappingFlag.SLOT_COLLISION.value) != 0).any()

    def test_the_kept_reading_is_still_a_measurement(self):
        """It was delivered. Putting it in NOT_A_MEASUREMENT would drop a real
        reading out of every project average."""
        from etdmap.flags import MappingFlag
        assert MappingFlag.SLOT_COLLISION & MappingFlag.SIGNALS
        assert not (MappingFlag.SLOT_COLLISION & MappingFlag.ACTIONS)
        assert not (MappingFlag.SLOT_COLLISION & MappingFlag.NOT_A_MEASUREMENT)


class TestSnappingKeepsAFlagWithItsValue:
    """A findings column is not a sensor stream. Measured on a synthetic frame
    before the fix: the value survived from the :50 row while its flag was
    taken from the :05 row, because set_flags makes a flag column non-null on
    every row, so it resolved to a different characteristic offset from the
    column it describes and .first() read the two from different rows.
    """

    COL = "ElektriciteitsgebruikWarmtepomp"

    def _frame(self, stamps, values, flagged):
        from etdmap.flags import MappingFlag, set_flags
        df = pd.DataFrame({
            "ReadingDate": pd.to_datetime(stamps),
            self.COL: pd.array(values, dtype="Float64"),
        })
        set_flags(df, self.COL, pd.Series(flagged), MappingFlag.COMBINED_COLUMNS)
        return df

    def test_the_flag_follows_the_value_that_survived(self):
        from etdmap.flags import MappingFlag, flag_column
        from etdmap.mapping_helpers import snap_readings_to_grid
        # The value is delivered only on the :50 rows and flagged there.
        out = snap_readings_to_grid(
            self._frame(
                ["2026-01-01 00:00:05", "2026-01-01 00:00:50",
                 "2026-01-01 00:05:05", "2026-01-01 00:05:50"],
                [pd.NA, 2.0, pd.NA, 4.0],
                [False, True, False, True],
            ),
            freq_minutes=5, context="t",
        )
        assert out[self.COL].tolist() == [2.0, 4.0]
        bits = out[flag_column(self.COL)].fillna(0).astype("int64")
        assert ((bits & MappingFlag.COMBINED_COLUMNS.value) != 0).all(), (
            "the surviving value kept its own flag"
        )

    def test_a_losing_rows_flag_is_not_inherited(self):
        """OR-ing the loser's bits onto the winner would fabricate history, and
        an inherited FILLED_DOWN would drop a delivered reading out of every
        project average. That siblings existed is what SLOT_COLLISION says."""
        from etdmap.flags import MappingFlag, flag_column, set_flags
        from etdmap.mapping_helpers import snap_readings_to_grid
        df = self._frame(
            ["2026-01-01 00:00:05", "2026-01-01 00:00:50"],
            [1.0, 2.0],
            [True, False],
        )
        # The farther row carries an INVENTED bit; it must not reach the winner.
        set_flags(df, self.COL, pd.Series([False, True]), MappingFlag.FILLED_DOWN)
        out = snap_readings_to_grid(df, freq_minutes=5, context="t")

        assert out[self.COL].tolist() == [1.0]
        bits = out[flag_column(self.COL)].fillna(0).astype("int64")
        assert bits.iloc[0] & MappingFlag.COMBINED_COLUMNS.value
        assert not (bits.iloc[0] & MappingFlag.FILLED_DOWN.value)
        assert not (bits.iloc[0] & MappingFlag.NOT_A_MEASUREMENT.value)
        # and the collision itself is recorded
        assert bits.iloc[0] & MappingFlag.SLOT_COLLISION.value


class TestNegativeDiffMagnitudesAreRecorded:
    """The flags on a corrected register say which readings were condemned and
    why. They cannot say how far it went back, and a dip of 5 reads the same as
    one of 5000 -- the magnitudes exist nowhere once the readings are blanked.
    """

    COL = "ElektriciteitNetgebruik"

    def _frame(self, values, households=None):
        data = {
            "ReadingDate": pd.date_range(
                "2026-01-01", periods=len(values), freq="5min",
            ),
            self.COL: pd.array(values, dtype="Float64"),
        }
        if households is not None:
            data["HuisIdBSV"] = pd.array(households, dtype="Int64")
        return pd.DataFrame(data)

    def test_a_dip_records_its_magnitude_and_classification(self):
        from etdmap.mapping_helpers import add_diff_columns
        # Dips by 5 at index 3 and recovers above the previous value: a
        # jump-back, so one reading is condemned.
        _, stats = add_diff_columns(
            self._frame([100.0, 110.0, 120.0, 115.0, 125.0, 135.0]),
            context="t", return_stats=True,
        )
        s = stats[self.COL]
        assert s["n_dips"] == 1
        assert s["sum_dip_magnitude"] == -5.0
        assert s["min_dip"] == -5.0
        assert s["max_dip"] == -5.0
        assert s["n_jump_back"] == 1
        assert s["n_readings_removed"] == 1

    def test_a_column_with_no_decrease_is_absent(self):
        """Absent rather than zero-filled, so an empty dict means nothing was
        corrected rather than nothing was looked at."""
        from etdmap.mapping_helpers import add_diff_columns
        _, stats = add_diff_columns(
            self._frame([100.0, 110.0, 120.0, 130.0]),
            context="t", return_stats=True,
        )
        assert self.COL not in stats

    def test_the_default_return_is_still_one_frame(self):
        """return_stats defaults to False, so every existing caller is
        unaffected."""
        from etdmap.mapping_helpers import add_diff_columns
        from etdmap.mapping_helpers import add_diff_columns
        out = add_diff_columns(
            self._frame([100.0, 110.0, 105.0, 120.0]), context="t",
        )
        assert isinstance(out, pd.DataFrame)

    def test_figures_accumulate_across_households(self):
        """One call may cover many households, and a column may be corrected in
        each. The counts add; the extremes take the outer value."""
        from etdmap.mapping_helpers import add_diff_columns
        _, stats = add_diff_columns(
            self._frame(
                [100.0, 110.0, 105.0, 120.0, 200.0, 210.0, 190.0, 230.0],
                households=[1, 1, 1, 1, 2, 2, 2, 2],
            ),
            id_column="HuisIdBSV", context="t", return_stats=True,
        )
        s = stats[self.COL]
        assert s["n_dips"] == 2
        assert s["sum_dip_magnitude"] == -25.0
        assert s["min_dip"] == -20.0
        assert s["max_dip"] == -5.0

    def test_rows_carry_the_household_and_column(self):
        from etdmap.mapping_helpers import negative_diff_stats_to_rows
        rows = negative_diff_stats_to_rows(42, {
            self.COL: {
                "n_dips": 1, "n_readings_removed": 1,
                "sum_dip_magnitude": -5.0, "min_dip": -5.0, "max_dip": -5.0,
                "n_jump_back": 1, "n_repeat_negative": 0, "n_meter_reset": 0,
                "n_no_recovery": 0, "n_zero_total": 0,
            },
        })
        assert rows[0]["HuisBatchIdBSV"] == 42
        assert rows[0]["column"] == self.COL
        assert rows[0]["sum_dip_magnitude"] == -5.0

    def test_empty_or_none_yields_no_rows(self):
        from etdmap.mapping_helpers import negative_diff_stats_to_rows
        assert negative_diff_stats_to_rows(1, {}) == []
        assert negative_diff_stats_to_rows(1, None) == []

    def test_the_sidecar_is_typed(self, tmp_path):
        from etdmap.mapping_helpers import write_negative_diff_removals
        rows = [{
            "HuisBatchIdBSV": 1, "column": self.COL,
            "n_dips": 2, "n_readings_removed": 2,
            "sum_dip_magnitude": -25.0, "min_dip": -20.0, "max_dip": -5.0,
            "n_jump_back": 1, "n_repeat_negative": 0, "n_meter_reset": 1,
            "n_no_recovery": 0, "n_zero_total": 0,
        }]
        path = write_negative_diff_removals(rows, str(tmp_path))
        out = pd.read_parquet(path, dtype_backend="numpy_nullable")
        assert len(out) == 1
        assert str(out["HuisBatchIdBSV"].dtype) == "Int64"
        assert str(out["column"].dtype) == "string"
        assert str(out["sum_dip_magnitude"].dtype) == "Float64"
        assert str(out["n_dips"].dtype) == "Int64"

    def test_an_empty_sidecar_still_has_the_schema(self, tmp_path):
        from etdmap.mapping_helpers import write_negative_diff_removals
        path = write_negative_diff_removals([], str(tmp_path))
        out = pd.read_parquet(path, dtype_backend="numpy_nullable")
        assert out.empty
        assert "sum_dip_magnitude" in out.columns

    def test_the_pipeline_carries_the_figures_out(self):
        """The mappers read _validation_summary, so the figures have to arrive
        there or they are computed and dropped."""
        from etdmap.mapping_helpers import add_diff_columns
        _, stats = add_diff_columns(
            self._frame([100.0, 110.0, 105.0, 120.0]),
            context="t", return_stats=True,
        )
        # The key run_standard_pipeline puts this under.
        summary = {"negative_diff_stats": stats}
        assert summary["negative_diff_stats"][self.COL]["n_dips"] == 1


class TestWriteThresholdRemovals:
    """write_threshold_removals writes a typed sidecar parquet (ADR-005)."""

    EXPECTED_COLS = [
        "HuisBatchIdBSV", "column", "n_below", "n_above",
        "sum_removed_negative", "sum_removed_positive", "balance_removed",
        "min_removed", "max_removed",
        "lower_bound", "upper_bound", "cadence_minutes",
    ]

    def test_writes_rows_with_nullable_dtypes(self, tmp_path):
        rows = [
            {"HuisBatchIdBSV": 1, "column": "A", "n_below": 2, "n_above": 0,
             "sum_removed_negative": -1.5, "sum_removed_positive": 0.0,
             "balance_removed": -1.5,
             "min_removed": -1.0, "max_removed": -0.5,
             "lower_bound": 0.0, "upper_bound": pd.NA},
            {"HuisBatchIdBSV": 1, "column": "B", "n_below": 0, "n_above": 4,
             "sum_removed_negative": 0.0, "sum_removed_positive": 600.0,
             "balance_removed": 600.0,
             "min_removed": 100.0, "max_removed": 200.0,
             "lower_bound": 0.0, "upper_bound": 50.0},
        ]
        path = write_threshold_removals(rows, str(tmp_path))
        out = pd.read_parquet(path, dtype_backend="numpy_nullable")
        assert list(out.columns) == self.EXPECTED_COLS
        assert len(out) == 2
        assert str(out["HuisBatchIdBSV"].dtype) == "Int64"
        assert str(out["n_below"].dtype) == "Int64"
        assert str(out["min_removed"].dtype) == "Float64"
        assert str(out["column"].dtype) == "string"
        # An unset bound survives the round trip as pd.NA, so a reader can tell
        # "nothing above the bound" from "no bound to check against".
        assert pd.isna(out.loc[0, "upper_bound"])
        assert out.loc[1, "upper_bound"] == 50.0

    def test_empty_rows_writes_typed_empty_frame(self, tmp_path):
        path = write_threshold_removals([], str(tmp_path))
        out = pd.read_parquet(path, dtype_backend="numpy_nullable")
        assert list(out.columns) == self.EXPECTED_COLS
        assert len(out) == 0
        assert str(out["min_removed"].dtype) == "Float64"


class TestAnnotateMappedBsvVariable:
    """annotate_mapped_bsv_variable tags each raw column with its BSV variable."""

    def test_maps_known_and_leaves_unknown_as_na(self):
        stats = pd.DataFrame({
            "column": ["HeatPumpPowerKWInst", "Datum", "Adres"],
            "count": pd.array([10, 10, 10], dtype="Int64"),
        })
        mapping = {"HeatPumpPowerKWInst": "ElektriciteitVermogenWarmtepomp"}
        out = annotate_mapped_bsv_variable(stats, mapping)
        assert str(out["mapped_bsv_variable"].dtype) == "string"
        assert out.loc[0, "mapped_bsv_variable"] == "ElektriciteitVermogenWarmtepomp"
        assert pd.isna(out.loc[1, "mapped_bsv_variable"])
        assert pd.isna(out.loc[2, "mapped_bsv_variable"])
        # original input is not mutated
        assert "mapped_bsv_variable" not in stats.columns

    def test_custom_raw_col_field_for_prefixed_keys(self):
        stats = pd.DataFrame({"sheet": ["elec", "heat"], "column": ["P", "Q"]})
        stats["_map_key"] = stats["sheet"] + "_" + stats["column"]
        mapping = {"elec_P": "ElektriciteitVermogen"}
        out = annotate_mapped_bsv_variable(stats, mapping, raw_col_field="_map_key")
        assert out.loc[0, "mapped_bsv_variable"] == "ElektriciteitVermogen"
        assert pd.isna(out.loc[1, "mapped_bsv_variable"])


# ---------------------------------------------------------------------------
# Threshold application (fail-paths). These exercise the actual masking logic
# with crafted out-of-bound input -- coverage the synthetic fixtures cannot
# give now that generation ranges are decoupled from thresholds (data drawn
# inside the bounds never trips them). This is where "a wrong/too-tight
# threshold is catchable" is proven.
# ---------------------------------------------------------------------------

class TestApplyThresholds:
    def test_apply_threshold_to_col_nulls_out_of_bounds(self):
        df = pd.DataFrame({"X": pd.array([-1.0, 0.5, 2.0, 0.0, 1.0], dtype="Float64")})
        out, stats = apply_threshold_to_col(df.copy(), "X", lower_bound=0.0, upper_bound=1.0)
        assert out["X"].isna().tolist() == [True, False, True, False, False]
        assert out["X"].dropna().astype(float).tolist() == [0.5, 0.0, 1.0]
        assert stats == {
            "n_below": 1, "n_above": 1,
            "sum_removed_negative": -1.0, "sum_removed_positive": 2.0,
            "balance_removed": 1.0,
            "min_removed": -1.0, "max_removed": 2.0,
            "lower_bound": 0.0, "upper_bound": 1.0,
        }

    def test_apply_threshold_to_col_all_in_bounds_returns_none_stats(self):
        df = pd.DataFrame({"X": pd.array([0.0, 0.5, 1.0], dtype="Float64")})
        out, stats = apply_threshold_to_col(df.copy(), "X", lower_bound=0.0, upper_bound=1.0)
        assert stats is None
        assert int(out["X"].isna().sum()) == 0

    def test_apply_thresholds_to_df_masks_using_live_threshold_dict(self, monkeypatch):
        # A wrong/too-tight threshold IS catchable: data crossing the bound is
        # nulled and reported.
        monkeypatch.setattr(
            "etdmap.mapping_helpers.thresholds_dict",
            {"X": {"Min": 0.0, "Max": 1.0}},
        )
        df = pd.DataFrame({"X": pd.array([0.5, 5.0, 0.2], dtype="Float64")})
        out, stats = apply_thresholds_to_df(df.copy(), return_stats=True)
        assert out["X"].isna().tolist() == [False, True, False]
        assert stats["X"]["n_above"] == 1
        assert stats["X"]["max_removed"] == 5.0


class TestPipelineDropsUnmappedColumns:
    """End-to-end: run_standard_pipeline must not write unmapped columns, but
    must still write its derived Diff and validate_* columns."""

    @pytest.fixture(autouse=True)
    def _flat_output_format(self):
        import etdmap
        prev = etdmap.options.mapped_output_format
        etdmap.options.mapped_output_format = "flat"
        yield
        etdmap.options.mapped_output_format = prev

    def test_unmapped_dropped_derived_kept(self, tmp_path):
        df = _minimal_pipeline_df(n=12)
        df["alklimaHeatPump_holiday"] = pd.array([1] * 12, dtype="Int64")
        df["nan [nan]"] = pd.array([0] * 12, dtype="Int64")

        run_standard_pipeline(
            df, household_id=77, household_batch_id=1077, household_id_supplier="H77",
            mapped_folder_path=tmp_path, grid_minutes=GRID,
            cadence=_pipeline_cadence(df))
        out = pd.read_parquet(tmp_path / "household_77_table.parquet", dtype_backend="numpy_nullable")

        assert "alklimaHeatPump_holiday" not in out.columns
        assert "nan [nan]" not in out.columns
        # the pipeline's own derived columns are added after the drop
        assert any(c.endswith("Diff") for c in out.columns)
        assert any(c.startswith("validate_") for c in out.columns)
