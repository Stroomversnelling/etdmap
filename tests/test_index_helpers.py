import os

import pandas as pd
import pytest

import etdmap
import etdmap.index_helpers as index_helpers
import etdmap.mapping_helpers
from etdmap import ids
from etdmap.data_model import cumulative_columns, interval_column
from etdmap.index_helpers import (
    bsv_metadata_columns,
    get_bsv_metadata,
    read_metadata,
)
from etdmap.record_validators import columns_with_value_bounds
from etdmap.storage import mapped_household_paths
from etdmap.testing import DEFAULT_HOUSEHOLDS_PER_PROJECT, DEFAULT_PROJECTS


def test_read_metadata(valid_metadata_file, invalid_metadata_file):
    # Test the function with the valid file fixture
    result = read_metadata(valid_metadata_file)
    assert isinstance(result, pd.DataFrame)
    assert set(result.columns) == set(["num_column", *bsv_metadata_columns])

    # Test the function with an invalid file
    with pytest.raises(Exception) as excinfo:
        read_metadata(invalid_metadata_file, required_columns=["TestCol"])
    assert "Not all required columns" in str(excinfo.value)


def test_get_bsv_metadata(valid_metadata_file, invalid_metadata_file):
    # test valid:
    etdmap.options.bsv_metadata_file = valid_metadata_file
    result = get_bsv_metadata()
    required_columns = set(bsv_metadata_columns)
    assert isinstance(result, pd.DataFrame)
    assert required_columns.issubset(set(result.columns))

    # test if it fails without proper columns
    etdmap.options.bsv_metadata_file = invalid_metadata_file
    with pytest.raises(ValueError) as excinfo:
        get_bsv_metadata()

    assert "Not all required columns in" in str(excinfo.value)


def test_creation_validation_columns_index_data_files(mapped_fixtures, request):
    """
    Functional test of dataset_validators.dataset_flag_conditions.

    0. The mapped_fixtures fixture (in conftest.py) creates the household
       parquet files and index.parquet.
    Then Checks if:
    1. Appropirate columns are created in datafiles (record_validators) and index file (``dataset_validators``)
    2. If True/False values are found as expected.
    """
    # The count the generated dataset was built with. The shape is a parameter
    # of the generator, so a literal here would contradict any other shape.
    limit_houses = DEFAULT_HOUSEHOLDS_PER_PROJECT * DEFAULT_PROJECTS
    folder_path = mapped_fixtures

    ### 1: Test household file creation ###
    # Through the storage reader, which resolves whichever layout the mapping
    # wrote.
    files = mapped_household_paths(folder_path)
    assert len(files) == limit_houses, f"Expected {limit_houses} files, but found {len(files)}"

    # check for 1 file if it contains all the right columns
    df_hh = pd.read_parquet(files[min(files)], dtype_backend="numpy_nullable")
    # columns that should be added by record_validators
    # The interval column is the one the data model links to the cumulative
    # variable, not its name plus "Diff".
    assert all(
        "validate_" + interval_column(col) in df_hh.columns
        for col in cumulative_columns
    )
    assert all(
        "validate_" + interval_column(col) + "_outliers" in df_hh.columns
        for col in cumulative_columns
    )
    assert all('validate_' + col in df_hh.columns for col in columns_with_value_bounds)

    # Check the index.parquet file
    df_index = pd.read_parquet(os.path.join(folder_path, 'index.parquet'), dtype_backend="numpy_nullable")
    # Columns that should be added to index.parquet
    # as defined in dataset_validors.py (and applied in the index_helpers.py)
    special_checks = (
        "validate_monitoring_data_counts",
        "validate_energiegebruik_warmteopwekker",
        "validate_approximately_one_year_of_records",
        "validate_columns_exist",
        "validate_no_readingdate_gap"
    )

    standard_columns = (
        "Meenemen",
        "Notities"
    )
    assert all(check in df_index.columns for check in standard_columns)
    assert all(check in df_index.columns for check in special_checks)
    assert all('validate_' + col in df_index.columns for col in cumulative_columns)
    assert all(
        "validate_" + interval_column(col) in df_index.columns
        for col in cumulative_columns
    )
    # check for columns with 'validate' if it contains bool or NA datatypes
    validate_cols = df_index.filter(like='validate')

    # Check if all values in these columns are either bool or pd.NA
    assert validate_cols.apply(lambda col: col.dropna().map(type).isin([bool]).all()).all()

    # check for 1 household file if the values are correct.
    # When we have files.

# ---------------------------------------------------------------------------
# update_include(): populates the index `Meenemen` column from BSV metadata,
# and is the ADR-002/003 guard (raises on household-key mismatch, key-value
# mismatch, and any NA Meenemen). Self-contained: builds a tiny aligned index +
# BSV metadata in tmp_path and points etdmap.options at them via monkeypatch
# (auto-restored). Never touches the shared mapped folder, so the session
# autouse clean fixture is irrelevant here.
# ---------------------------------------------------------------------------

# Creates fixtures
def _include_index_df(household_ids, household_ids_supplier, data_supplier="etdmap"):
    n = len(household_ids)
    assert n == len(household_ids_supplier)
    df = pd.DataFrame({
        ids.HOUSEHOLD_SUPPLIER: household_ids_supplier,
        ids.HOUSEHOLD: household_ids,
        ids.PROJECT_SUPPLIER: ["P1"] * n,
        ids.PROJECT: [1] * n,
        ids.SUPPLIER: [data_supplier] * n,
        "Meenemen": [pd.NA] * n,   # pre-existing column -> exercises the drop() path
        "Notities": [pd.NA] * n,
    })
    return df.astype(index_helpers.metadata_dtypes)


def _include_bsv_df(household_ids, household_ids_supplier, include, data_supplier="etdmap"):
    n = len(household_ids)
    assert n == len(household_ids_supplier)
    assert n == len(include)
    return pd.DataFrame({
        ids.HOUSEHOLD_SUPPLIER: household_ids_supplier,
        ids.HOUSEHOLD: household_ids,
        ids.PROJECT_SUPPLIER: ["P1"] * n,
        ids.PROJECT: [1] * n,
        ids.SUPPLIER: [data_supplier] * n,
        "Meenemen": include,
        "Notities": [pd.NA] * n,
    })


def _setup_include(tmp_path, monkeypatch, index_df, bsv_df):
    index_df.to_parquet(tmp_path / "index.parquet", engine="pyarrow")
    bsv_path = tmp_path / "bsv.csv"
    bsv_df.to_csv(bsv_path, index=False)
    monkeypatch.setattr(etdmap.options, "mapped_folder_path", tmp_path)
    monkeypatch.setattr(etdmap.options, "bsv_metadata_file", str(bsv_path))


class TestUpdateMeenemen:
    def test_happy_path_populates_include(self, tmp_path, monkeypatch):
        _setup_include(
            tmp_path, monkeypatch,
            _include_index_df([1, 2, 3], ["H1", "H2", "H3"]),
            _include_bsv_df([1, 2, 3], ["H1", "H2", "H3"], [True, False, True]),
        )
        result = index_helpers.update_include()
        got = dict(zip(result[ids.HOUSEHOLD].tolist(), result["Meenemen"].tolist()))
        assert got == {1: True, 2: False, 3: True}
        assert len(result) == 3
        # persisted to disk with no NA Meenemen
        on_disk = pd.read_parquet(tmp_path / "index.parquet", dtype_backend="numpy_nullable")
        assert int(on_disk["Meenemen"].isna().sum()) == 0

    def test_raises_when_household_missing_in_metadata(self, tmp_path, monkeypatch):
        _setup_include(
            tmp_path, monkeypatch,
            _include_index_df([1, 2, 3], ["H1", "H2", "H3"]),
            _include_bsv_df([1, 2], ["H1", "H2"], [True, False]),
        )
        with pytest.raises(ValueError, match="Household mismatch detected"):
            index_helpers.update_include()

    def test_raises_when_household_missing_in_index(self, tmp_path, monkeypatch):
        _setup_include(
            tmp_path, monkeypatch,
            _include_index_df([1, 2], ["H1", "H2"]),
            _include_bsv_df([1, 2, 3], ["H1", "H2", "H3"], [True, False, True]),
        )
        with pytest.raises(ValueError, match="Household mismatch detected"):
            index_helpers.update_include()

    def test_raises_on_key_value_mismatch(self, tmp_path, monkeypatch):
        # Same ids.HOUSEHOLD set (so the missing-household check passes) but a key
        # column value differs -> reaches and trips the compare/merge branch.
        _setup_include(
            tmp_path, monkeypatch,
            _include_index_df([1, 2, 3], ["H1", "H2", "H3"]),
            _include_bsv_df([1, 2, 3], ["H1", "DIFFERENT", "H3"], [True, False, True]),
        )
        with pytest.raises(Exception, match="Mismatching index and bsv metadata values"):
            index_helpers.update_include()

    def test_raises_on_na_include(self, tmp_path, monkeypatch):
        _setup_include(
            tmp_path, monkeypatch,
            _include_index_df([1, 2, 3], ["H1", "H2", "H3"]),
            _include_bsv_df([1, 2, 3], ["H1", "H2", "H3"], [True, pd.NA, True]),
        )
        with pytest.raises(Exception, match="Not all rows in the BSV metadata file have defined Meenemen"):
            index_helpers.update_include()


if __name__ == "__main__":
    # Run pytest for debugging the testing
    pytest.main(["-v"])
