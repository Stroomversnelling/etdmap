import logging
import os
import warnings
from pathlib import Path

import conftest
import pandas as pd
import pyarrow.parquet as pq
import pytest
import yaml
from test_helpers import generate_metadata_parquet_file

import etdmap
import etdmap.index_helpers as index_helpers
import etdmap.mapping_helpers
import etdmap.mapping_helpers as mapping
from etdmap.data_model import cumulative_columns
from etdmap.index_helpers import (
    bsv_metadata_columns,
    get_bsv_metadata,
    read_metadata,
)
from etdmap.record_validators import columns_5min_momentaan, record_flag_conditions


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
    1. Appropirate columns are created in datafiles (record_validators) and index file (dataset validators)
    2. If True/False values are found as expected.
    """
    limit_houses = 10
    folder_path = mapped_fixtures

    ### 1: Test household.parquet creation ###
    files = os.listdir(folder_path)
    files = [f for f in files if (
        os.path.isfile(os.path.join(folder_path, f)) and \
            ('household' in f))]
    assert len(files) == limit_houses, f"Expected {limit_houses} files, but found {len(files)}"

    # check for 1 file if it contains all the right columns
    df_hh = pd.read_parquet(os.path.join(folder_path, files[0]))
    # columns that should be added by record_validators
    assert all("validate_" + col + "Diff" in df_hh.columns for col in cumulative_columns)
    assert all('validate_' + col + 'Diff_outliers' in df_hh.columns for col in cumulative_columns)
    assert all('validate_' + col in df_hh.columns for col in columns_5min_momentaan)

    # Check the index.parquet file
    df_index = pd.read_parquet(os.path.join(folder_path, 'index.parquet'))
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
    assert all('validate_' + col + 'Diff' in df_index.columns for col in cumulative_columns)
    # check for columns with 'validate' if it contains bool or NA datatypes
    validate_cols = df_index.filter(like='validate')

    # Check if all values in these columns are either bool or pd.NA
    assert validate_cols.apply(lambda col: col.dropna().map(type).isin([bool]).all()).all()

    # check for 1 household file if the values are correct.
    # When we have files.

def _check_metadatafiles_are_equal(load_metadata, stored_path, generated_path):

    expected_metadata = load_metadata(stored_path)

    parquet_file = pq.ParquetFile(generated_path)
    actual_metadata = generate_metadata_parquet_file(parquet_file)
    # The meta data contains:
    # the number of rows & cols,
    # for each column the min, max values and null count

    results = _diff_json(expected_metadata, actual_metadata)

    if len(results) > 0:
        logging.info(f"Found {len(results)} differences in stats of variables (test fixture, generated stats)")

    return results, expected_metadata, actual_metadata


def _check_samples_are_equal(expected_path, generated_path):
    """
    Checks if expected vs. generated samples of .parquet files are equal.

    Only compares columns present in the expected fixture, so new columns
    in the generated output don't cause failures.

    Returns:
        (is_equal, new_cols, removed_cols)
        - is_equal: shared columns have identical values in both
        - new_cols: columns in generated but not in fixture (model expanded)
        - removed_cols: columns in fixture but not in generated (model shrank)
    """
    df_expected = pd.read_parquet(expected_path)

    df_generated_full = pd.read_parquet(generated_path)
    sample_size = min(100, len(df_generated_full))
    df_generated_sample = df_generated_full.sample(n=sample_size, random_state=42)

    expected_cols = set(df_expected.columns)
    generated_cols = set(df_generated_sample.columns)
    new_cols = sorted(generated_cols - expected_cols)
    removed_cols = sorted(expected_cols - generated_cols)
    shared_cols = sorted(expected_cols & generated_cols)

    is_equal = df_expected[shared_cols].equals(df_generated_sample[shared_cols])
    return is_equal, new_cols, removed_cols


def _check_sample_values_equal(expected_path, generated_path):
    """
    Like _check_samples_are_equal but compares VALUES only -- both sides have
    their indexes reset before comparison. The full _check_samples_are_equal
    additionally enforces that the row indexes match (i.e. fixture and fresh
    sample reference the same source-rows by position), which is the
    value-at-a-certain-time guarantee. This sibling helper isolates pure value
    drift from index drift, useful as a regression-safety net when validating
    that a fixture's stored values are still correct even if its row index has
    been altered by an out-of-band rewrite.

    Returns the same tuple shape as _check_samples_are_equal.
    """
    df_expected = pd.read_parquet(expected_path)
    df_generated_full = pd.read_parquet(generated_path)
    sample_size = min(100, len(df_generated_full))
    df_generated_sample = df_generated_full.sample(n=sample_size, random_state=42)

    expected_cols = set(df_expected.columns)
    generated_cols = set(df_generated_sample.columns)
    new_cols = sorted(generated_cols - expected_cols)
    removed_cols = sorted(expected_cols - generated_cols)
    shared_cols = sorted(expected_cols & generated_cols)

    fa = df_expected[shared_cols].reset_index(drop=True)
    ga = df_generated_sample[shared_cols].reset_index(drop=True)
    is_equal = fa.equals(ga)
    return is_equal, new_cols, removed_cols


def _diff_json(a, b, path=""):
    results = []

    def _record(diff):
        logging.info(diff)
        results.append(diff)

    def _recurse(a, b, path):
        if type(a) != type(b):
            _record(f"{path}: type mismatch {type(a).__name__} != {type(b).__name__}")
        elif isinstance(a, dict):
            keys = set(a.keys()).union(b.keys())
            for k in keys:
                if k not in a:
                    _record(f"{path}.{k}: missing in first")
                elif k not in b:
                    _record(f"{path}.{k}: missing in second")
                else:
                    _recurse(a[k], b[k], f"{path}.{k}")
        elif isinstance(a, list):
            for i in range(min(len(a), len(b))):
                _recurse(a[i], b[i], f"{path}[{i}]")
            if len(a) != len(b):
                _record(f"{path}: list length differs {len(a)} != {len(b)}")
        else:
            if a != b:
                _record(f"{path}: {a} != {b}")

    _recurse(a, b, path or "$")
    return results


def _classify_metadata_diffs(results, expected_json, generated_json):
    """
    Splits diff results into new columns (model expanded → warn only) and real failures.

    "Missing in first" means the column is in generated but not in the fixture — this
    is expected when the data model grows and the fixture hasn't been regenerated yet.

    "Missing in second" means the fixture has a column that generated output lost — that
    is a regression and counts as a failure.

    The num_columns count mismatch is suppressed as a failure only when it is fully
    accounted for by new columns and there are no other failures.

    Returns:
        (new_col_names, failure_diffs)
    """
    new_col_diffs = [
        d for d in results
        if d.startswith("$.column_details.") and ": missing in first" in d
    ]
    num_col_diffs = [d for d in results if d.startswith("$.num_columns:")]
    failure_diffs = [d for d in results if d not in new_col_diffs and d not in num_col_diffs]

    # Suppress num_columns mismatch only when it is purely explained by new columns
    expected_n = expected_json.get("num_columns", 0)
    generated_n = generated_json.get("num_columns", 0)
    if num_col_diffs and generated_n == expected_n + len(new_col_diffs) and not failure_diffs:
        pass  # num_columns difference is fully accounted for by new columns → warn only
    else:
        failure_diffs.extend(num_col_diffs)

    new_col_names = [
        d.split("$.column_details.")[1].rsplit(":", 1)[0] for d in new_col_diffs
    ]
    return new_col_names, failure_diffs


def test_files_equal_expected(mapped_fixtures, load_metadata):
    """
    Checks for each file generated by the workflow if its metadata and sample
    match the expected fixture files.

    New columns in the generated output (data model expansion) are reported as
    warnings rather than failures, so the test stays green after adding columns.
    Differences in existing columns or columns removed from generated output are
    treated as failures.

    To regenerate fixtures after intentional data model changes:
        1. Run the full mapping workflow so updated parquet files are produced.
        2. From the etdmap project root, run:
               python tests/test_helpers.py
           This overwrites all metadata_*.json and sample_*.parquet in tests/data/.
        3. Review the changes with ``git diff tests/data/`` before committing.
    """
    for name in conftest.file_names:
        name = name.split('.parquet')[0]
        generated_path = os.path.join(etdmap.options.mapped_folder_path, f"{name}.parquet")

        # --- metadata check ---
        expected_metadata_path = Path(f"tests/data/metadata_{name}.json")
        results, expected_json, generated_json = _check_metadatafiles_are_equal(
            load_metadata,
            expected_metadata_path,
            generated_path,
        )
        new_cols, failure_diffs = _classify_metadata_diffs(results, expected_json, generated_json)

        if new_cols:
            warnings.warn(
                f"metadata_{name}.json fixture is outdated — {len(new_cols)} new column(s) in "
                f"generated output not yet in fixture: {new_cols}. "
                "Regenerate: python tests/test_helpers.py (see test docstring for details).",
                UserWarning,
                stacklevel=2,
            )

        assert len(failure_diffs) == 0, (
            f"metadata_{name}.json: {len(failure_diffs)} unexpected difference(s) between fixture "
            f"and generated output (see log for details):\n" + "\n".join(failure_diffs)
        )

        # --- sample check ---
        expected_sample_path = Path(f"tests/data/sample_{name}.parquet")
        is_equal, sample_new_cols, sample_removed_cols = _check_samples_are_equal(
            expected_sample_path,
            generated_path,
        )

        if sample_new_cols:
            warnings.warn(
                f"sample_{name}.parquet fixture is outdated — new column(s) not in sample "
                f"fixture: {sample_new_cols}. "
                "Regenerate: python tests/test_helpers.py (see test docstring for details).",
                UserWarning,
                stacklevel=2,
            )

        assert not sample_removed_cols, (
            f"sample_{name}.parquet: column(s) present in fixture are missing from generated "
            f"output: {sample_removed_cols}"
        )
        assert is_equal, (
            f"sample_{name}.parquet: shared-column values differ between fixture and generated output"
        )

def test_sample_values_equal_expected(mapped_fixtures, load_metadata):
    """
    Sibling regression-safety check to test_files_equal_expected: verifies that
    generated sample VALUES match the fixture, ignoring row-index alignment.

    test_files_equal_expected is the strict comparison (values + index) and
    enforces the value-at-a-certain-time guarantee. This test is the relaxed
    comparison used to confirm that pipeline values are still correct even if
    a fixture's stored row index has drifted (e.g. from an out-of-band rewrite).
    A pass here in the absence of a pass on test_files_equal_expected indicates
    a fixture index issue, not a values regression.
    """
    for name in conftest.file_names:
        name = name.split('.parquet')[0]
        generated_path = os.path.join(etdmap.options.mapped_folder_path, f"{name}.parquet")
        expected_sample_path = Path(f"tests/data/sample_{name}.parquet")
        is_equal, _new_cols, removed_cols = _check_sample_values_equal(
            expected_sample_path, generated_path
        )
        assert not removed_cols, (
            f"sample_{name}.parquet: column(s) present in fixture are missing from generated "
            f"output: {removed_cols}"
        )
        assert is_equal, (
            f"sample_{name}.parquet: shared-column values differ between fixture and generated output"
        )


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
        "HuisIdLeverancier": household_ids_supplier,
        "HuisIdBSV": household_ids,
        "ProjectIdLeverancier": ["P1"] * n,
        "ProjectIdBSV": [1] * n,
        "Dataleverancier": [data_supplier] * n,
        "Meenemen": [pd.NA] * n,   # pre-existing column -> exercises the drop() path
        "Notities": [pd.NA] * n,
    })
    return df.astype(index_helpers.metadata_dtypes)


def _include_bsv_df(household_ids, household_ids_supplier, include, data_supplier="etdmap"):
    n = len(household_ids)
    assert n == len(household_ids_supplier)
    assert n == len(include)
    return pd.DataFrame({
        "HuisIdLeverancier": household_ids_supplier,
        "HuisIdBSV": household_ids,
        "ProjectIdLeverancier": ["P1"] * n,
        "ProjectIdBSV": [1] * n,
        "Dataleverancier": [data_supplier] * n,
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
        got = dict(zip(result["HuisIdBSV"].tolist(), result["Meenemen"].tolist()))
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
        # Same HuisIdBSV set (so the missing-household check passes) but a key
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
