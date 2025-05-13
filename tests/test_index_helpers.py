import logging
import os
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


def _list_files_data_fixture(folder_path):
    return {f[:-8]: f for f in os.listdir(folder_path) if f.endswith(".parquet") and "index" not in f}


def _process_data_fixture_file(huis_code, file_name, etd_test_fixture_path, mapped_folder_path):
    file_path = os.path.join(etd_test_fixture_path, file_name)
    new_file_path = os.path.join(
        mapped_folder_path, f"household_{int(huis_code)}_table.parquet"
    )

    data_fixture_df = pd.read_parquet(file_path)

    ## Later potentially use if using the renaming of columns in the test fixtures
    # data_fixture_df.rename(columns=fixture_mapping_dict, inplace=True)

    data_fixture_df = mapping.ensure_intervals(data_fixture_df)

    data_fixture_df = mapping.rearrange_model_columns(
        household_df=data_fixture_df, add_columns=True, context=f"{huis_code}/{file_name}"
    )

    data_fixture_df = mapping.fill_down_infrequent_devices(df=data_fixture_df)

    data_fixture_df = mapping.add_diff_columns(data_fixture_df, context=f"{huis_code}/{file_name}")

    # Add validation flags
    for flag, condition in record_flag_conditions.items():
        try:
            data_fixture_df[flag] = condition(data_fixture_df)
        except Exception as e:
            logging.error(
                f"Error validating with {flag} for household {huis_code} / {file_name}: {e}",
                exc_info=True,
            )
            data_fixture_df[flag] = pd.NA

    data_fixture_df.to_parquet(new_file_path, engine="pyarrow")

    return {
        "HuisIdLeverancier": f'Huis{int(file_name.replace("household_", "").replace("_table.parquet", "")):02}',
        # "ProjectId": 3,
        "HuisCode": huis_code,
        "HuisIdBSV": huis_code
    }

def _run_mapping_of_etd_fixtures(raw_data_fixture: str, limit_houses:int=20) -> None:
    """
    Generate the mapping of the etd_fixtures.

    Calls the necessary functions in mapping_helpers
    and index_helpers to generate the household_i_table.parquet
    files.

    Also calls the validator functions of the record_validators
    and dataset_validators.py creating the additional columns.

    Setup:
    Before running this, ensure the index_bsv.parquet is copied from
    the 01 Raw data/etd_test_fixtures folder and renamed to index_bsv
    in your mapping folder (specified in your config_test.yaml)
    The path to the metadata file should also be specified there and refers
    to the etd_bsv_metadata file in the 01 Raw data/etd_test_fixtures folder.
    """
    def load_config(config_path):
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)

    test_config_path = Path("config_test.yaml")
    if os.path.isfile(test_config_path):
        config = load_config(test_config_path)

    etdmap.options.mapped_folder_path = Path(config['etdmap_configuration']['mapped_folder_path'])
    fixture_metadata_file = Path(config['etdmap_configuration']['bsv_metadata_file'])
    etdmap.options.bsv_metadata_file = fixture_metadata_file

    index_df, index_path = index_helpers.read_index()
    if 'Dataleverancier' not in index_df.columns:
        index_df.loc[:, 'Dataleverancier'] = 'etdmap'
    household_id_pairs = index_helpers.get_household_id_pairs(
        index_df, raw_data_fixture, data_provider="etdmap", list_files_func=_list_files_data_fixture
    )

    # limit nmbr of files/households
    count = 0
    for huis_code, file_name in household_id_pairs:
        if count >= limit_houses:
            break
        else:
            count += 1
        logging.info(f"Starting {file_name}")
        new_entry = _process_data_fixture_file(
            huis_code, file_name, raw_data_fixture, etdmap.options.mapped_folder_path
        )
        index_df = etdmap.index_helpers.update_index(index_df, new_entry, data_provider="etdmap")

    metadata_file_path = Path(config['etdmap_configuration']['supplier_metadata_xlsx_file'])
    metadata_df = read_metadata(metadata_file_path)
    etdmap.index_helpers.add_supplier_metadata_to_index(index_df, metadata_df, data_leverancier="etdmap")


def test_creation_validation_columns_index_data_files(raw_data_fixture, request):
    """
    Functional test of dataset_validators.dataset_flag_conditions.

    0. Runs the creation of the household parquet files (a selection) and index.parquet
    Then Checks if:
    1. Appropirate columns are created in datafiles (record_validators) and index file (dataset validators)
    2. If True/False values are found as expected.
    """
    ### 0: Create files ###
    # limit houses:
    limit_houses=10
    # generate the mapped household parquet files & index file
    _run_mapping_of_etd_fixtures(raw_data_fixture, limit_houses)

    ### 1: Test household.parquet creation ###
    # Test if all household files are created
    folder_path = etdmap.options.mapped_folder_path
    files = os.listdir(folder_path)
    files = [f for f in files if (
        os.path.isfile(os.path.join(folder_path, f)) and \
            ('household' in f))]
    assert len(files) == limit_houses, f"Expected {limit_houses} files, but found {len(files)} \
        perhaps forgot to delete files in mapping directory at start of test."

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
    """
    df_expected = pd.read_parquet(expected_path)

    df_generated_full = pd.read_parquet(generated_path)
    sample_size = min(100, len(df_generated_full))
    df_generated_sample = df_generated_full.sample(n=sample_size, random_state=42)
    return df_expected.equals(df_generated_sample)


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

def test_files_equal_expected(load_metadata):
    """
    Checks for each file generated by the workflow if
    its sample and its metadata match the expected files.
    """
    for name in conftest.file_names:
        name=name.split('.parquet')[0]
        expected_path = Path(f"tests/data/metadata_{name}.json")
        generated_path = os.path.join(etdmap.options.mapped_folder_path, f"{name}.parquet")
        results, expected_json, generated_json = _check_metadatafiles_are_equal(
            load_metadata,
            expected_path,
            generated_path
            )

        assert len(results) == 0 and expected_json == generated_json, f"expected vs. generaged metadata files do not match for metadata_{name}.json; see log file for differences"

        # check sample of file
        expected_path = Path(f"tests/data/sample_{name}.parquet")
        generated_path = os.path.join(etdmap.options.mapped_folder_path, f"{name}.parquet")
        assert _check_samples_are_equal(
            expected_path,
            generated_path
            ), f"expected vs. generaged files do not match for sample_{name}.parquet"

if __name__ == "__main__":
    # Run pytest for debugging the testing
    pytest.main(["-v"])
