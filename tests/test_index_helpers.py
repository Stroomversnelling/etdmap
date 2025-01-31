import logging
import os
from pathlib import Path

import pandas as pd
import pytest
import yaml

import etdmap
import etdmap.index_helpers as index_helpers
import etdmap.mapping_helpers as mapping
from etdmap.dataset_validators import dataset_flag_conditions
from etdmap.index_helpers import (
    bsv_metadata_columns,
    get_bsv_metadata,
    read_metadata,
)
from etdmap.record_validators import record_flag_conditions


def test_read_metadata(valid_metadata_file, invalid_metadata_file):
    # Test the function with the valid file fixture
    result = read_metadata(valid_metadata_file)
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["num_column", *bsv_metadata_columns]

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
        # "ProjectIdLeverancier": 3,
        "HuisCode": huis_code,
        "HuisIdBSV": huis_code
    }


def test_creation_validation_columns(raw_data_fixture, request):
    """
    Functional test of dataset_validators.dataset_flag_conditions.

    Checks if:
    1. Appropirate columns are created
    2. If True/False values are found as expected.
    """

    # copy_to_persistent = request.config.getoption("--copy-data")

    # Paths:
    def load_config(config_path):
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)

    test_config_path = Path("config_test.yaml")
    if os.path.isfile(test_config_path):
        config = load_config(test_config_path)

    etdmap.options.mapped_folder_path = Path(config['etdmap_configuration']['mapped_folder_path'])
    # fixture_path = etdmap.options['etd_test_fixture_parquet_folder_path']
    fixture_metadata_file = Path(config['etdmap_configuration']['bsv_metadata_file'])
    etdmap.options.bsv_metadata_file = fixture_metadata_file

    # if copy_to_persistent:
    #     print(f"Copy to persistent data folder: {copy_to_persistent}")
    #     # Path to the persistent directory
    #     persistent_dir = os.path.join(os.getcwd(), "persistent_output")

    #     # Ensure the persistent directory exists
    #     os.makedirs(persistent_dir, exist_ok=True)

    #     # Create a subdirectory in the persistent directory with the same name as the temporary directory
    #     persistent_subdir = os.path.join(persistent_dir, raw_data_fixture.name)
    #     os.makedirs(persistent_subdir, exist_ok=True)

    log_file = "data_fixture_processing.log"
    if os.path.exists(log_file):
        os.remove(log_file)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )

    logging.info("Starting")
    index_df, index_path = index_helpers.read_index()
    # add data provider for test data:
    # index_df.loc[:, "Dataleverancier"] = 'etdmap'
    household_id_pairs = index_helpers.get_household_id_pairs(
        index_df, raw_data_fixture, data_provider="etdmap", list_files_func=_list_files_data_fixture
    )

    for huis_code, file_name in household_id_pairs:
        logging.info(f"Starting {file_name}")
        new_entry = _process_data_fixture_file(
            huis_code, file_name, raw_data_fixture, etdmap.options.mapped_folder_path
        )
        index_df = etdmap.index_helpers.update_index(index_df, new_entry, data_provider="etdmap")

    metadata_df = read_metadata(fixture_metadata_file)
    etdmap.index_helpers.add_metadata_to_index(index_df, metadata_df, data_leverancier="etdmap")


if __name__ == "__main__":
    # Run pytest for debugging the testing
    pytest.main(["-v"])
