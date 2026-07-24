import hashlib
import json
import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml
from numpy.random import PCG64, Generator

import etdmap
import etdmap.index_helpers as index_helpers
import etdmap.mapping_helpers as mapping
from etdmap.data_model import cumulative_columns
from etdmap.index_helpers import bsv_metadata_columns, metadata_dtypes, read_metadata
from etdmap.record_validators import record_flag_conditions


# Frozen synthetic generation ranges for the raw-data test fixture:
# cumulative column name -> (min_diff, max_diff) for its per-interval Diff.
#
# PROVENANCE: seeded 2026-06-19 from the then-current thresholds.csv effective
# values (each "{col}Diff" row's Min/Max, with a missing Max collapsed to 1.0).
# These are INTENTIONALLY INDEPENDENT of thresholds.csv from here on: test input
# must exercise behaviour, never be derived from the artifact under test
# (generating from thresholds is circular -- data drawn inside [Min, Max] can
# never violate its own thresholds, so the validators/clipping are never
# exercised and a wrong threshold cannot be caught). Editing thresholds.csv no
# longer rewrites this fixture. tests/test_threshold_fixture_coverage.py warns
# if thresholds.csv grows variables/columns/units this dict no longer covers.
SYNTHETIC_DIFF_RANGES: dict[str, tuple[float, float]] = {
    "ElektriciteitNetgebruik": (0.0, 1.5),
    "ElektriciteitNetgebruikHoog": (0.0, 1.5),
    "ElektriciteitNetgebruikLaag": (0.0, 1.5),
    "ElektriciteitTeruglevering": (0.0, 1.5),
    "ElektriciteitTerugleveringHoog": (0.0, 1.5),
    "ElektriciteitTerugleveringLaag": (0.0, 1.5),
    "ElektriciteitsgebruikBoilervat": (0.0, 0.32),
    "ElektriciteitsgebruikBooster": (0.0, 0.32),
    "ElektriciteitsgebruikOverigGG": (0.0, 1.0),
    "ElektriciteitsgebruikRadiator": (0.0, 0.32),
    "ElektriciteitsgebruikWTW": (0.0, 0.32),
    "ElektriciteitsgebruikWarmtepomp": (0.0, 0.96),
    "ElektriciteitsgebruikWarmtepompIntern": (0.0, 1.0),
    "Gasgebruik": (0.0, 0.8333),
    "WarmteproductieRuimteverwarming": (0.0, 0.0576),
    "WarmteproductieWarmTapwater": (0.0, 0.0576),
    "WarmteproductieWarmtepomp": (0.0, 0.0576),
    "WatergebruikRuimteverwarming": (0.0, 1.0),
    "WatergebruikWarmTapwater": (0.0, 100.0),
    "WatergebruikWarmtepomp": (0.0, 1.0),
    "Zon-opwekTotaal": (0.0, 0.96),
    "Zon-opwekVerbruik": (0.0, 1.0),
}


# set paths
def _clean_mapped_folder(mapped_folder_path):
    """Remove generated files from mapped folder to ensure clean test state."""
    from pathlib import Path
    mapped_folder = Path(mapped_folder_path)
    if mapped_folder.exists():
        for f in mapped_folder.glob("household_*.parquet"):
            f.unlink()
        index_file = mapped_folder / "index.parquet"
        if index_file.exists():
            index_file.unlink()


@pytest.fixture(scope="session", autouse=True)
def clean_mapped_folder_before_tests():
    """
    Clean mapped folder before etdmap tests run.

    This ensures tests start from a clean state without leftover files
    from previous test runs. The mapped files are regenerated as part
    of testing the mapping logic.
    """
    test_config_path = Path("config_test.yaml")
    if os.path.isfile(test_config_path):
        with open(test_config_path, 'r') as file:
            config = yaml.safe_load(file)
        _clean_mapped_folder(config['etdmap_configuration']['mapped_folder_path'])
    yield
def load_config(config_path):
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)

test_config_path = Path("config_test.yaml")
if os.path.isfile(test_config_path):
    config = load_config(test_config_path)
else:
        raise FileNotFoundError("no file named 'config_test.yaml'")

file_names = [
    "index.parquet",
    "household_1_table.parquet",
    "household_2_table.parquet",
    "household_3_table.parquet",
    "household_4_table.parquet",
    "household_5_table.parquet",
    "household_6_table.parquet",
    "household_7_table.parquet",
    "household_8_table.parquet",
    "household_9_table.parquet",
    "household_10_table.parquet",
]

def pytest_addoption(parser):
    parser.addoption("--copy-data", action="store_true", help="Copy data to a persistent folder")

@pytest.fixture
def valid_metadata_file(tmp_path):
    # tmp_path fixture provides a temp directory unique
    # to the test run
    # Create a valid Excel file with the required columns
    metadata_file = tmp_path / "metadata.xlsx"

    data = {}

    data = {
        "HuisIdLeverancier": ["A123", "B456", "C789"],
        "HuisIdBSV": [101, 202, None],  # One missing value for demonstration
        "Meenemen": [True, False, True],
        "ProjectIdLeverancier": ["P001", "P002", "P003"],
        "ProjectIdBSV": [1, 2, 3],
        "Notities": ["First note", None, "Final note"],  # One missing value
        "Dataleverancier": ["CompanyX", "CompanyY", "CompanyZ"],
    }

    data["num_column"] = np.random.randint(1, 100, size=3)
    data["num_column"].sort()
    df = pd.DataFrame(data)

    df.to_excel(metadata_file, sheet_name="Data", index=False)
    return str(metadata_file)


@pytest.fixture
def invalid_metadata_file(tmp_path):
    # Create an invalid Excel file missing the required column
    metadata_file = tmp_path / "invalid_metadata.xlsx"
    data = {
        "SomeColumn": [1, 2, 3],
    }
    df = pd.DataFrame(data)
    df.to_excel(metadata_file, sheet_name="Data", index=False)
    return str(metadata_file)



@pytest.fixture(scope="session")
def raw_data_fixture(tmp_path_factory):
    """
    Pytest fixture to generate raw household data for 2 projects with 5 households each with a minimal index file.
    Saves the generated files in a temporary directory.

    It will use household 1 and some variables to generate some expected mistakes/problems so we can test our validators and mapping process.

    Returns:
        str: Path to the directory containing the generated fixture data.
    """
    # Settings
    num_households_per_project = 5
    projects = [1, 2]
    num_records = 105120  # 1 year of data at 5-minute intervals
    base_date = pd.Timestamp("2023-01-01")
    time_interval = "5min"  # 5-minute intervals

    # Output directory (temporary directory for the test session)
    output_dir = tmp_path_factory.mktemp("raw_fixture")

    # Metadata index
    index_raw = []
    index_bsv = []

    # Explicitly define the bit generator to ensure the algorithm/seed don't change
    bit_generator = PCG64(seed=42)
    rng = Generator(bit_generator)

    # Generate data for each household
    for project_id in projects:
        for household_idx in range(1, num_households_per_project + 1):
            # Generate unique HuisIdBSV
            household_id = (project_id - 1) * num_households_per_project + household_idx

            # Preconstruct the prefixed strings
            household_prefixed = f"Huis{household_id:02}"
            project_prefixed = f"Project{project_id:02}"

            # Generate time series
            timestamps = pd.date_range(start=base_date, periods=num_records, freq=time_interval)
            household_data = {
                "HuisIdLeverancier": pd.Series([household_prefixed] * num_records, dtype="string"),
                "ProjectIdLeverancier": pd.Series([project_prefixed] * num_records, dtype="string"),
                "ReadingDate": timestamps,
            }

            # Generate cumulative column data.
            # Each column uses an independent seed derived from household + column name
            # so fixture data is stable regardless of the order of cumulative_columns.
            for col in sorted(cumulative_columns):
                if col not in SYNTHETIC_DIFF_RANGES:
                    raise ValueError(
                        f"Cannot generate raw data test fixture: no synthetic range for "
                        f"cumulative column '{col}' in SYNTHETIC_DIFF_RANGES (tests/conftest.py). "
                        f"Add it -- see tests/test_threshold_fixture_coverage.py."
                    )
                min_diff, max_diff = SYNTHETIC_DIFF_RANGES[col]
                col_seed = int(hashlib.md5(f"{household_id}_{col}".encode()).hexdigest(), 16) % (2**32)
                col_rng = Generator(PCG64(seed=col_seed))
                diffs = pd.Series(col_rng.uniform(min_diff, max_diff, size=num_records - 1), dtype="float64")
                cumulative = pd.concat([pd.Series([0]), diffs.cumsum()], ignore_index=True)
                household_data[col] = cumulative

            household_df = pd.DataFrame(household_data)
            household_df = add_raw_data_test_case(base_date, household_df, household_id_raw=household_prefixed, project_id_raw=project_prefixed, rng=rng)

            file_path = os.path.join(output_dir, f"household_{household_id}_table.parquet")
            household_df.to_parquet(file_path, index=False)

            # Add metadata
            index_bsv.append({"HuisIdLeverancier": household_prefixed, "ProjectIdLeverancier": project_prefixed, "HuisIdBSV": household_id, "ProjectIdBSV": project_id})
            index_raw.append({"HuisIdLeverancier": household_prefixed, "ProjectIdLeverancier": project_prefixed})

    index_raw_df = pd.DataFrame(index_raw)
    index_raw_file_path = os.path.join(output_dir, "index_raw.parquet")
    index_raw_df.to_parquet(index_raw_file_path, index=False)

    index_bsv_df = pd.DataFrame(index_bsv)
    index_bsv_file_path = os.path.join(output_dir, "index_bsv.parquet")
    index_bsv_df.to_parquet(index_bsv_file_path, index=False)

    logging.info(f"Fixture generated in directory: {output_dir}")
    return output_dir

# cases to add:
#    mixed data types in a single column - are they reported and coerced correctly?
#    drifting clocks - are they detected and corrected?
#    less records than expected - are they reported?
def add_raw_data_test_case(base_date, household_df, household_id_raw, project_id_raw, rng, interval = None):
    if interval is None:
        interval = pd.Timedelta(minutes=5)

    if (household_id_raw == "Huis1") and (project_id_raw == "Project1"):
        # There is a gap in data but after gap it continues (shift data down 24h)
        # Define conditions
        gap_start = base_date + pd.Timedelta(days=30)
        gap_length = pd.Timedelta(hours=24)
        var = cumulative_columns[0]
        household_df = introduce_gap(household_df=household_df, columns=[var], gap_start=gap_start, gap_length=gap_length, shift = True)

    elif (household_id_raw == "Huis2") and (project_id_raw == "Project1"):
        # there is a gap in data but after gap it continues with the same starting value (shift data down 24h + copying the last value)
        # Define conditions
        gap_start = base_date + pd.Timedelta(days=60)
        gap_length_adjusted = pd.Timedelta(hours=24) + interval
        gap_end = gap_start + pd.Timedelta(hours=24)
        var = cumulative_columns[1]
        last_value = household_df.loc[household_df['ReadingDate'] == gap_start - interval, var]
        if pd.isna(last_value).any():
            raise ValueError(f"Last value before gap at {gap_start - interval} is missing for {var}")
        household_df = introduce_gap(household_df=household_df, columns=[var], gap_start=gap_start, gap_length=gap_length_adjusted, shift = True)
        # fill value at gap_end time with the last value before the gap
        household_df.loc[household_df['ReadingDate'] == gap_end, var] = last_value

    elif (household_id_raw == "Huis3") and (project_id_raw == "Project1"):
        # there is a gap in data and after the gap, the meter was reset to 0. Subtract the last value before the gap from all values after the gap.
        # Define conditions
        gap_start = base_date + pd.Timedelta(days=90)
        gap_length = pd.Timedelta(hours=24)
        gap_end = gap_start + gap_length
        var = cumulative_columns[2]
        household_df = introduce_gap(household_df=household_df, columns=[var], gap_start=gap_start, gap_length=gap_length, shift = True)
        household_df = reset_cumulative_column(household_df=household_df, columns=[var], reset_time=gap_end, check_negative = True)

    elif (household_id_raw == "Huis4") and (project_id_raw == "Project1"):
        # there is no gap in the data but the meter was reset to 0 at some point. Subtract the last value before the reset from all values after the reset.
        meter_reset_date = base_date + pd.Timedelta(days=120)
        var = cumulative_columns[3]
        household_df = reset_cumulative_column(household_df=household_df, columns=[var], reset_time=meter_reset_date, check_negative = True)

    elif (household_id_raw == "Huis5") and (project_id_raw == "Project1"):
        # there is 24 hour gap in the data. Delete 24hrs of data and do not shift it down. There should be a big jump in the value as a result.
        gap_start = base_date + pd.Timedelta(days=150)
        gap_length = pd.Timedelta(hours=24)
        var = cumulative_columns[4]
        household_df = introduce_gap(household_df=household_df, columns=[var], gap_start=gap_start, gap_length=gap_length, shift = False)

    elif (household_id_raw in ["Huis6","Huis7", "Huis8", "Huis9", "Huis10"]) and (project_id_raw == "Project2"):
        # at the same time for each household there is 24h gap in all data. Delete 24hs of data and do not shift it down. There should be a big jump in the value as a result.
        gap_start = base_date + pd.Timedelta(days=180)
        gap_length = pd.Timedelta(hours=24)
        var = cumulative_columns[0]
        household_df = introduce_gap(household_df=household_df, columns=[var], gap_start=gap_start, gap_length=gap_length, shift = False)

    # There is a single variable that only has data every hour - insert pd.NA unless at the hour
    hourly_var = cumulative_columns[5]
    mask_hourly = (household_df['ReadingDate'].dt.minute != 0)
    household_df.loc[mask_hourly, hourly_var] = pd.NA

    # There is a single variable that only has data every 15 minutes - insert pd.NA unless at the 15 minute mark
    fifteen_min_var = cumulative_columns[6]
    mask_fifteen_min = (household_df['ReadingDate'].dt.minute % 15 != 0)
    household_df.loc[mask_fifteen_min, fifteen_min_var] = pd.NA

    ## There is a single variable that exceeds the upper limits
    household_df[cumulative_columns[7]] = household_df[cumulative_columns[7]]*2

    ## There is a single variable that is negative (some of the time)
    # Define the probability of flipping a value to negative (e.g., 0.1 means 10% chance)
    flip_probability = 0.005
    # Create a boolean mask with True where we want to flip the values to negative
    mask = rng.random(size=household_df.shape[0]) < flip_probability
    # Apply the mask to flip some of the values in the specified column to their negatives
    household_df.loc[mask, cumulative_columns[8]] *= -1

    return household_df


def shift_values(household_df, columns, shift_amount, gap_start=None):
    """
    Shift values in a column by a specific time delta.

    Args:
        household_df (pd.DataFrame): The household data.
        columns (list): List of columns to shift values in.
        shift_amount (pd.Timedelta): The amount of time to shift.
        gap_start (pd.Timestamp, optional): If provided, apply the shift only for rows after this time.

    Returns:
        pd.DataFrame: Modified DataFrame with shifted values.
    """
    shifted_df = household_df[["ReadingDate", *columns]].copy()
    if gap_start:
        mask = household_df['ReadingDate'] >= gap_start
        shifted_df.loc[mask, 'ReadingDate'] += shift_amount
    else:
        shifted_df['ReadingDate'] += shift_amount

    # Merge shifted values back into the original DataFrame
    household_df = household_df.drop(columns=columns)
    household_df = household_df.merge(shifted_df, on='ReadingDate', how='left')

    # Make sure the gap is filled with pd.NA
    if gap_start:
        mask_gap = (household_df['ReadingDate'] >= gap_start) & (household_df['ReadingDate'] < gap_start + shift_amount)
        household_df.loc[mask_gap, columns] = pd.NA

    return household_df


def introduce_gap(household_df, columns, gap_start, gap_length, shift=True):
    """
    Introduce a gap in the specified columns by setting values to pd.NA in the gap range.

    Args:
        household_df (pd.DataFrame): The household data.
        columns (list): List of columns to apply the gap.
        gap_start (pd.Timestamp): Start of the gap.
        gap_length (pd.Timedelta): The length of the gap.
        shift (bool): If True, shift existing values. If False, overwrite with pd.NA.

    Returns:
        pd.DataFrame: Modified DataFrame with the gap.
    """

    if shift:
        return shift_values(household_df=household_df, columns=columns, shift_amount=gap_length, gap_start=gap_start)
    else:
        gap_end = gap_start + gap_length
        mask = (household_df['ReadingDate'] >= gap_start) & (household_df['ReadingDate'] < gap_end)
        household_df.loc[mask, columns] = pd.NA
        return household_df

def reset_cumulative_column(household_df, columns, reset_time, check_negative = True):
    """
    Resets the cumulative column to 0 after a given time

    Args:
        household_df (pd.DataFrame): The household data.
        columns (list): List of columns to reset.
        reset_time (pd.Timestamp): The time at which the meter reset occurs.
        check_negative (bool): If True, asserts there should be no negative values after reset. Default is True.

    Returns:
        pd.DataFrame: Modified DataFrame with reset cumulative values.
    """
    for col in columns:
        reset_value = pd.Series(household_df.loc[household_df['ReadingDate'] == reset_time, col].values).iloc[0]
        if pd.isna(reset_value):
            raise ValueError(f"No data available at reset time {reset_time} for column: {col}")
        mask_reset = household_df['ReadingDate'] > reset_time
        household_df.loc[mask_reset, col] -= reset_value

        if check_negative:
            assert all(household_df.loc[mask_reset, col] >= 0), "Values of column `{col}` after reset should not be negative"

    return household_df

@pytest.fixture
def load_metadata():
    def _load_metadata(filepath):
        with open(filepath, "r") as f:
            return json.load(f)
    # return inner function as ficture
    return _load_metadata


def _list_files_data_fixture(folder_path):
    return {f[:-8]: f for f in os.listdir(folder_path) if f.endswith(".parquet") and "index" not in f}


def _process_data_fixture_file(household_id, file_name, etd_test_fixture_path, mapped_folder_path):
    file_path = os.path.join(etd_test_fixture_path, file_name)
    new_file_path = os.path.join(
        mapped_folder_path, f"household_{int(household_id)}_table.parquet"
    )

    data_fixture_df = pd.read_parquet(file_path)

    # Identifier columns belong to the index, not the household data, and are
    # not part of the data model -- read them before the mapping steps.
    project_id = (
        str(data_fixture_df["ProjectIdLeverancier"].iloc[0])
        if "ProjectIdLeverancier" in data_fixture_df.columns
        else "unknown"
    )

    data_fixture_df = mapping.ensure_intervals(data_fixture_df)

    data_fixture_df = mapping.rearrange_model_columns(
        household_df=data_fixture_df, add_columns=True, context=f"{household_id}/{file_name}"
    )

    data_fixture_df = mapping.fill_down_infrequent_devices(
        df=data_fixture_df,
        columns=("ElektriciteitsgebruikBoilervat", "ElektriciteitsgebruikRadiator", "ElektriciteitsgebruikBooster"),
    )

    data_fixture_df = mapping.add_diff_columns(data_fixture_df, context=f"{household_id}/{file_name}")

    for flag, condition in record_flag_conditions.items():
        try:
            data_fixture_df[flag] = condition(data_fixture_df)
        except Exception as e:
            logging.error(
                f"Error validating with {flag} for household {household_id} / {file_name}: {e}",
                exc_info=True,
            )
            data_fixture_df[flag] = pd.NA

    data_fixture_df.to_parquet(new_file_path, engine="pyarrow")

    return {
        "HuisIdLeverancier": f'Huis{int(file_name.replace("household_", "").replace("_table.parquet", "")):02}',
        "ProjectIdLeverancier": project_id,
        "HuisIdBSV": household_id,
    }


@pytest.fixture(scope="session")
def mapped_fixtures(raw_data_fixture):
    """
    Session-scoped fixture that ensures the mapped household parquet files and
    index.parquet exist in `etdmap.options.mapped_folder_path`. Tests that read
    those files request this fixture; pytest resolves the dependency
    automatically, so the dependent tests can run in any order or in isolation.

    Generated files are not removed at session end. They remain available to
    downstream test suites (e.g. etdtransform) that read from the same mapped
    folder as their input.

    Returns the path to the mapped folder.
    """
    test_config_path = Path("config_test.yaml")
    config = load_config(test_config_path)

    etdmap.options.mapped_folder_path = Path(config['etdmap_configuration']['mapped_folder_path'])
    etdmap.options.bsv_metadata_file = Path(config['etdmap_configuration']['bsv_metadata_file'])

    # Fixture HuisBatch sync CSV next to the fixture BSV metadata: the REAL
    # registry write path (save_index_to_parquet) then populates the
    # household-batch columns of index.parquet exactly as production does --
    # fixtures exercise production code, no fixture-only derivation.
    # Meenemen is EMPTY here: the researcher adds the rows (ids + batch +
    # cadence) and reviews later.
    _fixtures_dir = Path(str(etdmap.options.bsv_metadata_file)).parent
    _sync_rows = 10
    pd.DataFrame({
        "HuisIdBSV": pd.array(range(1, _sync_rows + 1), dtype="Int64"),
        "HuisBatchIdBSV": pd.array(range(1, _sync_rows + 1), dtype="Int64"),
        "BatchIdBSV": pd.array([1] * _sync_rows, dtype="Int64"),
        "ProjectIdBSV": pd.array([1] * _sync_rows, dtype="Int64"),
        "Meenemen": pd.array([pd.NA] * _sync_rows, dtype="boolean"),
        "Gegevensfrequentie": pd.array(["5min"] * _sync_rows, dtype="string"),
        "Leverancierfrequentie": pd.array([pd.NA] * _sync_rows, dtype="string"),
        "Startdatum": ["2024-01-01 00:00:00 UTC"] * _sync_rows,
        "Einddatum": ["2025-01-01 00:00:00 UTC"] * _sync_rows,
    }).to_csv(_fixtures_dir / "household_batch_sync.csv", index=False)
    etdmap.options.household_batch_csv_path = _fixtures_dir / "household_batch_sync.csv"

    # The fixture pipeline runs in the FIRST-MAPPING state: Meenemen is not
    # reviewed yet, so the BSV metadata the Meenemen stamp reads is a variant
    # of the fixture metadata with the Meenemen values blanked. The option is
    # restored to the reviewed metadata afterwards, so later tests (e.g. the
    # update_meenemen tests) exercise the review transition -- the suite
    # covers BOTH states, in lifecycle order.
    _reviewed_metadata = Path(str(etdmap.options.bsv_metadata_file))
    _first_mapping_metadata = _fixtures_dir / "metadata_first_mapping.csv"
    _meta_df = pd.read_csv(_reviewed_metadata, dtype_backend="numpy_nullable")
    _meta_df["Meenemen"] = pd.array([pd.NA] * len(_meta_df), dtype="boolean")
    _meta_df.to_csv(_first_mapping_metadata, index=False)
    etdmap.options.bsv_metadata_file = _first_mapping_metadata

    index_df, _index_path = index_helpers.read_index()
    if 'Dataleverancier' not in index_df.columns:
        index_df.loc[:, 'Dataleverancier'] = 'etdmap'
    household_id_pairs = index_helpers.get_household_id_pairs(
        index_df, raw_data_fixture, data_provider="etdmap", list_files_func=_list_files_data_fixture
    )

    limit_houses = 10
    count = 0
    for household_id, file_name in household_id_pairs:
        if count >= limit_houses:
            break
        count += 1
        logging.info(f"Starting {file_name}")
        new_entry = _process_data_fixture_file(
            household_id, file_name, raw_data_fixture, etdmap.options.mapped_folder_path
        )
        index_df = etdmap.index_helpers.update_index(index_df, new_entry, data_provider="etdmap")

    etdmap.options.project_mapping_csv_path = config['etdmap_configuration']['project_mapping_csv_path']
    metadata_file_path = Path(config['etdmap_configuration']['supplier_metadata_xlsx_file'])
    metadata_df = read_metadata(metadata_file_path)
    etdmap.index_helpers.add_supplier_metadata_to_index(index_df, metadata_df, data_supplier="etdmap")

    # The household-batch columns were written into index.parquet by the
    # save calls above (single registry write path; the fixture sync CSV
    # supplied the batch fields; Meenemen is EMPTY -- the honest
    # first-mapping state). Downstream suites (etdtransform) apply the
    # review step themselves. Restore the option to the REVIEWED metadata so
    # later etdmap tests exercise the review transition.
    etdmap.options.bsv_metadata_file = _reviewed_metadata
    return etdmap.options.mapped_folder_path