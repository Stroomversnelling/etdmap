import logging
import os

import numpy as np
import pandas as pd
import pytest
from numpy.random import PCG64, Generator

from etdmap.data_model import cumulative_columns, load_thresholds
from etdmap.index_helpers import bsv_metadata_columns, metadata_dtypes


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
    # Load thresholds
    thresholds = load_thresholds()

    # Settings
    num_households_per_project = 5
    projects = [1, 2]
    num_records = 105120  # 1 year of data at 5-minute intervals
    base_date = pd.Timestamp("2023-01-01")
    time_interval = "5min"  # 5-minute intervals
    default_max_value = 1  # Default max value if not provided in thresholds

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
            huis_id = (project_id - 1) * num_households_per_project + household_idx

            # Preconstruct the prefixed strings
            huis_prefixed = f"Huis{huis_id:02}"
            project_prefixed = f"Project{project_id:02}"

            # Generate time series
            timestamps = pd.date_range(start=base_date, periods=num_records, freq=time_interval)
            household_data = {
                "HuisIdLeverancier": pd.Series([huis_prefixed] * num_records, dtype="string"),
                "ProjectIdLeverancier": pd.Series([project_prefixed] * num_records, dtype="string"),
                "ReadingDate": timestamps,
            }

            # Generate cumulative column data
            for col in cumulative_columns:
                diff_col = f"{col}Diff"
                if diff_col in thresholds["Variabele"].values:
                    min_diff = thresholds.loc[thresholds["Variabele"] == diff_col, "Min"].values[0]
                    max_diff = thresholds.loc[thresholds["Variabele"] == diff_col, "Max"].values[0]
                    if pd.isna(max_diff):
                        max_diff = default_max_value

                    diffs = pd.Series(rng.uniform(min_diff, max_diff, size=num_records - 1), dtype="float64")
                    cumulative = pd.concat([pd.Series([0]), diffs.cumsum()], ignore_index=True)
                    household_data[col] = cumulative
                else:
                    raise ValueError(f"Cannot generate raw data test fixture. No threshold exists for {diff_col} in `thresholds.csv`")

            household_df = pd.DataFrame(household_data)
            household_df = add_raw_data_test_case(base_date, household_df, huis_id_raw=huis_prefixed, project_id_raw=project_prefixed, rng=rng)

            file_path = os.path.join(output_dir, f"household_{huis_id}_table.parquet")
            household_df.to_parquet(file_path, index=False)

            # Add metadata
            index_bsv.append({"HuisIdLeverancier": huis_prefixed, "ProjectIdLeverancier": project_prefixed, "HuisIdBSV": huis_id, "ProjectIdBSV": project_id})
            index_raw.append({"HuisIdLeverancier": huis_prefixed, "ProjectIdLeverancier": project_prefixed})

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
def add_raw_data_test_case(base_date, household_df, huis_id_raw, project_id_raw, rng, interval = None):
    if interval is None:
        interval = pd.Timedelta(minutes=5)

    if (huis_id_raw == "Huis1") and (project_id_raw == "Project1"):
        # There is a gap in data but after gap it continues (shift data down 24h)
        # Define conditions
        gap_start = base_date + pd.Timedelta(days=30)
        gap_length = pd.Timedelta(hours=24)
        var = cumulative_columns[0]
        household_df = introduce_gap(household_df=household_df, columns=[var], gap_start=gap_start, gap_length=gap_length, shift = True)

    elif (huis_id_raw == "Huis2") and (project_id_raw == "Project1"):
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

    elif (huis_id_raw == "Huis3") and (project_id_raw == "Project1"):
        # there is a gap in data and after the gap, the meter was reset to 0. Subtract the last value before the gap from all values after the gap.
        # Define conditions
        gap_start = base_date + pd.Timedelta(days=90)
        gap_length = pd.Timedelta(hours=24)
        gap_end = gap_start + gap_length
        var = cumulative_columns[2]
        household_df = introduce_gap(household_df=household_df, columns=[var], gap_start=gap_start, gap_length=gap_length, shift = True)
        household_df = reset_cumulative_column(household_df=household_df, columns=[var], reset_time=gap_end, check_negative = True)

    elif (huis_id_raw == "Huis4") and (project_id_raw == "Project1"):
        # there is no gap in the data but the meter was reset to 0 at some point. Subtract the last value before the reset from all values after the reset.
        meter_reset_date = base_date + pd.Timedelta(days=120)
        var = cumulative_columns[3]
        household_df = reset_cumulative_column(household_df=household_df, columns=[var], reset_time=meter_reset_date, check_negative = True)

    elif (huis_id_raw == "Huis5") and (project_id_raw == "Project1"):
        # there is 24 hour gap in the data. Delete 24hrs of data and do not shift it down. There should be a big jump in the value as a result.
        gap_start = base_date + pd.Timedelta(days=150)
        gap_length = pd.Timedelta(hours=24)
        var = cumulative_columns[4]
        household_df = introduce_gap(household_df=household_df, columns=[var], gap_start=gap_start, gap_length=gap_length, shift = False)

    elif (huis_id_raw in ["Huis6","Huis7", "Huis8", "Huis9", "Huis10"]) and (project_id_raw == "Project2"):
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

