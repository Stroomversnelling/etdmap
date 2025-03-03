import logging
import os

import pandas as pd

import etdmap
from etdmap.data_model import (
    allowed_supplier_metadata_columns,
    cumulative_columns,
)
from etdmap.dataset_validators import dataset_flag_conditions

bsv_metadata_columns = [
    "HuisIdLeverancier",
    "HuisIdBSV",
    "ProjectIdLeverancier",
    "ProjectIdBSV",
    "Dataleverancier",
    "Meenemen",
    "Notities",
]

# all nullable pandas series types
metadata_dtypes = {
    "HuisIdLeverancier": pd.StringDtype(),
    "HuisIdBSV": pd.Int64Dtype(),
    "Meenemen": pd.BooleanDtype(),
    "ProjectIdLeverancier": pd.StringDtype(),
    "ProjectIdBSV": pd.Int64Dtype(),
    "Notities": pd.StringDtype(),
    "Dataleverancier": pd.StringDtype(),
}

def get_bsv_metadata():
    """
    Reads and returns metadata from the BSV metadata file, ensuring that all required columns are present.

    Returns
    -------
    DataFrame
        A pandas DataFrame containing the BSV metadata with the specified columns.

    Raises
    ------
    ValueError
        If any of the required columns are missing in the metadata file.

    Notes
    -----
    - The function relies on the `read_metadata` utility to read the file and check for required columns.
    - The path to the BSV metadata file is obtained from `etdmap.options.bsv_metadata_file`.
    - The required columns are defined in the `bsv_metadata_columns` list.
    """
    return read_metadata(
        etdmap.options.bsv_metadata_file,
        required_columns=bsv_metadata_columns,
    )


def read_metadata(metadata_file: str, required_columns=None) -> pd.DataFrame:
    """
    Read metadata from an Excel file and check for the presence of required columns.

    Parameters
    ----------
    metadata_file : str
        The path to the Excel file containing the metadata for a data source.
    required_columns : list, optional
        A list of column names that must be present in the metadata. Defaults to ['HuisIdLeverancier'].

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the metadata from the specified sheet.

    Raises
    ------
    Exception
        If not all required columns are found in the metadata file.
    """
    if required_columns is None:
        required_columns = ["HuisIdLeverancier"]
    if metadata_file is not None:
        xl = pd.ExcelFile(metadata_file)
    else:
        raise ValueError(
            f"invalid file path: {metadata_file} "
            "perhaps you forgot to set the option. You can "
            "do this with etdmap.options.bsv_metadata_file = 'your/path",
        )
    df = xl.parse(sheet_name="Data")
    df = set_metadata_dtypes(metadata_df = df)

    if all(col in df.columns for col in required_columns): 
        return df
    else:
        logging.error(
            f'Not all required columns in sheet "Data" in metadata file: '
            f"{metadata_file}",
        )
        raise ValueError(
            f'Not all required columns in sheet "Data" in metadata file:'
            f"{metadata_file}",
        )


def read_index() -> tuple[pd.DataFrame, str]:
    """
    Reads the index parquet file from the specified folder path.

    Returns
    -------
    tuple
        A tuple containing:
            - DataFrame: The DataFrame of the index.
            - str: The path to the index file.
    """

    index_path = os.path.join(etdmap.options.mapped_folder_path, "index.parquet")
    if os.path.exists(index_path):
        index_df = pd.read_parquet(index_path)
    else:
        index_df = pd.DataFrame(
            columns=bsv_metadata_columns
        )

    if "HuisId" in index_df.columns:
        index_df.rename(columns={"HuisId": "HuisIdLeverancier"}, inplace=True)
    if "ProjectId" in index_df.columns:
        index_df.rename(columns={"ProjectId": "ProjectIdLeverancier"}, inplace=True)

    index_df = set_metadata_dtypes(metadata_df=index_df, strict=True)


    return index_df, index_path


def get_household_id_pairs(
    index_df: pd.DataFrame,
    data_folder_path: str,
    data_provider: str,
    list_files_func: callable,
) -> list:
    """Generates pairs of HuisIdBSV and filenames for new and existing entries.

    Parameters
    ----------
    index_df : pd.DataFrame
        The index DataFrame.
    data_folder_path : str
        The path to the folder containing data files.
    data_provider : str
        The name of the data provider.
    list_files_func : callable
        A function to get a dictionary of id and the files in the data folder.

    Returns
    -------
    list
        A list of tuples containing HuisIdBSV and filenames.
    """
    existing_ids = (
        index_df[index_df["Dataleverancier"] == data_provider]
        .set_index("HuisIdLeverancier")
        .to_dict()["HuisIdBSV"]
    )
    data_files = list_files_func(data_folder_path)

    household_id_pairs = []
    next_id = int(max(index_df["HuisIdBSV"], default=0) + 1)

    for huis_id, file in data_files.items():
        if huis_id in existing_ids:
            x = (int(existing_ids[huis_id]), file)
        else:
            x = (next_id, file)
            next_id += 1

        household_id_pairs.append(x)
        logging.info(f"Household id pair: {x}")

    return household_id_pairs


def update_index(
    index_df: pd.DataFrame,
    new_entry: dict,
    data_provider: str,
) -> pd.DataFrame:
    """Update the index with new entries and recalculate or add flag columns for dataset validators.

    Parameters
    ----------
    index_df : pd.DataFrame
        The index DataFrame.
    new_entry : dict
        The new entry to be added or updated in the index.
    data_provider : str
        The name of the data provider.

    Returns
    -------
    pd.DataFrame
        The updated index DataFrame.
    """

    # Ensure HuisIdLeverancier is a string in new_entry
    new_entry["HuisIdLeverancier"] = str(new_entry["HuisIdLeverancier"])
    if "ProjectIdLeverancier" in new_entry:
        new_entry["ProjectIdLeverancier"] = str(new_entry["ProjectIdLeverancier"])
    new_entry["Dataleverancier"] = data_provider

    if new_entry["HuisIdLeverancier"] in index_df["HuisIdLeverancier"].values:
        index_df.loc[
            index_df["HuisIdLeverancier"] == new_entry["HuisIdLeverancier"],
            ["HuisIdBSV", "Dataleverancier"],
        ] = (new_entry["HuisIdBSV"], data_provider)
    else:
        new_entry_df = pd.DataFrame([new_entry])
        index_df = pd.concat([index_df, new_entry_df], ignore_index=True)

    # Recalculate or add flag columns
    household_code = new_entry["HuisIdBSV"]
    dataset_file = os.path.join(
        etdmap.options.mapped_folder_path,
        f"household_{household_code}_table.parquet",
    )
    if os.path.exists(dataset_file):
        df = pd.read_parquet(dataset_file)
        for flag, condition in dataset_flag_conditions.items():
            # Add flag column if it does not exist and ensure it's BooleanDtype
            if flag not in index_df.columns:
                index_df[flag] = pd.Series(
                    pd.NA,
                    dtype="boolean", # "bool" is the standard non-nullable Boolean type (backed by NumPy), while "boolean" is pandas' nullable Boolean extension type (pd.BooleanDtype) that supports NA values.
                    index=index_df.index,
                )
            try:
                validation_result = condition(df)
                index_df.loc[index_df["HuisIdBSV"] == household_code, flag] = (
                    validation_result
                )
            except Exception as e:
                logging.error(
                    f"Error validating with {flag} for household "
                    f"{household_code}: {e}",
                    exc_info=True,
                )
                index_df.loc[
                    index_df["HuisIdBSV"] == household_code,
                    flag,
                ] = pd.NA

    # Ensure all flag columns are of BooleanDtype
    for flag in dataset_flag_conditions.keys():
        if flag in index_df.columns:
            index_df[flag] = index_df[flag].astype("boolean")

    index_df = update_meta_validators(index_df)

    save_index_to_parquet(index_df=index_df)

    return index_df


def update_meta_validators(index_df):
    """
    Updates the index DataFrame with a new column 'validate_cumulative_diff_ok' that indicates whether
    all cumulative difference columns in the DataFrame are valid.

    Parameters
    ----------
    index_df (pandas.DataFrame): The input DataFrame containing cumulative data and validation columns.

    Returns
    -------
    pandas.DataFrame: The updated DataFrame with an additional column 'validate_cumulative_diff_ok'.

    Notes
    -----
    - The function constructs a list of column names based on the global variable `cumulative_columns`.
    - It checks if all these constructed column names exist in the input DataFrame.
    - If they do, it creates a new boolean column 'validate_cumulative_diff_ok' where each entry is True
      if all corresponding row-wise values in the cumulative difference columns are True (indicating validity).
    - If any of the expected columns are missing, it fills the 'validate_cumulative_diff_ok' column with pd.NA.
    """
    cols = ["validate_" + col + "Diff" for col in cumulative_columns]

    if all(col in index_df.columns for col in cols):
        index_df["validate_cumulative_diff_ok"] = index_df[cols].all(axis=1)
    else:
        index_df["validate_cumulative_diff_ok"] = pd.Series(
            pd.NA,
            dtype="boolean",
            index=index_df.index,
        )

    return index_df


def update_meenemen() -> pd.DataFrame:
    """Updates the index DataFrame to include information about which households should be included in the "Meenemen" column based on BSV metadata.

    This function performs the following steps:
    1. Logs an informational message indicating the start of the update process.
    2. Reads the current index DataFrame and its file path using the `read_index` function.
    3. Removes any existing 'Meenemen' column from the index DataFrame.
    4. Retrieves the BSV metadata DataFrame using the `get_bsv_metadata` function.
    5. Extracts the 'HuisIdBSV' and 'Meenemen' columns from the BSV metadata DataFrame.
    6. Merges the extracted BSV 'Meenemen' data with the index DataFrame on the 'HuisIdBSV' column.
    7. Saves the updated index DataFrame back to its original file path in Parquet format using the PyArrow engine.
    8. Returns the updated index DataFrame.

    Returns
    -------
        pd.DataFrame: The updated index DataFrame with the new "Meenemen" information included.
    """
    logging.info(
        'Updating index with which households to include in column "Meenemen"',
    )

    index_df, index_path = read_index()

    # index_df.drop(columns=["Meenemen"], inplace=True)

    bsv_metadata_df = get_bsv_metadata()

    # bsv_meenemen = bsv_metadata_df[["HuisIdBSV", "Meenemen"]]
    # index_df = index_df.merge(bsv_meenemen, on=["HuisIdBSV"])
    bsv_metadata_df.set_index("HuisIdBSV", inplace=True)
    index_df.set_index('HuisIdBSV', inplace=True)
    columns_for_update = bsv_metadata_df.columns.intersection(allowed_supplier_metadata_columns)

    index_df.update(bsv_metadata_df.loc[:, columns_for_update])
    index_df.reset_index(inplace=True)
    save_index_to_parquet(index_df=index_df)

    return index_df


def add_supplier_metadata_to_index(
    index_df: pd.DataFrame,
    metadata_df: pd.DataFrame,
    data_leverancier=None,
) -> pd.DataFrame:
    """Adds metadata columns to the index matching on the HuisIdLeverancier column.

    Parameters
    ----------
    index_df : pd.DataFrame
        The index DataFrame.
    metadata_df : pd.DataFrame
        The metadata DataFrame to be added to the index.
    data_leverancier : str, optional
        The data supplier name (required).

    Returns
    -------
    pd.DataFrame
        The updated index DataFrame.

    """

    if "level_0" in index_df.columns:
        index_df.drop(columns=["level_0"], inplace=True)

    if data_leverancier is None:
        raise Exception(
            "Need to provide a supplier explicitly (data_leverancier is None).",  # E501
        )

    def metadata_format(df: pd.DataFrame):
        df["HuisIdLeverancier"] = df["HuisIdLeverancier"].astype(str)
        return df

    metadata_df = metadata_format(metadata_df)

    bsv_metadata_df = get_bsv_metadata()

    bsv_metadata_filtered_df = metadata_format(
        bsv_metadata_df[bsv_metadata_df["Dataleverancier"] == data_leverancier].copy(),
    )

    # Shared columns
    shared_columns = metadata_df.columns.intersection(bsv_metadata_columns).intersection(index_df.columns)

    # Make sure data supplier is defined
    if "Dataleverancier" not in metadata_df.columns:
        if data_leverancier is None:
            raise Exception("Data source not identified. Cannot add metadata.")
        else:
            metadata_df["Dataleverancier"] = data_leverancier

    # Define protected columns and drop them from provider metadata
    protected_columns = ["HuisIdBSV", "ProjectIdBSV"]
    metadata_df = metadata_df.drop(
        columns=[col for col in protected_columns if col in metadata_df.columns],
    )

    # # bsv_metadata_df (do not change bsv_metadata_df)
    metadata_df = metadata_df.merge(
        bsv_metadata_filtered_df[
            [
                "HuisIdLeverancier",
                "ProjectIdLeverancier",
                "Dataleverancier",
                "HuisIdBSV",
                "ProjectIdBSV",
            ]
        ],
        on=["HuisIdLeverancier", "ProjectIdLeverancier", "Dataleverancier"],
        how="left",
    )


    # Add new columns with pd.NA if they do not already exist in index_df
    for column in metadata_df.columns:
        if column not in index_df.columns:
            index_df[column] = pd.NA

    # Update existing records
    index_df.set_index(
        ["HuisIdLeverancier", "Dataleverancier"],
        inplace=True,
    )
    metadata_df.set_index(
        ["HuisIdLeverancier", "Dataleverancier"],
        inplace=True,
    )

    columns_for_update = metadata_df.columns.intersection(allowed_supplier_metadata_columns)
    index_df.update(metadata_df.loc[:, columns_for_update])
    index_df.reset_index(inplace=True)

    # Save the updated index to the parquet file
    save_index_to_parquet(index_df=index_df)

    return index_df

def save_index_to_parquet(index_df: pd.DataFrame) -> None:
    """
    Save the index DataFrame to a Parquet file.

    Parameters
    ----------
    index_df : pd.DataFrame
        The DataFrame containing the index data.

    Returns
    -------
    None

    Notes
    -----
    This function saves the provided DataFrame to a Parquet file.
    """

    index_path = os.path.join(etdmap.options.mapped_folder_path, "index.parquet")

    index_df = set_metadata_dtypes(metadata_df=index_df, strict=True)

    index_df.to_parquet(index_path, engine="pyarrow")

    return None

def set_metadata_dtypes(metadata_df: pd.DataFrame, strict: bool = False) -> pd.DataFrame:
    """
    Set the data types of columns in the index or metdata DataFrame based on metadata_dtypes.

    Parameters
    ----------
    metadata_df : pandas.DataFrame
        The DataFrame containing the metadata.
    strict : bool, optional
        If True, raises an error if a column specified in metadata_dtypes is not found in the DataFrame. Default is False.

    Returns
    -------
    pandas.DataFrame
        The DataFrame with updated column data types.
    """

    for col, data_type in metadata_dtypes.items():
        if col in metadata_df.columns:
            metadata_df[col] = metadata_df[col].astype(data_type)
        else:
            if strict:
                print(f"Column {col} not found in DataFrame columns.")  # Debugging line to check for missing columns.
                raise ValueError(f"Column {col} is specified in metadata_dtypes but not present in the index_df to be saved.")  # Raise an error if a column is missing.

    return metadata_df

def get_mapped_file_path(huis_id_bsv: int) -> str:
    """
    Generates the file path for the mapped household data based on the BSV household ID.

    Parameters
    ----------
    huis_id_bsv : int
        The BSV household ID.

    Returns
    -------
    str
        The full file path to the mapped household data in Parquet format.
    """

    file_name = f"household_{huis_id_bsv}_table.parquet"
    file_path = os.path.join(etdmap.options.mapped_folder_path, file_name)
    return file_path

def get_mapped_data(huis_id_bsv: int) -> pd.DataFrame:
    """
    Retrieves the mapped household data for a given BSV household ID from the Parquet file.

    Parameters
    ----------
    huis_id_bsv : int
        The BSV household ID.

    Returns
    -------
    pd.DataFrame
        The DataFrame containing the household data.

    Raises
    ------
    FileNotFoundError
        If the specified file does not exist at the expected path.
    """

    file_path = get_mapped_file_path(huis_id_bsv)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file for HuisIdBSV {huis_id_bsv} does not exist at {file_path}.")
    household_df = pd.read_parquet(file_path)
    return household_df
