import logging
import os
from pathlib import Path
from typing import NamedTuple

import pandas as pd

import etdmap
from etdmap.data_model import (
    allowed_supplier_metadata_columns,
    cumulative_columns,
)
from etdmap.dataset_validators import dataset_flag_conditions

class HouseholdKey(NamedTuple):
    """Composite key uniquely identifying a household within a supplier dataset.

    HuisIdLeverancier alone is never unique — the same address string can exist
    in multiple projects. Always use both fields together.
    """
    project_id_supplier: str
    household_id_supplier: str


# Standard BSV column name constants.
# Import these in supplier mapping scripts rather than redefining them locally.
BSV_HOUSE_COL = "HuisIdLeverancier"
BSV_ID_COL = "HuisIdBSV"
BSV_PROJECT_COL = "ProjectIdLeverancier"

bsv_metadata_columns = [
    BSV_HOUSE_COL,
    BSV_ID_COL,
    BSV_PROJECT_COL,
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

class HouseBatchOverlapError(ValueError):
    """
    Raised when a household (HuisIdBSV) has data in more than one household
    batch (HuisBatchIdBSV) in a context that can only represent one batch per
    household -- for example the per-household index file, a single-file read
    for one household, or a computation that would silently combine two
    deliveries. A household appearing in more than one batch is valid in the
    data model but not yet supported by these code paths, so they stop loudly
    instead of mixing deliveries.

    Subclasses ValueError so existing ``except ValueError`` sites still
    catch it.
    """


def validate_data_frequency_present(index_df):
    """
    Raise if a row with Meenemen == True lacks a Gegevensfrequentie.

    Included rows enter the pipeline and must be fully specified. Other rows
    may legitimately have NA batch fields: Meenemen starts empty when a
    household is first mapped, and a household may not have its synced
    HuisBatch row yet. An index without a Gegevensfrequentie column (an
    older dataset) is not checked.
    """
    if "Gegevensfrequentie" not in index_df.columns:
        return
    col = index_df["Gegevensfrequentie"]
    missing_mask = col.isna() | (col.fillna("").astype(str).str.strip() == "")
    if "Meenemen" in index_df.columns:
        included_mask = index_df["Meenemen"].fillna(False).astype(bool)
        missing_mask = missing_mask & included_mask
    if bool(missing_mask.any()):
        id_col = (
            "HuisBatchIdBSV"
            if "HuisBatchIdBSV" in index_df.columns
            else "HuisIdBSV"
        )
        bad = sorted(
            int(h) for h in
            index_df.loc[missing_mask, id_col].dropna().tolist()
        )
        raise ValueError(
            f"Gegevensfrequentie missing for {id_col} {bad} although "
            f"Meenemen is True for these rows."
        )


# Household-batch columns of the index (one row per HuisBatchIdBSV).
# All nullable pandas dtypes (ADR-005); Startdatum/Einddatum are parsed to
# tz-aware UTC datetimes when the index is written.
batch_field_dtypes = {
    "HuisBatchIdBSV": pd.Int64Dtype(),
    "BatchIdBSV": pd.Int64Dtype(),
    "Gegevensfrequentie": pd.StringDtype(),
    "Leverancierfrequentie": pd.StringDtype(),
}


def _parse_synced_datetime(series: pd.Series) -> pd.Series:
    """
    Parse a synced DateTime column to tz-aware UTC, accepting BOTH forms the
    metadata source produces:

    - epoch SECONDS (int) -- its records API convention;
    - FORMATTED strings, e.g. "2018-12-31 23:05:00 UTC" -- its CSV export
      convention (the synced household-batch CSV uses this form).

    Both forms occur in practice depending on which endpoint produced the
    file; assuming a single form is a crash on the other.
    """
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().sum() == series.notna().sum():
        return pd.to_datetime(numeric, unit="s", utc=True)
    return pd.to_datetime(series, utc=True)


def _read_include_from_bsv_metadata():
    """
    HuisIdBSV -> Meenemen (include boolean) from the combined BSV metadata, or None when no
    metadata file is configured or present (legacy and fixture datasets).

    Reads ONLY those two columns: the metadata file carries PII columns
    (HuisIdLeverancier) that must not be loaded here (ADR-002).
    """
    try:
        path = etdmap.options.bsv_metadata_file
    except Exception:
        return None
    if not path or not os.path.exists(str(path)):
        return None
    p = str(path)
    if p.lower().endswith(".csv"):
        df = pd.read_csv(p, usecols=["HuisIdBSV", "Meenemen"],
                         dtype_backend="numpy_nullable")
    else:
        df = get_bsv_metadata()[["HuisIdBSV", "Meenemen"]]
    return pd.DataFrame({
        "HuisIdBSV": df["HuisIdBSV"].astype("Int64"),
        "Meenemen": df["Meenemen"].astype("boolean"),
    })


def get_bsv_metadata():
    """
    Reads and returns metadata from the BSV metadata file, ensuring that all required columns are present.

    Supports both CSV (new) and Excel xlsx (legacy) formats, determined by the file extension.

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
    - The path to the BSV metadata file is obtained from `etdmap.options.bsv_metadata_file`.
    - The required columns are defined in the `bsv_metadata_columns` list.
    """
    path = Path(etdmap.options.bsv_metadata_file)
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, dtype=metadata_dtypes)
    else:
        df = read_metadata(str(path), required_columns=bsv_metadata_columns)
    missing = [c for c in bsv_metadata_columns if c not in df.columns]
    if missing:
        raise ValueError(f"get_bsv_metadata: missing columns {missing} in {path}")
    return df


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


def read_index(mapped_folder_path=None) -> tuple[pd.DataFrame, str]:
    """
    Read index.parquet.

    One row per household batch (HuisBatchIdBSV). Older datasets carry one
    row per household without the household-batch columns; they are read
    unchanged. Raises when an included row (Meenemen == True) lacks a
    Gegevensfrequentie while the batch fields are present.

    Parameters
    ----------
    mapped_folder_path : str or Path, optional
        Folder holding index.parquet. Defaults to
        ``etdmap.options.mapped_folder_path``.

    Returns
    -------
    tuple
        A tuple containing:
            - DataFrame: The DataFrame of the index.
            - str: The path to the index file.
    """
    if mapped_folder_path is None:
        mapped_folder_path = etdmap.options.mapped_folder_path
    index_path = os.path.join(str(mapped_folder_path), "index.parquet")
    if os.path.exists(index_path):
        index_df = pd.read_parquet(index_path, dtype_backend="numpy_nullable")
    else:
        index_df = pd.DataFrame(
            columns=bsv_metadata_columns
        )

    if "HuisId" in index_df.columns:
        index_df.rename(columns={"HuisId": "HuisIdLeverancier"}, inplace=True)
    if "ProjectId" in index_df.columns:
        index_df.rename(columns={"ProjectId": "ProjectIdLeverancier"}, inplace=True)

    index_df = set_metadata_dtypes(metadata_df=index_df, strict=True)
    for col, dtype in batch_field_dtypes.items():
        if col in index_df.columns:
            index_df[col] = index_df[col].astype(dtype)
    validate_data_frequency_present(index_df)

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

    for household_id, file in data_files.items():
        if household_id in existing_ids:
            x = (int(existing_ids[household_id]), file)
        else:
            x = (next_id, file)
            next_id += 1

        household_id_pairs.append(x)
        logging.info(f"Household id pair: {x}")

    return household_id_pairs


def assign_bsv_ids(
    index_df: pd.DataFrame,
    data_provider: str,
    household_keys: list,
) -> dict:
    """Assign HuisIdBSV to (ProjectIdLeverancier, HuisIdLeverancier) pairs.

    Looks up existing assignments from the index by composite key. New pairs
    receive sequential integers starting from max(HuisIdBSV) + 1 globally
    (across all suppliers), so HuisIdBSV is unique across the entire dataset.

    Parameters
    ----------
    index_df : pd.DataFrame
        The current index DataFrame.
    data_provider : str
        The supplier name (Dataleverancier).
    household_keys : list[HouseholdKey]
        Ordered list of (project_id_supplier, household_id_supplier) pairs
        to assign. Duplicates are safe — each unique key gets one ID.

    Returns
    -------
    dict[HouseholdKey, int]
        Mapping from each HouseholdKey to its HuisIdBSV.
    """
    provider_rows = index_df[index_df["Dataleverancier"] == data_provider]

    existing = {}
    if not provider_rows.empty and "ProjectIdLeverancier" in provider_rows.columns:
        lookup = (
            provider_rows
            .set_index(["ProjectIdLeverancier", "HuisIdLeverancier"])["HuisIdBSV"]
            .dropna()
            .to_dict()
        )
        existing = {HouseholdKey(str(p), str(h)): int(v) for (p, h), v in lookup.items()}

    valid_ids = index_df["HuisIdBSV"].dropna()
    next_id = int(valid_ids.max()) + 1 if not valid_ids.empty else 1

    result = {}
    for key in household_keys:
        if key in result:
            continue  # already assigned in this batch
        if key in existing:
            result[key] = existing[key]
        else:
            result[key] = next_id
            existing[key] = next_id
            next_id += 1

    logging.info(
        f"[assign_bsv_ids] {data_provider}: {len(result)} household(s) assigned. "
        f"Next available HuisIdBSV: {next_id}"
    )
    return result


def update_index(
    index_df: pd.DataFrame,
    new_entry: dict,
    data_provider: str,
    save: bool = True,
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

    household_id = new_entry["HuisIdLeverancier"]
    project_id = new_entry.get("ProjectIdLeverancier")
    if not project_id:
        raise ValueError(
            f"update_index: 'ProjectIdLeverancier' is required but missing or empty "
            f"for HuisIdLeverancier='{household_id}'. All mappers must supply it."
        )

    mask = (
        (index_df["HuisIdLeverancier"] == household_id)
        & (index_df["ProjectIdLeverancier"] == project_id)
    )

    if mask.any():
        index_df.loc[mask, ["HuisIdBSV", "Dataleverancier"]] = (
            new_entry["HuisIdBSV"],
            data_provider,
        )
    else:
        new_entry_df = pd.DataFrame([new_entry])
        index_df = pd.concat([index_df, new_entry_df], ignore_index=True)

    # Recalculate or add flag columns
    household_id = new_entry["HuisIdBSV"]
    dataset_file = os.path.join(
        etdmap.options.mapped_folder_path,
        f"household_{household_id}_table.parquet",
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
                index_df.loc[index_df["HuisIdBSV"] == household_id, flag] = (
                    validation_result
                )
            except Exception as e:
                logging.error(
                    f"Error validating with {flag} for household "
                    f"{household_id}: {e}",
                    exc_info=True,
                )
                index_df.loc[
                    index_df["HuisIdBSV"] == household_id,
                    flag,
                ] = pd.NA

    # Ensure all flag columns are of BooleanDtype
    for flag in dataset_flag_conditions.keys():
        if flag in index_df.columns:
            index_df[flag] = index_df[flag].astype("boolean")

    index_df = update_meta_validators(index_df)

    if save:
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


def update_include() -> pd.DataFrame:
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

    bsv_metadata_df = get_bsv_metadata()

    # Validate that all households have a corresponding row
    index_key_columns = ["HuisIdBSV", "HuisIdLeverancier", "Dataleverancier"]

    metadata_keys_df = (
        bsv_metadata_df[index_key_columns]
        .sort_values(index_key_columns)
        .reset_index(drop=True)
    )
    index_keys_df = (
        index_df[index_key_columns]
        .sort_values(index_key_columns)
        .reset_index(drop=True)
    )

    # compare indices:
    # Check for missing HuisIdBSV in either direction
    metadata_householdids = set(metadata_keys_df['HuisIdBSV'])
    index_householdids = set(index_keys_df['HuisIdBSV'])

    missing_in_metadata = index_householdids - metadata_householdids
    missing_in_index = metadata_householdids - index_householdids

    # Build error message if there are mismatches
    if missing_in_metadata or missing_in_index:
        error_msg = "Household mismatch detected between metadata and index.\n"

        if missing_in_metadata:
            # Retrieve ProjectIdBSV from the ORIGINAL index_df (not the filtered one)
            missing_rows = index_df[index_df['HuisIdBSV'].isin(missing_in_metadata)][
                ['HuisIdBSV', 'ProjectIdBSV']
            ].sort_values('HuisIdBSV')

            missing_str = "\n".join(
                f"{row['HuisIdBSV']}\t{row['ProjectIdBSV']}"
                for _, row in missing_rows.iterrows()
            )
            error_msg += f"HuisIdBSV missing in metadata:\n{missing_str}\n"

        if missing_in_index:
            # Retrieve ProjectIdBSV from the ORIGINAL bsv_metadata_df
            missing_rows = bsv_metadata_df[bsv_metadata_df['HuisIdBSV'].isin(missing_in_index)][
                ['HuisIdBSV', 'ProjectIdBSV']
            ].sort_values('HuisIdBSV')

            missing_str = "\n".join(
                f"{row['HuisIdBSV']}\t{row['ProjectIdBSV']}"
                for _, row in missing_rows.iterrows()
            )
            error_msg += f"HuisIdBSV missing in index:\n{missing_str}\n"

        # Report row counts at the end
        error_msg += f"\nRow counts: metadata_keys_df={metadata_keys_df.shape[0]}, index_keys_df={index_keys_df.shape[0]}\n"

        raise ValueError(
            error_msg + 
            "Double check that the household table with Meenemen has been updated and has the exact same households as the index.parquet file. When adding new datasets, new household HuisIdBSV must be added."
        )

    # 1) value level differences
    comparison = metadata_keys_df.compare(
        index_keys_df,
        result_names=('bsv_metadata', 'index'),
    )

    # 2) missing/extra key‐rows
    merge_df = metadata_keys_df.merge(
        index_keys_df,
        on=index_key_columns,
        how='outer',
        indicator=True,
    )
    row_mismatches = merge_df[merge_df["_merge"] != "both"]

    if not comparison.empty or not row_mismatches.empty:
        logging.warning("Value mismatches:\n%s", comparison)
        logging.warning("Key row mismatches:\n%s", row_mismatches)
        raise Exception(
            f"Mismatching index and bsv metadata values: "
            f"{len(comparison)} value diff rows, "
            f"{len(row_mismatches)} key row mismatches."
        )

    if bsv_metadata_df["Meenemen"].isna().sum() > 0:
        raise Exception("Not all rows in the BSV metadata file have defined Meenemen")

    bsv_include = bsv_metadata_df[["HuisIdBSV", "Meenemen"]]

    index_df.drop(columns=["Meenemen"], inplace=True)
    index_df = index_df.merge(bsv_include, on=["HuisIdBSV"])


    #bsv_metadata_df.set_index("HuisIdBSV", inplace=True)
    #index_df.set_index('HuisIdBSV', inplace=True)
    #columns_for_update = bsv_metadata_df.columns.intersection(allowed_supplier_metadata_columns)

    #index_df.update(bsv_metadata_df.loc[:, columns_for_update])
    #index_df.reset_index(inplace=True)

    save_index_to_parquet(index_df=index_df)

    return index_df


def validate_project_id_coverage(
    metadata_df: pd.DataFrame,
    data_supplier: str,
) -> None:
    """Preflight check: every ProjectIdLeverancier in *metadata_df* must have a
    matching row in the project mapping CSV for this supplier.

    Call this immediately after loading the physical metadata file, before the
    per-household mapping loop, so missing project entries are caught up-front
    rather than at index-write time.

    Parameters
    ----------
    metadata_df : pd.DataFrame
        The supplier's physical metadata DataFrame. Must contain a
        ``ProjectIdLeverancier`` column.
    data_supplier : str
        Supplier name.  Used to filter the project CSV.

    Raises
    ------
    ValueError
        If ``etdmap.options.project_mapping_csv_path`` is not configured, or if
        any ``ProjectIdLeverancier`` value in *metadata_df* is absent from the
        project mapping CSV for this supplier.
    """
    project_mapping_csv_path = etdmap.options.project_mapping_csv_path
    if project_mapping_csv_path is None:
        raise ValueError(
            f"validate_project_id_coverage [{data_supplier}]: "
            "etdmap.options.project_mapping_csv_path is not set. "
            "Configure it in your overrides file."
        )

    project_df = pd.read_csv(Path(project_mapping_csv_path), dtype=str)
    known = set(
        project_df.loc[
            project_df["Dataleverancier"] == data_supplier, "ProjectIdLeverancier"
        ]
    )

    if not known:
        raise ValueError(
            f"validate_project_id_coverage [{data_supplier}]: "
            f"No rows found for '{data_supplier}' in project mapping CSV "
            f"({project_mapping_csv_path}). Add the supplier before running."
        )

    metadata_projects = set(metadata_df["ProjectIdLeverancier"].dropna().unique())
    missing = metadata_projects - known
    if missing:
        raise ValueError(
            f"validate_project_id_coverage [{data_supplier}]: "
            f"{len(missing)} project(s) in the physical metadata have no entry in "
            f"the project mapping CSV. Add them before running.\n"
            f"  Missing: {sorted(missing)}\n"
            f"  Known for {data_supplier}: {sorted(known)}"
        )


def add_supplier_metadata_to_index(
    index_df: pd.DataFrame,
    metadata_df: pd.DataFrame,
    data_supplier=None,
) -> pd.DataFrame:
    """Adds metadata columns to the index matching on the HuisIdLeverancier column. It also adds the ProjectIdBSV.

    Parameters
    ----------
    index_df : pd.DataFrame
        The index DataFrame.
    metadata_df : pd.DataFrame
        The metadata DataFrame to be added to the index.
    data_supplier : str, optional
        The data supplier name (required).

    Returns
    -------
    pd.DataFrame
        The updated index DataFrame.

    """

    if "level_0" in index_df.columns:
        index_df.drop(columns=["level_0"], inplace=True)

    if data_supplier is None:
        raise Exception(
            "Need to provide a supplier explicitly (data_supplier is None).",  # E501
        )

    def metadata_format(df: pd.DataFrame):
        df["HuisIdLeverancier"] = df["HuisIdLeverancier"].astype(str)
        return df

    metadata_df = metadata_format(metadata_df)

    # Make sure data supplier is defined
    if "Dataleverancier" not in metadata_df.columns:
        if data_supplier is None:
            raise Exception("Data source not identified. Cannot add metadata.")
        else:
            metadata_df["Dataleverancier"] = data_supplier

    # Define protected columns and drop them from provider metadata
    protected_columns = ["HuisIdBSV", "ProjectIdBSV"]
    metadata_df = metadata_df.drop(
        columns=[col for col in protected_columns if col in metadata_df.columns],
    )

    # Validate that ProjectIdLeverancier is populated before the join —
    # a null here means a silent failure (null != null in join keys).
    null_project_supplier = metadata_df["ProjectIdLeverancier"].isna().sum() if "ProjectIdLeverancier" in metadata_df.columns else len(metadata_df)
    if null_project_supplier > 0:
        raise ValueError(
            f"add_supplier_metadata_to_index [{data_supplier}]: "
            f"{null_project_supplier} row(s) in the supplier metadata file have "
            f"no ProjectIdLeverancier. Fill these in before running."
        )

    # Load the project mapping CSV (Dataleverancier × ProjectIdLeverancier → ProjectIdBSV).
    # This is a project-level mapping, not household-level — correct authority for ProjectIdBSV.
    project_mapping_csv_path = etdmap.options.project_mapping_csv_path
    if project_mapping_csv_path is None:
        raise ValueError(
            f"add_supplier_metadata_to_index [{data_supplier}]: "
            "etdmap.options.project_mapping_csv_path is not set. "
            "Configure it in your overrides file."
        )
    project_df = pd.read_csv(Path(project_mapping_csv_path), dtype=str)
    project_df_raw = project_df[
        project_df["Dataleverancier"] == data_supplier
    ][["Dataleverancier", "ProjectIdLeverancier", "ProjectIdBSV"]].copy()
    project_df_raw["ProjectIdBSV"] = project_df_raw["ProjectIdBSV"].astype(
        pd.Int64Dtype()
    )

    if project_df_raw.empty:
        raise ValueError(
            f"add_supplier_metadata_to_index [{data_supplier}]: "
            f"No rows found for '{data_supplier}' in project mapping CSV "
            f"({project_mapping_csv_path})."
        )

    # The CSV stores household-level data (one row per household), but we only
    # need the project-level mapping (ProjectIdLeverancier -> ProjectIdBSV) here.
    # Deduplicate to one row per project before the merge so we don't multiply
    # metadata rows. Raise if the same supplier project maps to conflicting BSV
    # project IDs — that is a data entry error.
    conflicts = (
        project_df_raw.dropna(subset=["ProjectIdBSV"])
        .groupby(["Dataleverancier", "ProjectIdLeverancier"])["ProjectIdBSV"]
        .nunique()
    )
    bad = conflicts[conflicts > 1]
    if not bad.empty:
        raise ValueError(
            f"add_supplier_metadata_to_index [{data_supplier}]: "
            f"Conflicting ProjectIdBSV values for the same project in the mapping CSV. "
            f"Affected: {bad.index.tolist()}"
        )
    project_df_filtered = project_df_raw.drop_duplicates(
        subset=["Dataleverancier", "ProjectIdLeverancier"]
    )

    # Join ProjectIdBSV from the project mapping (project-level join, not household-level)
    metadata_df = metadata_df.merge(
        project_df_filtered,
        on=["Dataleverancier", "ProjectIdLeverancier"],
        how="left",
    )

    null_bsv_project_id = metadata_df["ProjectIdBSV"].isna().sum()
    if null_bsv_project_id > 0:
        bad_rows = metadata_df.loc[
            metadata_df["ProjectIdBSV"].isna(), ["ProjectIdLeverancier", "Dataleverancier"]
        ].drop_duplicates().to_dict("records")
        raise ValueError(
            f"add_supplier_metadata_to_index [{data_supplier}]: "
            f"{null_bsv_project_id} row(s) have no ProjectIdBSV in the project "
            f"mapping CSV. Add the missing project(s) before running. "
            f"Unmatched: {bad_rows}"
        )

    # Add new columns with pd.NA if they do not already exist in index_df
    for column in metadata_df.columns:
        if column not in index_df.columns:
            index_df[column] = pd.NA

    index_key_columns = ["HuisIdLeverancier", "ProjectIdLeverancier", "Dataleverancier"]

    # Update existing records
    index_df.set_index(
        index_key_columns,
        inplace=True,
    )
    index_df.sort_index(inplace=True)
    metadata_df.set_index(
        index_key_columns,
        inplace=True,
    )
    metadata_df.sort_index(inplace=True)

    columns_for_update = metadata_df.columns.intersection([*allowed_supplier_metadata_columns, "ProjectIdBSV"])
    index_df.update(metadata_df.loc[:, columns_for_update])
    index_df.reset_index(inplace=True)

    # Save the updated index to the parquet file
    save_index_to_parquet(index_df=index_df)

    return index_df

def save_index_to_parquet(index_df: pd.DataFrame) -> None:
    """
    THE single index write: one file, index.parquet, one row per
    household batch (HuisBatchIdBSV).

    1. Meenemen values are copied onto the index from the combined BSV
       metadata file -- the single source for Meenemen on disk. Households
       not in that file get NA: Meenemen starts empty when a household is
       first mapped, and the researcher fills it in afterwards in the
       metadata administration after reviewing the mapping. Without a
       metadata file the index values are left unchanged. Meenemen is
       defined per household within each batch (see the README,
       "Households, batches, and the index").
    2. The household-batch columns are added: HuisBatchIdBSV (assigned
       locally -- the externally maintained HuisBatch table is a
       hand-maintained copy of these ids, never a source of new rows) plus
       the batch fields (BatchIdBSV, Gegevensfrequentie,
       Leverancierfrequentie, Startdatum, Einddatum) joined from the synced
       HuisBatch CSV. Households without a synced row yet are PENDING: they
       keep NA batch fields, a warning is logged, and a paste-ready
       ``pending_household_batch_additions.csv`` is written to support adding
       them to the metadata administration. When the synced CSV is absent
       entirely (datasets without batches, test fixtures) the batch fields
       are omitted with a warning.
    3. index.parquet is written.

    Raises
    ------
    HouseBatchOverlapError
        The synced CSV holds more than one batch row for a household. A
        household in more than one batch is valid in the data model but
        not yet supported.
    ValueError
        Missing Gegevensfrequentie on a row with Meenemen == True.
    """
    index_path = os.path.join(etdmap.options.mapped_folder_path, "index.parquet")
    index_df = index_df.copy().reset_index(drop=True)

    # -- 1. Meenemen from the combined BSV metadata --------------------------
    include_df = _read_include_from_bsv_metadata()
    if include_df is not None:
        stamp = dict(zip(
            (int(h) for h in include_df["HuisIdBSV"].dropna()),
            include_df.loc[include_df["HuisIdBSV"].notna(), "Meenemen"],
        ))
        index_df["Meenemen"] = pd.array(
            [stamp.get(int(h), pd.NA) if pd.notna(h) else pd.NA
             for h in index_df["HuisIdBSV"]],
            dtype="boolean",
        )

    # -- 2. the household-batch columns ---------------------------------------
    index_df["HuisBatchIdBSV"] = index_df["HuisIdBSV"].astype("Int64")

    try:
        sync_csv = etdmap.options.household_batch_csv_path
    except Exception:
        sync_csv = None
    if not sync_csv or not os.path.exists(str(sync_csv)):
        logging.warning(
            "[save_index_to_parquet] household-batch fields not populated: "
            "etdmap.options.household_batch_csv_path is not set or the file does "
            "not exist."
        )
        _write_index(index_df, index_path)
        return None
    sync_csv = str(sync_csv)

    mapped_folder = str(etdmap.options.mapped_folder_path)
    sync_df = pd.read_csv(sync_csv, dtype_backend="numpy_nullable")

    dup = sync_df["HuisIdBSV"].duplicated()
    if bool(dup.any()):
        multi = sorted(
            int(h) for h in sync_df.loc[dup, "HuisIdBSV"].dropna().unique()
        )
        raise HouseBatchOverlapError(
            f"The synced HuisBatch table holds more than one batch row for "
            f"HuisIdBSV {multi}. A household in more than one batch is valid "
            f"in the data model but not yet supported."
        )

    index_ids = [int(h) for h in index_df["HuisIdBSV"].dropna().tolist()]
    synced_ids = set(int(h) for h in sync_df["HuisIdBSV"].dropna().tolist())

    pending_path = os.path.join(
        mapped_folder, "pending_household_batch_additions.csv"
    )
    missing = sorted(set(index_ids) - synced_ids)
    if missing:
        # PENDING households: newly mapped, not yet added to the HuisBatch
        # table by the researcher. They stay in the index with NA batch
        # fields; the paste-ready proposal supports the manual addition
        # (HuisBatchIdBSV = HuisIdBSV for a first batch).
        missing_df = index_df[index_df["HuisIdBSV"].isin(missing)]
        proposal = pd.DataFrame({
            "HuisIdBSV": pd.array(missing, dtype="Int64"),
            "HuisBatchIdBSV": pd.array(missing, dtype="Int64"),
            "Dataleverancier": (
                missing_df.set_index(missing_df["HuisIdBSV"].astype("Int64"))
                ["Dataleverancier"].reindex(missing).astype("string").values
                if "Dataleverancier" in missing_df.columns
                else pd.array([pd.NA] * len(missing), dtype="string")
            ),
        })
        proposal.to_csv(pending_path, index=False)
        logging.warning(
            f"[save_index_to_parquet] {len(missing)} household(s) PENDING "
            f"their HuisBatch addition (HuisIdBSV): {missing}. "
            f"Paste-ready proposal written to {pending_path}."
        )
    else:
        # All households resolved: a stale proposal file must not linger.
        if os.path.exists(pending_path):
            os.remove(pending_path)

    synced_only = sorted(synced_ids - set(index_ids))
    if synced_only:
        logging.warning(
            f"[save_index_to_parquet] {len(synced_only)} synced HuisBatch "
            f"row(s) reference households not in this index (HuisIdBSV): "
            f"{synced_only}. Ignored: ids are assigned from the index."
        )

    join_cols = [
        c for c in ["BatchIdBSV", "Gegevensfrequentie",
                    "Leverancierfrequentie", "Startdatum", "Einddatum"]
        if c in sync_df.columns
    ]
    merged = index_df[["HuisIdBSV"]].merge(
        sync_df[["HuisIdBSV"] + join_cols], on="HuisIdBSV", how="left"
    )

    n = len(index_df)
    index_df["BatchIdBSV"] = (
        merged["BatchIdBSV"].astype("Int64")
        if "BatchIdBSV" in merged.columns
        else pd.array([pd.NA] * n, dtype="Int64")
    )
    index_df["Gegevensfrequentie"] = (
        merged["Gegevensfrequentie"].astype("string")
        if "Gegevensfrequentie" in merged.columns
        else pd.array([pd.NA] * n, dtype="string")
    )
    index_df["Leverancierfrequentie"] = (
        merged["Leverancierfrequentie"].astype("string")
        if "Leverancierfrequentie" in merged.columns
        else pd.array([pd.NA] * n, dtype="string")
    )
    # DateTime columns arrive as epoch seconds or formatted strings;
    # _parse_synced_datetime accepts both, and unmatched (pending) rows
    # parse to NaT.
    index_df["Startdatum"] = (
        _parse_synced_datetime(merged["Startdatum"])
        if "Startdatum" in merged.columns
        else pd.Series(pd.NaT, index=range(n)).dt.tz_localize("UTC")
    )
    index_df["Einddatum"] = (
        _parse_synced_datetime(merged["Einddatum"])
        if "Einddatum" in merged.columns
        else pd.Series(pd.NaT, index=range(n)).dt.tz_localize("UTC")
    )

    validate_data_frequency_present(index_df)

    # -- 3. the write ----------------------------------------------------------
    _write_index(index_df, index_path)
    return None


def _write_index(index_df: pd.DataFrame, index_path: str) -> None:
    """Cast the index dtypes and write index.parquet."""
    index_df = set_metadata_dtypes(metadata_df=index_df, strict=True)
    for col, dtype in batch_field_dtypes.items():
        if col in index_df.columns:
            index_df[col] = index_df[col].astype(dtype)
    index_df.to_parquet(index_path, engine="pyarrow")
    logging.info(
        f"[save_index_to_parquet] Wrote index.parquet "
        f"({len(index_df)} household-batch row(s)) -> {index_path}"
    )

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

def get_mapped_file_path(household_id: int) -> str:
    """
    Generates the file path for the mapped household data based on the BSV household ID.

    Parameters
    ----------
    household_id : int
        The BSV household ID.

    Returns
    -------
    str
        The full file path to the mapped household data in Parquet format.
    """

    file_name = f"household_{household_id}_table.parquet"
    file_path = os.path.join(etdmap.options.mapped_folder_path, file_name)
    return file_path

def get_mapped_data(household_id: int) -> pd.DataFrame:
    """
    Retrieves the mapped household data for a given BSV household ID from the Parquet file.

    Parameters
    ----------
    household_id : int
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

    file_path = get_mapped_file_path(household_id)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file for HuisIdBSV {household_id} does not exist at {file_path}.")
    household_df = pd.read_parquet(file_path)
    return household_df
