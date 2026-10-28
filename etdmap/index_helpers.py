import inspect
import logging
import os
from pathlib import Path
from typing import NamedTuple

import pandas as pd

import etdmap
import etdmap.ids as ids
from etdmap.data_model import (
    BOOLEAN_SYNONYMS,
    allowed_supplier_metadata_columns,
    cumulative_columns,
    interval_column,
    supplier_metadata_dtypes,
)
from etdmap.dataset_validators import dataset_flag_conditions
from etdmap.storage import detect_mapped_format, read_mapped_households


class HouseholdKey(NamedTuple):
    """Composite key uniquely identifying a household within a supplier dataset.

    The supplier's own household id alone is never unique -- the same address
    string can exist in multiple projects. Always use both fields together.
    """
    project_id_supplier: str
    household_id_supplier: str


# Standard BSV column name constants.
# Import these in supplier mapping scripts rather than redefining them locally.
BSV_HOUSE_COL = ids.HOUSEHOLD_SUPPLIER
BSV_ID_COL = ids.HOUSEHOLD
BSV_PROJECT_COL = ids.PROJECT_SUPPLIER

bsv_metadata_columns = [
    BSV_HOUSE_COL,
    BSV_ID_COL,
    BSV_PROJECT_COL,
    ids.PROJECT,
    ids.SUPPLIER,
    "Meenemen",
    "Notities",
]

# All nullable pandas dtypes (ADR-005). The identifier columns take theirs
# from etdmap.ids, so the two maps cannot disagree about one column; the
# review fields are this module's own.
metadata_dtypes = {
    ids.HOUSEHOLD_SUPPLIER: ids.DTYPES[ids.HOUSEHOLD_SUPPLIER],
    ids.HOUSEHOLD: ids.DTYPES[ids.HOUSEHOLD],
    "Meenemen": pd.BooleanDtype(),
    ids.PROJECT_SUPPLIER: ids.DTYPES[ids.PROJECT_SUPPLIER],
    ids.PROJECT: ids.DTYPES[ids.PROJECT],
    "Notities": pd.StringDtype(),
    ids.SUPPLIER: ids.DTYPES[ids.SUPPLIER],
}

class HouseBatchOverlapError(ValueError):
    """
    Raised when a household (``ids.HOUSEHOLD``) has data in more than one
    household batch (``ids.HOUSEHOLD_BATCH``) in a context that can only
    represent one household batch per household -- for example the
    per-household index file, a single-file read for one household, or a
    computation that would silently combine two household batches. A household appearing
    in more than one household batch is valid in the data model but not
    supported by these code paths, so they stop loudly instead of mixing
    household batches.

    Subclasses ValueError so an ``except ValueError`` site catches it.
    """


def validate_data_frequency_present(index_df):
    """
    Raise if a row with Meenemen == True lacks a Gegevensfrequentie.

    Included rows enter the pipeline and must be fully specified. Other rows
    may legitimately have NA household-batch fields: Meenemen starts empty when
    a household is first mapped, and a household may not have its synced
    HuisBatch row yet. An index without a Gegevensfrequentie column (an
    older household batch) is not checked.
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
            ids.HOUSEHOLD_BATCH
            if ids.HOUSEHOLD_BATCH in index_df.columns
            else ids.HOUSEHOLD
        )
        bad = sorted(
            int(h) for h in
            index_df.loc[missing_mask, id_col].dropna().tolist()
        )
        raise ValueError(
            f"Gegevensfrequentie missing for {id_col} {bad} although "
            f"Meenemen is True for these rows."
        )


# The index's household-batch columns: one row per household batch, carrying the
# delivery batch it was delivered in and the frequencies declared for it. All nullable
# pandas dtypes (ADR-005), the identifier ones from etdmap.ids;
# Startdatum/Einddatum are parsed to tz-aware UTC datetimes when the index is
# written.
batch_field_dtypes = {
    ids.HOUSEHOLD_BATCH: ids.DTYPES[ids.HOUSEHOLD_BATCH],
    ids.BATCH: ids.DTYPES[ids.BATCH],
    "Gegevensfrequentie": pd.StringDtype(),
    "Leverancierfrequentie": pd.StringDtype(),
}


def frequency_to_minutes(frequency):
    """
    Parse a declared frequency into minutes.

    Frequencies are written as durations -- ``5min``, ``15min``, ``1h``,
    ``30s`` -- and parsed as such, so the same value may be written in whichever
    unit suits it. Comparing the text instead would make ``1h`` and ``60min``
    unequal.

    Returns pd.NA for an absent or unparseable value, so a caller can decide
    what an undeclared frequency means for it.
    """
    if frequency is None or pd.isna(frequency):
        return pd.NA
    try:
        delta = pd.Timedelta(str(frequency).strip())
    except ValueError:
        logging.warning(
            f"[index_helpers] '{frequency}' is not a duration and cannot be "
            f"read as a frequency."
        )
        return pd.NA
    minutes = delta.total_seconds() / 60
    return minutes if minutes > 0 else pd.NA


def _household_batch_table():
    """
    The synced HuisBatch table, or None when it is not configured or present.

    Read on demand rather than at import: the path is an option the caller sets,
    and mapping runs that do not need the household-batch fields should not
    require it.
    """
    try:
        path = etdmap.options.household_batch_csv_path
    except Exception:
        return None
    if not path or not os.path.exists(str(path)):
        return None
    return pd.read_csv(str(path), dtype_backend="numpy_nullable")


def _batch_table_for_grid(context):
    """
    The synced HuisBatch table, with the two columns a grid lookup needs
    present. *context* names the subject of the error message.
    """
    table = _household_batch_table()
    if table is None:
        raise ValueError(
            f"{context}: no household batch table is configured or present, so "
            f"the grid it is recorded on is unknown. Check the data model: set "
            f"etdmap.options.household_batch_csv_path and sync HuisBatch."
        )
    for column in (ids.HOUSEHOLD_BATCH, "Gegevensfrequentie"):
        if column not in table.columns:
            raise ValueError(
                f"{context}: the household batch table carries no {column}, so "
                f"the grid it is recorded on is unknown. Check the data model."
            )
    return table


def household_batch_for_household(household_id):
    """
    The household batch of a household that has exactly one.

    ``ids.HOUSEHOLD_BATCH`` identifies a household batch and is what the mapped
    artifacts and their sidecars are keyed on; ``ids.HOUSEHOLD`` identifies the
    household it belongs to. This resolves one to the other through the
    household batch table, for callers that start from a household.

    Parameters
    ----------
    household_id : int
        The household.

    Returns
    -------
    int
        The household batch.

    Raises
    ------
    HouseBatchOverlapError
        When the household has more than one household batch. There is then no
        single household batch to return, and the caller has to say which one it means.
    ValueError
        When the household batch table is absent or unconfigured, does not hold
        the household, or holds it with no household batch.
    """
    context = f"Household {household_id}"
    table = _batch_table_for_grid(context)

    rows = table[table[ids.HOUSEHOLD] == household_id]
    if rows.empty:
        raise ValueError(
            f"{context} is not in the household batch table, so the household batch it "
            f"was delivered as is unknown. Check the data model: sync "
            f"HuisBatch before mapping it."
        )

    batch_ids = sorted(int(b) for b in rows[ids.HOUSEHOLD_BATCH].dropna().unique())
    if len(batch_ids) > 1:
        raise HouseBatchOverlapError(
            f"{context} has data in more than one household batch "
            f"({ids.HOUSEHOLD_BATCH} {batch_ids}), so there is no single household batch to "
            f"return. Name the household batch the caller means."
        )
    if not batch_ids:
        raise ValueError(
            f"{context} has a household batch row carrying no {ids.HOUSEHOLD_BATCH}, "
            f"so it cannot be identified. Check the data model."
        )
    return batch_ids[0]


def grid_minutes_for_household_batch(household_batch_id):
    """
    The grid a household batch is recorded on, in minutes.

    The grid is declared per household batch as ``Gegevensfrequentie``: it is
    the interval the mapped data sits on, not the interval any one column
    reports at. It is what ``ensure_intervals`` pads to, and what every later
    stage reads back off the index to scale a threshold with.

    ``ids.HOUSEHOLD_BATCH`` is the key. One household may have several household batches
    and each declares its own frequency, so the household does not identify a
    grid.

    An undeclared grid is an error rather than a default. Padding to the wrong
    grid changes the row count of the whole dataset and leaves the index
    declaring a spacing the rows are not on, which every threshold downstream
    is then scaled against.

    Parameters
    ----------
    household_batch_id : int
        The household batch.

    Returns
    -------
    float
        Grid interval in minutes.

    Raises
    ------
    ValueError
        When the household batch table is absent or unconfigured, carries
        neither required column, does not hold this household batch, holds it
        more than once, or declares no readable frequency for it.
    """
    context = f"{ids.HOUSEHOLD_BATCH} {household_batch_id}"
    table = _batch_table_for_grid(context)

    rows = table[table[ids.HOUSEHOLD_BATCH] == household_batch_id]
    if rows.empty:
        raise ValueError(
            f"{context} is not in the household batch table, so the grid it is "
            f"recorded on is unknown. Check the data model: sync HuisBatch "
            f"before mapping it."
        )
    if len(rows) > 1:
        raise ValueError(
            f"{context} has {len(rows)} rows in the household batch table. A "
            f"household batch has one row there, so its grid is ambiguous. "
            f"Check the data model."
        )

    declared = rows["Gegevensfrequentie"].iloc[0]
    minutes = frequency_to_minutes(declared)
    if pd.isna(minutes):
        raise ValueError(
            f"{context} declares Gegevensfrequentie '{declared}', which is not "
            f"a readable duration, so the grid it is recorded on is unknown. "
            f"Check the data model."
        )
    return float(minutes)


def grid_minutes_for_household(household_id):
    """
    The grid a household's single household batch is recorded on, in minutes.

    Resolves the household's ``ids.HOUSEHOLD_BATCH`` from the household batch
    table and delegates to :func:`grid_minutes_for_household_batch`, which is the
    model-keyed form. Callers that know which household batch they are processing
    should use that one directly.

    Parameters
    ----------
    household_id : int
        The household.

    Returns
    -------
    float
        Grid interval in minutes.

    Raises
    ------
    HouseBatchOverlapError
        When the household has more than one household batch. The grid is a
        property of the household batch, so there is no household-wide answer to return.
    ValueError
        As :func:`grid_minutes_for_household_batch`, plus when the household is
        not in the household batch table.
    """
    context = f"Household {household_id}"
    try:
        return grid_minutes_for_household_batch(
            household_batch_for_household(household_id)
        )
    except HouseBatchOverlapError:
        raise
    except ValueError as e:
        raise ValueError(f"{context}: {e}") from e


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
    ``ids.HOUSEHOLD`` -> Meenemen (include boolean) from the combined BSV
    metadata, or None when no metadata file is configured or present (legacy
    and fixture datasets).

    Reads ONLY those two columns: the metadata file carries PII columns (the
    supplier's own household id) that must not be loaded here (ADR-002).
    """
    try:
        path = etdmap.options.bsv_metadata_file
    except Exception:
        return None
    if not path or not os.path.exists(str(path)):
        return None
    p = str(path)
    if p.lower().endswith(".csv"):
        df = pd.read_csv(p, usecols=[ids.HOUSEHOLD, "Meenemen"],
                         dtype_backend="numpy_nullable")
    else:
        df = get_bsv_metadata()[[ids.HOUSEHOLD, "Meenemen"]]
    return pd.DataFrame({
        ids.HOUSEHOLD: df[ids.HOUSEHOLD].astype("Int64"),
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
        A list of column names that must be present in the metadata. Defaults to [ids.HOUSEHOLD_SUPPLIER].

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
        required_columns = [ids.HOUSEHOLD_SUPPLIER]
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


def read_index(
    mapped_folder_path=None, allow_missing: bool = False
) -> tuple[pd.DataFrame, str]:
    """
    Read index.parquet.

    One row per household batch (``ids.HOUSEHOLD_BATCH``). An older mapped dataset carries
    one row per household without the household-batch columns; they are read
    unchanged. Raises when an included row (Meenemen == True) lacks a
    Gegevensfrequentie while the household-batch fields are present.

    An ABSENT index is refused unless *allow_missing* says otherwise. The
    index is the household-id registry: ``assign_ids`` looks the household id
    up by (supplier, supplier project, supplier household) and invents a new id
    only for a key it cannot find. Reading an empty index therefore makes every
    household look new, so a mapping run into a folder without one renumbers
    the whole dataset in mapper execution order -- silently, and the household
    id is the join key of every comparison against an earlier run. Only a
    genuinely new dataset starts without an index.

    Parameters
    ----------
    mapped_folder_path : str or Path, optional
        Folder holding index.parquet. Defaults to
        ``etdmap.options.mapped_folder_path``.
    allow_missing : bool, default False
        Return an empty index instead of raising when index.parquet does not
        exist. For bootstrapping a new dataset only.

    Returns
    -------
    tuple
        A tuple containing:
            - DataFrame: The DataFrame of the index.
            - str: The path to the index file.

    Raises
    ------
    FileNotFoundError
        When index.parquet is absent and *allow_missing* is False.
    """
    if mapped_folder_path is None:
        mapped_folder_path = etdmap.options.mapped_folder_path
    index_path = os.path.join(str(mapped_folder_path), "index.parquet")
    if os.path.exists(index_path):
        index_df = pd.read_parquet(index_path, dtype_backend="numpy_nullable")
    elif allow_missing:
        index_df = pd.DataFrame(
            columns=bsv_metadata_columns
        )
    else:
        raise FileNotFoundError(
            f"No index found at {index_path}. The index is the household-id "
            f"registry: without it every household looks new, so ids are "
            f"reassigned in mapper execution order and every comparison "
            f"against an earlier run silently pairs the wrong households. "
            f"Either copy the canonical index.parquet into this folder "
            f"first, which is what a remap of existing households needs, or "
            f"ask for it explicitly (read_index(allow_missing=True), or the "
            f"mappers' --allow-new-index flag) if this really is a brand-new "
            f"dataset."
        )

    if "HuisId" in index_df.columns:
        index_df.rename(columns={"HuisId": ids.HOUSEHOLD_SUPPLIER}, inplace=True)
    if "ProjectId" in index_df.columns:
        index_df.rename(columns={"ProjectId": ids.PROJECT_SUPPLIER}, inplace=True)

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
    """Generates pairs of household id and filename for new and existing entries.

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
        A list of tuples containing a household id and a filename.
    """
    existing_ids = (
        index_df[index_df[ids.SUPPLIER] == data_provider]
        .set_index(ids.HOUSEHOLD_SUPPLIER)
        .to_dict()[ids.HOUSEHOLD]
    )
    data_files = list_files_func(data_folder_path)

    household_id_pairs = []
    next_id = int(max(index_df[ids.HOUSEHOLD], default=0) + 1)

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
    """Assign a household id to (supplier project, supplier household) pairs.

    Looks up existing assignments from the index by composite key. New pairs
    receive sequential integers starting from the highest household id in the
    index + 1 globally (across all suppliers), so a household id is unique
    across the entire dataset.

    Parameters
    ----------
    index_df : pd.DataFrame
        The current index DataFrame.
    data_provider : str
        The supplier name.
    household_keys : list[HouseholdKey]
        Ordered list of (project_id_supplier, household_id_supplier) pairs
        to assign. Duplicates are safe -- each unique key gets one ID.

    Returns
    -------
    dict[HouseholdKey, int]
        Mapping from each HouseholdKey to its household id.
    """
    provider_rows = index_df[index_df[ids.SUPPLIER] == data_provider]

    existing = {}
    if not provider_rows.empty and ids.PROJECT_SUPPLIER in provider_rows.columns:
        lookup = (
            provider_rows
            .set_index([ids.PROJECT_SUPPLIER, ids.HOUSEHOLD_SUPPLIER])[ids.HOUSEHOLD]
            .dropna()
            .to_dict()
        )
        existing = {HouseholdKey(str(p), str(h)): int(v) for (p, h), v in lookup.items()}

    valid_ids = index_df[ids.HOUSEHOLD].dropna()
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
        f"Next available {ids.HOUSEHOLD}: {next_id}"
    )
    return result


def supported_kwargs(condition, **kwargs) -> dict:
    """
    Of *kwargs*, only those *condition* declares a parameter for.

    The validator registries are dispatched in a loop, so every caller offers
    the same context to every validator. A validator that has no use for a
    piece of it does not declare it, rather than declaring it and ignoring it:
    the signature is then the record of what the check actually depends on, and
    a reader does not have to look for a use that is not there.

    Parameters
    ----------
    condition : callable
        A validator from ``record_flag_conditions`` or
        ``dataset_flag_conditions``.
    **kwargs
        The context on offer, e.g. ``cadence`` or ``grid_minutes``.

    Returns
    -------
    dict
        The subset to pass on.
    """
    accepted = inspect.signature(condition).parameters
    return {name: value for name, value in kwargs.items() if name in accepted}


def update_index(
    index_df: pd.DataFrame,
    new_entry: dict,
    data_provider: str,
    save: bool = True,
) -> pd.DataFrame:
    """Update the index with new entries and recalculate or add flag columns for ``dataset_validators``.

    The index has one row per household batch, so *new_entry* names its row by
    the index's own key: ``ids.HOUSEHOLD_BATCH`` is required. Its flags are then
    computed from that household batch's readings alone.

    Parameters
    ----------
    index_df : pd.DataFrame
        The index DataFrame.
    new_entry : dict
        The new entry to be added or updated in the index. Must carry
        ``ids.HOUSEHOLD_BATCH``, which ``run_standard_pipeline`` returns.
    data_provider : str
        The name of the data provider.

    Returns
    -------
    pd.DataFrame
        The updated index DataFrame.

    Raises
    ------
    ValueError
        When *new_entry* names no household batch, when it names no supplier
        project, or when the supplier pair matches more than one row that does
        not yet name its household batch.
    """

    # The lookup mask below matches the supplier ids as strings.
    new_entry[ids.HOUSEHOLD_SUPPLIER] = str(new_entry[ids.HOUSEHOLD_SUPPLIER])
    if ids.PROJECT_SUPPLIER in new_entry:
        new_entry[ids.PROJECT_SUPPLIER] = str(new_entry[ids.PROJECT_SUPPLIER])
    new_entry[ids.SUPPLIER] = data_provider

    household_id_supplier = new_entry[ids.HOUSEHOLD_SUPPLIER]
    project_id_supplier = new_entry.get(ids.PROJECT_SUPPLIER)
    if not project_id_supplier:
        raise ValueError(
            f"update_index: ids.PROJECT_SUPPLIER is required but missing or empty "
            f"for {ids.HOUSEHOLD_SUPPLIER}='{household_id_supplier}'. "
            f"All mappers must supply it."
        )

    household_batch_id = new_entry.get(ids.HOUSEHOLD_BATCH)
    if household_batch_id is None or pd.isna(household_batch_id):
        raise ValueError(
            f"update_index: {ids.HOUSEHOLD_BATCH} is required but missing or "
            f"empty for {ids.HOUSEHOLD_SUPPLIER}='{household_id_supplier}' in "
            f"{ids.PROJECT_SUPPLIER}='{project_id_supplier}'. It is the key of "
            f"the row this entry is for, and of the readings its flags are "
            f"computed from. run_standard_pipeline returns it; a mapper "
            f"building its own entry resolves it with "
            f"household_batch_for_household."
        )
    household_batch_id = int(household_batch_id)
    new_entry[ids.HOUSEHOLD_BATCH] = household_batch_id

    if ids.HOUSEHOLD_BATCH not in index_df.columns:
        index_df[ids.HOUSEHOLD_BATCH] = pd.array(
            [pd.NA] * len(index_df), dtype="Int64"
        )

    batch_mask = index_df[ids.HOUSEHOLD_BATCH] == household_batch_id
    if batch_mask.any():
        mask = batch_mask
    else:
        # The row for this supplier pair that does not yet name its household
        # batch: written before HuisBatch was synced, or by a mapper that could
        # not resolve it. Two such rows cannot be told apart; no such row
        # appends instead.
        unassigned = (
            (index_df[ids.HOUSEHOLD_SUPPLIER] == household_id_supplier)
            & (index_df[ids.PROJECT_SUPPLIER] == project_id_supplier)
            & index_df[ids.HOUSEHOLD_BATCH].isna()
        )
        n_unassigned = int(unassigned.sum())
        if n_unassigned > 1:
            raise ValueError(
                f"update_index: {n_unassigned} index rows for "
                f"{ids.HOUSEHOLD_SUPPLIER}='{household_id_supplier}' in "
                f"{ids.PROJECT_SUPPLIER}='{project_id_supplier}' name no "
                f"{ids.HOUSEHOLD_BATCH}, so the row for {household_batch_id} "
                f"is ambiguous. Sync HuisBatch so each row names its household "
                f"batch."
            )
        mask = unassigned if n_unassigned == 1 else None

    if mask is not None:
        index_df.loc[mask, [ids.HOUSEHOLD, ids.HOUSEHOLD_BATCH, ids.SUPPLIER]] = (
            new_entry[ids.HOUSEHOLD],
            household_batch_id,
            data_provider,
        )
    else:
        new_entry_df = pd.DataFrame([new_entry])
        index_df = pd.concat([index_df, new_entry_df], ignore_index=True)

    index_df[ids.HOUSEHOLD_BATCH] = index_df[ids.HOUSEHOLD_BATCH].astype("Int64")

    # Recalculate or add flag columns.
    #
    # Read format-aware. A mapped dataset is either flat
    # (household_<id>_table.parquet) or sharded
    # (sharded/HuisIdBSV=<id>/HuisBatchIdBSV=<n>/*.parquet), and sharded is the
    # default. A flat filename built here and guarded on os.path.exists would
    # never match under the sharded layout, leaving every household with no
    # household-batch flag and no error to say so.
    #
    # ONE household batch, not the household. read_mapped_households prunes
    # directories by household and then selects the household batch exactly, so
    # a household delivered twice is not concatenated into one series: these
    # validators ask whether a series is complete, and a gap between two
    # deliveries is not a gap in either of them. The flat layout has no
    # household-batch level, so there the household's one file IS the household
    # batch and the selection has nothing to narrow.
    household_id = new_entry[ids.HOUSEHOLD]
    sharded = detect_mapped_format() == "sharded"
    try:
        df = read_mapped_households(
            [household_id],
            household_batch_ids=[household_batch_id] if sharded else None,
        )
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"update_index: cannot compute household-batch flags for "
            f"{ids.HOUSEHOLD_BATCH} {household_batch_id} ({ids.HOUSEHOLD} "
            f"{household_id}) -- it has no mapped data. {e}"
        ) from e

    # The grid these validators judge against. A row count only means something
    # against the interval the household batch is recorded on, and each
    # household batch declares its own.
    grid_minutes = grid_minutes_for_household_batch(household_batch_id)

    for flag, condition in dataset_flag_conditions.items():
        kwargs = supported_kwargs(condition, grid_minutes=grid_minutes)
        # Add flag column if it does not exist and ensure it's BooleanDtype
        if flag not in index_df.columns:
            index_df[flag] = pd.Series(
                pd.NA,
                dtype="boolean", # "bool" is the standard non-nullable Boolean type (backed by NumPy), while "boolean" is pandas' nullable Boolean extension type (pd.BooleanDtype) that supports NA values.
                index=index_df.index,
            )
        # Only a data-level failure becomes pd.NA. A TypeError, AttributeError
        # or KeyError here is a broken validator or a changed contract, and
        # recording it as "not checked" hides it in a column of NA rather than
        # stopping the run (ADR-012).
        try:
            validation_result = condition(df, **kwargs)
            index_df.loc[
                index_df[ids.HOUSEHOLD_BATCH] == household_batch_id, flag
            ] = validation_result
        except (ValueError, ArithmeticError) as e:
            logging.error(
                f"Error validating with {flag} for {ids.HOUSEHOLD_BATCH} "
                f"{household_batch_id}: {e}",
                exc_info=True,
            )
            index_df.loc[
                index_df[ids.HOUSEHOLD_BATCH] == household_batch_id,
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
    cols = [
        "validate_" + interval_column(col)
        for col in cumulative_columns
        if interval_column(col) is not None
    ]

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
    5. Extracts the ids.HOUSEHOLD and 'Meenemen' columns from the BSV metadata DataFrame.
    6. Merges the extracted BSV 'Meenemen' data with the index DataFrame on the ids.HOUSEHOLD column.
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
    index_key_columns = [ids.HOUSEHOLD, ids.HOUSEHOLD_SUPPLIER, ids.SUPPLIER]

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
    # Check for a missing household in either direction
    metadata_householdids = set(metadata_keys_df[ids.HOUSEHOLD])
    index_householdids = set(index_keys_df[ids.HOUSEHOLD])

    missing_in_metadata = index_householdids - metadata_householdids
    missing_in_index = metadata_householdids - index_householdids

    # Build error message if there are mismatches
    if missing_in_metadata or missing_in_index:
        error_msg = "Household mismatch detected between metadata and index.\n"

        if missing_in_metadata:
            # Retrieve the project from the ORIGINAL index_df (not the filtered one)
            missing_rows = index_df[index_df[ids.HOUSEHOLD].isin(missing_in_metadata)][
                [ids.HOUSEHOLD, ids.PROJECT]
            ].sort_values(ids.HOUSEHOLD)

            missing_str = "\n".join(
                f"{row[ids.HOUSEHOLD]}\t{row[ids.PROJECT]}"
                for _, row in missing_rows.iterrows()
            )
            error_msg += f"{ids.HOUSEHOLD} missing in metadata:\n{missing_str}\n"

        if missing_in_index:
            # Retrieve the project from the ORIGINAL bsv_metadata_df
            missing_rows = bsv_metadata_df[bsv_metadata_df[ids.HOUSEHOLD].isin(missing_in_index)][
                [ids.HOUSEHOLD, ids.PROJECT]
            ].sort_values(ids.HOUSEHOLD)

            missing_str = "\n".join(
                f"{row[ids.HOUSEHOLD]}\t{row[ids.PROJECT]}"
                for _, row in missing_rows.iterrows()
            )
            error_msg += f"{ids.HOUSEHOLD} missing in index:\n{missing_str}\n"

        # Report row counts at the end
        error_msg += f"\nRow counts: metadata_keys_df={metadata_keys_df.shape[0]}, index_keys_df={index_keys_df.shape[0]}\n"

        raise ValueError(
            error_msg +
            "Double check that the household table with Meenemen has been updated and has the exact same households as the index.parquet file. When adding new datasets, the new households must be added to it."
        )

    # 1) value level differences
    comparison = metadata_keys_df.compare(
        index_keys_df,
        result_names=('bsv_metadata', 'index'),
    )

    # 2) missing/extra key-rows
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

    bsv_include = bsv_metadata_df[[ids.HOUSEHOLD, "Meenemen"]]

    index_df.drop(columns=["Meenemen"], inplace=True)
    index_df = index_df.merge(bsv_include, on=[ids.HOUSEHOLD])


    #bsv_metadata_df.set_index(ids.HOUSEHOLD, inplace=True)
    #index_df.set_index(ids.HOUSEHOLD, inplace=True)
    #columns_for_update = bsv_metadata_df.columns.intersection(allowed_supplier_metadata_columns)

    #index_df.update(bsv_metadata_df.loc[:, columns_for_update])
    #index_df.reset_index(inplace=True)

    save_index_to_parquet(index_df=index_df)

    return index_df


def validate_project_id_coverage(
    metadata_df: pd.DataFrame,
    data_supplier: str,
) -> None:
    """Preflight check: every supplier project id in *metadata_df* must have a
    matching row in the project mapping CSV for this supplier.

    Call this immediately after loading the physical metadata file, before the
    per-household mapping loop, so missing project entries are caught up-front
    rather than at index-write time.

    Parameters
    ----------
    metadata_df : pd.DataFrame
        The supplier's physical metadata DataFrame. Must contain an
        ``ids.PROJECT_SUPPLIER`` column.
    data_supplier : str
        Supplier name.  Used to filter the project CSV.

    Raises
    ------
    ValueError
        If ``etdmap.options.project_mapping_csv_path`` is not configured, or if
        any ``ids.PROJECT_SUPPLIER`` value in *metadata_df* is absent from the
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
            project_df[ids.SUPPLIER] == data_supplier, ids.PROJECT_SUPPLIER
        ]
    )

    if not known:
        raise ValueError(
            f"validate_project_id_coverage [{data_supplier}]: "
            f"No rows found for '{data_supplier}' in project mapping CSV "
            f"({project_mapping_csv_path}). Add the supplier before running."
        )

    metadata_projects = set(metadata_df[ids.PROJECT_SUPPLIER].dropna().unique())
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
    """Adds metadata columns to the index matching on the supplier's own household id. It also adds the BSV project id.

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
        df[ids.HOUSEHOLD_SUPPLIER] = df[ids.HOUSEHOLD_SUPPLIER].astype(str)
        return df

    metadata_df = metadata_format(metadata_df)

    # Make sure data supplier is defined
    if ids.SUPPLIER not in metadata_df.columns:
        if data_supplier is None:
            raise Exception("Data source not identified. Cannot add metadata.")
        else:
            metadata_df[ids.SUPPLIER] = data_supplier

    # Define protected columns and drop them from provider metadata
    protected_columns = [ids.HOUSEHOLD, ids.PROJECT]
    metadata_df = metadata_df.drop(
        columns=[col for col in protected_columns if col in metadata_df.columns],
    )

    # Validate that the supplier's project id is populated before the join --
    # a null here means a silent failure (null != null in join keys).
    null_project_supplier = metadata_df[ids.PROJECT_SUPPLIER].isna().sum() if ids.PROJECT_SUPPLIER in metadata_df.columns else len(metadata_df)
    if null_project_supplier > 0:
        raise ValueError(
            f"add_supplier_metadata_to_index [{data_supplier}]: "
            f"{null_project_supplier} row(s) in the supplier metadata file have "
            f"no {ids.PROJECT_SUPPLIER}. Fill these in before running."
        )

    # Load the project mapping CSV (supplier x supplier project -> BSV project).
    # This is a project-level mapping, not household-level -- the correct
    # authority for the BSV project id.
    project_mapping_csv_path = etdmap.options.project_mapping_csv_path
    if project_mapping_csv_path is None:
        raise ValueError(
            f"add_supplier_metadata_to_index [{data_supplier}]: "
            "etdmap.options.project_mapping_csv_path is not set. "
            "Configure it in your overrides file."
        )
    project_df = pd.read_csv(Path(project_mapping_csv_path), dtype=str)
    project_df_raw = project_df[
        project_df[ids.SUPPLIER] == data_supplier
    ][[ids.SUPPLIER, ids.PROJECT_SUPPLIER, ids.PROJECT]].copy()
    project_df_raw[ids.PROJECT] = project_df_raw[ids.PROJECT].astype(
        pd.Int64Dtype()
    )

    if project_df_raw.empty:
        raise ValueError(
            f"add_supplier_metadata_to_index [{data_supplier}]: "
            f"No rows found for '{data_supplier}' in project mapping CSV "
            f"({project_mapping_csv_path})."
        )

    # The CSV stores household-level data (one row per household), but we only
    # need the project-level mapping (supplier project -> BSV project) here.
    # Deduplicate to one row per project before the merge so we don't multiply
    # metadata rows. Raise if the same supplier project maps to conflicting BSV
    # project IDs -- that is a data entry error.
    conflicts = (
        project_df_raw.dropna(subset=[ids.PROJECT])
        .groupby([ids.SUPPLIER, ids.PROJECT_SUPPLIER])[ids.PROJECT]
        .nunique()
    )
    bad = conflicts[conflicts > 1]
    if not bad.empty:
        raise ValueError(
            f"add_supplier_metadata_to_index [{data_supplier}]: "
            f"Conflicting {ids.PROJECT} values for the same project in the mapping CSV. "
            f"Affected: {bad.index.tolist()}"
        )
    project_df_filtered = project_df_raw.drop_duplicates(
        subset=[ids.SUPPLIER, ids.PROJECT_SUPPLIER]
    )

    # Join the BSV project id from the project mapping (project-level join, not household-level)
    metadata_df = metadata_df.merge(
        project_df_filtered,
        on=[ids.SUPPLIER, ids.PROJECT_SUPPLIER],
        how="left",
    )

    null_bsv_project_id = metadata_df[ids.PROJECT].isna().sum()
    if null_bsv_project_id > 0:
        bad_rows = metadata_df.loc[
            metadata_df[ids.PROJECT].isna(), [ids.PROJECT_SUPPLIER, ids.SUPPLIER]
        ].drop_duplicates().to_dict("records")
        raise ValueError(
            f"add_supplier_metadata_to_index [{data_supplier}]: "
            f"{null_bsv_project_id} row(s) have no {ids.PROJECT} in the project "
            f"mapping CSV. Add the missing project(s) before running. "
            f"Unmatched: {bad_rows}"
        )

    # Add new columns with pd.NA if they do not already exist in index_df
    for column in metadata_df.columns:
        if column not in index_df.columns:
            index_df[column] = pd.NA

    index_key_columns = [ids.HOUSEHOLD_SUPPLIER, ids.PROJECT_SUPPLIER, ids.SUPPLIER]

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

    columns_for_update = metadata_df.columns.intersection([*allowed_supplier_metadata_columns, ids.PROJECT])

    index_df.update(metadata_df.loc[:, columns_for_update])
    index_df.reset_index(inplace=True)

    # Save the updated index to the parquet file
    save_index_to_parquet(index_df=index_df)

    return index_df

def save_index_to_parquet(index_df: pd.DataFrame) -> None:
    """
    THE single index write: one file, index.parquet, one row per
    household batch (``ids.HOUSEHOLD_BATCH``).

    1. Meenemen values are copied onto the index from the combined BSV
       metadata file -- the single source for Meenemen on disk. Households
       not in that file get NA: Meenemen starts empty when a household is
       first mapped, and the researcher fills it in afterwards in the
       metadata administration after reviewing the mapping. Without a
       metadata file the index values are left unchanged. Meenemen is
       defined per household within each household batch (see the README,
       "Households, household batches, and the index").
    2. The household-batch columns are joined from the synced HuisBatch CSV:
       ``ids.HOUSEHOLD_BATCH`` -- the household batch each row IS, and the key the
       mapped artifacts and their sidecars are written under -- plus
       ``ids.BATCH``, Gegevensfrequentie, Leverancierfrequentie, Startdatum
       and Einddatum.
       The synced table is the source of the household batch id, which is what
       ``household_batch_for_household`` resolves for a mapper before it
       writes anything. Households without a synced row yet are PENDING: they
       keep NA household-batch fields, a warning is logged, and a paste-ready
       ``pending_household_batch_additions.csv`` is written to support adding
       them to the metadata administration. When the synced CSV is absent
       entirely (household batches with no synced row, test fixtures)
       the household-batch fields are omitted with a warning, the household
       household batch id among them.
    3. index.parquet is written.

    Raises
    ------
    HouseBatchOverlapError
        The synced CSV holds more than one household batch row for a household.
        A household in more than one household batch is valid in the data model
        but not yet supported.
    ValueError
        Missing Gegevensfrequentie on a row with Meenemen == True.
    """
    index_path = os.path.join(etdmap.options.mapped_folder_path, "index.parquet")
    index_df = index_df.copy().reset_index(drop=True)

    # -- 1. Meenemen from the combined BSV metadata --------------------------
    include_df = _read_include_from_bsv_metadata()
    if include_df is not None:
        stamp = dict(zip(
            (int(h) for h in include_df[ids.HOUSEHOLD].dropna()),
            include_df.loc[include_df[ids.HOUSEHOLD].notna(), "Meenemen"],
        ))
        index_df["Meenemen"] = pd.array(
            [stamp.get(int(h), pd.NA) if pd.notna(h) else pd.NA
             for h in index_df[ids.HOUSEHOLD]],
            dtype="boolean",
        )

    # -- 2. the household-batch columns ---------------------------------------
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

    dup = sync_df[ids.HOUSEHOLD].duplicated()
    if bool(dup.any()):
        multi = sorted(
            int(h) for h in sync_df.loc[dup, ids.HOUSEHOLD].dropna().unique()
        )
        raise HouseBatchOverlapError(
            f"The synced HuisBatch table holds more than one household batch row "
            f"for {ids.HOUSEHOLD} {multi}. A household in more than one household "
            f"batch is valid in the data model but not yet supported."
        )

    index_ids = [int(h) for h in index_df[ids.HOUSEHOLD].dropna().tolist()]
    synced_ids = set(int(h) for h in sync_df[ids.HOUSEHOLD].dropna().tolist())

    pending_path = os.path.join(
        mapped_folder, "pending_household_batch_additions.csv"
    )
    missing = sorted(set(index_ids) - synced_ids)
    if missing:
        # PENDING households: newly mapped, not yet added to the HuisBatch
        # table by the researcher. They stay in the index with NA
        # household-batch fields; the paste-ready proposal supports the manual
        # addition. It
        # numbers the household batch with the household id, which is what the
        # mapping writes; a household's further household batches are added in
        # the HuisBatch table itself.
        missing_df = index_df[index_df[ids.HOUSEHOLD].isin(missing)]
        proposal = pd.DataFrame({
            ids.HOUSEHOLD: pd.array(missing, dtype="Int64"),
            ids.HOUSEHOLD_BATCH: pd.array(missing, dtype="Int64"),
            ids.SUPPLIER: (
                missing_df.set_index(missing_df[ids.HOUSEHOLD].astype("Int64"))
                [ids.SUPPLIER].reindex(missing).astype("string").values
                if ids.SUPPLIER in missing_df.columns
                else pd.array([pd.NA] * len(missing), dtype="string")
            ),
        })
        proposal.to_csv(pending_path, index=False)
        logging.warning(
            f"[save_index_to_parquet] {len(missing)} household(s) PENDING "
            f"their HuisBatch addition ({ids.HOUSEHOLD}): {missing}. "
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
            f"row(s) reference households not in this index ({ids.HOUSEHOLD}): "
            f"{synced_only}. Ignored: ids are assigned from the index."
        )

    join_cols = [
        c for c in [ids.HOUSEHOLD_BATCH, ids.BATCH, "Gegevensfrequentie",
                    "Leverancierfrequentie", "Startdatum", "Einddatum"]
        if c in sync_df.columns
    ]
    merged = index_df[[ids.HOUSEHOLD]].merge(
        sync_df[[ids.HOUSEHOLD] + join_cols], on=ids.HOUSEHOLD, how="left"
    )

    n = len(index_df)
    # The household batch each row IS, read from the synced table rather than derived
    # from the household: a household may be delivered as more than one
    # household batch, and the mapped artifacts are already keyed on the synced id
    # (household_batch_for_household is what the mappers resolve it through).
    synced_batch = (
        merged[ids.HOUSEHOLD_BATCH].astype("Int64")
        if ids.HOUSEHOLD_BATCH in merged.columns
        else pd.array([pd.NA] * n, dtype="Int64")
    )
    # Two sources name the household batch: the synced table above, and what
    # update_index wrote from the entry -- the id the shard was actually written
    # under. A disagreement means a shard registered as one household batch and
    # written as another, which no later read can reconcile, so it stops here
    # rather than one source silently winning.
    if ids.HOUSEHOLD_BATCH in index_df.columns:
        written = index_df[ids.HOUSEHOLD_BATCH].astype("Int64").reset_index(drop=True)
        synced = pd.Series(synced_batch).reset_index(drop=True)
        disagrees = written.notna() & synced.notna() & (written != synced)
        if disagrees.any():
            rows = index_df.reset_index(drop=True).loc[disagrees, ids.HOUSEHOLD]
            pairs = ", ".join(
                f"{ids.HOUSEHOLD} {int(household)}: written "
                f"{int(written[i])}, synced {int(synced[i])}"
                for i, household in rows.items()
            )
            raise ValueError(
                f"[save_index_to_parquet] {int(disagrees.sum())} row(s) name a "
                f"different {ids.HOUSEHOLD_BATCH} than the synced HuisBatch "
                f"table does ({pairs}). The shard was written under one id and "
                f"is registered under another. Check the data model: sync "
                f"HuisBatch, or re-map under the synced id."
            )
        # Agreed, or only one source has it. A row the synced table does not
        # reach keeps what was written for it; a row nothing wrote stays absent.
        index_df[ids.HOUSEHOLD_BATCH] = synced.fillna(written).array
    else:
        index_df[ids.HOUSEHOLD_BATCH] = synced_batch
    index_df[ids.BATCH] = (
        merged[ids.BATCH].astype("Int64")
        if ids.BATCH in merged.columns
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

    # The supplier metadata variables, typed from the data model rather than left
    # to inference. A column whose values happen to be whole numbers in one
    # supplier's file would otherwise infer an integer dtype, and another
    # supplier's fractional value could then not be written into it.
    for col, data_type in supplier_metadata_dtypes.items():
        if col not in metadata_df.columns or col in metadata_dtypes:
            continue
        values = metadata_df[col]
        if data_type == "boolean" and values.dtype != "boolean":
            values = _as_boolean(values, col)
        try:
            metadata_df[col] = values.astype(data_type)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Metadata column '{col}' cannot hold the declared type "
                f"{data_type}: {exc}. Check the data model -- the declared "
                f"'Type variabele' and the delivered values disagree."
            ) from exc

    return metadata_df


def _as_boolean(values: pd.Series, column: str) -> pd.Series:
    """A declared-boolean metadata column, from whatever the supplier wrote.

    Suppliers write yes/no in their own language and some write 1/0, so the
    recognised spellings are declared in ``BOOLEAN_SYNONYMS``. An unrecognised
    value raises rather than becoming pd.NA: a new spelling is then a question to
    answer, not a column that quietly empties.
    """
    text = values.astype("string").str.strip().str.lower()
    mapped = text.map(BOOLEAN_SYNONYMS).astype("boolean")

    unrecognised = sorted(set(text[text.notna() & mapped.isna()].unique()))
    if unrecognised:
        raise ValueError(
            f"Metadata column '{column}' is declared boolean but carries "
            f"{unrecognised}, which is not one of {sorted(BOOLEAN_SYNONYMS)}. "
            f"Add the spelling to BOOLEAN_SYNONYMS if it is a synonym, or fix "
            f"the value."
        )
    return mapped

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

    Warnings
    --------
    FLAT LAYOUT ONLY. This builds the legacy ``household_<id>_table.parquet``
    name and is not format-aware, so the path it returns does not exist under
    the sharded layout (the default). To read a household's mapped data use
    ``get_mapped_data`` / ``etdmap.storage.read_mapped_households``; to resolve
    its file(s) use ``etdmap.storage.mapped_household_files``.
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
        If the household has no mapped data under either layout.

    Notes
    -----
    Format-aware: resolves the flat ``household_<id>_table.parquet`` or the
    sharded ``sharded/HuisIdBSV=<id>/HuisBatchIdBSV=<n>/*.parquet`` files. A
    household spanning several household batches is returned as one frame.

    The returned frame carries the household's mapped COLUMNS ONLY. The
    identifier columns that ``read_mapped_households`` injects for a
    multi-household read are dropped here, because identifiers live in the
    index and in the shard
    path, not in the household frame -- callers of this function iterate its
    columns (per-column statistics), so an injected id would be counted as a
    measured variable.
    """

    household_df = read_mapped_households([household_id])
    return household_df.drop(columns=[ids.HOUSEHOLD, ids.HOUSEHOLD_BATCH])


def households_of_household_batches(household_batch_ids) -> list:
    """The households the given household batch datasets belong to.

    From the index, which is the only place the two are related. A read prunes
    directories by household because that is the level the shard path has;
    selecting the household batches themselves is a separate, exact step.
    """
    wanted = {int(batch_id) for batch_id in household_batch_ids}
    index_df, _ = read_index()
    match = index_df.loc[
        index_df[ids.HOUSEHOLD_BATCH].isin(wanted), ids.HOUSEHOLD
    ]
    return sorted(int(household) for household in match.dropna().unique())


def household_batches_of_households(household_ids) -> dict:
    """The household batches each given household was delivered as.

    From the index, which is the only place the two are related, as
    ``{household: [household batch, ...]}``, each list sorted. The
    inverse of :func:`households_of_household_batches`, for a caller that
    starts from a household -- an operator naming one on a command line, say --
    and has to reach the key.

    A household the index does not hold is absent from the result rather than
    mapped to an empty list, so a caller can tell "not in the index" from
    "in the index with no household batch named".
    """
    wanted = {int(household_id) for household_id in household_ids}
    index_df, _ = read_index()
    rows = index_df.loc[
        index_df[ids.HOUSEHOLD].isin(wanted),
        [ids.HOUSEHOLD, ids.HOUSEHOLD_BATCH],
    ].dropna()
    found: dict = {}
    for household, household_batch in zip(
        rows[ids.HOUSEHOLD], rows[ids.HOUSEHOLD_BATCH]
    ):
        found.setdefault(int(household), set()).add(int(household_batch))
    return {household: sorted(batches) for household, batches in found.items()}


def get_mapped_household_batch(household_batch_id: int) -> pd.DataFrame:
    """
    Retrieve ONE household batch dataset's mapped data.

    Parameters
    ----------
    household_batch_id : int
        The BSV household batch id: one household over one period.

    Returns
    -------
    pd.DataFrame
        The household batch's mapped columns, with no identifier columns, so a caller
        iterating columns does not count an id as a measured variable.

    Raises
    ------
    FileNotFoundError
        If the index does not place the household batch, or the shard holds no data.

    Notes
    -----
    The household-batch-keyed read. :func:`get_mapped_data` returns a household's
    household batches concatenated, which pools two periods of one dwelling into one
    series -- right for a question about the dwelling's files, wrong for any
    statistic, because the household batch dataset is the unit.
    """
    households = households_of_household_batches([household_batch_id])
    if not households:
        raise FileNotFoundError(
            f"The index does not place {ids.HOUSEHOLD_BATCH} "
            f"{household_batch_id}, so its shard cannot be resolved."
        )
    frame = read_mapped_households(
        households, household_batch_ids=[household_batch_id]
    )
    return frame.drop(columns=[ids.HOUSEHOLD, ids.HOUSEHOLD_BATCH])
