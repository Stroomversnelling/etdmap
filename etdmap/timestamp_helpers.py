"""
Reusable timestamp utilities for ETD supplier mapping scripts.

These functions handle the common parts of timestamp derivation and UTC normalization
that appear across multiple data sources. Each new supplier script should use these
rather than reimplementing the same logic.

Typical usage in a supplier mapping script
------------------------------------------

    from etdmap.timestamp_helpers import derive_and_normalize_reading_date

    # In load_and_map_parquet():
    df = derive_and_normalize_reading_date(
        df,
        candidate_columns=["Datum", "readingDate"],  # supplier-specific names
        context=context,
    )

For sources that receive data in local time (CET) and need to store as UTC:

    from etdmap.timestamp_helpers import normalize_to_utc_for_storage

    df = normalize_to_utc_for_storage(df, from_offset_hours=1.0, context=context)

For sources where ReadingDate is already present or pre-processed by the supplier,
neither function is needed — just ensure the column is named 'ReadingDate' and is
datetime dtype before calling run_standard_pipeline().
"""

import logging

import pandas as pd


def derive_and_normalize_reading_date(
    df: pd.DataFrame,
    candidate_columns: list[str],
    context: str = "",
) -> pd.DataFrame:
    """
    Derive the 'ReadingDate' column from a raw DataFrame.

    Intended for suppliers where the timestamp column name varies across projects
    or data files (e.g. a supplier may use either 'Datum' or 'readingDate'). The function
    tries each name in ``candidate_columns`` in order and uses the first one found.

    If 'ReadingDate' is already present in the DataFrame, returns it unchanged.

    This is intentionally separate from the CSV-driven column mapping because the
    timestamp column may require more than a simple rename: the column name can vary,
    the format may need automatic detection, and future suppliers could split the
    timestamp across separate date and time columns.

    Processing steps
    ----------------
    1. If 'ReadingDate' already in df: return unchanged.
    2. Try each name in candidate_columns; use the first found.
    3. Log the column dtype and a sample of raw values (DEBUG) so the format is
       visible in the log without running the full pipeline.
    4. Parse with pd.to_datetime(errors='coerce', utc=True). pandas handles ISO 8601,
       common European formats, and Unix integers automatically. Add an explicit
       format= argument in the caller if a specific project file requires it.
    5. Warn and drop rows where parsing failed (NaT), logging sample bad values.
    6. Drop the source column (avoid confusion with 'ReadingDate').
    7. Return df with 'ReadingDate' column (datetime64 with UTC timezone info).

    Parameters
    ----------
    df : pd.DataFrame
        Raw supplier DataFrame with original column names.
    candidate_columns : list[str]
        Ordered list of column names to try. The first one found in df is used.
        Keep this list in the supplier-specific script as a module-level constant
        so it is easy to extend when a new project variant appears.
    context : str
        Optional log prefix, e.g. 'project_name/data_file'.

    Returns
    -------
    pd.DataFrame
        DataFrame with 'ReadingDate' column (datetime64, UTC) and the source
        timestamp column removed.

    Raises
    ------
    ValueError
        If no candidate column is found in df. The error message lists all
        available columns so the correct name can be identified and added to
        candidate_columns.
    ValueError
        If all rows have unparseable timestamps (nothing left after dropping NaT rows).
    """
    ctx = f"{context}: " if context else ""

    if "ReadingDate" in df.columns:
        logging.debug(
            f"[derive_and_normalize_reading_date] {ctx}'ReadingDate' already present — skipping."
        )
        return df

    source_col = None
    for candidate in candidate_columns:
        if candidate in df.columns:
            source_col = candidate
            break

    if source_col is None:
        raise ValueError(
            f"[derive_and_normalize_reading_date] {ctx}No timestamp column found. "
            f"Checked: {candidate_columns}. "
            f"Available columns: {list(df.columns)}. "
            f"Identify the correct column and add its name to the candidate_columns list."
        )

    n_notna = int(df[source_col].notna().sum())
    sample = df[source_col].dropna().head(3).tolist()
    logging.info(
        f"[derive_and_normalize_reading_date] {ctx}Deriving 'ReadingDate' from '{source_col}' "
        f"(dtype={df[source_col].dtype}, {n_notna} non-null rows). "
        f"Sample values: {sample}"
    )

    df = df.copy()
    df["ReadingDate"] = pd.to_datetime(df[source_col], errors="coerce", utc=True)

    n_failed = int(df["ReadingDate"].isna().sum())
    if n_failed > 0:
        bad_samples = (
            df.loc[df["ReadingDate"].isna() & df[source_col].notna(), source_col]
            .head(5)
            .tolist()
        )
        logging.warning(
            f"[derive_and_normalize_reading_date] {ctx}{n_failed} value(s) in '{source_col}' "
            f"could not be parsed as datetime and will be dropped. "
            f"Unparseable samples: {bad_samples}. "
            f"If this is a format issue, add an explicit format= argument to "
            f"the pd.to_datetime() call in the supplier's load_and_map_parquet()."
        )
        n_before = len(df)
        df = df.dropna(subset=["ReadingDate"])
        logging.warning(
            f"[derive_and_normalize_reading_date] {ctx}Dropped {n_before - len(df)} row(s) "
            f"with unparseable timestamps."
        )

    if df.empty:
        raise ValueError(
            f"[derive_and_normalize_reading_date] {ctx}All rows were dropped after "
            f"timestamp parsing — no valid ReadingDate values in '{source_col}'."
        )

    logging.debug(
        f"[derive_and_normalize_reading_date] {ctx}'{source_col}' -> 'ReadingDate' "
        f"(dtype={df['ReadingDate'].dtype}). "
        f"Range: {df['ReadingDate'].min()} to {df['ReadingDate'].max()}"
    )

    df = df.drop(columns=[source_col])
    return df


def normalize_to_utc_for_storage(
    df: pd.DataFrame,
    datetime_col: str = "ReadingDate",
    from_offset_hours: float = 1.0,
    context: str = "",
) -> pd.DataFrame:
    """
    Convert a datetime column from local time to UTC for storage.

    Use this when ReadingDate is stored in local time (e.g. CET = UTC+1) and
    must be written to parquet as UTC. The default offset of 1.0 hour covers CET
    (Europe/Amsterdam in winter / standard time).

    For data sources that already parse timestamps as UTC (e.g. using
    pd.to_datetime(..., utc=True) as in derive_and_normalize_reading_date()),
    do NOT call this function — the data is already in UTC.

    Some suppliers apply this correction as a `- 1 hour` subtraction
    before saving. New sources that receive data in local time should use this function
    rather than hardcoding the subtraction inline.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the datetime column to normalize.
    datetime_col : str
        Name of the datetime column. Default 'ReadingDate'.
    from_offset_hours : float
        Hours to subtract from the local timestamp to get UTC.
        Default 1.0 (CET = UTC+1 in winter). Use 2.0 for CEST (summer time) if
        the data is not yet DST-corrected.
    context : str
        Optional log prefix for debugging.

    Returns
    -------
    pd.DataFrame
        A copy of df with the datetime column shifted to UTC.
    """
    ctx = f"{context}: " if context else ""

    if datetime_col not in df.columns:
        raise KeyError(
            f"[normalize_to_utc_for_storage] {ctx}Column '{datetime_col}' not found in DataFrame."
        )

    df = df.copy()
    df[datetime_col] = df[datetime_col] - pd.Timedelta(hours=from_offset_hours)
    logging.debug(
        f"[normalize_to_utc_for_storage] {ctx}'{datetime_col}' shifted by "
        f"-{from_offset_hours}h to UTC."
    )
    return df
