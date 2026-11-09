import logging

import pandas as pd
from pandas import DataFrame

from etdmap.data_model import (
    cumulative_columns,
    data_analysis_columns,
    interval_column,
    load_thresholds,
    thresholds_dict,
)

"""
Functions and code to generate a dict `dataset_flag_conditions`
which contains validator functions that check comulative colums
over a timeperiod. It checks for example:
- if the annual increase is above the min/below the max value in
    the thresholds file.
- if not too much data is missing
- if columns exist
- if the diff of cumulative columns is never negative.
"""

def validate_columns(df: DataFrame, columns: list, condition_func) -> bool:
    """
    Validate the given DataFrame's *columns* against *condition_func*.

    Generic over whatever frame it is handed: the caller decides what the rows
    are.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame to be validated.
    columns : list
        A list of column names to be checked for validity.
    condition_func : callable
        A function that takes a DataFrame as an argument and returns a boolean Series indicating which rows meet the validation criteria.

    Returns
    -------
    bool or pd.NA
        True if all valid rows meet the specified condition, pd.NA if no valid rows are found or if any of the specified columns do not exist in the DataFrame.

    Notes
    -----
    This function checks if all specified columns exist in the DataFrame, applies the condition function to valid rows, and returns the overall validation result.
    """
    if all(col in df.columns for col in columns):
        valid_mask = df[columns].notna().all(axis=1)
        if valid_mask.any():
            condition = pd.Series(pd.NA, dtype="boolean", index=df.index)
            condition[valid_mask] = condition_func(df[valid_mask])
            # note need typecast to bool because returns np.True_/np.False_ otherwise
            return bool(condition.all(skipna=True))
        else:
            return pd.NA
    else:
        return pd.NA

def validate_monitoring_data_counts(df: DataFrame, grid_minutes=None) -> bool:
    """
    Validate the number of records in a DataFrame against its grid.

    How many rows a complete year holds depends on the interval the household is
    recorded on: 105,120 at five minutes, 35,040 at fifteen, 8,760 hourly. A
    fixed row count is therefore a statement about one grid only, and marks
    every household on any other grid invalid.

    The expected count is derived from the grid and the span the rows cover,
    with the same tolerance ``validate_approximately_one_year_of_records`` uses.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame to be validated.
    grid_minutes : float, optional
        The interval the household is recorded on, in minutes. Declared per
        household batch as ``Gegevensfrequentie``.

    Returns
    -------
    bool or pd.NA
        True if the number of records matches the grid over roughly a year.
        pd.NA if the DataFrame is empty, if it carries no ReadingDate, or if
        the grid is unknown -- in which case nothing was checked.
    """
    if df.empty or "ReadingDate" not in df.columns:
        return pd.NA

    if grid_minutes is None or pd.isna(grid_minutes):
        logging.warning(
            "[dataset_validators] validate_monitoring_data_counts: no grid was "
            "given, so the expected row count is unknown and nothing was "
            "checked. The grid is declared per household batch as "
            "Gegevensfrequentie."
        )
        return pd.NA

    rows_per_day = (24 * 60) / float(grid_minutes)
    expected_min = (365 - year_allowed_jitter) * rows_per_day
    expected_max = (365 + year_allowed_jitter) * rows_per_day
    return bool(expected_min <= len(df) <= expected_max)


def validate_cumulative_variable(df: DataFrame, column: str) -> bool:
    """
    Validate that a cumulative variable in a DataFrame is non-decreasing.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame containing the data to be validated.
    column : str
        The name of the column to be validated.

    Returns
    -------
    bool or pd.NA
        True if the cumulative variable is non-decreasing, pd.NA if validation cannot be performed.

    Notes
    -----
    This function checks if the differences between consecutive values in the specified column are non-negative.
    """
    columns = [column]

    def condition_func(df):
        return df[column].diff().dropna() >= 0

    return validate_columns(df, columns, condition_func)


def validate_range(
        df: DataFrame,
    column: str,
    min_value: float,
    max_value: float,
    ) -> bool:
    """
    Validate that a column in a DataFrame falls within a specified range over approximately one year.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame containing the data to be validated.
    column : str
        The name of the column to be validated.
    min_value : float
        The minimum acceptable value for the yearly difference.
    max_value : float
        The maximum acceptable value for the yearly difference.

    Returns
    -------
    bool or pd.NA
        True if the column values fall within the specified range over approximately one year, pd.NA if validation cannot be performed.

    Notes
    -----
    This function checks if the difference between the first and last non-null values in the specified column,
    over a period of approximately one year, falls within the given range.
    """
    if column in df.columns:
        enough_days = 365 - year_allowed_jitter
        df_sorted = df.sort_values("ReadingDate")
        df_sorted = df_sorted[df_sorted[column].notna()]
        if df_sorted.empty:
            return pd.NA
        date_diff = (
            df_sorted["ReadingDate"].max() - df_sorted["ReadingDate"].min()
        ).days
        yearly_diff = df_sorted[column].iloc[-1] - df_sorted[column].iloc[0]
        return (
            pd.NA
            if pd.isna(date_diff) or pd.isna(yearly_diff)
            else (date_diff >= enough_days) and (min_value <= yearly_diff <= max_value)
        )
    else:
        return pd.NA


def validate_approximately_one_year_of_records(df: DataFrame) -> bool:
    """
    Validate that a DataFrame contains approximately one year of records.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame containing the data to be validated.

    Returns
    -------
    bool or pd.NA
        True if the DataFrame contains approximately one year of records, pd.NA if validation cannot be performed.

    Notes
    -----
    This function checks if the difference between the maximum and minimum dates in the 'ReadingDate' column
    falls within a range of approximately one year, allowing for a specified jitter.
    """
    if "ReadingDate" in df.columns:
        date_diff = (df["ReadingDate"].max() - df["ReadingDate"].min()).days
        return (365 - year_allowed_jitter) <= date_diff <= (365 + year_allowed_jitter)
    else:
        return pd.NA


def validate_column_exists(df: DataFrame, column_name: str) -> bool:
    """
    Validate that a specific column exists in a DataFrame.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame to be checked.
    column_name : str
        The name of the column to check for existence.

    Returns
    -------
    bool
        True if the column exists in the DataFrame, False otherwise.

    Notes
    -----
    This function simply checks if the specified column name is present in the DataFrame's columns.
    """
    return column_name in df.columns


def validate_columns_exist(df: DataFrame) -> bool:
    """
    Validate that all required columns for data analysis exist in a DataFrame.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame to be checked.

    Returns
    -------
    bool
        True if all required columns exist in the DataFrame, False otherwise.

    Notes
    -----
    This function checks if all columns specified in the 'data_analysis_columns' list are present in the DataFrame.
    """
    return all(
        validate_column_exists(df, col) for col in data_analysis_columns
            )


def validate_energiegebruik_warmteopwekker(df: DataFrame) -> bool:
    """
    Validate the energy usage of the heat generator in a DataFrame.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame containing the data to be validated.

    Returns
    -------
    bool or pd.NA
        True if the calculated energy usage falls within the specified range, pd.NA if validation cannot be performed.

    Notes
    -----
    This function calculates the total energy usage of the heat generator by summing the electricity usage
    of the heat pump, booster, and boiler tank, and then validates if this total falls within a specified range.

    Returns pd.NA when any of the three columns is absent: the total cannot be
    formed, so there is nothing to judge. The sum is built on a copy, so the
    frame handed in keeps the columns it arrived with.
    """
    parts = [
        "ElektriciteitsgebruikWarmtepomp",
        "ElektriciteitsgebruikBooster",
        "ElektriciteitsgebruikBoilervat",
    ]
    missing = [col for col in parts if col not in df.columns]
    if missing:
        logging.debug(
            f"[dataset_validators] Cannot validate "
            f"EnergiegebruikWarmteopwekker: {missing} not in DataFrame -- "
            f"returning NA."
        )
        return pd.NA

    totals = df[["ReadingDate"]].copy() if "ReadingDate" in df.columns else df.copy()
    totals["EnergiegebruikWarmteopwekker"] = sum(df[col] for col in parts)
    return validate_range(totals, "EnergiegebruikWarmteopwekker", 100, 20000)


def validate_no_readingdate_gap(df: DataFrame) -> bool:
    """
    Validate that there are no gaps in the reading dates of a DataFrame.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame containing the data to be validated.

    Returns
    -------
    bool
        True if there are no gaps in the reading dates, False otherwise.

    Notes
    -----
    The spacing checked against is the most common spacing in *df*, not a fixed
    interval: a household batch is recorded on the grid it declares. A
    fixed interval reports every row of a coarser household batch as a gap.
    """
    time_diffs = df["ReadingDate"].diff().dt.total_seconds()[1:]
    if time_diffs.empty:
        return True

    spacing = time_diffs.mode()
    if spacing.empty:
        return False
    return bool((time_diffs == spacing.iloc[0]).all())


def create_validate_func_col(col:str, tresholds) -> callable:
    """
    Create a validation function for a specific column based on given thresholds.

    Parameters
    ----------
    col : str
        The name of the column to be validated.
    tresholds : dict
        A dictionary containing the threshold values for the specified column.

    Returns
    -------
    callable
        A function that validates the specified column in a DataFrame.

    Notes
    -----
    This function creates a validation function that checks if the specified column is cumulative
    and falls within the given thresholds.
    """
    low_thres = tresholds[col]['Min']
    high_thres = tresholds[col]['Max']

    def validate_func(df: DataFrame) -> bool:
        return validate_cumulative_variable(
            df,
            col,
        ) & validate_range(df, col, low_thres, high_thres)

    return validate_func


def create_validate_func_outliers_neg_cum(
        col:str) -> callable:
    """
    Create a validation function to check for outliers and negative cumulative differences in a specific column.

    Parameters
    ----------
    col : str
        The name of the column to be validated.

    Returns
    -------
    callable
        A function that validates the specified column for outliers and negative cumulative differences in a DataFrame.

    Notes
    -----
    This function creates a validation function that checks if all non-null values in the 'validate_<col>Diff' column are True.

    The flag column it reads is written by the record validators during mapping.
    A frame that does not carry it yields pd.NA: the per-record verdict this
    summarises was never recorded, so there is nothing to summarise.
    """

    interval_col = interval_column(col)
    if interval_col is None:
        raise ValueError(
            f"etdmodel.csv: '{col}' is a cumulative variable with no "
            f"ComplementVariabele, so its interval column is unknown and no "
            f"validator can be built for it. Declare the link in the data model."
        )

    def validate_no_outliers_negative_cumulative_diff(
        df: DataFrame,
        flag_col="validate_" + interval_col,
    ) -> bool:
        if flag_col not in df.columns:
            logging.debug(
                f"[dataset_validators] Cannot validate {flag_col}: not in "
                f"DataFrame -- returning NA."
            )
            return pd.NA
        return all(
            df[flag_col].isna() | df[flag_col],
        )
    return validate_no_outliers_negative_cumulative_diff

year_allowed_jitter = 18  # approx 5% of the year can be missing
thresholds_df = load_thresholds()

cumulative_columns_thresholds = thresholds_df[
    thresholds_df['ThresholdType']=='cumulatief']

# print(cumulative_columns_threholds.columns)

# dictionary with validators.
# Each key/value pair defines the new column name with
# the corresponding validator function.
# Additional key/value pairs are added in the loop.
dataset_flag_conditions = {
    "validate_monitoring_data_counts": validate_monitoring_data_counts,
    "validate_energiegebruik_warmteopwekker": validate_energiegebruik_warmteopwekker,  # E501
    "validate_approximately_one_year_of_records": validate_approximately_one_year_of_records,  # E501
    "validate_columns_exist": validate_columns_exist,
    "validate_no_readingdate_gap": validate_no_readingdate_gap,
}

column_diff = set(cumulative_columns_thresholds['Variabele'].values) \
        - set(cumulative_columns)

if column_diff:
    logging.info(
        f'More comulative_columns found in thesholds.csv '
        f'then used in validation. For validation only the '
        f'columns from `data_model.cumulative_columns` are used. '
        f'Missing: {list(column_diff)}'
        )

for col in cumulative_columns:
    if col not in thresholds_dict:
        logging.warning(
            f"Column name: {col} found in data_model.cumulative_columns "
            f"that is not present in the `thresholds.csv`."
            )

    # These validators will be added only to the index.parquet file
    # because they validate the complete file, not each column/row
    dataset_flag_conditions["validate_" + col] = \
        create_validate_func_col(col, thresholds_dict)

    # The household parquet files also carry a "validate_<interval column>"
    # flag, written by the record validators from that column and its threshold
    # row. The interval column is the one the data model links to this
    # cumulative variable, which is not always its name plus "Diff".
    dataset_flag_conditions["validate_" + interval_column(col)] = (
        create_validate_func_outliers_neg_cum(col)
    )


#: The checks that hold whatever the model declares, as opposed to the
#: per-column ones a rebuild replaces.
_FIXED_FLAG_CONDITIONS = dict(dataset_flag_conditions)


def rebuild_flag_conditions() -> None:
    """
    Rebuild the per-column ``dataset_flag_conditions`` in place from the model
    as it stands.

    Called by :func:`etdmap.data_model.rebuild_model`, which owns when the
    model changes. The per-column entries are rebuilt rather than added to, so
    a column the model no longer declares leaves no check behind; the
    whole-file checks are kept as they are.
    """
    dataset_flag_conditions.clear()
    dataset_flag_conditions.update(_FIXED_FLAG_CONDITIONS)

    for col in cumulative_columns:
        if col not in thresholds_dict:
            raise ValueError(
                f"'{col}' is a cumulative variable with no row in the "
                f"thresholds, so the bounds its whole-file check reads are "
                f"unknown. A cumulative variable needs a 'cumulatief' "
                f"threshold row of its own, and its interval column needs one "
                f"written per interval."
            )
        dataset_flag_conditions["validate_" + col] = create_validate_func_col(
            col, thresholds_dict
        )
        dataset_flag_conditions["validate_" + interval_column(col)] = (
            create_validate_func_outliers_neg_cum(col)
        )
