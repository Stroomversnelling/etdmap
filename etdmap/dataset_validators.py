import logging

import pandas as pd
from pandas import DataFrame

from etdmap.data_model import (
    cumulative_columns,
    data_analysis_columns,
    load_thresholds,
    load_thresholds_as_dict,
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
    Validate a dataset based on specified columns in a DataFrame based on a given condition function.

    Parameters:
    df (DataFrame): The input DataFrame to be validated.

    columns (list): A list of column names to be checked for validity.

    condition_func (function): A function that takes a DataFrame as an argument and returns a boolean Series indicating which rows meet the validation criteria.

    Returns:
    bool: True if all valid rows meet the specified condition, pd.NA if no valid rows are found or if any of the specified columns do not exist in the DataFrame.
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

def validate_cumm_thesholds(df: DataFrame, col: str, thresholds:dict) -> bool:
    min_tresh = thresholds[col]['Min']
    max_tresh = thresholds[col]['Max']
    def condition_func(df: pd.DataFrame) -> bool:
        return (df[col].diff().dropna() >= min_tresh) & (
            df[col].diff().dropna() <= max_tresh
        )
    return validate_columns(df, [col], condition_func)


def validate_monitoring_data_counts(df: DataFrame) -> bool:
    min_count, max_count = 100000, 110000
    return pd.NA if df.empty else min_count <= len(df) <= max_count


def validate_cumulative_variable(df: DataFrame, column: str) -> bool:
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
    if "ReadingDate" in df.columns:
        date_diff = (df["ReadingDate"].max() - df["ReadingDate"].min()).days
        return (365 - year_allowed_jitter) <= date_diff <= (365 + year_allowed_jitter)
    else:
        return pd.NA


def validate_column_exists(df: DataFrame, column_name: str) -> bool:
    return column_name in df.columns


def validate_columns_exist(df: DataFrame) -> bool:
    """Check if the minimum columns for
    doing datanalysis exit.
    """
    return all(
        validate_column_exists(df, col) for col in data_analysis_columns
        )


def validate_energiegebruik_warmteopwekker(df: DataFrame) -> bool:
    df["EnergiegebruikWarmteopwekker"] = (
        df["ElektriciteitsgebruikWarmtepomp"]
        + df["ElektriciteitsgebruikBooster"]
        + df["ElektriciteitsgebruikBoilervat"]
    )
    return validate_range(df, "EnergiegebruikWarmteopwekker", 100, 20000)


def validate_no_readingdate_gap(df: DataFrame) -> bool:
    time_diffs = df["ReadingDate"].diff().dt.total_seconds()
    valid = all(time_diffs[1:] == 300)
    return valid


def create_validate_func_col(col:str, tresholds) -> callable:
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

    def validate_no_outliers_negative_cumulative_diff(
        df: DataFrame,
        cum_col=col,
    ) -> bool:
        return all(
            df["validate_" + cum_col + "Diff"].isna()
            | df["validate_" + cum_col + "Diff"],
        )
    return validate_no_outliers_negative_cumulative_diff

year_allowed_jitter = 18  # approx 5% of the year can be missing
thresholds_df = load_thresholds()
thresholds_dict = load_thresholds_as_dict()

if "Variabele" not in thresholds_df.columns:
    raise KeyError(f"Column 'Variabele' is missing in thresholds data frame: {thresholds_df.columns}. Full df: {thresholds_df} ")

cumulative_columns_thresholds = thresholds_df[
    thresholds_df['VariabelType']=='cumulatief']

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


    dataset_flag_conditions["validate_" + col] = \
        create_validate_func_col(col, thresholds_dict)

    dataset_flag_conditions["validate_" + col + "Diff"] = (
        create_validate_func_outliers_neg_cum(col)
    )
