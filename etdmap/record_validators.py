import logging
from typing import Callable

import pandas as pd
from pandas import DataFrame, Series

from etdmap.data_model import (
    cumulative_columns,
    load_thresholds,
    load_thresholds_as_dict,
)

"""
It contains functions & script that generates a dictionary
with the names of column and the corresponding validator functions.
They check for each record individually if the values are:
- are within the thresholds
- don't have statistical outliers

Using these functions, new (temp) columns per colname can be
created that can be used to get insight in the data. The columns are:
- 'validate_' + col + 'Diff_outliers' (statistical)
- 'validate'+ col (thresholds)
- 'validate_combined' (checks for each row if at least one value
    is outside of thresholds) -- so combines the validate + col columns
"""

def validate_thresholds_combined(df: pd.DataFrame) -> pd.Series:
    """Per row, determine if at least one value fall outside of the Thresholds.

    Args:
        df (pd.DataFrame): Dataframe with columns to be checked.

    Returns:
        pd.Series: Booleans, True when at least one value is outside bounds.
    """
    columns = [col for col in df.columns if col in thresholds_dict]

    ge_masks = pd.DataFrame(
        {col: df[col] >= thresholds_dict[col]['Min'] for col in columns},
    )
    le_masks = pd.DataFrame(
        {col: df[col] <= thresholds_dict[col]['Max'] for col in columns},
    )

    valid_masks = ge_masks & le_masks
    valid_combined = valid_masks.any(axis=1)

    # All relevant columns have NA values (set to pd.NA)
    all_na_rows = df[columns].isna().all(axis=1)
    valid_combined[all_na_rows] = pd.NA

    # Replace any NaN with pd.NA explicitly
    valid_combined = valid_combined.where(pd.notna(valid_combined), pd.NA)

    return valid_combined


def get_columns_threshold_validator(cols):
    return lambda df: validate_thresholds_combined(df[cols])


def condition_func_threshold(col: str) -> Callable[[pd.DataFrame], bool]:
    """Define condition function for column based on min, max from thresholdscsv.

    Args:
        col (str): column name for which the function holds.
    """
    if (col in pd.DataFrame.columns) and (col in thresholds_dict):
        min_treshold = thresholds_dict[col]['Min']
        max_treshold = thresholds_dict[col]['Max']
        # Note: This can be problematic if values can be higher/lower than 999999
        min_treshold_conv = min_treshold if min_treshold != 'n.a.' else -999999
        max_treshold_conv = max_treshold if max_treshold != 'n.a.' else 999999

        def condition_func(df: pd.DataFrame) -> bool:
            return (df[col] >= min_treshold_conv) & (
                df[col] <= max_treshold_conv
            )
        return condition_func
    else:
        logging.warning(
            f"Either no column named {col} in df, "
            f"or no valid threshold: {min_treshold, max_treshold}"
        )
        return None


def validate_column(df: DataFrame, columns: list, condition_func) -> Series:
    """
    Helper function to validate columns with a given condition function.
    """
    if all(col in df.columns for col in columns):
        valid_mask = df[columns].notna().all(axis=1)
        condition = pd.Series(pd.NA, dtype='boolean', index=df.index)
        condition[valid_mask] = condition_func(df[valid_mask])
        return condition
    else:
        logging.warning(
            f"trying to validate columns {columns}, but at least one is not in the DataFrame."
        )
        return pd.Series(pd.NA, dtype='boolean', index=df.index)


def validate_reading_date_uniek(df: DataFrame) -> Series:
    # df['ReadingDate'] should only have unique values
    return ~df.duplicated(subset=['ReadingDate'])


def validate_300sec(df: DataFrame) -> Series:
    df = df.sort_values('ReadingDate')
    df['ReadingDateDiff'] = df['ReadingDate'].diff().dt.total_seconds().abs()
    column = ['ReadingDateDiff']

    def condition_func(df):
        return df['ReadingDateDiff'] == 300

    result = validate_column(df, column, condition_func)
    df.drop(columns=['ReadingDateDiff'], inplace=True)
    return result


def validate_elektriciteitgebruik(df: DataFrame) -> Series:
    columns = [
        'ElektriciteitsgebruikHuishoudelijk',
        'Zon-opwekTotaal',
        'ElektriciteitNetgebruikHoog',
        'ElektriciteitNetgebruikLaag',
    ]

    def condition_func(df: pd.DataFrame) -> bool:
        return (
            df['ElektriciteitsgebruikHuishoudelijk']
            <= df['Zon-opwekTotaal']
            + df['ElektriciteitNetgebruikHoog']
            + df['ElektriciteitNetgebruikLaag']
        )

    return validate_column(df, columns, condition_func)


def validate_warmteproductie(df: DataFrame) -> Series:
    columns = ['WarmteproductieWarmtepomp', 'WarmteproductieWarmTapwater']

    def condition_func(df: pd.DataFrame) -> bool:
        return df['WarmteproductieWarmtepomp'] >= df['WarmteproductieWarmTapwater']

    return validate_column(df, columns, condition_func)


def validate_not_outliers(x: Series):
    x = x[x > 0]
    q1 = x.quantile(0.25)
    q3 = x.quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    return ~((x < lower_bound) | (x > upper_bound))


def create_validate_momentaan(
        columns_5min_momentaan: list,
        record_flag_conditions: dict
        ) -> None:
    for col in columns_5min_momentaan:
        def validate_momentaan(df: DataFrame, col=col) -> Series:
            # Checks thresholds
            condition_func = condition_func_threshold(col)
            result = validate_column(df, [col], condition_func)
            return result
        record_flag_conditions['validate_' + col] = validate_momentaan


def create_validate_comulative(
        cumulative_columns: list,
        record_flag_conditions: dict
        ) -> None:
    # loop to produce cumulative outlier validators per cumulative column
    for col in cumulative_columns:

        def validate_cumulative_outliers(df: DataFrame, cum_col=col) -> Series:
            result = validate_column(df, [cum_col + 'Diff'] , validate_not_outliers)
            return result

        record_flag_conditions['validate_' + col + 'Diff_outliers'] = validate_cumulative_outliers

thresholds_df = load_thresholds()
thresholds_dict = load_thresholds_as_dict()
# Combine all specific validators into the dictionary
record_flag_conditions = {
    'validate_reading_date_uniek': validate_reading_date_uniek,
    'validate_300sec': validate_300sec,
    #'validate_zonopwek_totaal_tegen_gebruik': validate_zonopwek_totaal_tegen_gebruik,
    'validate_elektriciteitgebruik': validate_elektriciteitgebruik,
    'validate_warmteproductie': validate_warmteproductie,
    'validate_thresholds_combined': validate_thresholds_combined,
}
columns_5min_momentaan = thresholds_df[
    thresholds_df['VariabelType'].isin(['5-minute', 'momentaan'])
    ]
create_validate_momentaan(columns_5min_momentaan, record_flag_conditions)
create_validate_comulative(cumulative_columns, record_flag_conditions)
