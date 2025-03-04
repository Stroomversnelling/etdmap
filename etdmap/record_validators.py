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

Using these functions, new boolean columns can be
created that can be used to get insight in the data. The columns are:
- 'validate_' + col + 'Diff_outliers' (statistical for cumulative columns)
- 'validate'+ col (thresholds for both cumulative and diff/delta columns)
- 'validate_combined' (checks for each row if any value
    is outside of thresholds)
"""

def validate_thresholds_combined(df: pd.DataFrame) -> pd.Series:
    """
    Per row, determine if at least one value fall outside of the Thresholds.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with columns to be checked.

    Returns
    -------
    pd.Series
        Booleans, True when at least one value is outside bounds.
    """
    columns = [col for col in df.columns if col in thresholds_dict]

    ge_masks = pd.DataFrame(
        {
            col: (df[col] >= thresholds_dict[col]['Min'])
            if pd.notna(thresholds_dict[col]['Min'])
            else pd.Series(True, index=df.index)
            for col in columns
        }
    )
    le_masks = pd.DataFrame(
        {
            col: df[col] <= thresholds_dict[col]['Max']
            if pd.notna(thresholds_dict[col]['Max'])
            else pd.Series(True, index=df.index)
            for col in columns
        }
    )

    valid_masks = (ge_masks & le_masks).fillna(True)
    valid_combined = valid_masks.all(axis=1)

    # All relevant columns have NA values (set to pd.NA)
    all_na_rows = df[columns].isna().all(axis=1)
    valid_combined[all_na_rows] = pd.NA

    # Replace any NaN with pd.NA explicitly - Need to check if no edge cases are missed - will delete this later
    # valid_combined = valid_combined.where(pd.notna(valid_combined), pd.NA)

    return valid_combined


def get_columns_threshold_validator(cols):
    """
    Get a validator function for thresholds of specified columns.

    Parameters
    ----------
    cols : list
        List of column names to validate.

    Returns
    -------
    function
        A function that validates thresholds for the specified columns.
    """
    return lambda df: validate_thresholds_combined(df[cols])


def condition_func_threshold(col: str) -> Callable[[pd.DataFrame], bool]:
    """
    Define condition function for column based on min, max from thresholdscsv.

    Parameters
    ----------
    col : str
        Column name for which the function holds.

    Returns
    -------
    function or None
        Condition function for the specified column, or None if the column is not in the thresholds dictionary.
    """
    if (col in thresholds_dict):
        def condition_func(df: pd.DataFrame) -> bool:
            return (
                    (df[col] >= thresholds_dict[col]['Min']) | pd.isna(thresholds_dict[col]['Min'])
                ) & (
                    (df[col] <= thresholds_dict[col]['Max']) | pd.isna(thresholds_dict[col]['Max'])
                )
        return condition_func
    else:
        logging.error(
            f"Cannot generate function: no column named {col} in the thresholds dictionary"
        )
        return None


def validate_columns(df: DataFrame, columns: list, condition_func) -> Series:
    """
    Helper function to validate columns with a given condition function.

    Parameters
    ----------
    df : DataFrame
        The DataFrame containing the columns to validate.
    columns : list
        List of column names to validate.
    condition_func : function
        The condition function to apply for validation.

    Returns
    -------
    Series
        A boolean Series indicating which rows meet the condition.
    """
    if all(col in df.columns for col in columns):
        valid_mask = df[columns].notna().all(axis=1)
        condition = pd.Series(pd.NA, dtype='boolean', index=df.index)
        condition[valid_mask] = condition_func(df.loc[valid_mask, columns])
        return condition
    else:
        logging.warning(
            f"Cannot validate columns {columns}: At least one is not available in the DataFrame."
        )
        return pd.Series(pd.NA, dtype='boolean', index=df.index)


def validate_reading_date_uniek(df: DataFrame) -> Series:
    """
    Validate that ReadingDate column has only unique values.

    Parameters
    ----------
    df : DataFrame
        The DataFrame to validate.

    Returns
    -------
    Series
        A boolean Series indicating which rows have unique ReadingDate values.
    """
    # df['ReadingDate'] should only have unique values
    return ~df.duplicated(subset=['ReadingDate'])


def validate_300sec(df: DataFrame) -> Series:
    """
    Validate that the time difference between consecutive ReadingDate values is 300 seconds.

    Parameters
    ----------
    df : DataFrame
        The DataFrame to validate.

    Returns
    -------
    Series
        A boolean Series indicating which rows have a 300-second difference from the previous row.
    """
    df = df.sort_values('ReadingDate')
    df['ReadingDateDiff'] = df['ReadingDate'].diff().dt.total_seconds().abs()
    column = ['ReadingDateDiff']

    def condition_func(df):
        return df['ReadingDateDiff'] == 300

    result = validate_columns(df, column, condition_func)
    df.drop(columns=['ReadingDateDiff'], inplace=True)
    return result


def validate_elektriciteitgebruik(df: DataFrame) -> Series:
    """
    Validate that ElektriciteitsgebruikHuishoudelijk is less than or equal to the sum of Zon-opwekTotaal,
    ElektriciteitNetgebruikHoog, and ElektriciteitNetgebruikLaag.

    Parameters
    ----------
    df : DataFrame
        The DataFrame to validate.

    Returns
    -------
    Series
        A boolean Series indicating which rows meet the condition.
    """
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

    return validate_columns(df, columns, condition_func)


def validate_warmteproductie(df: DataFrame) -> Series:
    """
    Validate that WarmteproductieWarmtepomp is greater than or equal to WarmteproductieWarmTapwater.

    Parameters
    ----------
    df : DataFrame
        The DataFrame to validate.

    Returns
    -------
    Series
        A boolean Series indicating which rows meet the condition.
    """
    columns = ['WarmteproductieWarmtepomp', 'WarmteproductieWarmTapwater']

    def condition_func(df: pd.DataFrame) -> bool:
        return df['WarmteproductieWarmtepomp'] >= df['WarmteproductieWarmTapwater']

    return validate_columns(df, columns, condition_func)


def validate_not_outliers(x: pd.DataFrame):
    """
    Validate that values in the DataFrame are not outliers.

    Parameters
    ----------
    x : pd.DataFrame
        A DataFrame with one column to check for outliers.

    Returns
    -------
    Series
        A boolean Series indicating which values are not outliers.

    Raises
    ------
    ValueError
        If the input is not a DataFrame with one column.
    TypeError
        If the column contains non-numeric values.
    """
    if not isinstance(x, pd.DataFrame) or len(x.columns) > 1:
        raise ValueError("Input must be a pandas DataFrame with one column.")

    # Convert DataFrame to Series
    x = x.iloc[:, 0]

    if not pd.api.types.is_numeric_dtype(x):
        unique_values = x.dropna().unique()

        non_numeric_values = unique_values[~unique_values.apply(pd.api.types.is_number)]

        raise TypeError(
            f"Trying to validate outliers for a non-numerical type in Series: {x.dtype}. "
            f"Non-numeric values found: {', '.join(map(str, non_numeric_values))}"
        )

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
    """
    Create validation functions for momentary columns and add them to the record_flag_conditions dictionary.

    Parameters
    ----------
    columns_5min_momentaan : list
        List of column names for momentary data.
    record_flag_conditions : dict
        Dictionary to store the validation functions.

    Returns
    -------
    None
    """
    for col in columns_5min_momentaan:
        def validate_momentaan(df: DataFrame, col=col) -> Series:
            # Checks thresholds
            condition_func = condition_func_threshold(col)
            result = validate_columns(df, [col], condition_func)
            return result
        record_flag_conditions['validate_' + col] = validate_momentaan


def create_validate_cumulative(
        cumulative_columns: list,
        record_flag_conditions: dict
        ) -> None:
    """
    Create validation functions for cumulative columns and add them to the record_flag_conditions dictionary.

    Parameters
    ----------
    cumulative_columns : list
        List of column names for cumulative data.
    record_flag_conditions : dict
        Dictionary to store the validation functions.

    Returns
    -------
    None
    """
    # loop to produce cumulative outlier validators per cumulative column
    for col in cumulative_columns:
        logging.info(f"Creating {col} outliers check")
        def validate_cumulative_outliers(df: DataFrame, cum_col=col) -> Series:
            logging.info(f"Checking {cum_col} outliers")
            try:
                result = validate_columns(df, [cum_col + 'Diff'], validate_not_outliers)
                return result
            except Exception as e:
                logging.error(f"Error in {cum_col} outliers check")
                raise Exception(f"Error in {cum_col} outliers check: {e!s}")  # noqa: B904

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
    ]['Variabele']

create_validate_momentaan(columns_5min_momentaan, record_flag_conditions)
create_validate_cumulative(cumulative_columns, record_flag_conditions)