import logging
from typing import Callable

import pandas as pd
from pandas import DataFrame, Series

from etdmap.data_model import (
    cadence_minutes_by_column,
    cumulative_columns,
    dataset_cadence_minutes,
    interval_column,
    load_thresholds,
    resolve_bounds,
    thresholds_dict,
)
from etdmap.flags import MappingFlag, bound_flags

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

Every validator takes a DataFrame and returns a Series, so one loop can call
them all. A bound written per interval has to be scaled to the interval the
column reports in before it means anything, and *cadence* is where that comes
from -- so a validator that scales a bound declares a ``cadence`` parameter and
one that does not, does not. The caller offers the context to all of them and
:func:`etdmap.index_helpers.supported_kwargs` passes each only what it declares,
which leaves every signature a record of what that check depends on.
"""


def _bounds_for(col: str, cadence) -> tuple:
    """
    The bounds to judge *col* by, scaled to the interval it reports in.

    Scales through the same :func:`etdmap.data_model.resolve_bounds` the mapping
    stage uses, so this check and the threshold pass agree on which values are
    out of range.

    Parameters
    ----------
    col : str
        The column to resolve bounds for.
    cadence : pd.DataFrame
        Per-column cadence for this household batch, from
        :func:`etdmap.mapping_helpers.detect_column_cadence`: one row per
        column, ``cadence_minutes`` holding the interval its readings cover. A
        column absent from it falls back to the interval the household batch itself is
        recorded on.
    """
    by_column = cadence_minutes_by_column(cadence)
    return resolve_bounds(
        thresholds_dict[col],
        by_column.get(col, dataset_cadence_minutes(cadence)),
        col,
    )


def validate_thresholds_combined(df: pd.DataFrame, cadence=None) -> pd.Series:
    """
    Per row, determine if at least one value fall outside of the Thresholds.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with columns to be checked.
    cadence : pd.DataFrame, optional
        Per-column cadence for this household, from
        :func:`etdmap.mapping_helpers.detect_column_cadence`. Required for any
        column whose bound is written per interval.

    Returns
    -------
    pd.Series
        Booleans, True when at least one value is outside bounds.
    """
    columns = [col for col in df.columns if col in thresholds_dict]

    # A column with no values has nothing to judge, so it needs no bound and no
    # cadence. It would contribute an all-valid mask anyway.
    checkable = [col for col in columns if df[col].notna().any()]

    # A missing value is not a violation, so it leaves the row valid here.
    valid_masks = pd.DataFrame(
        {
            col: bound_flags(df[col], *_bounds_for(col, cadence))
            == MappingFlag.NONE.value
            for col in checkable
        },
        index=df.index,
    )
    valid_combined = valid_masks.all(axis=1)

    # All relevant columns have NA values (set to pd.NA)
    all_na_rows = df[columns].isna().all(axis=1)
    valid_combined[all_na_rows] = pd.NA

    # Replace any NaN with pd.NA explicitly - Need to check if no edge cases are missed - will delete this later
    # valid_combined = valid_combined.where(pd.notna(valid_combined), pd.NA)

    return valid_combined


def condition_func_threshold(
    col: str, cadence=None
) -> Callable[[pd.DataFrame], bool]:
    """
    Define condition function for column based on min, max from thresholdscsv.

    The bounds are scaled to the interval *col* reports in, through the same
    :func:`etdmap.data_model.scale_bounds` the mapping stage uses, so this check
    and the threshold pass agree on which values are out of range.

    Parameters
    ----------
    col : str
        Column name for which the function holds.
    cadence : pd.DataFrame, optional
        Per-column cadence for this household, from
        :func:`etdmap.mapping_helpers.detect_column_cadence`. Required for any
        column whose bound is written per interval.

    Returns
    -------
    function or None
        Condition function for the specified column, or None if the column is not in the thresholds dictionary.
    """
    if (col in thresholds_dict):
        def condition_func(df: pd.DataFrame) -> bool:
            present = df[col].notna()
            # Resolved here, not when this closure was built: a column with no
            # values has nothing to judge, so it needs no bound and no cadence.
            if not present.any():
                return pd.Series(pd.NA, dtype='boolean', index=df.index)

            flags = bound_flags(df[col], *_bounds_for(col, cadence))
            # A missing value gets a missing result: nothing was checked.
            return (flags == MappingFlag.NONE.value).where(present, pd.NA)
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
        logging.debug(
            f"[record_validators] Cannot validate {columns}: column(s) not in DataFrame -- returning NA."
        )
        return pd.Series(pd.NA, dtype='boolean', index=df.index)


def validate_reading_date_unique(df: DataFrame) -> Series:
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
    return (~df.duplicated(subset=['ReadingDate'])).astype('boolean')


def validate_observed_spacing(df: DataFrame) -> Series:
    """
    Validate that each row sits one grid interval after the one before it.

    The interval checked against is the most common spacing in *df*, measured
    rather than assumed: a household batch is recorded on the grid it
    declares, so a fixed interval would mark every row of a coarser household batch
    invalid.

    Parameters
    ----------
    df : DataFrame
        The DataFrame to validate.

    Returns
    -------
    Series
        A boolean Series indicating which rows sit one grid interval after the
        previous row.
    """
    df = df.sort_values('ReadingDate')
    df['ReadingDateDiff'] = df['ReadingDate'].diff().dt.total_seconds().abs()
    column = ['ReadingDateDiff']

    spacing = df['ReadingDateDiff'].mode()
    expected = spacing.iloc[0] if not spacing.empty else pd.NA

    def condition_func(df):
        return df['ReadingDateDiff'] == expected

    result = validate_columns(df, column, condition_func)
    df.drop(columns=['ReadingDateDiff'], inplace=True)
    return result


def validate_elektriciteitgebruik(df: DataFrame) -> Series:
    """
    Validate that ElektriciteitsgebruikTotaalHuishoudelijk is less than or equal to the sum of ZonopwekTotaal,
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
        'ElektriciteitsgebruikTotaalHuishoudelijk',
        'ZonopwekTotaal',
        'ElektriciteitNetgebruikHoog',
        'ElektriciteitNetgebruikLaag',
    ]

    def condition_func(df: pd.DataFrame) -> bool:
        return (
            df['ElektriciteitsgebruikTotaalHuishoudelijk']
            <= df['ZonopwekTotaal']
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
        columns_with_value_bounds: list,
        record_flag_conditions: dict
        ) -> None:
    """
    Create validation functions for columns whose declared bound applies to a
    single reading, and add them to the record_flag_conditions dictionary.

    Parameters
    ----------
    columns_with_value_bounds : list
        Column names whose Min/Max can be checked against one reading.
    record_flag_conditions : dict
        Dictionary to store the validation functions.

    Returns
    -------
    None
    """
    for col in columns_with_value_bounds:
        register_validate_momentaan(col, record_flag_conditions)


def register_validate_momentaan(col: str, record_flag_conditions: dict) -> None:
    """
    Add the single-reading bound check for one column.

    The per-column form of :func:`create_validate_momentaan`, so a column
    declared after import gets the same check the loop builds.

    Parameters
    ----------
    col : str
        Column whose Min/Max can be checked against one reading.
    record_flag_conditions : dict
        Dictionary to store the validation function.
    """
    def validate_momentaan(df: DataFrame, cadence=None, col=col) -> Series:
        """Whether each reading of *col* is inside its declared bounds.

        *cadence* is the per-column table from
        :func:`etdmap.mapping_helpers.detect_column_cadence`; the bounds are
        scaled to the interval *col* reports in, so a column reporting
        hourly is judged against an hour's worth rather than a slot's.
        """
        condition_func = condition_func_threshold(col, cadence)
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
    # loop to produce cumulative outlier validators per cumulative column.
    for col in cumulative_columns:
        register_validate_cumulative(col, record_flag_conditions)


def register_validate_cumulative(col: str, record_flag_conditions: dict) -> None:
    """
    Add the outlier check for one cumulative column.

    The per-column form of :func:`create_validate_cumulative`, so a column
    declared after import gets the same check the loop builds. The column
    checked is the interval column the data model links to the cumulative
    variable, which is not always its name plus "Diff".

    Parameters
    ----------
    col : str
        The cumulative variable.
    record_flag_conditions : dict
        Dictionary to store the validation function.

    Raises
    ------
    ValueError
        If *col* declares no ComplementVariabele, so its interval column is
        unknown.
    """
    interval_col = interval_column(col)
    if interval_col is None:
        raise ValueError(
            f"etdmodel.csv: '{col}' is a cumulative variable with no "
            f"ComplementVariabele, so its interval column is unknown and no "
            f"outlier check can be built for it. Declare the link in the "
            f"data model."
        )

    logging.info(f"Creating {interval_col} outliers check")
    # cadence is accepted and ignored: an outlier here is judged against the
    # column's own distribution, which is already in its reporting interval.
    def validate_cumulative_outliers(
        df: DataFrame, diff_col=interval_col
    ) -> Series:
        logging.debug(f"[record_validators] Checking {diff_col} outliers")
        try:
            result = validate_columns(df, [diff_col], validate_not_outliers)
            return result
        except Exception as e:
            logging.error(f"Error in {diff_col} outliers check")
            raise Exception(f"Error in {diff_col} outliers check: {e!s}")  # noqa: B904

    record_flag_conditions['validate_' + interval_col + '_outliers'] = (
        validate_cumulative_outliers
    )


thresholds_df = load_thresholds()

# Category 1: hand-written record quality checks
record_quality_flag_conditions = {
    'validate_reading_date_unique': validate_reading_date_unique,
    'validate_observed_spacing': validate_observed_spacing,
    #'validate_zonopwek_totaal_tegen_gebruik': validate_zonopwek_totaal_tegen_gebruik,
    'validate_elektriciteitgebruik': validate_elektriciteitgebruik,
    'validate_warmteproductie': validate_warmteproductie,
    'validate_thresholds_combined': validate_thresholds_combined,
}

# Category 2: per-column threshold checks, for every type whose bound can be
# judged against a single reading. 'cumulatief' is excluded because a meter
# total is checked by category 3 instead. 'algemeen' is a dimensionless bound,
# such as a percentage, which holds at any cadence. '5-minute' is written per
# interval, so its bound is scaled to the column's cadence before the check --
# see _bounds_for.
SINGLE_READING_THRESHOLD_TYPES = ['5-minute', 'momentaan', 'algemeen']

momentaan_flag_conditions: dict = {}
columns_with_value_bounds = thresholds_df[
    thresholds_df['ThresholdType'].isin(SINGLE_READING_THRESHOLD_TYPES)
]['Variabele']
create_validate_momentaan(columns_with_value_bounds, momentaan_flag_conditions)

# Category 3: cumulative diff outlier checks (negative / statistical)
cumulative_diff_flag_conditions: dict = {}
create_validate_cumulative(cumulative_columns, cumulative_diff_flag_conditions)

# Combined dict (backwards-compatible export used by run_standard_pipeline)
record_flag_conditions = {
    **record_quality_flag_conditions,
    **momentaan_flag_conditions,
    **cumulative_diff_flag_conditions,
}


def rebuild_flag_conditions(thresholds_frame=None) -> None:
    """
    Rebuild the per-column checks in place from the model as it now stands.

    Called by :func:`etdmap.data_model.rebuild_model`, which owns when the
    model changes. The categories are rebuilt rather than added to, so a column
    the model no longer declares leaves no check behind.

    ``record_flag_conditions`` is a merge of the three categories taken once at
    import, not a view of them, and it is the dict the mapping pipeline
    iterates -- so it is rebuilt here too, or a column loaded after import
    would have a check nothing reads.

    Parameters
    ----------
    thresholds_frame : pd.DataFrame, optional
        The thresholds the checks are built against. Defaults to the shipped
        table.
    """
    frame = load_thresholds() if thresholds_frame is None else thresholds_frame

    momentaan_flag_conditions.clear()
    create_validate_momentaan(
        frame[frame['ThresholdType'].isin(SINGLE_READING_THRESHOLD_TYPES)][
            'Variabele'
        ],
        momentaan_flag_conditions,
    )

    cumulative_diff_flag_conditions.clear()
    create_validate_cumulative(
        cumulative_columns, cumulative_diff_flag_conditions
    )

    record_flag_conditions.clear()
    record_flag_conditions.update(record_quality_flag_conditions)
    record_flag_conditions.update(momentaan_flag_conditions)
    record_flag_conditions.update(cumulative_diff_flag_conditions)
