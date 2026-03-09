import logging
import os
from concurrent.futures import ProcessPoolExecutor

import pandas as pd

from etdmap.data_model import cumulative_columns, model_column_order, model_column_type, load_thresholds_as_dict
from etdmap.index_helpers import get_mapped_data, read_index


def rearrange_model_columns(
    household_df: pd.DataFrame,
    add_columns: bool = True,
    context: str = '',
) -> pd.DataFrame:
    """
    Rearrange and validate columns in a DataFrame according to a predefined model.

    This function performs the following operations:
    1. Validates and coerces column types to match expected types.
    2. Rearranges columns to match the order defined in model_column_order.
    3. Keeps original columns that are not included in the ETD data model at the end of the dataframe.
    4. Optionally adds missing columns with NA values.

    Parameters
    ----------
    household_df : pd.DataFrame
        The input DataFrame containing household data.
    add_columns : bool, optional
        If True, add missing columns from model_column_order to the DataFrame.
        If False, only keep columns that are in both the DataFrame and model_column_order.
        Default is True.
    context : str, optional
        A string to prepend to log messages for context. If provided, a colon and space
        will be appended to it. Default is an empty string.

    Returns
    -------
    pd.DataFrame
        A new DataFrame with rearranged and validated columns.

    Raises
    ------
    ValueError
        If type coercion fails for any column.

    Notes
    -----
    - The function uses the global variables model_column_type and model_column_order.
    - Columns not in model_column_order are appended at the end of the DataFrame.
    - When coercing types, any values that fail to convert are replaced with pd.NA.
    - Logging is used to warn about type mismatches and missing columns.

    Examples
    --------
    >>> df = pd.DataFrame({'A': [1, 2, 3], 'B': ['x', 'y', 'z']})
    >>> rearranged_df = rearrange_model_columns(df, add_columns=True, context='Example')
    """
    if context != '':
        context = context + ': '

    for col, expected_type in model_column_type.items():
        if col in household_df.columns:
            actual_type = household_df[col].dtype
            if actual_type != expected_type:
                logging.warning(
                    f"{context}Column '{col}' has type '{actual_type}' "
                    f"but expected type is '{expected_type}'. Coercing type."
                )

                # Coerce type and ensure any failures are pd.NA
                try:
                    if pd.api.types.is_numeric_dtype(expected_type):
                        household_df[col] = pd.to_numeric(household_df[col], errors='coerce').astype(expected_type)
                    elif expected_type == 'boolean':
                        household_df[col] = household_df[col].astype('boolean')
                    elif expected_type == 'string':
                        household_df[col] = household_df[col].astype('string')
                    elif expected_type == 'category':
                        household_df[col] = household_df[col].astype('category')
                    elif expected_type == 'datetime64[ns]':
                        household_df[col] = pd.to_datetime(household_df[col], errors='coerce')

                    household_df[col] = household_df[col].where(pd.notna(household_df[col]), pd.NA)

                except Exception as e:
                    logging.error(f"{context}Failed to coerce column '{col}' type: {e}")
                    raise ValueError(f"{context}Failed to coerce column '{col}' type: {e!s}")  # noqa: B904

    if add_columns:
        # Add all model columns and keep any additional columns from the original DataFrame
        household_df = household_df.reindex(
            columns=model_column_order
            + [col for col in household_df.columns if col not in model_column_order],
        )
        for col in model_column_order:
            if col not in household_df.columns:
                logging.warning(
                    f"{context}Missing column {col} added "
                    'and filled with NA values.',
                )
                household_df[col] = pd.Series(
                    pd.NA,
                    dtype=model_column_type[col],
                    index=household_df.index,
                )
    else:
        # Keep only columns that are in both model_column_order and the original DataFrame
        household_df = household_df[
            [col for col in model_column_order if col in household_df.columns]
            + [col for col in household_df.columns if col not in model_column_order]
        ]
    return household_df
    return household_df

# Check for any gaps greater than one hour
# Check if at least 90% of the values are not NA
def validate_cumulative_variables(
                group: pd.DataFrame,
                timedelta=pd.Timedelta(hours=1),
                available=0.9,
                context='',
            ) -> bool:
    """
    Validate cumulative variables in a DataFrame group.

    This function performs several checks on cumulative columns:
    1. Checks for gaps greater than the specified timedelta.
    2. Checks for decreasing cumulative values.
    3. Checks for unexpected zero values.
    4. Checks if at least 90% of the values are not NA.

    Parameters
    ----------
    group : pd.DataFrame
        The DataFrame group to validate.
    timedelta : pd.Timedelta, optional
        The maximum allowed time gap between readings, by default 1 hour.
    available : float, optional
        The minimum fraction of non-NA values required, by default 0.9 (90%).
    context : str, optional
        A string to prepend to log messages for context, by default ''.

    Returns
    -------
    dict
        A dictionary with boolean values indicating the results of various checks:
        - 'column_found': True if all expected columns are present.
        - 'max_delta_allowed': True if no gaps exceed the specified timedelta.
        - 'no_negative_diff': True if no decreasing cumulative values are found.
        - 'no_unexpected_zero': True if no unexpected zero values are found.
        - 'enough_values': True if at least 90% of values are non-NA.

    Notes
    -----
    - The function uses the global variable `cumulative_columns` to determine which columns to check.
    - Logging is used to warn about any issues found during validation.
    """

    if not context == '':
        context = context + ': '
    result = {
        'column_found': True,
        'max_delta_allowed': True,
        'no_negative_diff': True,
        'no_unexpected_zero': True,
        'enough_values': True,
    }
    for col in cumulative_columns:
        if col in group.columns:
            # Check for gaps greater than specified timedelta after first value
            filtered_group = group[['ReadingDate', col]]
            filtered_group = group[['ReadingDate', col]].dropna()
            filtered_group['ReadingDateDiff'] = filtered_group['ReadingDate'].diff()
            if (filtered_group['ReadingDateDiff'] > timedelta).any():
                max_delta = filtered_group['ReadingDateDiff'].max()
                max_gap_start = filtered_group.loc[
                    filtered_group['ReadingDateDiff'].idxmax(),
                    'ReadingDate',
                ]
                max_gap_start_unix = max_gap_start.timestamp()
                logging.warning(
                    f"{context}Group has a gap of {max_delta} > allowed "
                    f"({timedelta}) starting at {max_gap_start} "
                    f"({max_gap_start_unix}) in 'ReadingDate' for "
                    f"column '{col}'.",
                )
                result['max_delta_allowed'] = False

            # Find decreasing cumulative values in the column
            filtered_group['negative_diff'] = (
                round(filtered_group[[col]].diff(), 10) < 0
            )

            # Ensure only the first row can have NA (from diff()), but no other rows should
            if filtered_group['negative_diff'].isna().sum() > 1:
                raise ValueError(f"Unexpected NA values found in 'negative_diff' for column '{col}'")

            # Explicitly fill only the first row with False (since it always gets NA)
            if not filtered_group.empty:
                filtered_group.iloc[0, filtered_group.columns.get_loc('negative_diff')] = False

            if any(filtered_group['negative_diff']):
                reading_dates = filtered_group[filtered_group['negative_diff']][
                    'ReadingDate'
                ]
                logging.warning(
                    f"{context}Column {col} has a decrease in subsequent "
                    "cumulative values at 'ReadingDate': "
                    f"{reading_dates.to_list()}.",
                )
                result['no_negative_diff'] = False

                first_date = (reading_dates).min()
                filtered_group['zero'] = filtered_group[[col]] == 0
                if any(filtered_group['zero']):
                    reading_dates = filtered_group[
                        (filtered_group['ReadingDate'] >= first_date)
                        & (filtered_group['zero'])
                    ]['ReadingDate']
                    last_date = reading_dates.max()
                    logging.warning(
                        f"{context}Column {col} has unexpected zero values "
                        f"in cumulative values from {first_date} to "
                        f"{last_date}. This will be removed",
                    )
                    result['no_unexpected_zero'] = False

            if group[col].ffill().notna().sum() / len(group) < available:
                logging.warning(
                    f"{context}Column '{col}' in group has less than "
                    f"{available*100}% non-NA values.",
                )
                result['enough_values'] = False

        else:
            logging.error(f"{context}Column '{col}' not found in DataFrame.")
            result['column_found'] = False

    return result


def add_diff_columns(
    data: pd.DataFrame,
    id_column: str = None,
    validate_func=validate_cumulative_variables,
    context: str = '',
    drop_unvalidated: bool = False,
) -> pd.DataFrame:
    """
    Add difference columns for cumulative variables and handle some data inconsistencies.

    This function calculates the difference between consecutive readings for cumulative columns,
    validates the data, and handles various inconsistencies such as negative differences and unexpected zeros.

    Parameters
    ----------
    data : pd.DataFrame or pd.core.groupby.DataFrameGroupBy
        The input data, either as a DataFrame or a GroupBy object.
    id_column : str, optional
        The name of the column to use for grouping if data is a DataFrame, by default None.
    validate_func : callable, optional
        A function to validate the data, by default validate_cumulative_variables.
    context : str, optional
        A string to prepend to log messages for context, by default ''.
    drop_unvalidated : bool, optional
        If True, drop groups that fail validation; if False, keep them with warnings, by default False.

    Returns
    -------
    pd.DataFrame
        A DataFrame with added difference columns for cumulative variables.

    Raises
    ------
    TypeError
        If the input data is neither a DataFrame nor a GroupBy object.

    Notes
    -----
    - The function uses the global variable `cumulative_columns` to determine which columns to process.
    - It handles various data inconsistencies:
      - Removes unexpected zeros between valid readings.
      - Handles cases where the meter appears to have been reset.
      - Removes data after a negative difference if no subsequent increases are found.
    - Extensive logging is used to document the data cleaning process.
    - If the meter has had negative dip and after that there were no subsequent increases, we choose to ignore all other values from an apparently broken meter by setting them to pd.NA
    - If the meter has a negative dip and the meter simply jumps back up to the last value before the negative dip (or above) then we assume there is one bad value to remove. This cases does not consider time, so may miss edge cases, for example that it did not jump back up but rather so much time passed that the next reading is much higher - this may be addressed in the future but requires assumption about rate of growth.

    """

    if not context == '':
        context_string = context + ': '
    else:
        context_string = context

    data = data.sort_values('ReadingDate')

    def calculate_diff(group):
        valid_result = validate_func(group=group, context=context)
        if not all(valid_result.values()):
            invalid = [key for key, value in valid_result.items() if value is False]
            if drop_unvalidated:
                logging.error(
                    f"{context_string}Some cumulative columns did "
                    f"not pass validation ({invalid}). Dropping group/data.",
                )
                # Return empty DataFrame to drop invalid group
                return pd.DataFrame()
            else:
                logging.warning(
                    f"{context_string}Some cumulative columns did not "
                    f"pass validation ({invalid}). Keeping group/data.",
                )

        for col in cumulative_columns:
            if col not in group.columns:
                logging.warning(
                    f"{context_string}Cumulative column '{col}' not found. "
                    'No Diff column created.',
                )
                continue

            logging.info(f"Calculating diff for {col}")
            group[col + 'Diff'] = group[col].diff().round(10)
            group.loc[group.index[0], col + 'Diff'] = 0

            if not valid_result['no_negative_diff']:
                filtered_group = group[['ReadingDate', col]].dropna()
                filtered_group[col + 'Diff_no_gap'] = (
                    filtered_group[col].diff().round(10)
                )

                reading_dates = filtered_group[
                    filtered_group[col + 'Diff_no_gap'] < 0
                ]['ReadingDate']

                # recalculate = False

                for rd in reading_dates:
                    gap = filtered_group[(filtered_group['ReadingDate'] == rd)][
                        col + 'Diff_no_gap'
                    ].to_list()[0]
                    next_value_row = filtered_group[
                        (filtered_group['ReadingDate'] > rd)
                        & (filtered_group[col + 'Diff_no_gap'] != 0)
                    ].head(1)

                    # There is another meter reading after the negative dip
                    # This code block addresses different cases
                    if not next_value_row.empty:
                        # We want to know what the next meter reading value and the next date is
                        next_value = next_value_row[col + 'Diff_no_gap'].iloc[0]
                        next_value_date = next_value_row['ReadingDate'].iloc[0]

                        # If the meter simply jumps back up to the last value before the negative dip (or above) then we assume there is one bad value to remove
                        # This cases does not consider time, so may miss edge cases, for example that it did not jump back up but rather so much time passed that the next reading is much higher - this may be fixed in the future but requires assumption about rate of growth
                        if next_value >= -1 * gap:
                            logging.info(
                                f"{context_string}Removing unexpected "
                                f"zeros from '{col}' between {rd} and "
                                f"{next_value_date}",
                            )
                            group.loc[
                                (group['ReadingDate'] >= rd)
                                & (group['ReadingDate'] < next_value_date),
                                col,
                            ] = pd.NA
                            # recalculate = True

                        # After the negative dip, the meter dips down again (still broken)
                        elif next_value < 0:
                            logging.error(
                                f"{context_string}Two negative diffs "
                                f"one after the other between {rd} and "
                                f"{next_value_date}. Will remove all "
                                f"these values for {col}.",
                            )
                            group.loc[
                                (group['ReadingDate'] >= rd)
                                & (group['ReadingDate'] < next_value_date),
                                col,
                            ] = pd.NA
                            # recalculate = True

                        # The meter has values but they are non-negative and not larger than the negative dip
                        # we consider the meter to have been reset to the value it dipped to
                        # In this case we sacrifice one value because we cannot calculate a diff from it (it will be negative)

                        # It would be better to save all 'sacrificed' value reading dates in a list and then only mark the recalculated diff as <NA>
                        # It is only one value so leaving like this for now
                        else:

                            # In the case where we know the colDiff is NA, we actualy don't have to delete the original meter reading
                            # This happens when there is a pause/missing data before the negative dip so it does not impact our diff calculation
                            if (group.loc[
                                    group['ReadingDate'] == rd,
                                    col + 'Diff',
                                ].isna().all()):
                                logging.info(
                                        f"{context_string}Negative gap jump "
                                        f"at {rd}. Diff is NA, not "
                                        'removing any values.',
                                    )

                            # When there are negative diffs calculated we in fact do remove the original value from the column
                            # so that no negative diff may be calculated
                            else:
                                diff_belowzero = group.loc[
                                    (group['ReadingDate'] == rd) & (group[col + 'Diff'] < 0),
                                    col + 'Diff'
                                ]
                                if len(diff_belowzero) > 0:
                                    group.loc[
                                        group['ReadingDate'] == rd,
                                        col,] = pd.NA
                                    logging.info(
                                        f"{context_string}Negative gap jump "
                                        f"at {rd}. Removing single cumulative "
                                        'value.',
                                    )

                                # Handling where all values are
                                else:
                                    logging.error(
                                        f"{context_string}Negative gap jump "
                                        f"at {rd}. Diff is not negative, and "
                                        'not <NA>. Check for errors, e.g duplicate reading dates!'
                                    )
                    else:
                        # The meter has had negative dip and after that there were no subsequent increases so we choose to ignore all other values from an apparently broken meter
                        group.loc[(group['ReadingDate'] >= rd), col] = pd.NA
                        logging.error(
                            f"{context_string}Removing all values in "
                            f"'{col}' after date '{rd}' as there are "
                            f"no subsequent increases after the negative "
                            f"diff.",
                        )

                logging.info(
                    f"{context_string}Re-calculating diff for "
                    f"{col} after corrections.",
                )
                group[col + 'Diff'] = group[col].diff().round(10)
                group.loc[group.index[0], col + 'Diff'] = 0


                if (group[col + 'Diff'] < 0).any(skipna=True):
                    logging.error(
                        f"{context_string}Removed zeros but "
                        f"diff still has negative values! Check data and "
                        f"consider removing.",
                    )

        return group

    if isinstance(data, pd.core.groupby.DataFrameGroupBy):
        return data.apply(calculate_diff).reset_index(drop=True)
    elif isinstance(data, pd.DataFrame):
        if id_column is not None:
            return (
                data.groupby(id_column, group_keys=False)
                .apply(calculate_diff)
                .reset_index(drop=True)
            )
        else:
            return calculate_diff(data)
    else:
        raise TypeError(
            f"{context_string}Input data must be a pandas DataFrame "
            f"or a pandas GroupBy object.",
        )


def fill_down_infrequent_devices(
    df: pd.DataFrame,
    columns=(
        'ElektriciteitsgebruikBoilervat',
        'ElektriciteitsgebruikRadiator',
        'ElektriciteitsgebruikBooster',
    )
):
    """
    Fill down (forward fill) and then up (backward fill) values for specified columns.

    This function is used to impute missing values for devices that report infrequently.
    It first forward fills (ffill) the values, then backward fills (bfill) any remaining NAs,
    and finally replaces any remaining NAs with 0.0.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing the device data.
    columns : tuple of str, optional
        The names of the columns to fill. Default is
        ('ElektriciteitsgebruikBoilervat', 'ElektriciteitsgebruikRadiator', 'ElektriciteitsgebruikBooster').

    Returns
    -------
    pd.DataFrame
        The input DataFrame with the specified columns filled.

    Notes
    -----
    - This function may be problematic if the data source or devices are misbehaving,
      as the imputation will still be performed.
    - The imputation order is: forward fill, backward fill, then fill remaining NAs with 0.0.
    """

    for col in columns:
        if col in df.columns:
            df[col] = df[col].ffill().bfill().fillna(0.0)

    return df


def snap_readings_to_grid(
    df: pd.DataFrame,
    date_column: str = 'ReadingDate',
    freq_minutes: int = 5,
    max_deviation_seconds: float = None,
    context: str = '',
) -> pd.DataFrame:
    """
    Snap timestamps to the nearest freq-minute grid slot and merge rows
    that fall on the same slot.

    Intended for datasets where different sensor streams in the same table
    have different time offsets (e.g. columns A-C arrive at HH:MM:22 every
    5 minutes while columns D-F arrive at HH:MM:00 every 15 minutes).
    Both sets of rows snap to the same 5-minute slot and their column values
    are merged by taking the first non-NA value per column.

    Within a slot, when multiple rows are present, each column independently
    selects the row whose timestamp is closest to that column's characteristic
    offset (the most common signed offset observed for that column across all
    rows where it is non-null).  This is implemented as a K-pass vectorized
    merge where K is the number of distinct characteristic offsets (typically
    2-3 for O-Nexus data).

    Per-column characteristic offsets matter especially for cumulative columns.
    If a cumulative column (e.g. ElektriciteitNetgebruikHoogCum) always arrives
    at +22 s and we snapped it using a +0 s reference, adjacent slots would
    alternately draw from the +22 s stream and the +0 s stream.  The resulting
    diffs would alternate between ~4 min 38 s and ~5 min 22 s of physical
    consumption — a ±7.3% error per slot that self-cancels over longer windows
    but pollutes 5-minute diff analysis.  Using the per-column characteristic
    offset ensures each cumulative column always draws from its own stream
    across all slots, so diffs always span a consistent physical interval.

    This function does no fill-down or fill-up.  It only snaps and merges.
    Call ensure_intervals afterwards to pad any remaining missing grid slots.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.  The date_column must already be datetime dtype.
    date_column : str
        Name of the datetime column.  Default 'ReadingDate'.
    freq_minutes : int
        Grid slot size in minutes.  Default 5.
    max_deviation_seconds : float, optional
        Log a warning if any reading deviates from its nearest slot by more
        than this many seconds.  Default: half the slot size (150 s for 5-min).
    context : str
        Optional prefix for log messages, e.g. a household identifier.

    Returns
    -------
    pd.DataFrame
        One row per grid slot, columns merged from all input rows that
        mapped to that slot.  The date_column contains exact grid timestamps.
    """
    ctx = (context + ': ') if context else ''
    freq_seconds = freq_minutes * 60
    if max_deviation_seconds is None:
        max_deviation_seconds = freq_seconds / 2.0  # 150 s for 5-min grid

    df = df.copy()

    # ------------------------------------------------------------------
    # 1. Snap each timestamp to the nearest freq-minute grid slot
    # ------------------------------------------------------------------
    df['_SnappedSlot'] = df[date_column].dt.round(f'{freq_minutes}min')

    # ------------------------------------------------------------------
    # 2. Signed offset from the snapped slot (seconds, rounded to 1 s)
    # ------------------------------------------------------------------
    raw_offsets = (df[date_column] - df['_SnappedSlot']).dt.total_seconds()
    offset_rounded = raw_offsets.round(0)

    max_dev = raw_offsets.abs().max()
    logging.debug(
        f"{ctx}snap_readings_to_grid: max deviation from nearest "
        f"{freq_minutes}-min slot = {max_dev:.2f}s "
        f"(tolerance {max_deviation_seconds:.0f}s)"
    )
    if max_dev > max_deviation_seconds:
        logging.warning(
            f"{ctx}snap_readings_to_grid: some readings deviate by more than "
            f"{max_deviation_seconds:.0f}s from their nearest {freq_minutes}-min "
            f"slot (max={max_dev:.2f}s).  This may indicate irregular data."
        )

    value_cols = [
        c for c in df.columns
        if c not in [date_column, '_SnappedSlot']
    ]

    # ------------------------------------------------------------------
    # 3. Per-column characteristic offset
    #    For each value column, the most common signed offset (in seconds)
    #    among all rows where that column is non-null.  This identifies
    #    which sensor stream "owns" each column.
    # ------------------------------------------------------------------
    col_modal: dict = {}
    for col in value_cols:
        notna_mask = df[col].notna()
        if notna_mask.any():
            counts = offset_rounded[notna_mask].value_counts()
            col_modal[col] = float(counts.index[0])
        else:
            col_modal[col] = 0.0

    # Group columns that share the same characteristic offset
    cols_by_modal: dict = {}
    for col, modal in col_modal.items():
        cols_by_modal.setdefault(modal, []).append(col)

    # ------------------------------------------------------------------
    # 3b. Per-column offset diagnostics (DEBUG)
    #     Group summary: which columns share each characteristic offset.
    #     Column detail: modal offset + fraction of non-null values that
    #     actually fall on that offset — a low percentage (< ~80 %) signals
    #     an irregular/staggered column that may sit near a group boundary.
    # ------------------------------------------------------------------
    if logging.getLogger().isEnabledFor(logging.DEBUG):
        group_lines = []
        for modal_val, cols in sorted(cols_by_modal.items()):
            group_lines.append(
                f"  offset {modal_val:+.0f}s ({len(cols)} col(s)): {cols}"
            )
        logging.debug(
            f"{ctx}snap_readings_to_grid: characteristic offset groups "
            f"({len(cols_by_modal)} group(s)):\n" + "\n".join(group_lines)
        )

        col_lines = []
        for col in value_cols:
            notna_mask = df[col].notna()
            n_notna = int(notna_mask.sum())
            if n_notna > 0:
                n_at_modal = int(
                    (offset_rounded[notna_mask] == col_modal[col]).sum()
                )
                pct = 100.0 * n_at_modal / n_notna
                col_lines.append(
                    f"  {col}: modal={col_modal[col]:+.0f}s  "
                    f"{n_at_modal}/{n_notna} = {pct:.0f}% at modal"
                )
            else:
                col_lines.append(f"  {col}: all-NA (modal assigned 0s)")
        logging.debug(
            f"{ctx}snap_readings_to_grid: per-column offset detail:\n"
            + "\n".join(col_lines)
        )

    # ------------------------------------------------------------------
    # 4. Slot diagnostics
    # ------------------------------------------------------------------
    slot_counts = df['_SnappedSlot'].value_counts()
    multi_row_slots = int((slot_counts > 1).sum())
    if multi_row_slots > 0:
        logging.debug(
            f"{ctx}snap_readings_to_grid: {multi_row_slots} grid slot(s) have "
            f"multiple rows (max {int(slot_counts.max())} per slot) — merging."
        )

    # ------------------------------------------------------------------
    # 5. K-pass vectorized merge
    #    One pass per distinct characteristic offset group (K ≈ 2-3).
    #    Within each pass, sort by distance from the group's characteristic
    #    offset so that groupby.first() picks the most precisely-timed
    #    non-NA value per column (evaluated independently per column).
    # ------------------------------------------------------------------
    slot_index = df['_SnappedSlot'].sort_values().unique()
    result = pd.DataFrame({date_column: slot_index})

    for modal_val, cols in sorted(cols_by_modal.items()):
        dist = (raw_offsets - modal_val).abs()
        subset = df[['_SnappedSlot'] + cols].copy()
        subset['_dist'] = dist.values
        subset = subset.sort_values(['_SnappedSlot', '_dist'])
        group_result = (
            subset.groupby('_SnappedSlot', sort=True)[cols]
            .first()
            .reset_index()
            .rename(columns={'_SnappedSlot': date_column})
        )
        result = result.merge(group_result, on=date_column, how='left')

    logging.info(
        f"{ctx}snap_readings_to_grid: {len(df)} rows → {len(result)} grid slots "
        f"({len(cols_by_modal)} offset group(s), {multi_row_slots} slot(s) with "
        f"multiple rows merged)"
    )
    return result


def ensure_intervals(
    df: pd.DataFrame,
    date_column: str = 'ReadingDate',
    freq='5min',
) -> pd.DataFrame:
    """
    Ensure that the DataFrame has a consistent number of records and expected time intervals.
    It will add missing intervals or remove excess records to ensure consistency.

    This function checks if the input DataFrame has the expected number of records
    based on its date range and the specified frequency. If not, it adds missing
    intervals or removes excess records.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing the time series data.
    date_column : str, optional
        The name of the column containing the datetime information, by default 'ReadingDate'.
    freq : str, optional
        The expected frequency of the time series, by default '5min'.

    Returns
    -------
    pd.DataFrame
        A DataFrame with consistent time intervals.

    Notes
    -----
    - If the number of records matches the expected number, the function returns the input DataFrame unchanged.
    - If there are fewer records than expected, the function adds missing intervals.
    - If there are more records than expected, the function performs a left merge to reduce the number of records.
    - The function uses logging to inform about the actions taken.

    Warnings
    --------
    - If there are more records than expected, this might indicate issues with the data source.
      The function will log an error in this case.
    - This function assumes that an effort has already been made to prepare the data source in the intervals.
    - If raw data has more frequent data or if it records are coming in at a variable or different frequence, it will first need to be processed to meet the given interval.
    """

    df[date_column] = pd.to_datetime(df[date_column])

    earliest = df[date_column].min()
    latest = df[date_column].max()

    expected_num_records = (
        int(
            (latest - earliest) / pd.Timedelta(minutes=5),
        )
        + 1
    )

    if expected_num_records == len(df.index):
        logging.info(
            f"Expected number of records based on start and end date. "
            f"Not attempting to add {freq} intervals.",
        )
        return df

    all_dates_df = pd.DataFrame(
        {date_column: pd.date_range(start=earliest, end=latest, freq=freq)},
    )

    def merge_left(df):
        return pd.merge(all_dates_df, df, on=date_column, how='left')

    if expected_num_records > len(df.index):
        logging.info(f"Adding {freq} intervals.")
        all_dates_df = pd.DataFrame(
            {
                date_column: pd.date_range(
                    start=earliest,
                    end=latest,
                    freq=freq,
                ),
            },
        )
        merged_df = pd.merge(all_dates_df, df, on=date_column, how='outer')
        if len(merged_df.index) > expected_num_records:
            logging.error(
                f"There are more records than possible if {freq} "
                f"interval would be respected. Merging left to reduce records."
                f"Check data source.",
            )
            merged_df = merge_left(df)
        return merged_df
    else:  # (expected_num_records<len(df.index)):
        logging.error(
            f"There are more records than possible if {freq} interval would "
            f"be respected. Merging left to reduce records. Check data source",
        )
        merged_df = merge_left(df)
        return merged_df

def collect_mapped_data_stats(huis_id_bsv):
    """
    Collect statistics for each column in the DataFrame corresponding to a specific HuisIdBSV.

    This function retrieves data for a given `huis_id_bsv`, processes it, and collects summary statistics
    for each column. It logs errors if any issues occur during processing.

    Parameters
    ----------
    huis_id_bsv : str or int
        The identifier for the household to process.

    Returns
    -------
    list of dict
        A list of dictionaries, where each dictionary contains summary statistics for a column in the DataFrame.
        Each dictionary has keys 'column_name', 'mean', 'std', 'min', and 'max'.

    Notes
    -----
    - The function uses `get_mapped_data` to retrieve the data for the given `huis_id_bsv`.
    - It logs errors if there are issues retrieving or processing the data.
    """
    logging.info(f"Processing stats from columns where HuisIdBSV = {huis_id_bsv}")
    file_summary_data = []
    try:
        df = get_mapped_data(huis_id_bsv)
        for column in df.columns:
            column_data = df[column]
            file_summary_data.append(
                collect_column_stats(huis_id_bsv, column_data)
            )
    except Exception as e:
        logging.error(f"Failed to process stats from columns where HuisIdBSV = {huis_id_bsv}: {str(e)}", exc_info=True)

    return file_summary_data

def collect_column_stats(identifier, column_data):
    """
    Collect summary statistics for a given column of data.

    Parameters
    ----------
    identifier : str or int
        The identifier for the dataset.
    column_data : pd.Series
        The data in the column to analyze.

    Returns
    -------
    dict
        A dictionary containing summary statistics for the column. The keys are:
            - 'Identifier': The identifier for the dataset.
            - 'column': The name of the column.
            - 'type': The data type of the column.
            - 'count': The number of non-null values in the column.
            - 'missing': The number of missing values in the column.
            - 'errors': The number of errors (NA) in the column.
            - 'min': The minimum value in the column, if applicable.
            - 'max': The maximum value in the column, if applicable.
            - 'mean': The mean value in the column, if applicable.
            - 'median': The median value in the column, if applicable.
            - 'iqr': The interquartile range (IQR) of the column, if applicable.
            - 'quantile_25': The 25th percentile value in the column, if applicable.
            - 'quantile_75': The 75th percentile value in the column, if applicable.
            - 'top5': A dictionary with the top 5 most frequent values and their counts, if applicable.

    Notes
    -----
    - This function handles different data types (numeric, boolean, datetime, object) and computes relevant statistics accordingly.
    - At the moment there is no effective difference between missing and errors.
    """
    dtype = column_data.dtype
    n_values = column_data.count()
    n_missing = column_data.isnull().sum()
    n_errors = column_data.isna().sum()

    # Initialize statistics variables
    _min, _max, _mean, _median, _iqr, quantile_25, quantile_75, top5 = (
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )

    if not column_data.isna().all():
        if pd.api.types.is_bool_dtype(column_data):
            _mean = column_data.mean()
        elif pd.api.types.is_numeric_dtype(column_data):
            _min = column_data.min()
            _max = column_data.max()
            _mean = column_data.mean()
            _median = column_data.median()
            quantile_25 = column_data.quantile(0.25)
            quantile_75 = column_data.quantile(0.75)
            _iqr = quantile_75 - quantile_25
        elif pd.api.types.is_datetime64_any_dtype(column_data):
            _min = column_data.min()
            _max = column_data.max()
        if pd.api.types.is_object_dtype(column_data):
            top5 = column_data.value_counts().head(5).to_dict()

    return {
        "Identifier": identifier,
        "column": column_data.name,
        "type": dtype,
        "count": n_values,
        "missing": n_missing,
        "errors": n_errors,
        "min": _min,
        "max": _max,
        "mean": _mean,
        "median": _median,
        "iqr": _iqr,
        "quantile_25": quantile_25,
        "quantile_75": quantile_75,
        "top5": top5,
    }


def process_raw_data_file(args):
    file, raw_data_folder_path = args

    file_path = os.path.join(raw_data_folder_path, file)
    logging.info(f"Opening {file_path}")

    df = pd.read_parquet(file_path)
    summary_data = []

    for column in df.columns:
        column_data = df[column]
        summary_data.append(
            collect_column_stats(file, column_data)
        )
    return summary_data

def get_raw_data_stats(raw_data_folder_path, multi=False, max_workers=2):
    """
    Collect and aggregate statistics for all columns in the DataFrame corresponding to each raw data file in a folder.

    Parameters
    ----------
    raw_data_folder_path : str
        The path to the folder containing the raw data files.
    multi : bool, optional
        If True, use multiprocessing to collect stats. Default is False.
    max_workers : int, optional
        The maximum number of workers to use for multiprocessing. Default is 2.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the aggregated statistics for each column in the DataFrame corresponding to each file name.
        Each row represents a column from a specific file and contains summary statistics.

    Notes
    -----
    - Only parquet files are supported
    - It logs errors if there are issues retrieving or processing the data.
    """
    file_extension = 'parquet'
    summary_data = []

    try:
        files = os.listdir(raw_data_folder_path)

        if multi:
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                args_list = [(file, raw_data_folder_path) for file in files if file.endswith(f".{file_extension}")]
                results = executor.map(process_raw_data_file, args_list)
                summary_data = [item for sublist in results for item in sublist]
        else:
            for file in files:
                if not file.endswith(f".{file_extension}"):
                    continue
                summary_data.extend(
                    process_raw_data_file((file, raw_data_folder_path))
                )

        df_raw_stats = pd.DataFrame(summary_data)
        return df_raw_stats

    except Exception as e:
        logging.error(f"Failed to complete the main process: {str(e)}", exc_info=True)

def get_mapped_data_stats(multi=False, max_workers=2):
    """
    Collect and aggregate statistics for all columns in the DataFrame corresponding to each HuisIdBSV.

    Parameters
    ----------
    multi : bool, optional
        If True, use multiprocessing to collect stats. Default is False.
    max_workers : int, optional
        The maximum number of workers to use for multiprocessing. Default is 2.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the aggregated statistics for each column in the DataFrame corresponding to each HuisIdBSV.
        Each row represents a column from a specific HuisIdBSV and contains summary statistics.

    Notes
    -----
    - The function uses `read_index` to retrieve the index of households.
    - It logs errors if there are issues retrieving or processing the data.
    """
    summary_data = []
    try:
        index_df, _ = read_index()

        if multi:
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                results = executor.map(collect_mapped_data_stats, index_df["HuisIdBSV"])
                summary_data = [item for sublist in results for item in sublist]
        else:
            for huis_id in index_df["HuisIdBSV"]:
                logging.info(f"Collecting stats for HuisIdBSV = {huis_id}")
                result = collect_mapped_data_stats(huis_id)
                summary_data.extend(result)

        data_summary = pd.DataFrame(summary_data)

        data_summary = data_summary.rename(columns={"Identifier": "HuisIdBSV"})

        data_summary = pd.merge(data_summary, index_df, how="left", on="HuisIdBSV")

        return data_summary

    except Exception as e:
        logging.error(f"Failed to complete the main process: {str(e)}", exc_info=True)

def apply_thresholds_to_df(
    df
):
    """
    Apply thresholds to columns and update imputation flags.

    This function applies lower and upper bounds to a column in the
    DataFrame. Values outside these bounds are replaced with pd.NA.
    """

    thresholds_dict = load_thresholds_as_dict()

    logging.info("Checking and removing any values outside of column thresholds.")
    for col in df.columns:
        if col in thresholds_dict:
            df = apply_threshold_to_col(
                df=df,
                col=col,
                lower_bound=thresholds_dict[col]["Min"],
                upper_bound=thresholds_dict[col]["Max"]
            )

    return df

def apply_threshold_to_col(
    df,
    col,
    lower_bound,
    upper_bound
):
    """
    Apply thresholds to a column.

    This function applies lower and upper bounds to a column in the
    DataFrame. Values outside these bounds are replaced with pd.NA.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame containing the data.
    col : str
        The name of the column to apply thresholds to.
    lower_bound : float
        The lower threshold for the column.
    upper_bound : float
        The upper threshold for the column.

    Returns
    -------
    pandas.DataFrame
        The DataFrame with thresholds applied.

    Notes
    -----
    This function modifies the input DataFrame in-place and also returns it.
    Values outside the thresholds are replaced with pd.NA.
    """
    mask = ((df[col] < lower_bound) | (df[col] > upper_bound)) & df[
        col
    ].notna()

    n_out_of_bounds = mask.sum()

    if n_out_of_bounds > 0:
        logging.info(f"Removing {n_out_of_bounds} values from {col} based on thresholds.")

    df.loc[mask, col] = pd.NA

    return df
