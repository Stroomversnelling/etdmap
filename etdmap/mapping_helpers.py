import logging
import os
import re
import shutil
from typing import NamedTuple

import pandas as pd

import etdmap

from etdmap.data_model import (
    cumulative_columns,
    model_column_order,
    model_column_type,
    performance_column_dtypes,
    required_performance_data_columns,
    thresholds_dict,
)
from etdmap.index_helpers import get_mapped_data, read_index
from etdmap.flags import MappingFlag, bound_flags
from etdmap.record_validators import (
    record_flag_conditions,
    record_quality_flag_conditions,
    momentaan_flag_conditions,
    cumulative_diff_flag_conditions,
)

# Backward-compat re-exports: the stats producer, the per-Series
# kernel, the schema constants, and their helpers were relocated to
# etdmap.data_stats for module-path symmetry with
# etdtransform.data_stats. Existing
# `from etdmap.mapping_helpers import get_data_stats` (and similar)
# imports keep working through these re-exports. New code should
# import directly from etdmap.data_stats.
from etdmap.data_stats import (  # noqa: F401, E402
    DEFAULT_SEASON_MONTHS,
    NUMERIC_STATS_SCHEMA,
    _COLD_MONTHS,
    _STATS_DTYPES,
    _WARM_MONTHS,
    _cast_stats_dtypes,
    _seasonal_slices,
    _synthesise_tariff_roots,
    collect_column_stats,
    collect_mapped_data_stats,
    compute_numeric_column_stats,
    get_data_stats,
    process_raw_data_file,
)


def rearrange_model_columns(
    household_df: pd.DataFrame,
    add_columns: bool = True,
    context: str = '',
) -> pd.DataFrame:
    """
    Rearrange and validate columns in a DataFrame according to a predefined model.

    This function performs the following operations:
    1. Validates and coerces column types to match expected types.
    2. Rearranges columns into data model order.
    3. Drops columns that are not part of the ETD data model -- supplier
       columns that were never mapped do not belong in mapped output. Any
       data model column is kept, whatever its Entiteit: the model is a
       theoretical model of what may exist, not a claim about what a given
       supplier delivers.
    4. Optionally adds missing columns with NA values.

    Parameters
    ----------
    household_df : pd.DataFrame
        The input DataFrame containing household data.
    add_columns : bool, optional
        If True, materialise the model_column_order placeholders that are missing,
        as all-NA columns of the declared dtype. If False, nothing is added.
        Either way the output keeps every delivered data model column.
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
    - Membership is decided by performance_column_dtypes, which spans the whole
      data model; model_column_order only supplies the add_columns placeholders.
    - Columns outside the data model are dropped. Statistics over the supplier
      files still cover them: those are produced by the raw-data checks, which
      read the supplier files directly.
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

    # A column in the data model may exist; anything else is dropped.
    # Membership decides, nothing else — any model column is valid delivered
    # data regardless of Entiteit. add_columns materialises the empty placeholders
    # a mapped file must carry; the rest are kept when delivered, not invented here.
    if add_columns:
        for col in model_column_order:
            if col not in household_df.columns:
                household_df[col] = pd.array(
                    [pd.NA] * len(household_df),
                    dtype=performance_column_dtypes.get(col, "Float64"),
                )

    household_df = household_df[
        [col for col in performance_column_dtypes if col in household_df.columns]
    ]

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

                # A register that has recorded a total cannot return to zero.
                # Leading zeros are a meter that has not started counting and
                # are not a fault.
                has_accumulated = (
                    (filtered_group[col] > 0).cumsum().shift(1, fill_value=0) > 0
                )
                zero_after_total = (filtered_group[col] == 0) & has_accumulated
                if zero_after_total.any():
                    reading_dates = filtered_group.loc[
                        zero_after_total, 'ReadingDate'
                    ]
                    logging.warning(
                        f"{context}Column {col} returns to zero after recording "
                        f"a total at 'ReadingDate': {reading_dates.to_list()}. "
                        f"This will be removed",
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


class _NegativeDiffCases(NamedTuple):
    """
    Every negative diff in one cumulative column, classified.

    ``readings`` holds the rows of the column that carry a value, each with the
    diff to the previous reading (``<col>Diff_no_gap``) and, in ``_next_val`` /
    ``_next_date``, the value and date of the next non-zero diff that follows it.
    ``dates_ns`` is the ReadingDate of those same rows as epoch nanoseconds.

    ``flags`` is aligned to ``readings`` and carries the ``MappingFlag`` bits
    found in this column. Every decreasing reading gets exactly one of
    ``CUMULATIVE_JUMP_BACK``, ``CUMULATIVE_REPEAT_NEGATIVE``,
    ``CUMULATIVE_METER_RESET`` or ``CUMULATIVE_NO_RECOVERY``.
    ``CUMULATIVE_ZERO_TOTAL`` is independent of those four and may sit on the
    same reading or on its own.
    """

    readings: pd.DataFrame
    dates_ns: pd.Series
    flags: pd.Series


def _detect_negative_diff_cases(group, col):
    """
    Classify the negative diffs in one cumulative column.

    Reads ``group['ReadingDate']`` and ``group[col]`` and returns the
    classification. Nothing is written back to *group*, so this is safe to run
    on every column regardless of what any validator reported.

    Parameters
    ----------
    group : pd.DataFrame
        Per-household DataFrame, sorted by ReadingDate, containing ``col``.
    col : str
        Name of the cumulative column being classified.

    Returns
    -------
    _NegativeDiffCases or None
        None when the column holds no negative diff, so callers can skip the
        correction step entirely.
    """
    readings = group[['ReadingDate', col]].dropna().copy()
    diff_no_gap = col + 'Diff_no_gap'
    readings[diff_no_gap] = readings[col].diff().round(10)

    # The first reading has no predecessor, so its NA diff is not a decrease.
    # Resolving it here keeps pd.NA out of the flags entirely.
    is_negative = (readings[diff_no_gap] < 0).fillna(False)
    if not is_negative.any():
        return None

    # Next non-zero diff for every row in one pass: blank the zero-diff rows,
    # shift up by one so each row looks past itself, then back-fill so every row
    # carries the first non-zero diff that follows it.
    is_nonzero = readings[diff_no_gap] != 0
    readings['_next_val'] = readings[diff_no_gap].where(is_nonzero).shift(-1).bfill()
    readings['_next_date'] = readings['ReadingDate'].where(is_nonzero).shift(-1).bfill()

    has_next = readings['_next_val'].notna()
    jump_back = is_negative & has_next & (readings['_next_val'] >= -readings[diff_no_gap])
    repeat_negative = is_negative & has_next & (readings['_next_val'] < 0)
    meter_reset = is_negative & has_next & ~(jump_back | repeat_negative)
    no_recovery = is_negative & ~has_next

    # A register that has recorded a total cannot return to zero. Leading zeros
    # are a meter that has not started counting, which is not a fault -- what
    # follows them may be (a large first reading is accumulated catch-up), but
    # that is a different fault on a different reading. Not exclusive with the
    # four cases above: a reset can land on a zero reading.
    has_accumulated = (readings[col] > 0).cumsum().shift(1, fill_value=0) > 0
    zero_total = (readings[col] == 0) & has_accumulated

    # .value throughout: a Flag member is iterable on Python 3.11+, so pandas
    # would read the enum itself as array-like.
    flags = (
        jump_back.astype('Int64') * MappingFlag.CUMULATIVE_JUMP_BACK.value
        | repeat_negative.astype('Int64') * MappingFlag.CUMULATIVE_REPEAT_NEGATIVE.value
        | meter_reset.astype('Int64') * MappingFlag.CUMULATIVE_METER_RESET.value
        | no_recovery.astype('Int64') * MappingFlag.CUMULATIVE_NO_RECOVERY.value
        | zero_total.astype('Int64') * MappingFlag.CUMULATIVE_ZERO_TOTAL.value
    )

    return _NegativeDiffCases(
        readings=readings,
        # Epoch nanoseconds so that searchsorted against the group never compares
        # a tz-aware timestamp with a tz-naive one: some suppliers deliver
        # ReadingDate as datetime64[ns, UTC] and the coercion in
        # rearrange_model_columns does not always strip the tz.
        dates_ns=readings['ReadingDate'].astype('int64'),
        flags=flags,
    )


def _apply_negative_diff_corrections(group, col, cases, context_string):
    """
    Set the cumulative readings that *cases* condemns to pd.NA.

    Modifies *group* in place, with a single assignment at the end. The four
    flags resolve to three deletion scopes:

    - ``CUMULATIVE_DECREASE_RANGE`` (jump-back or repeat-negative) blanks the
      half-open date range from the dip up to the recovery, accumulated with a
      sweep line so overlapping ranges collapse.
    - ``CUMULATIVE_NO_RECOVERY`` blanks everything from the earliest unrecovered
      dip onward.
    - ``CUMULATIVE_METER_RESET`` blanks the single reading at the dip, and only
      when the diff already computed for that row is negative. Where that diff is
      NA a gap in the data already prevents a negative diff, so the reading is
      kept.

    Parameters
    ----------
    group : pd.DataFrame
        Per-household DataFrame, sorted by ReadingDate. Must already contain
        ``col`` and ``col + 'Diff'``.
    col : str
        Name of the cumulative column being corrected.
    cases : _NegativeDiffCases
        Classification from ``_detect_negative_diff_cases``.
    context_string : str
        Prefix for log messages (e.g. ``"HH-42: "``).
    """
    readings = cases.readings

    def selected(flag):
        return (cases.flags & flag.value) != 0

    # Positional index over group rows; group itself may carry any index label.
    group_dates_ns = group['ReadingDate'].astype('int64').reset_index(drop=True)
    n = len(group_dates_ns)
    na_mask = pd.Series(False, index=range(n))

    repeat_negative = selected(MappingFlag.CUMULATIVE_REPEAT_NEGATIVE)
    no_recovery = selected(MappingFlag.CUMULATIVE_NO_RECOVERY)
    meter_reset = selected(MappingFlag.CUMULATIVE_METER_RESET)
    range_scope = selected(MappingFlag.CUMULATIVE_DECREASE_RANGE)
    if range_scope.any():
        if repeat_negative.any():
            for rd, next_date in zip(
                readings.loc[repeat_negative, 'ReadingDate'].values,
                readings.loc[repeat_negative, '_next_date'].values,
            ):
                logging.error(
                    f"{context_string}Two negative diffs "
                    f"one after the other between {rd} and "
                    f"{next_date}. Will remove all "
                    f"these values for {col}."
                )

        starts_ns = cases.dates_ns.loc[range_scope]
        ends_ns = readings.loc[range_scope, '_next_date'].astype('int64')
        si_arr = group_dates_ns.searchsorted(starts_ns.values)
        ei_arr = group_dates_ns.searchsorted(ends_ns.values)

        # +1 at each range start, -1 at each range end; groupby handles duplicates
        events = pd.concat([
            pd.Series(1,  index=si_arr),
            pd.Series(-1, index=ei_arr[ei_arr < n]),
        ]).groupby(level=0).sum().reindex(range(n), fill_value=0)

        na_mask |= events.cumsum() > 0
        logging.debug(
            f"{context_string}Marking {int(range_scope.sum())} interval(s) "
            f"as NA in '{col}'"
        )

    if no_recovery.any():
        earliest_ns = int(cases.dates_ns.loc[no_recovery].min())
        si = int(group_dates_ns.searchsorted(earliest_ns))
        na_mask.iloc[si:] = True
        earliest_disp = readings.loc[no_recovery, 'ReadingDate'].min()
        logging.warning(
            f"{context_string}Removing all values in '{col}' after {earliest_disp} -- "
            f"no subsequent increases after the negative diff."
        )

    if meter_reset.any():
        reset_dates_ns = cases.dates_ns.loc[meter_reset]
        si_reset = group_dates_ns.searchsorted(reset_dates_ns.values)
        diff_vals = group.iloc[si_reset][col + 'Diff']
        is_neg_diff = (diff_vals < 0).fillna(False)
        is_na_diff = diff_vals.isna()
        is_error = ~is_neg_diff & ~is_na_diff

        if is_error.any():
            reset_dates_disp = readings.loc[meter_reset, 'ReadingDate']
            for rd in reset_dates_disp.values[is_error.values]:
                logging.error(
                    f"{context_string}Negative gap jump at {rd}. Diff is not "
                    f"negative, and not <NA>. Check for errors, e.g duplicate "
                    f"reading dates!"
                )
        na_mask.iloc[si_reset[is_neg_diff.values]] = True
        if is_na_diff.any():
            logging.debug(
                f"{context_string}Negative gap jump(s) in '{col}'. "
                f"Diff is NA, not removing any values."
            )

    group.loc[group.index[na_mask.values], col] = pd.NA


def add_diff_columns(
    data: pd.DataFrame,
    id_column: str = None,
    validate_func=validate_cumulative_variables,
    context: str = '',
    drop_unvalidated: bool = False,
) -> pd.DataFrame:
    """
    Add difference columns for cumulative variables and handle data inconsistencies.

    Per cumulative column: compute the difference to the previous reading,
    classify any decrease with ``_detect_negative_diff_cases``, blank the
    readings that classification condemns with
    ``_apply_negative_diff_corrections``, then recompute the difference from the
    corrected readings.

    Detection runs for every column and has no effect on the data; only the
    correction depends on what was detected. ``validate_func`` decides whether a
    group is kept or dropped, not whether its readings are corrected.

    Parameters
    ----------
    data : pd.DataFrame or pd.core.groupby.DataFrameGroupBy
    id_column : str, optional
    validate_func : callable, optional
    context : str, optional
    drop_unvalidated : bool, optional

    Returns
    -------
    pd.DataFrame
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

            # A supplier may deliver the interval column itself. Derive only when
            # the cumulative column carries data (it stays authoritative), or when
            # there is no delivered interval column to preserve. An all-NA
            # cumulative beside a delivered interval column is the one case where
            # deriving would destroy a real measurement.
            diff_col = col + 'Diff'
            _delivered_interval = (
                diff_col in group.columns and group[diff_col].notna().any()
            )
            if group[col].notna().any() or not _delivered_interval:
                logging.debug(f"{context_string}Calculating diff for {col}")
                group[diff_col] = group[col].diff().round(10)
                group.loc[group.index[0], diff_col] = 0
            else:
                logging.info(
                    f"{context_string}'{col}' is empty and '{diff_col}' was "
                    f"delivered; keeping the delivered interval values."
                )

            # Detection reads the column and writes nothing, so it runs for every
            # column. Only the correction is conditional, on what was detected.
            cases = _detect_negative_diff_cases(group, col)
            if cases is not None:
                _apply_negative_diff_corrections(group, col, cases, context_string)

                logging.debug(
                    f"{context_string}Re-calculating diff for {col} after corrections."
                )
                group[col + 'Diff'] = group[col].diff().round(10)
                group.loc[group.index[0], col + 'Diff'] = 0

                if (group[col + 'Diff'] < 0).any(skipna=True):
                    logging.warning(
                        f"{context_string}Removed zeros but diff still has negative "
                        f"values in '{col}'! Check data and consider removing.",
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


def add_diff_columns_legacy(
    data: pd.DataFrame,
    id_column: str = None,
    validate_func=validate_cumulative_variables,
    context: str = '',
    drop_unvalidated: bool = False,
) -> pd.DataFrame:
    """
    Original loop-based implementation of add_diff_columns (kept for validation).

    Uses a ``for rd in reading_dates:`` loop with per-date DataFrame filters — O(N²)
    for columns with many negative diffs (e.g. solar with daily resets).
    Retained until ``add_diff_columns`` (vectorized) has been fully validated in production.

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


# Nullable dtypes (ADR-005) for the per-column cadence table. cadence_minutes
# and cadence_source are added by the resolution step at the end.
_CADENCE_DTYPES = {
    'column': 'string',
    'cadence_detected': 'Int64',
    'confidence': 'Float64',
    'n_readings': 'Int64',
}

_CADENCE_TABLE_DTYPES = {
    'column': 'string',
    'cadence_minutes': 'Int64',
    'cadence_source': 'string',
    'cadence_detected': 'Int64',
    'confidence': 'Float64',
    'n_readings': 'Int64',
}


def cadence_label(minutes) -> str:
    """
    Render a cadence the way the metadata writes it, in its natural unit.

    A cadence is a duration, so it is written the way durations are written:
    ``30s``, ``5min``, ``1h``, ``24h``. Whole hours read as hours rather than as
    ``1440min``, which is the same duration but not how anyone says it. Hours are
    the coarsest unit written, so a daily reading is ``24h`` rather than ``1d``.

    This is for display. **Do not compare cadences as text.** The metadata writes
    a cadence in whichever unit suits the variable, so ``24h`` and ``1440min``
    are the same cadence written two ways and would compare unequal. Read a
    declared cadence with ``pd.Timedelta`` and compare the numbers.

    Returns pd.NA unchanged, so an unknown cadence stays unknown.
    """
    if pd.isna(minutes):
        return pd.NA

    minutes = float(minutes)
    if minutes < 1:
        return f"{int(round(minutes * 60))}s"
    if minutes % 60 == 0:
        return f"{int(minutes // 60)}h"
    return f"{int(minutes)}min"


def _modal_gap_minutes(timestamps: pd.Series):
    """
    The most common gap between consecutive timestamps, in minutes.

    Returns (minutes, confidence), where confidence is the share of gaps that sit
    at that value, or (pd.NA, pd.NA) when there is nothing to measure.

    Gaps are rounded to the minute so that a timestamp offset varying slightly
    between readings does not split one cadence into several.
    """
    gaps = timestamps.diff().dropna().dt.total_seconds().div(60).round()
    gaps = gaps[gaps > 0]
    if gaps.empty:
        return pd.NA, pd.NA

    modal = gaps.mode()
    if modal.empty:
        return pd.NA, pd.NA

    minutes = int(modal.iloc[0])
    return minutes, float((gaps == minutes).sum() / len(gaps))


def detect_column_cadence(
    df: pd.DataFrame,
    columns: tuple[str, ...] | list[str] | None = None,
    date_column: str = 'ReadingDate',
    declared: dict | None = None,
    min_readings: int = 3,
    min_confidence: float = 0.8,
    context: str = '',
) -> pd.DataFrame:
    """
    Infer how often each column actually reports, from its own timestamps.

    A column's cadence is the most common gap between the readings it carries.
    Only rows where the column has a value count, so a 15-minute register sitting
    on a 5-minute grid reports 15 minutes rather than the grid's 5.

    ``date_column`` is measured too and comes back as its own row. That is the
    dataset's cadence -- the grid the data sits on, and the floor on any column's
    cadence, since nothing can report more often than the dataset records.

    Cadence is settled here, once. Three sources, in order of authority:

    1. **Declared.** The metadata wins. Corrections are made there, because the
       delivered data is static and cannot be changed.
    2. **Detected**, where the readings were regular enough to trust.
    3. **The dataset's own cadence**, as the floor and last resort.

    A declaration that disagrees with a confident detection is logged and kept in
    ``cadence_detected`` so a report can surface it. The declaration still wins.

    Everything downstream reads ``cadence_minutes`` rather than measuring again.

    Must run on data as delivered, before any fill. ``fill_down_infrequent_devices``
    makes every row non-missing, which erases the evidence this reads.

    Taking the most common *gap* rather than counting readings per day is what
    makes a partial opening day harmless. The ingestion window is cut on Dutch
    midnight while data is bucketed in UTC, so a record's first day can carry
    only a couple of hours -- 24 readings on a 5-minute register, exactly what a
    complete hourly register produces. The counts collide; the gaps do not.

    Parameters
    ----------
    df : pd.DataFrame
        Data as delivered. ``date_column`` must be datetime dtype and is assumed
        sorted ascending.
    columns : tuple or list of str, optional
        Columns to measure. Defaults to every column, ``date_column`` included.
    date_column : str
        Name of the timestamp column. Default 'ReadingDate'.
    declared : dict, optional
        Column -> cadence in minutes, as declared in the metadata. Absent columns
        mean nothing was declared, which is the normal case: ``Meetfrequentie``
        is unpopulated by design.
    min_readings : int
        Fewest readings that can support a cadence. Below this the column gets
        pd.NA rather than a guess. Default 3, which is two gaps.
    min_confidence : float
        Lowest confidence at which a detected cadence is trusted.
    context : str
        Optional prefix for log messages, e.g. a household identifier.

    Returns
    -------
    pd.DataFrame
        One row per column:

        - ``cadence_minutes`` (Int64) -- the cadence to use.
        - ``cadence_source`` (string) -- 'declared', 'detected' or 'dataset'.
        - ``cadence_detected`` (Int64) -- what the readings themselves showed,
          kept even when a declaration overrides it so the report can surface
          the disagreement.
        - ``confidence`` (Float64) -- the share of gaps at the detected cadence.
          A low share means the readings are irregular and the measurement is a
          weak claim.
        - ``n_readings`` (Int64).
    """
    ctx = (context + ': ') if context else ''
    if columns is None:
        columns = list(df.columns)

    dates = df[date_column]
    rows = []
    for col in columns:
        if col not in df.columns:
            continue

        present = dates[df[col].notna()]
        n_readings = len(present)
        minutes, confidence = pd.NA, pd.NA

        if n_readings >= min_readings:
            minutes, confidence = _modal_gap_minutes(present)

        if pd.isna(minutes):
            logging.debug(
                f"{ctx}detect_column_cadence: '{col}' has {n_readings} "
                f"reading(s), too few to infer a cadence."
            )
        rows.append(
            {
                'column': col,
                'cadence_detected': minutes,
                'confidence': confidence,
                'n_readings': n_readings,
            }
        )

    out = pd.DataFrame(rows, columns=_CADENCE_DTYPES.keys()).astype(_CADENCE_DTYPES)

    declared_minutes = pd.array(
        [(declared or {}).get(col, pd.NA) for col in out['column']], dtype='Int64'
    )
    declared_minutes = pd.Series(declared_minutes, index=out.index)
    # An unmeasured column has no confidence, which is not a low confidence --
    # but a mask must not carry pd.NA, or it reads as True where it is unknown.
    trusted = (out['confidence'] >= min_confidence).fillna(False)
    grid = dict(zip(out['column'], out['cadence_detected'])).get(date_column, pd.NA)

    # Declared, then a detection regular enough to trust, then the grid.
    out['cadence_minutes'] = (
        declared_minutes
        .combine_first(out['cadence_detected'].where(trusted))
        .fillna(grid)
        .astype('Int64')
    )
    out['cadence_source'] = (
        pd.Series('dataset', index=out.index, dtype='string')
        .mask(trusted, 'detected')
        .mask(declared_minutes.notna(), 'declared')
    )

    drifted = (
        declared_minutes.notna() & trusted
        & (declared_minutes != out['cadence_detected'])
    )
    if drifted.any():
        logging.warning(
            f"{ctx}Declared cadence disagrees with the data for "
            + ', '.join(
                f"'{col}' ({cadence_label(dec)} declared, reports every "
                f"{cadence_label(det)})"
                for col, dec, det in zip(
                    out.loc[drifted, 'column'],
                    declared_minutes[drifted],
                    out.loc[drifted, 'cadence_detected'],
                )
            )
            + ". Using the declared values."
        )

    return out[list(_CADENCE_TABLE_DTYPES)]


def fill_down_infrequent_devices(
    df: pd.DataFrame,
    columns: tuple[str, ...] | list[str],
    cadence: pd.DataFrame | None = None,
    grid_minutes: int = 5,
) -> pd.DataFrame:
    """
    Fill down (forward fill) and then up (backward fill) values for specified columns.

    Intended for cumulative columns from optional devices that report infrequently.
    When the device is idle the cumulative meter holds its last value (forward-fill).
    When the device is absent from a household the column is set to 0.0.

    The column list is intentionally required -- it must be defined per project/mapper
    script because the set of optional devices differs between data suppliers.

    Filling at the device's cadence
    -------------------------------
    Given *cadence*, a filled value is placed only on the slots the device
    actually reports on, leaving pd.NA in between. A reading that covers fifteen
    minutes then sits on one fifteen-minute slot rather than being repeated
    across three five-minute rows, so imputation spreads its energy over the
    window it accumulated in instead of seeing a flat line and one spike.

    Delivered readings are never touched. Only the *filled* values are placed on
    cadence slots; a real reading that arrives off-slot is kept where it is.

    Without *cadence*, or for a column whose cadence could not be measured, every
    row is filled -- the behaviour this function has always had.

    *grid_minutes* is the grid the pipeline imposes, not something measured. The
    grid is chosen: ``snap_readings_to_grid`` puts readings on it and
    ``ensure_intervals`` pads it. A column reporting faster than the grid cannot
    have a sparser fill than the grid, so it fills every row.

    Slots are stepped in **rows**, not in wall-clock time, so a grid of any
    cadence works and a column's cadence need not divide the hour. The step is
    anchored on the device's first real reading, which puts the slots where its
    data actually is. A cadence that does not land on a whole number of grid rows
    has no slots to place fills on, so that column is filled every row.

    A cadence is a measurement or a declaration, and either can be wrong. The
    fill does not depend on it being right: a delivered reading is always kept,
    so only restatements are ever removed. Forward-fill the result back onto the
    grid and it reproduces the every-row fill exactly, whatever cadence was used.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing the device data.
    columns : tuple or list of str
        Cumulative column names to fill. Must be provided by the caller; there is no
        default. Only columns that exist in df are processed; others are silently skipped.
    cadence : pd.DataFrame, optional
        The output of ``detect_column_cadence``, measured on this data before any
        fill. Columns absent from it are filled every row.
    grid_minutes : int
        The grid the pipeline imposes, in minutes. Default 5.

    Returns
    -------
    pd.DataFrame
        The input DataFrame with the specified columns filled in place.

    Notes
    -----
    Fill order: forward fill, then backward fill, then 0.0 for any remaining pd.NA
    (households where the sensor was never present).
    Only meaningful for cumulative variables -- do not apply to instantaneous sensors.
    """
    minutes_by_column = {}
    if cadence is not None and not cadence.empty:
        # detect_column_cadence has already settled this. Nothing is measured or
        # decided here -- the value is read.
        minutes_by_column = dict(
            zip(cadence['column'], cadence['cadence_minutes'])
        )

    position = pd.Series(range(len(df)), index=df.index)

    for col in columns:
        if col not in df.columns:
            continue

        delivered = df[col].notna()
        filled = df[col].ffill().bfill().fillna(0.0)

        minutes = minutes_by_column.get(col, pd.NA)
        if pd.isna(minutes):
            df[col] = filled
            continue

        # Step in rows, not in wall-clock time: the grid may be any cadence, and
        # a column's cadence need not divide the hour. Anchoring on the device's
        # first real reading puts the slots where its data actually is.
        step, remainder = divmod(int(minutes), int(grid_minutes))
        if remainder:
            # The cadence does not land on the grid, so there are no slots to
            # place fills on. Rounding to the nearest whole step would fill at a
            # spacing that is not the cadence and not say so.
            logging.info(
                f"[mapping_helpers] '{col}' reports every "
                f"{cadence_label(minutes)}, which does not land on the "
                f"{cadence_label(grid_minutes)} grid. Filling every row."
            )
            df[col] = filled
            continue

        if step <= 1 or not delivered.any():
            df[col] = filled
            continue

        anchor = int(position[delivered].iloc[0])
        on_cadence = (position - anchor) % step == 0
        df[col] = filled.where(on_cadence | delivered, pd.NA)

    return df


def fill_zeros_for_device_not_installed(
    df: pd.DataFrame,
    columns: tuple[str, ...] | list[str],
) -> pd.DataFrame:
    """
    Add constant-zero cumulative columns for devices that are not installed in
    this project or supplier export.

    Use this when a device is physically absent (e.g. no electric radiator in a
    heat-pump-only project) and the supplier therefore omits the column entirely.
    A zero-value cumulative column produces a zero Diff after aggregation, which
    satisfies catalog formulas that require the column as an input.

    Difference from fill_down_infrequent_devices
    --------------------------------------------
    fill_down_infrequent_devices: the device IS present but reports rarely.
      Forward/backward fill holds the last known cumulative value; remaining
      NA (device never seen for a household) is set to 0.

    fill_zeros_for_device_not_installed: the device is NOT present at all.
      The entire column is set to 0 unconditionally. Do not use this for
      devices that may be present in some households but absent in others --
      use fill_down_infrequent_devices for that case.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame (pre-aggregation mapped output).
    columns : tuple or list of str
        Column names to add/fill with 0.  Columns already present with
        non-NA values are NOT overwritten -- the function is a no-op for
        those columns, so it is safe to call unconditionally.

    Returns
    -------
    pd.DataFrame
        The input DataFrame with the specified columns filled or added.

    Notes
    -----
    Only meaningful for cumulative variables.
    Requires Float64 dtype (pandas nullable) per ADR-005.
    """
    for col in columns:
        if col not in df.columns or df[col].isna().all():
            df[col] = pd.array([0] * len(df), dtype="Float64")
    return df


def combine_source_columns(
    df: pd.DataFrame,
    *,
    target: str,
    sources: tuple[str, ...] | list[str],
    operator: str = "sum",
    target_dtype: str | None = None,
    context: str = "",
) -> pd.DataFrame:
    """
    Resolve multiple raw source columns mapping to one standard target column.

    Used by supplier mapping scripts when the supplier exposes more than one
    raw column for the same standard model variable. Examples:
      - heat-pump energy: `_binnen` + `_buiten` -> `ElektriciteitsgebruikWarmtepomp`
      - heat-pump power:  `_binnen` + `_buiten` -> `ElektriciteitVermogenWarmtepomp`
      - CO2 from two co-located room sensors -> `CO2`

    Assumes the supplier-specific rename has already happened, so the source
    columns carry staging names (e.g. `<target>_binnen`, `<target>_1`).

    Behaviour, in priority order:
      1. If `target` is already in df, drop any source columns that are also
         present and return -- the existing target wins. Handles suppliers
         that expose both a total and a decomposition.
      2. If `target` is absent and any source columns are present, compute
         `target = operator(present_sources)` row-by-row and drop the
         sources. NA-preserving: any timestamp where a contributing source
         is NA produces NA in the target. A missing reading on one sensor
         never silently halves or distorts the combined value.
      3. If neither target nor any source is present, fill an all-NA target
         column using `target_dtype` (or `model_column_type[target]`).

    Parameters
    ----------
    df : pd.DataFrame
        Input frame, typically a single-household mapped frame. Modified
        in place; also returned.
    target : str
        Standard model column name to produce.
    sources : tuple or list of str
        Source column names that combine into `target`. Must already be
        renamed to their staging names.
    operator : {"sum", "mean"}, default "sum"
        Combination operator. Use `sum` for energy/power components that
        add (e.g. binnen + buiten heat-pump power) and `mean` for
        concentration / temperature / humidity readings from co-located
        sensors that should be averaged.
    target_dtype : str or None, default None
        Pandas dtype for the all-NA target if no sources are present.
        If None, looked up from `model_column_type[target]`.
    context : str, default ""
        Free-form prefix for log messages (e.g. household ID).

    Returns
    -------
    pd.DataFrame
        The same df, modified in place.
    """
    if operator not in ("sum", "mean"):
        raise ValueError(
            f"[combine_source_columns] operator must be 'sum' or 'mean', got {operator!r}"
        )

    present = [s for s in sources if s in df.columns]
    ctx = f"{context} " if context else ""

    # State 1: target already present, drop sources, target wins
    if target in df.columns:
        if present:
            logging.info(
                f"[combine_source_columns] {ctx}target '{target}' already present; "
                f"dropping source(s) {present}."
            )
            df.drop(columns=present, inplace=True)
        return df

    # State 3: no sources -- create all-NA target column.
    # Silent for optional targets (matches the rest of the pipeline's quiet
    # NA fill for missing optional columns). Warning for required targets
    # (Vereist=ja in etdmodel.csv) so a sensor-absent required variable
    # surfaces in the logs without halting the run -- the dynamic rule
    # calculations downstream may still re-derive it.
    if not present:
        dtype = target_dtype if target_dtype is not None else model_column_type.get(target)
        if dtype is None:
            raise KeyError(
                f"[combine_source_columns] No dtype known for target '{target}'. "
                f"Pass target_dtype= or add '{target}' to model_column_type."
            )
        if target in required_performance_data_columns:
            logging.warning(
                f"[combine_source_columns] {ctx}required target '{target}' "
                f"(Vereist=ja) missing and no source(s) present; filling all-NA."
            )
        df[target] = pd.Series(pd.NA, dtype=dtype, index=df.index)
        return df

    # State 2: at least one source present -- combine and drop
    combined = df[present[0]].copy()
    for s in present[1:]:
        combined = combined + df[s]  # NA-preserving
    if operator == "mean":
        combined = combined / len(present)

    df[target] = combined
    df.drop(columns=present, inplace=True)
    logging.info(
        f"[combine_source_columns] {ctx}target '{target}' = {operator}({present}); "
        f"sources dropped."
    )
    return df


def snap_readings_to_grid(
    df: pd.DataFrame,
    date_column: str = 'ReadingDate',
    freq_minutes: int = 5,
    max_deviation_seconds: float = None,
    context: str = '',
) -> pd.DataFrame:
    """
    General utility for merging multiple sensor streams with different time
    offsets into a single row per grid slot. Handles any number of streams
    automatically via per-column characteristic offset detection.

    Use this whenever a dataset has columns that arrive at different timestamps
    within the same grid period (e.g. columns A-C arrive at HH:MM:22 every
    5 minutes while columns D-F arrive at HH:MM:00 every 15 minutes).
    Both sets of rows snap to the same 5-minute slot and their column values
    are merged by taking the first non-NA value per column.

    For datasets with multiple separate input files per sensor group, concatenate
    all files into one DataFrame first (leaving columns from other files as NaN),
    then call this function. This is equivalent to a Timestep-based
    sheet merge, but works on datetime columns rather than integer offsets.

    Within a slot, when multiple rows are present, each column independently
    selects the row whose timestamp is closest to that column's characteristic
    offset (the most common signed offset observed for that column across all
    rows where it is non-null).  This is implemented as a K-pass vectorized
    merge where K is the number of distinct characteristic offsets (typically
    2-3 for typical supplier data).

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
        """Keep only readings that land on the grid, and refuse to do it quietly.

        A reading that does not land on a grid slot is dropped by this merge. That
        is data loss, so it is counted and raised rather than logged and forgotten:
        an off-grid reading means the data reached here without being snapped, and
        ``snap_readings_to_grid`` is what places readings on slots.
        """
        on_grid = df[date_column].isin(all_dates_df[date_column])
        n_dropped = int((~on_grid).sum())
        if n_dropped:
            pct = 100.0 * n_dropped / len(df.index)
            raise ValueError(
                f"{n_dropped} of {len(df.index)} readings ({pct:.1f}%) do not "
                f"land on the {freq} grid and would be discarded. Call "
                f"snap_readings_to_grid before ensure_intervals to place them "
                f"on slots, or correct the source data."
            )
        return pd.merge(all_dates_df, df, on=date_column, how='left')

    if expected_num_records > len(df.index):
        logging.info(f"Adding {freq} intervals.")
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
            f"be respected ({len(df.index)} rows, {expected_num_records} slots). "
            f"Merging left to reduce records. Check data source",
        )
        merged_df = merge_left(df)
        return merged_df

def expand_tz_columns(df: pd.DataFrame, vectorized: bool = False) -> pd.DataFrame:
    """
    For every column whose non-null cells include tz-aware Timestamps,
    expand into three columns:

      - the original column with tz info stripped (so CSV / Excel can
        round-trip it)
      - {col}_TZ          -- pandas StringDtype, holds the tz name per
                             row (pd.NA for cells that weren't tz-aware)
      - {col}_UTC_naive   -- datetime64[ns] holding the UTC-normalised
                             naive datetime (pd.NaT otherwise)

    This exists because Excel / CSV cannot store tz-aware Timestamps,
    and object columns in real parquets sometimes hold per-row varying
    tzinfo (different households / different ingest sources). The
    default behaviour walks each non-null cell individually so per-row
    variation is preserved exactly.

    Parameters
    ----------
    df : pd.DataFrame
    vectorized : bool, default False
        Default (False): per-cell inspection of object columns. Correct
        in all cases, including per-row varying tzinfo.
        True: opt-in fast path. Raises ValueError on object columns
        whose non-null cells are not all tz-aware Timestamps with the
        same tzinfo. Loud failure beats a silent wrong answer. Proper
        datetime64 dtypes are uniform by construction and always use
        the vectorised path regardless of this flag.

    Returns
    -------
    pd.DataFrame
        New DataFrame with expanded columns. Untouched columns are
        preserved unchanged.
    """
    out_cols: dict = {}
    for col in df.columns:
        s = df[col]

        # Proper datetime64 dtype: uniform by construction, vectorise.
        if pd.api.types.is_datetime64_any_dtype(s):
            tz = getattr(s.dt, "tz", None)
            if tz is None:
                out_cols[col] = s
                continue
            tz_name = str(tz)
            utc_naive = s.dt.tz_convert("UTC").dt.tz_localize(None)
            tz_col = pd.Series(
                [tz_name if not pd.isna(v) else pd.NA for v in s],
                index=s.index, dtype="string", name=f"{col}_TZ",
            )
            out_cols[col] = utc_naive.rename(col)
            out_cols[f"{col}_TZ"] = tz_col
            out_cols[f"{col}_UTC_naive"] = utc_naive.rename(f"{col}_UTC_naive")
            continue

        if s.dtype != object:
            out_cols[col] = s
            continue

        non_null = s.dropna()
        if non_null.empty:
            out_cols[col] = s
            continue

        # Per-cell probe: which non-null cells are tz-aware Timestamps?
        is_tz_dt = non_null.map(
            lambda v: isinstance(v, pd.Timestamp) and v.tzinfo is not None
        )
        if not is_tz_dt.any():
            out_cols[col] = s
            continue
        tz_aware_index = is_tz_dt[is_tz_dt].index

        if vectorized:
            non_ts = [v for v in non_null if not isinstance(v, pd.Timestamp)]
            if non_ts:
                raise ValueError(
                    f"Column {col!r}: vectorized=True requires every non-null "
                    f"cell to be a Timestamp; found {len(non_ts)} non-Timestamp "
                    "values. Use the default vectorized=False path."
                )
            naive_ts = [v for v in non_null if v.tzinfo is None]
            if naive_ts:
                raise ValueError(
                    f"Column {col!r}: vectorized=True requires every Timestamp "
                    f"to be tz-aware; found {len(naive_ts)} naive Timestamps. "
                    "Use the default vectorized=False path."
                )
            tzinfos = {str(v.tzinfo) for v in non_null}
            if len(tzinfos) > 1:
                raise ValueError(
                    f"Column {col!r}: vectorized=True requires uniform tzinfo "
                    f"across all non-null cells; found {len(tzinfos)} distinct "
                    f"tz values: {sorted(tzinfos)}. Use the default "
                    "vectorized=False path."
                )
            tz_name = next(iter(tzinfos))
            converted = pd.to_datetime(s, utc=True, errors="raise").dt.tz_localize(None)
            tz_values = [tz_name if not pd.isna(v) else pd.NA for v in s]
            out_cols[col] = converted.rename(col)
            out_cols[f"{col}_TZ"] = pd.Series(
                tz_values, index=s.index, dtype="string", name=f"{col}_TZ",
            )
            out_cols[f"{col}_UTC_naive"] = converted.rename(f"{col}_UTC_naive")
            continue

        # Per-row safe path. Batch-assign so pandas does the heavy lifting.
        stripped = s.copy()
        new_stripped_values = [s.loc[idx].tz_localize(None) for idx in tz_aware_index]
        stripped.loc[tz_aware_index] = new_stripped_values

        tz_values = [pd.NA] * len(s)
        utc_values = [pd.NaT] * len(s)
        positions = {idx: pos for pos, idx in enumerate(s.index)}
        for idx in tz_aware_index:
            v = s.loc[idx]
            pos = positions[idx]
            tz_values[pos] = str(v.tzinfo)
            utc_values[pos] = v.tz_convert("UTC").tz_localize(None)
        out_cols[col] = stripped
        out_cols[f"{col}_TZ"] = pd.Series(
            pd.array(tz_values, dtype="string"),
            index=s.index, name=f"{col}_TZ",
        )
        out_cols[f"{col}_UTC_naive"] = pd.Series(
            pd.array(utc_values, dtype="datetime64[ns]"),
            index=s.index, name=f"{col}_UTC_naive",
        )
    return pd.DataFrame(out_cols)


def apply_thresholds_to_df(
    df,
    return_stats=False,
):
    """
    Apply thresholds to columns and update imputation flags.

    This function applies lower and upper bounds to a column in the
    DataFrame. Values outside these bounds are replaced with pd.NA.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame containing the data.
    return_stats : bool, optional
        If True, also return a dict describing what each column lost to
        the threshold pass. Default False (returns only the DataFrame,
        backward-compatible with existing callers).

    Returns
    -------
    pandas.DataFrame
        The DataFrame with thresholds applied (when return_stats is False).
    tuple(pandas.DataFrame, dict)
        When return_stats is True: (df, stats) where stats maps each
        affected column name to {"n_below", "n_above", "min_removed",
        "max_removed"}. Columns that lost no values are omitted.
    """

    stats = {}
    for col in df.columns:
        if col in thresholds_dict:
            df, col_stats = apply_threshold_to_col(
                df=df,
                col=col,
                lower_bound=thresholds_dict[col]["Min"],
                upper_bound=thresholds_dict[col]["Max"]
            )
            if col_stats is not None:
                stats[col] = col_stats

    if return_stats:
        return df, stats
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
    tuple(pandas.DataFrame, dict or None)
        The DataFrame with thresholds applied, and a stats dict describing
        what was removed: {"n_below", "n_above", "min_removed",
        "max_removed"}. The stats are computed from the same out-of-bounds
        mask used to null the values (no extra column scan). Returns None
        for the stats when the column lost no values.

    Notes
    -----
    This function modifies the input DataFrame in-place and also returns it.
    Values outside the thresholds are replaced with pd.NA. min_removed /
    max_removed are taken from the out-of-bounds values *before* they are
    nulled, so they record how far past the bounds the data reached.
    """
    flags = bound_flags(df[col], lower_bound, upper_bound)
    mask = flags != MappingFlag.NONE.value

    n_below = int((flags & MappingFlag.OUT_OF_BOUNDS_LOW.value != 0).sum())
    n_above = int((flags & MappingFlag.OUT_OF_BOUNDS_HIGH.value != 0).sum())

    if n_below or n_above:
        removed = df.loc[mask, col]
        col_stats = {
            "n_below": n_below,
            "n_above": n_above,
            "min_removed": float(removed.min()),
            "max_removed": float(removed.max()),
            # The bounds actually checked. pd.NA means that side was not set,
            # so its count is "not checked", not "nothing found".
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
        }
    else:
        col_stats = None

    df.loc[mask, col] = pd.NA

    return df, col_stats


# Sidecar written next to the per-household mapped parquets (one row per
# HuisIdBSV + column that lost values in the threshold pass).
THRESHOLD_REMOVALS_FILENAME = "mapping_threshold_removals.parquet"

# Nullable dtypes (ADR-005) for the sidecar columns.
_THRESHOLD_REMOVAL_DTYPES = {
    "HuisIdBSV": "Int64",
    "column": "string",
    "n_below": "Int64",
    "n_above": "Int64",
    "min_removed": "Float64",
    "max_removed": "Float64",
    "lower_bound": "Float64",
    "upper_bound": "Float64",
}


def threshold_stats_to_rows(household_id, threshold_stats):
    """
    Flatten one household's threshold_stats dict into sidecar rows.

    Parameters
    ----------
    household_id : int
        The household's HuisIdBSV.
    threshold_stats : dict
        {column: {"n_below", "n_above", "min_removed", "max_removed",
        "lower_bound", "upper_bound"}} as produced by
        apply_thresholds_to_df(return_stats=True). May be empty or None.

    Returns
    -------
    list of dict
        One row per column that lost values; empty when nothing was removed.
    """
    rows = []
    for col, s in (threshold_stats or {}).items():
        rows.append(
            {
                "HuisIdBSV": household_id,
                "column": col,
                "n_below": s["n_below"],
                "n_above": s["n_above"],
                "min_removed": s["min_removed"],
                "max_removed": s["max_removed"],
                "lower_bound": s["lower_bound"],
                "upper_bound": s["upper_bound"],
            }
        )
    return rows


def write_threshold_removals(
    rows, mapped_folder_path, filename=THRESHOLD_REMOVALS_FILENAME
):
    """
    Write accumulated threshold-removal rows to a sidecar parquet.

    One row per (HuisIdBSV, column) that lost values in the threshold pass.
    An empty but correctly-typed frame is written when no values were removed,
    so downstream readers get a stable schema either way.

    Returns the path written.
    """
    df = pd.DataFrame(rows, columns=list(_THRESHOLD_REMOVAL_DTYPES)).astype(
        _THRESHOLD_REMOVAL_DTYPES
    )
    path = os.path.join(mapped_folder_path, filename)
    df.to_parquet(path, engine="pyarrow")
    logging.info(
        f"[mapping_helpers] Wrote {len(df)} threshold-removal row(s) to {path}"
    )
    return path


CADENCE_FILENAME = "mapping_column_cadence.parquet"


def cadence_to_rows(household_id, cadence_df):
    """
    Flatten one household's cadence table into sidecar rows.

    Parameters
    ----------
    household_id : int
        The household's HuisIdBSV.
    cadence_df : pd.DataFrame or None
        The output of detect_column_cadence. May be empty or None.

    Returns
    -------
    list of dict
        One row per column measured; empty when nothing was measured.
    """
    if cadence_df is None or cadence_df.empty:
        return []
    rows = []
    for record in cadence_df.to_dict(orient="records"):
        rows.append({"HuisIdBSV": household_id, **record})
    return rows


def write_column_cadence(rows, mapped_folder_path, filename=CADENCE_FILENAME):
    """
    Write accumulated per-column cadence rows to a sidecar parquet.

    One row per (HuisIdBSV, column). Cadence is measured per household because
    it varies between them -- a sensor swap or a partial year can change how
    often one household's column reports without affecting any other.

    An empty but correctly-typed frame is written when nothing was measured, so
    downstream readers get a stable schema either way.

    Returns the path written.
    """
    dtypes = {"HuisIdBSV": "Int64", **_CADENCE_TABLE_DTYPES}
    df = pd.DataFrame(rows, columns=list(dtypes)).astype(dtypes)
    path = os.path.join(mapped_folder_path, filename)
    df.to_parquet(path, engine="pyarrow")
    logging.info(
        f"[mapping_helpers] Wrote {len(df)} column-cadence row(s) to {path}"
    )
    return path


def save_household_shard(df, household_id, household_batch_id, sharded_folder_path):
    """
    Write one household's mapped data as a hive-partitioned shard:
    ``<sharded_folder_path>/HuisIdBSV=<n>/HuisBatchIdBSV=<p>/part.parquet``.

    Sharding originates at mapping (etdmap), not in a downstream combine step. The
    partition columns (the ids) live in the PATH, not the file -- hive convention,
    and consistent with the flat mapped file which also carries no id columns.

    Per-household and idempotent: re-running one household overwrites only its shard
    (exactly one part file, never stale duplicates), so a single changed household
    can be re-mapped without touching the rest. Different batches of the same
    household coexist as sibling ``HuisBatchIdBSV=*`` partitions.

    Parameters
    ----------
    df : pd.DataFrame
        The mapped household frame (ReadingDate + data columns, nullable dtypes).
    household_id : int
        HuisIdBSV -- the household id (the flat pipeline's ``household_id``).
    household_batch_id : int
        HuisBatchIdBSV (equals HuisIdBSV in the initial 1:1 state).
    sharded_folder_path : str or Path
        Root of the sharded mapped dataset.

    Returns
    -------
    str
        Path to the written part file.
    """
    part_dir = os.path.join(
        str(sharded_folder_path),
        f"HuisIdBSV={int(household_id)}",
        f"HuisBatchIdBSV={int(household_batch_id)}",
    )
    # Idempotent per (household, batch): clear any prior part files for this shard
    # so a re-run leaves exactly one file, never stale duplicates.
    if os.path.isdir(part_dir):
        for name in os.listdir(part_dir):
            if name.endswith(".parquet"):
                os.remove(os.path.join(part_dir, name))
    os.makedirs(part_dir, exist_ok=True)
    out_path = os.path.join(part_dir, "part.parquet")
    df.to_parquet(out_path, engine="pyarrow")
    logging.info(f"[mapping_helpers] Wrote household shard -> {out_path}")
    return out_path


def save_mapped_household(df, household_id, mapped_folder_path) -> str:
    """
    Save one household's mapped data in the configured output format -- THE
    single save primitive for all mappers.

    Honours ``etdmap.options.mapped_output_format``:
    - "sharded" (default): hive shard under
      ``<mapped_folder_path>/sharded/HuisIdBSV=<n>/HuisBatchIdBSV=<p>/``
      (this writer assigns HuisBatchIdBSV = HuisIdBSV; mapping a household
      that appears in more than one batch is not yet supported);
    - "flat": the legacy ``household_<n>_table.parquet`` (tests / legacy escape
      hatch via the map scripts' --flat flag).

    run_standard_pipeline calls this internally; mapper scripts that write
    household output directly (e.g. map_watch_e) MUST use this instead of a raw
    ``to_parquet`` -- a direct write silently bypasses the format branch (this is
    exactly how Watch-E produced zero shards on the first real sharded run).

    Returns the path written.
    """
    output_format = etdmap.options.mapped_output_format
    if output_format == "flat":
        path = os.path.join(
            str(mapped_folder_path), f"household_{int(household_id)}_table.parquet"
        )
        df.to_parquet(path, engine="pyarrow")
        return path
    if output_format == "sharded":
        return save_household_shard(
            df,
            household_id=int(household_id),
            household_batch_id=int(household_id),
            sharded_folder_path=os.path.join(str(mapped_folder_path), "sharded"),
        )
    raise ValueError(
        f"[save_mapped_household] Unknown mapped_output_format {output_format!r}; "
        f"expected 'sharded' or 'flat'."
    )


_HOUSEHOLD_FILE_RE = re.compile(r"^household_(\d+)_table\.parquet$")
_HOUSEID_DIR_RE = re.compile(r"^HuisIdBSV=(\d+)$")
_HOUSEBATCHID_DIR_RE = re.compile(r"^HuisBatchIdBSV=(\d+)$")


def prune_stale_household_files(mapped_folder_path, keep_household_ids):
    """
    Delete flat mapped household files whose HuisIdBSV is KNOWN stale -- i.e. not
    in ``keep_household_ids`` (the current authoritative household set, from the index).

    Only files matching ``household_<n>_table.parquet`` are considered; ``index.parquet``
    and any other content are never touched. This is a targeted delete-with-manifest,
    NOT a blanket glob-delete: it returns a manifest (one dict per removal with
    ``HuisIdBSV`` + ``path``) so the caller can log/audit exactly what was removed.

    Parameters
    ----------
    mapped_folder_path : str or Path
        Folder holding the flat mapped household files.
    keep_household_ids : iterable of int
        HuisIdBSV values that SHOULD exist; everything else matching the pattern is
        removed.

    Returns
    -------
    list of dict
        Manifest of removed files: ``[{"HuisIdBSV": n, "path": ...}, ...]``.
    """
    keep = {int(h) for h in keep_household_ids}
    folder = str(mapped_folder_path)
    removed = []
    if not os.path.isdir(folder):
        return removed
    for name in sorted(os.listdir(folder)):
        m = _HOUSEHOLD_FILE_RE.match(name)
        if not m:
            continue
        hid = int(m.group(1))
        if hid in keep:
            continue
        path = os.path.join(folder, name)
        os.remove(path)
        removed.append({"HuisIdBSV": hid, "path": path})
        logging.info(f"[mapping_helpers] Pruned stale household file: {path}")
    return removed


def prune_stale_household_shards(sharded_folder_path, keep_pairs):
    """
    Delete sharded household partitions whose ``(HuisIdBSV, HuisBatchIdBSV)`` is KNOWN
    stale -- i.e. not in ``keep_pairs`` (the current authoritative set from
    the index).

    Only ``HuisIdBSV=<n>/HuisBatchIdBSV=<p>`` partition directories are considered;
    other content is never touched. A ``HuisIdBSV=<n>`` parent is removed once its last
    remaining batch partition is pruned. Targeted delete-with-manifest, not a blanket
    wipe: returns a manifest (one dict per removal with ``HuisIdBSV`` + ``HuisBatchIdBSV``
    + ``path``).

    Parameters
    ----------
    sharded_folder_path : str or Path
        Root of the sharded mapped dataset.
    keep_pairs : iterable of (int, int)
        ``(HuisIdBSV, HuisBatchIdBSV)`` pairs that SHOULD exist.

    Returns
    -------
    list of dict
        Manifest of removed shards.
    """
    keep = {(int(a), int(b)) for (a, b) in keep_pairs}
    root = str(sharded_folder_path)
    removed = []
    if not os.path.isdir(root):
        return removed
    for hid_name in sorted(os.listdir(root)):
        hid_m = _HOUSEID_DIR_RE.match(hid_name)
        if not hid_m:
            continue
        hid_dir = os.path.join(root, hid_name)
        if not os.path.isdir(hid_dir):
            continue
        hid = int(hid_m.group(1))
        for hbid_name in sorted(os.listdir(hid_dir)):
            hbid_m = _HOUSEBATCHID_DIR_RE.match(hbid_name)
            if not hbid_m:
                continue
            hbid = int(hbid_m.group(1))
            if (hid, hbid) in keep:
                continue
            part_dir = os.path.join(hid_dir, hbid_name)
            shutil.rmtree(part_dir)
            removed.append({"HuisIdBSV": hid, "HuisBatchIdBSV": hbid, "path": part_dir})
            logging.info(f"[mapping_helpers] Pruned stale shard: {part_dir}")
        # Drop the HuisIdBSV=<n> parent if pruning emptied it.
        if os.path.isdir(hid_dir) and not os.listdir(hid_dir):
            os.rmdir(hid_dir)
    return removed


def run_standard_pipeline(
    df: pd.DataFrame,
    household_id: int,
    household_id_supplier: str,
    mapped_folder_path,
    context: str = "",
) -> dict:
    """
    Standard per-household processing pipeline shared by all ETD supplier mappers.

    Call this after all supplier-specific preprocessing is complete: column rename,
    unit conversion, ReadingDate derived, identifier columns dropped, and any
    supplier-specific grid snapping (e.g. snap_readings_to_grid).

    Steps:
      1. Validate ReadingDate present — raise KeyError if missing
      2. Coerce ReadingDate to datetime if not already; raise ValueError if any rows fail
      3. Sort by ReadingDate
      4. ensure_intervals
      5. Log model column type mismatches at DEBUG
      6. rearrange_model_columns(add_columns=True)
      7. add_diff_columns
      8. Apply record_flag_conditions (try/except per flag; pd.NA on error)
      9. apply_thresholds_to_df
     10. Save to {mapped_folder_path}/household_{household_id}_table.parquet

    Note: fill_down_infrequent_devices is intentionally NOT part of this pipeline.
    Filling down is a supplier-specific imputation choice — for some data sources
    (e.g. cumulative meter readings) filling down is wrong and linear interpolation
    or other strategies are needed. Apply it explicitly in the supplier mapper before
    calling this function.

    Parameters
    ----------
    df : pd.DataFrame
        Household time-series data with BSV column names.
    household_id : int
        BSV household ID (HuisIdBSV).
    household_id_supplier : str
        Supplier household ID (HuisIdLeverancier).
    mapped_folder_path : str or Path
        Destination folder for the processed parquet file.
    context : str
        Optional log prefix, e.g. '{household_id_supplier}/{household_id}'.

    Returns
    -------
    dict
        {'HuisIdLeverancier': household_id_supplier, 'HuisIdBSV': household_id}
    """
    ctx = f"{context}: " if context else ""
    new_file_path = os.path.join(
        mapped_folder_path, f"household_{household_id}_table.parquet"
    )
    logging.info(
        f"[run_standard_pipeline] {ctx}Processing household -> {new_file_path}"
    )

    # ------------------------------------------------------------------
    # 1. Validate ReadingDate present
    # ------------------------------------------------------------------
    if "ReadingDate" not in df.columns:
        raise KeyError(
            f"[run_standard_pipeline] {ctx}'ReadingDate' column not found. "
            f"Available columns: {list(df.columns)}. "
            f"Ensure derive_and_normalize_reading_date() (or equivalent) was called before this function."
        )

    # ------------------------------------------------------------------
    # 2. Coerce ReadingDate to datetime
    # ------------------------------------------------------------------
    if not pd.api.types.is_datetime64_any_dtype(df["ReadingDate"]):
        logging.debug(f"[run_standard_pipeline] {ctx}Coercing 'ReadingDate' to datetime.")
        df = df.copy()
        df["ReadingDate"] = pd.to_datetime(df["ReadingDate"], errors="coerce")
        n_bad = int(df["ReadingDate"].isna().sum())
        if n_bad > 0:
            raise ValueError(
                f"[run_standard_pipeline] {ctx}{n_bad} ReadingDate value(s) could not be "
                f"parsed. Every row must have a valid ReadingDate."
            )

    # ------------------------------------------------------------------
    # 3. Sort by ReadingDate
    # ------------------------------------------------------------------
    df = df.sort_values("ReadingDate").reset_index(drop=True)
    logging.debug(
        f"[run_standard_pipeline] {ctx}Sorted by ReadingDate. "
        f"Range: {df['ReadingDate'].min()} to {df['ReadingDate'].max()}"
    )

    # ------------------------------------------------------------------
    # 4. Ensure 5-minute intervals
    # ------------------------------------------------------------------
    df = ensure_intervals(df)

    # ------------------------------------------------------------------
    # 5. Log model column type mismatches (DEBUG)
    # ------------------------------------------------------------------
    for column in df.columns:
        if column in model_column_type and df[column].dtype != model_column_type[column]:
            logging.debug(
                f"[run_standard_pipeline] {ctx}Column '{column}' "
                f"has dtype {df[column].dtype}, expected {model_column_type[column]}."
            )
    for column in model_column_type:
        if column not in df.columns:
            logging.debug(
                f"[run_standard_pipeline] {ctx}Model column '{column}' "
                f"not present — will be added as NaN by rearrange_model_columns."
            )

    # ------------------------------------------------------------------
    # 6. Rearrange columns to match ETD model order; add missing as NaN
    # ------------------------------------------------------------------
    df = rearrange_model_columns(household_df=df, add_columns=True, context=context)

    # ------------------------------------------------------------------
    # 7. Add diff columns (5-minute differences for cumulative variables)
    # ------------------------------------------------------------------
    df = add_diff_columns(df, context=context)

    # ------------------------------------------------------------------
    # 8. Apply record-level validation flags
    # ------------------------------------------------------------------
    for flag, condition in record_flag_conditions.items():
        df[flag] = condition(df)

    # Count flag failures per category (False = failed validation)
    def _count_failures(flag_dict):
        return {
            flag: int((df[flag] == False).sum())  # noqa: E712
            for flag in flag_dict
            if flag in df.columns and (df[flag] == False).any()  # noqa: E712
        }

    flag_counts = {
        "record_quality":   _count_failures(record_quality_flag_conditions),
        "momentaan":        _count_failures(momentaan_flag_conditions),
        "cumulative_diff":  _count_failures(cumulative_diff_flag_conditions),
    }

    # ------------------------------------------------------------------
    # 9. Apply threshold filters -- capture removal stats in the same pass
    # ------------------------------------------------------------------
    df, threshold_stats = apply_thresholds_to_df(df, return_stats=True)
    # Per-column count of values nulled (n_below + n_above), kept for the
    # existing DEBUG summary line and _validation_summary consumers.
    threshold_counts = {
        c: s["n_below"] + s["n_above"] for c, s in threshold_stats.items()
    }

    # Per-household summary at DEBUG (full detail in log file)
    _all_flag_failures = {k: v for cat in flag_counts.values() for k, v in cat.items()}
    if _all_flag_failures or threshold_counts:
        _parts = []
        if _all_flag_failures:
            _parts.append("flags: " + ", ".join(f"{k}={v}" for k, v in sorted(_all_flag_failures.items())))
        if threshold_counts:
            _parts.append("thresholds: " + ", ".join(f"{k}={v}" for k, v in sorted(threshold_counts.items())))
        logging.debug(f"[run_standard_pipeline] {ctx}Validation: " + "; ".join(_parts))

    # ------------------------------------------------------------------
    # 10. Save via the single format-aware primitive (sharded default; flat is
    #     the legacy escape hatch)
    # ------------------------------------------------------------------
    saved_path = save_mapped_household(df, household_id, mapped_folder_path)
    logging.info(
        f"[run_standard_pipeline] {ctx}Saved to {saved_path} "
        f"({len(df)} rows, {len(df.columns)} columns)"
    )

    return {
        "HuisIdLeverancier": household_id_supplier,
        "HuisIdBSV": household_id,
        "_validation_summary": {
            "flags": flag_counts,
            "thresholds": threshold_counts,
            "threshold_stats": threshold_stats,
        },
    }
