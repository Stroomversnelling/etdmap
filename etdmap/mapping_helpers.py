import logging
from datetime import datetime
import os
import re
import shutil
from typing import NamedTuple

import pandas as pd

import etdmap

from etdmap.data_model import (
    # cadence_minutes_by_column lives in data_model, beside cadence_lookup_names
    # which it uses, so record_validators can reach it too. Re-exported here for
    # callers that import it from this module.
    cadence_minutes_by_column,  # noqa: F401
    dataset_cadence_minutes,
    resolve_bounds,
    complement_column,
    cumulative_column,
    cumulative_columns,
    diff_columns,
    interval_column,
    model_column_order,
    model_column_type,
    monotonic_columns,
    performance_column_dtypes,
    required_performance_data_columns,
    tariff_root_to_splits,
    thresholds_dict,
)
from etdmap.index_helpers import (
    get_mapped_data,
    grid_minutes_for_household,
    read_index,
    supported_kwargs,
)
from etdmap.flags import (
    _SPAN_DTYPES,
    MappingFlag,
    assert_flag_history_complete,
    bound_flags,
    frozen_spans,
    flag_column,
    mark_undelivered_slots,
    record_delivery,
    scale_bound_to_cadence,
    set_flags,
    threshold_base_minutes,
)
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
                    # errors='coerce' removes a value it cannot parse, so what
                    # held a value before and holds none after was delivered
                    # and discarded here.
                    held_a_value = household_df[col].notna()

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

                    unparseable = held_a_value & household_df[col].isna()
                    n_unparseable = int(unparseable.sum())
                    if n_unparseable:
                        # The signal says why the value went, the action says it
                        # went. Every other removal site records both.
                        set_flags(
                            household_df, col, unparseable,
                            MappingFlag.UNPARSEABLE_VALUE,
                        )
                        set_flags(
                            household_df, col, unparseable, MappingFlag.NULLED,
                        )
                        logging.warning(
                            f"{context}Column '{col}': {n_unparseable} value(s) "
                            f"could not be read as '{expected_type}' and were "
                            f"removed. Flagged NULLED, so the count is "
                            f"recoverable per cell from the mapped artifact."
                        )

                except Exception as e:
                    logging.error(f"{context}Failed to coerce column '{col}' type: {e}")
                    raise ValueError(f"{context}Failed to coerce column '{col}' type: {e!s}")  # noqa: B904

    # Membership of the data model is the only test: any model column is valid
    # delivered data whatever its Entiteit. add_columns materialises the empty
    # placeholders a mapped file must carry; other model columns are kept when
    # delivered and not created here.
    if add_columns:
        for col in model_column_order:
            if col not in household_df.columns:
                household_df[col] = pd.array(
                    [pd.NA] * len(household_df),
                    dtype=performance_column_dtypes.get(col, "Float64"),
                )

    # A findings column belongs to the column it describes and is kept on the
    # same terms. Mappers fill values before calling this function, so dropping
    # the findings columns here would discard the record of which values were
    # written by the pipeline rather than delivered.
    keep = []
    for col in performance_column_dtypes:
        if col in household_df.columns:
            keep.append(col)
        findings = flag_column(col)
        if findings in household_df.columns:
            keep.append(findings)

    household_df = household_df[keep]

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

                # A cumulative variable that has recorded a total cannot return to zero.
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

    # A cumulative variable that has recorded a total cannot return to zero. Leading zeros
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
    Record what *cases* found and set the readings it condemns to pd.NA.

    Modifies *group* in place, with a single assignment to *col* at the end.
    Every finding in *cases* is written to the findings column of *col*, on the
    reading it was observed on, whether or not that reading is removed. The four
    decrease classifications then resolve to three deletion scopes:

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
        ``col`` and the interval column linked to it.
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
        diff_vals = group.iloc[si_reset][interval_column(col)]
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

    # The classification itself, recorded on the reading it describes. Without it
    # the output says a reading was removed and never which fault condemned it,
    # and a fault that removes nothing goes unrecorded altogether. The findings
    # do not share the deletion scopes above: a no-recovery dip condemns every
    # later reading but is observed on one, and a reset whose interval is NA is
    # observed without any reading being removed.
    # cases.flags is aligned to the readings that carried a value, so it is
    # reindexed back onto the whole group; a reading the classifier never saw has
    # no finding.
    findings = cases.flags.reindex(group.index, fill_value=MappingFlag.NONE.value)
    for flag in MappingFlag.CUMULATIVE_UNUSABLE:
        set_flags(group, col, (findings & flag.value) != 0, flag)

    _removed = pd.Series(na_mask.values, index=group.index) & group[col].notna()
    set_flags(group, col, _removed, MappingFlag.NULLED)
    group.loc[group.index[na_mask.values], col] = pd.NA

    # The flags say which readings were condemned and why; they cannot say how
    # far the register went back, and a decrease of 5 reads the same as one of
    # 5000. The dip magnitudes are the only measure of what the correction
    # removed, and they exist nowhere else once the readings are blanked.
    dips = cases.readings[col + 'Diff_no_gap']
    dips = dips[(dips < 0).fillna(False)]
    return {
        "n_dips": int(len(dips)),
        "n_readings_removed": int(_removed.sum()),
        "sum_dip_magnitude": float(dips.sum()) if len(dips) else 0.0,
        "min_dip": float(dips.min()) if len(dips) else pd.NA,
        "max_dip": float(dips.max()) if len(dips) else pd.NA,
        "n_jump_back": int(selected(MappingFlag.CUMULATIVE_JUMP_BACK).sum()),
        "n_repeat_negative": int(repeat_negative.sum()),
        "n_meter_reset": int(meter_reset.sum()),
        "n_no_recovery": int(no_recovery.sum()),
        "n_zero_total": int(
            selected(MappingFlag.CUMULATIVE_ZERO_TOTAL).sum()
        ),
    }


def _carry_findings_to_intervals(group, col) -> None:
    """
    Carry a cumulative column's findings onto the interval column derived from it.

    An interval is the difference between two readings, so it inherits the
    findings of both: an interval is no more measured than the readings it comes
    from. Only the findings naming a value the pipeline wrote are carried;
    findings about a delivered reading, such as a decrease, describe that reading
    rather than the intervals around it.

    Downstream stages filter interval columns, so a finding recorded only on the
    cumulative column would not reach them.

    Modifies *group* in place.
    """
    source = flag_column(col)
    if source not in group.columns:
        return

    diff_col = interval_column(col)
    if diff_col is None:
        return

    flags = group[source].fillna(MappingFlag.NONE.value)
    for flag in MappingFlag.INVENTED:
        on = (flags & flag.value) != 0
        set_flags(group, diff_col, on | on.shift(1, fill_value=False), flag)


def combine_tariff_registers(df, context: str = "") -> pd.DataFrame:
    """Combine delivered Hoog/Laag tariff registers into their root column.

    Peak and off-peak registers are two halves of one quantity: the root is
    their sum wherever either register delivered, and NA only where both are
    missing -- the same semantics the stats-side synthesis uses, applied per
    cell. A cell where the root itself was delivered is never overwritten:
    the delivered root wins and the cell is untouched.

    Every cell this function writes is flagged TARIFF_COMBINED. Like a
    constructed cumulative, the value carries what the delivered registers
    reported, so the flag is an action without being INVENTED.

    An interval in which BOTH registers advanced is flagged
    TARIFF_SIMULTANEOUS on the INTERVAL root, whether or not the root cell
    was delivered: tariff registers alternate, so consumption on both
    within one interval is a suspicious delivered pattern. Observation
    only; nothing is removed.

    Alternation is an INTERVAL property and the flag belongs only to the
    interval form. Testing "both registers nonzero" on the CUMULATIVE
    registers is meaningless -- a cumulative reading is a running total, so
    both tariff counters hold nonzero totals essentially always. Measured on
    the 2026-10-01 estate remap, that test was true for 103,001 of 103,001
    delivered readings on one household (100%) and fired on 18.5M Watch-E
    and 11.3M Factory Zero cells, while the interval test was true for 277
    of ~74,400 consuming intervals (0.37%). So the cumulative root cell
    never carries this signal: flagging it would re-create an always-on bit
    that says nothing. Where only cumulative registers are delivered, the
    interval each one consumed is its advance between readings, which is
    what the test uses -- and the flag still lands on the interval root.
    The surviving 0.37% are the readings worth looking at; an interval in
    which the tariff switch fell part-way through would advance both
    registers legitimately, which is the likeliest benign explanation but
    is not established here.

    Covers the cumulative pairs and the interval (Diff) pairs alike. Call it
    BEFORE ``add_diff_columns`` so a combined cumulative root produces its
    Diff naturally; interval splits a supplier delivers directly combine
    into the Diff root the same way.

    Modifies *df* in place and returns it.
    """
    from etdmap.data_model import (
        cumulative_column,
        interval_column,
        tariff_root_to_splits,
    )

    ctx = f"{context}: " if context else ""
    for root, (hoog, laag) in tariff_root_to_splits.items():
        hoog_present = hoog in df.columns
        laag_present = laag in df.columns
        if not (hoog_present or laag_present):
            continue

        absent = pd.Series(pd.NA, index=df.index, dtype="Float64")
        hoog_num = (
            pd.to_numeric(df[hoog], errors="coerce").astype("Float64")
            if hoog_present else absent
        )
        laag_num = (
            pd.to_numeric(df[laag], errors="coerce").astype("Float64")
            if laag_present else absent
        )
        either = hoog_num.notna() | laag_num.notna()
        if not bool(either.any()):
            continue

        combined = (
            (hoog_num.fillna(0) + laag_num.fillna(0))
            .mask(~either, pd.NA)
            .astype("Float64")
        )

        if root not in df.columns:
            df[root] = pd.Series(pd.NA, index=df.index, dtype="Float64")
            # The supplier sent nothing under this name, so every cell of it
            # answers DELIVERED_NA. Recorded here because this is where the
            # column comes into being: mark_delivered ran on the delivered
            # frame, which had no such column to answer for, and a column with
            # no delivery answer is indistinguishable from one no step reached.
            # Before the combine writes into it, so the answer describes what
            # the supplier sent rather than what this function wrote.
            record_delivery(df, [root])
        delivered = df[root].notna()
        write = either & ~delivered
        if bool(write.any()):
            df.loc[write, root] = combined[write]
            set_flags(df, root, write, MappingFlag.TARIFF_COMBINED)
            logging.info(
                f"{ctx}Combined '{hoog}'/'{laag}' into '{root}' on "
                f"{int(write.sum())} row(s) "
                f"({int(delivered.sum())} delivered root cell(s) untouched)."
            )

        # The interval form carries the alternation signal (see the
        # docstring): this pair is either already the interval form, or a
        # cumulative pair whose interval root is its linked Diff column.
        if cumulative_column(root) is not None:
            interval_root = root
            hoog_step, laag_step = hoog_num, laag_num
        else:
            interval_root = interval_column(root)
            if interval_root is None:
                continue
            # The interval each register consumed is its advance between
            # readings. The first reading has no predecessor, which matches
            # add_diff_columns seeding the first interval rather than
            # measuring it.
            hoog_step = hoog_num.diff().round(10)
            laag_step = laag_num.diff().round(10)

        # Both ADVANCED, not both nonzero: a negative step is a meter reset
        # or a correction case, which the CUMULATIVE_* findings already
        # record, and conflating the two would hide both.
        simultaneous = (hoog_step > 0).fillna(False) & (laag_step > 0).fillna(False)
        if bool(simultaneous.any()):
            set_flags(
                df, interval_root, simultaneous, MappingFlag.TARIFF_SIMULTANEOUS
            )
            logging.info(
                f"{ctx}'{hoog}' and '{laag}' both advanced within "
                f"{int(simultaneous.sum())} interval(s); flagged "
                f"TARIFF_SIMULTANEOUS on '{interval_root}'."
            )

    return df


def add_tariff_root_cadence(df, cadence, context: str = ""):
    """Record a cadence row for each tariff root built from delivered splits.

    Cadence is measured on the columns as delivered, which is before
    :func:`combine_tariff_registers` builds the root the two registers sum to.
    So the sidecar describes the quantity only under the names it arrived as,
    and the root -- the column the averaging and the fill work on, the splits
    being excluded from both -- has no row. Every reader asking what form the
    root was delivered in, or what interval its readings cover, then gets no
    answer for a register holding a full year of data.

    The root's cadence is its splits': it holds a value exactly where one of
    them did. Readings are counted on the combined column itself, so the count
    is the union of the two rather than either half.

    A root the supplier delivered directly keeps its own measured row. This
    fills in a root that has none, which is also what keeps the two cases
    distinguishable: ``cadence_source`` reads ``combined`` only for a row
    derived here.

    Raises
    ------
    ValueError
        When the two splits were measured at different cadences. The root has no
        single cadence then, and which one governs is a question about how that
        supplier delivers rather than something to settle by picking one here.

    Call AFTER :func:`combine_tariff_registers`, so the root column holds its
    final values, and before the pipeline, so the delivery marking sees the row
    too. Returns a new table; *cadence* is not modified.
    """
    from etdmap.data_model import tariff_root_to_splits

    if cadence is None or getattr(cadence, "empty", True):
        return cadence

    ctx = f"{context}: " if context else ""
    named = set(cadence["column"].dropna())
    rows = []
    for root, (hoog, laag) in tariff_root_to_splits.items():
        if root not in df.columns or root in named:
            continue
        split_rows = cadence[cadence["column"].isin([hoog, laag])]
        if split_rows.empty:
            continue

        minutes = sorted(
            {int(m) for m in split_rows["cadence_minutes"] if pd.notna(m)}
        )
        if len(minutes) > 1:
            raise ValueError(
                f"{ctx}'{hoog}' and '{laag}' were measured at different "
                f"cadences ({', '.join(cadence_label(m) for m in minutes)}), so "
                f"the '{root}' they combine into has no single cadence. Settle "
                f"how this supplier delivers the two registers in the supplier "
                f"mapping, then re-map."
            )
        if not minutes:
            continue

        detected = sorted(
            {int(d) for d in split_rows["cadence_detected"] if pd.notna(d)}
        )
        confidences = [
            float(c) for c in split_rows["confidence"] if pd.notna(c)
        ]
        rows.append({
            "column": root,
            "cadence_minutes": minutes[0],
            "cadence_source": "combined",
            # Only where the splits agree. Disagreeing measurements average to
            # a number neither register showed.
            "cadence_detected": detected[0] if len(detected) == 1 else pd.NA,
            # The weaker claim governs: the root is no better measured than the
            # less regular of the registers it came from.
            "confidence": min(confidences) if confidences else pd.NA,
            # Counted on the combined column, so a row either register reported
            # in counts once.
            "n_readings": int(df[root].notna().sum()),
        })
        logging.info(
            f"{ctx}Recorded '{root}' cadence {cadence_label(minutes[0])} from "
            f"'{hoog}'/'{laag}' ({rows[-1]['n_readings']} reading(s))."
        )

    if not rows:
        return cadence
    return pd.concat(
        [cadence, pd.DataFrame(rows).astype(_CADENCE_TABLE_DTYPES)],
        ignore_index=True,
    )


def _merge_dip_stats(a, b):
    """Combine two groups' negative-diff figures for one column."""
    out = {
        key: a[key] + b[key]
        for key in (
            "n_dips", "n_readings_removed", "sum_dip_magnitude",
            "n_jump_back", "n_repeat_negative", "n_meter_reset",
            "n_no_recovery", "n_zero_total",
        )
    }
    # The extremes are over all dips seen, so they take the outer value rather
    # than adding. pd.NA means no dip, which loses to any real figure.
    for key, pick in (("min_dip", min), ("max_dip", max)):
        values = [v for v in (a[key], b[key]) if not pd.isna(v)]
        out[key] = pick(values) if values else pd.NA
    return out


def add_diff_columns(
    data: pd.DataFrame,
    id_column: str = None,
    validate_func=validate_cumulative_variables,
    context: str = '',
    drop_unvalidated: bool = False,
    return_stats: bool = False,
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

    Required of the input
    ---------------------
    One reading per instant per id, and every ReadingDate present. An interval
    is the step between consecutive readings, and the correction locates the
    reading a dip recovers at by position in the sorted series.

    ``run_standard_pipeline`` raises on a duplicate or absent ReadingDate, and
    ``snap_readings_to_grid`` collapses sub-grid readings to one row per slot.

    Parameters
    ----------
    data : pd.DataFrame or pd.core.groupby.DataFrameGroupBy
    id_column : str, optional
    validate_func : callable, optional
    context : str, optional
    drop_unvalidated : bool, optional
    return_stats : bool, optional
        If True, also return a dict describing what the negative-diff
        correction removed from each column. Default False (returns only the
        DataFrame, so existing callers are unaffected).

    Returns
    -------
    pd.DataFrame
        The DataFrame with difference columns added (when return_stats is
        False).
    tuple of (pd.DataFrame, dict)
        When return_stats is True: the frame, and {column: figures} for each
        column a decrease was corrected in. Figures are ``n_dips``,
        ``n_readings_removed``, ``sum_dip_magnitude``, ``min_dip``,
        ``max_dip``, and a count per classification. Columns with no decrease
        are absent, so an empty dict means nothing was corrected.
    """
    if not context == '':
        context_string = context + ': '
    else:
        context_string = context

    data = data.sort_values('ReadingDate')

    # Written by the nested calculate_diff, which pandas calls once per group.
    dip_stats = {}

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

            # The interval column to write is the one the data model links to
            # this cumulative variable, which is not always its name plus
            # "Diff": ZonopwekBruto is the interval form of Zon-opwekTotaal.
            diff_col = interval_column(col)
            if diff_col is None:
                raise ValueError(
                    f"{context_string}'{col}' is a cumulative variable with no "
                    f"ComplementVariabele, so the interval column to write is "
                    f"unknown. Declare the link in the data model rather than "
                    f"deriving no interval values for it."
                )

            # A supplier may deliver the interval column itself. Derive only when
            # the cumulative column carries data (it stays authoritative), or when
            # there is no delivered interval column to preserve. An all-NA
            # cumulative beside a delivered interval column is the one case where
            # deriving would destroy a real measurement.
            _delivered_interval = (
                diff_col in group.columns and group[diff_col].notna().any()
            )
            if group[col].notna().any() or not _delivered_interval:
                logging.debug(f"{context_string}Calculating diff for {col}")
                _derived = group[col].diff().round(10)
                # Taken BEFORE the seed, or row 0 holds a value and the column
                # can no longer say the difference was null there. Row 0 always
                # falls in this mask: .diff() has no predecessor for it.
                _null_from_derivation = _derived.isna()
                group[diff_col] = _derived
                # No predecessor to difference against, so the first interval is
                # seeded rather than measured.
                group.loc[group.index[0], diff_col] = 0
                _seed = pd.Series(False, index=group.index)
                _seed.iloc[0] = True
                set_flags(group, diff_col, _seed, MappingFlag.FIRST_ZERO)
                # Every cell of the column was written by the assignment above,
                # so every one records what the differencing produced there --
                # a value, or nothing. Row 0 records the null outcome AND the
                # seed: both happened to it, and recording only the seed left
                # the derivation's visit unrecorded.
                #
                # Where the supplier delivered the interval too, this cell held
                # that value a moment ago and now holds the difference; the
                # RAW_DELIVERED_VALUE already on it is what makes the
                # replacement readable. Without an action here
                # mark_unchanged_values stamps NOT_CHANGED on the whole column,
                # claiming mapping left values it wrote in full.
                set_flags(
                    group, diff_col, ~_null_from_derivation,
                    MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE,
                )
                set_flags(
                    group, diff_col, _null_from_derivation,
                    MappingFlag.NULL_DERIVED_FROM_CUMULATIVE,
                )
            else:
                logging.info(
                    f"{context_string}'{col}' is empty and '{diff_col}' was "
                    f"delivered; keeping the delivered interval values."
                )

            # A decrease is only a fault in a variable that cannot decrease.
            # Elsewhere it is the reading, so neither detection nor correction
            # applies.
            if col not in monotonic_columns:
                _carry_findings_to_intervals(group, col)
                continue

            # Detection reads the column and writes nothing, so it runs for every
            # monotonic column. Only the correction is conditional, on what was
            # detected.
            cases = _detect_negative_diff_cases(group, col)
            if cases is not None:
                _col_stats = _apply_negative_diff_corrections(
                    group, col, cases, context_string,
                )
                # Accumulated across groups: one call may cover many households
                # when a groupby is passed, and a column may be corrected in
                # each of them.
                if _col_stats is not None:
                    _prior = dip_stats.get(col)
                    dip_stats[col] = (
                        _col_stats if _prior is None
                        else _merge_dip_stats(_prior, _col_stats)
                    )

                logging.debug(
                    f"{context_string}Re-calculating diff for {col} after corrections."
                )
                _derived = group[col].diff().round(10)
                # As in the first pass: the null mask is taken before the seed
                # writes over row 0.
                _null_from_derivation = _derived.isna()
                group[diff_col] = _derived
                group.loc[group.index[0], diff_col] = 0
                _seed = pd.Series(False, index=group.index)
                _seed.iloc[0] = True
                set_flags(group, diff_col, _seed, MappingFlag.FIRST_ZERO)
                # The recompute ADDS its outcome; it does not replace the first
                # pass's. Bits only ever accumulate, because each one records
                # something that happened: a cell differenced to a value,
                # blanked by the corrections, then re-differenced to nothing
                # carries all three of VALUE_DERIVED, NULLED and NULL_DERIVED,
                # and that sequence is the history. NULLED is what says the
                # cell is empty by removal rather than because the derivation
                # produced nothing, so the two null cases stay distinguishable
                # without either member having to be withdrawn.
                #
                # So the partition -- exactly one derivation outcome per cell --
                # holds for a SINGLE pass, which is the only case
                # add_diff_columns runs without corrections.
                set_flags(
                    group, diff_col, ~_null_from_derivation,
                    MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE,
                )
                set_flags(
                    group, diff_col, _null_from_derivation,
                    MappingFlag.NULL_DERIVED_FROM_CUMULATIVE,
                )

                if (group[diff_col] < 0).any(skipna=True):
                    logging.warning(
                        f"{context_string}Removed zeros but diff still has negative "
                        f"values in '{col}'! Check data and consider removing.",
                    )

            # Reached only by a monotonic variable -- the others left at the
            # continue above -- and by both the derived and the kept-delivered
            # interval, which both fall through to here. A negative interval
            # means the cumulative decreased where it cannot, whoever produced
            # the value. Recorded independently of the threshold pass, which
            # reports a negative only where the interval column declares a Min.
            if diff_col in group.columns:
                set_flags(
                    group, diff_col, (group[diff_col] < 0).fillna(False),
                    MappingFlag.NEGATIVE_INTERVAL,
                )

            _carry_findings_to_intervals(group, col)

        return group

    if isinstance(data, pd.core.groupby.DataFrameGroupBy):
        result = data.apply(calculate_diff).reset_index(drop=True)
    elif isinstance(data, pd.DataFrame):
        if id_column is not None:
            result = (
                data.groupby(id_column, group_keys=False)
                .apply(calculate_diff)
                .reset_index(drop=True)
            )
        else:
            result = calculate_diff(data)
    else:
        raise TypeError(
            f"{context_string}Input data must be a pandas DataFrame "
            f"or a pandas GroupBy object.",
        )

    if return_stats:
        return result, dip_stats
    return result


# Nullable dtypes for the per-column cadence table. cadence_minutes and
# cadence_source are added by the resolution step at the end.
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

    A cadence is a duration and is written as one: ``30s``, ``5min``, ``1h``,
    ``24h``. Whole hours are written in hours rather than minutes. Hours are the
    coarsest unit used, so a daily reading is ``24h`` rather than ``1d``.

    This is for display only. Cadences must not be compared as text: the
    metadata writes each one in whichever unit suits the variable, so ``24h``
    and ``1440min`` are the same cadence written two ways and compare unequal.
    Parse a declared cadence with ``pd.Timedelta`` and compare the durations.

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


def _reject_sub_minute(minutes_by_name: dict, ctx: str = "") -> None:
    """
    Refuse an interval the cadence table cannot record.

    ``cadence_minutes`` is Int64, so the table holds whole minutes. A 30-second
    interval has no representation in it: rounding it gives a zero-minute
    cadence, and the Int64 cast raises a TypeError that names neither the
    column nor the value.

    Raises
    ------
    ValueError
        Naming each interval that is not a whole number of minutes.
    """
    offenders = {
        name: value
        for name, value in minutes_by_name.items()
        if value is not None
        and pd.notna(value)
        and float(value) != int(float(value))
    }
    if not offenders:
        return

    listed = ', '.join(
        f"'{name}' at {cadence_label(value)}" for name, value in offenders.items()
    )
    raise ValueError(
        f"{ctx}detect_column_cadence: the cadence table records whole minutes, "
        f"so these intervals cannot be stored in it: {listed}."
    )


def detect_column_cadence(
    df: pd.DataFrame,
    columns: tuple[str, ...] | list[str] | None = None,
    date_column: str = 'ReadingDate',
    declared: dict | None = None,
    min_readings: int = 3,
    min_confidence: float = 0.8,
    context: str = '',
    grid_minutes: float | None = None,
    household_id: int | None = None,
) -> pd.DataFrame:
    """
    Infer how often each column actually reports, from its own timestamps.

    A column's cadence is the most common gap between the readings it carries.
    Only rows where the column has a value are counted, so a column reporting
    every 15 minutes on a 5-minute grid is measured at 15 minutes, not 5.

    ``date_column`` is measured as well and returned as its own row, which
    records the spacing the data was actually delivered at.

    Cadence is settled here, once, from three sources, checked in this order:

    1. Declared, from ``Meetfrequentie`` in the supplier mapping table. The
       metadata is used in preference to measurement, since corrections are made
       there and the delivered data is static.
    2. Measured, where the readings were regular enough to trust.
    3. The grid the household batch declares.

    A declaration that disagrees with a confident measurement is logged and the
    measured value is kept in ``cadence_detected`` so a report can surface the
    disagreement. The declared value is still the one used.

    The grid is required, because it is what source 3 uses. Pass it as
    *grid_minutes*, or pass *household_id* and it is read from the batch
    metadata. A column too short or too irregular to measure then still ends up
    with the interval its household is recorded on, rather than nothing.

    Everything downstream reads ``cadence_minutes`` rather than measuring again.

    Must run on data as delivered, before any fill. ``fill_down_infrequent_devices``
    leaves no row missing, which removes the evidence this reads.

    Taking the most common gap rather than counting readings per day is what
    makes a partial first day harmless. The ingestion window is cut on Dutch
    midnight while data is bucketed in UTC, so a record's first day can carry
    only a few hours -- 24 readings on a 5-minute column, which is also what a
    complete day at hourly cadence produces. The counts coincide; the gaps do
    not.

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
        Column -> cadence in minutes, as declared in the metadata. Read from
        ``Meetfrequentie`` with ``load_supplier_cadence``. Absent columns mean
        nothing was declared for them, which is the common case.
    min_readings : int
        Fewest readings that can support a cadence. Below this the column gets
        pd.NA rather than a guess. Default 3, which is two gaps.
    min_confidence : float
        Lowest confidence at which a measured cadence is trusted.
    context : str
        Optional prefix for log messages, e.g. a household identifier.
    grid_minutes : float, optional
        The grid the household batch is recorded on, in minutes. Required unless
        *household_id* is given.
    household_id : int, optional
        HuisIdBSV, used to read the grid from the batch metadata when
        *grid_minutes* is not given.

    Returns
    -------
    pd.DataFrame
        One row per column:

        - ``cadence_minutes`` (Int64) -- the cadence to use.
        - ``cadence_source`` (string) -- 'declared', 'detected' or 'dataset'.
        - ``cadence_detected`` (Int64) -- what the readings themselves showed,
          kept even when a declaration overrides it so the report can surface
          the disagreement.
        - ``confidence`` (Float64) -- the share of gaps at the measured cadence.
          A low share means the readings are irregular and the measurement is a
          weak claim.
        - ``n_readings`` (Int64).

    Raises
    ------
    ValueError
        When neither *grid_minutes* nor *household_id* is given, or when the
        household declares no readable frequency.
    """
    ctx = (context + ': ') if context else ''
    if grid_minutes is None and household_id is not None:
        grid_minutes = grid_minutes_for_household(household_id)
    if grid_minutes is None or pd.isna(grid_minutes):
        raise ValueError(
            f"{ctx}detect_column_cadence: neither grid_minutes nor household_id "
            f"was given, so the grid a column falls back to is unknown. It is "
            f"declared per household batch as Gegevensfrequentie."
        )

    # The cadence table records whole minutes (_CADENCE_TABLE_DTYPES), so a
    # sub-minute interval cannot be represented in it. Checked here rather than
    # left to the Int64 cast below, which raises an opaque TypeError, or to
    # rounding, which would turn 30s into a zero-minute cadence.
    _reject_sub_minute(
        {date_column: grid_minutes, **(declared or {})}, ctx
    )

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
    # An unmeasured column has no confidence rather than a low one. The mask
    # must still be free of pd.NA, which .loc would read as False silently.
    trusted = (out['confidence'] >= min_confidence).fillna(False)

    # The grid the batch declares, not the spacing measured off date_column. A
    # record too short or too irregular to measure has no measured spacing to
    # fall back to, which is the case this source exists to cover.
    grid = round(float(grid_minutes))

    # Declared, then a measurement regular enough to trust, then the grid.
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

    measured_grid = dict(
        zip(out['column'], out['cadence_detected'])
    ).get(date_column, pd.NA)
    if pd.notna(measured_grid) and round(float(measured_grid)) != grid:
        logging.warning(
            f"{ctx}The batch declares a {cadence_label(grid_minutes)} grid but "
            f"the readings arrive every {cadence_label(measured_grid)}. Using "
            f"the declared grid."
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
    grid_minutes: float | None = None,
) -> pd.DataFrame:
    """
    Fill down (forward fill) and then up (backward fill) values for specified columns.

    Intended for cumulative columns from optional devices that report infrequently.
    When the device is idle the cumulative meter holds its last value (forward-fill).
    A column with no readings at all is left untouched: a never-seen device is
    an absence claim, and absence is declared per household in the metadata
    (ApparaatOntbrekend), never invented as zeros.

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

    The grid is required. It is declared per household batch as
    ``Gegevensfrequentie``; resolve it with ``grid_minutes_for_household``. A
    caller that does not know the grid cannot know which rows a filled value
    belongs on, and assuming one places every fill on the wrong slots.

    Slots are stepped in rows rather than in wall-clock time, so a grid of any
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
    grid_minutes : float
        The grid the pipeline imposes, in minutes. Required.

    Returns
    -------
    pd.DataFrame
        The input DataFrame with the specified columns filled in place.

    Raises
    ------
    ValueError
        When *grid_minutes* is not given.

    Notes
    -----
    Fill order: forward fill, then backward fill. A column that remains
    all-NA (sensor never seen for the household) stays all-NA -- device
    absence is a per-household metadata declaration, not something this
    function may infer from missingness.
    Only meaningful for cumulative variables -- do not apply to instantaneous sensors.
    """
    if grid_minutes is None or pd.isna(grid_minutes):
        raise ValueError(
            "fill_down_infrequent_devices: grid_minutes was not given, so the "
            "grid these columns are filled onto is unknown. It is declared per "
            "household batch as Gegevensfrequentie."
        )

    minutes_by_column = {}
    if cadence is not None and not cadence.empty:
        # Cadence is settled by detect_column_cadence and only read here.
        minutes_by_column = dict(
            zip(cadence['column'], cadence['cadence_minutes'])
        )

    position = pd.Series(range(len(df)), index=df.index)

    for col in columns:
        if col not in df.columns:
            continue

        delivered = df[col].notna()
        filled = df[col].ffill().bfill()
        # Every cell this writes was not measured. Record it before the values
        # go in, while the two are still distinguishable. A never-seen column
        # produces no fills here (ffill/bfill of all-NA is all-NA), so it
        # gains no values and no flags.
        set_flags(df, col, ~delivered & filled.notna(), MappingFlag.FILLED_DOWN)

        minutes = minutes_by_column.get(col, pd.NA)
        if pd.isna(minutes):
            df[col] = filled
            continue

        # Step in rows, not in wall-clock time: the grid may be any cadence, and
        # a column's cadence need not divide the hour. Anchoring on the device's
        # first real reading puts the slots where its data actually is.
        #
        # Compared in whole seconds, so a sub-minute grid is neither truncated
        # to zero nor decided by a float equality.
        step, remainder = divmod(
            round(float(minutes) * 60), round(float(grid_minutes) * 60)
        )
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
        # Removes fills this function just wrote, on the rows the column was
        # never due to report in. FILLED_DOWN stays -- a flag is the cell's
        # history, not its current state -- so the cell reads FILLED_DOWN |
        # NULLED: filled down, then removed. Only cells that held a value are
        # marked, so a row that was empty throughout records no removal.
        _undone = ~(on_cadence | delivered) & filled.notna()
        set_flags(df, col, _undone, MappingFlag.NULLED)
        df[col] = filled.where(on_cadence | delivered, pd.NA)

    return df


def _declared_absent_family(variable: str) -> set[str]:
    """Every column the declared device can report through.

    A declaration names a physical device, so whether the data falsifies it
    is a question about the device and not about one column name. Two
    declared relations say which columns belong to the same device, and the
    closure over both is taken so the answer is the same whichever member is
    named:

    - ``ComplementVariabele`` links a cumulative variable to its interval
      form;
    - the tariff split links a combined variable to the Hoog/Laag registers
      that mapping sums into it. The guard runs before that sum, so on a
      tariff device the registers are where the readings are -- a register is
      a family member exactly as a complement is.
    """
    family = {variable}
    while True:
        grown = set(family)
        for member in family:
            complement = complement_column(member)
            if complement:
                grown.add(complement)
            for root, splits in tariff_root_to_splits.items():
                if member == root or member in splits:
                    grown.add(root)
                    grown.update(splits)
        if grown == family:
            return family
        family = grown


def _declaration_owner(household_id) -> str:
    """Name whose declaration is at fault, for the error message.

    A declaration is corrected in Grist, found by supplier and project, and
    it belongs to one household's dataset -- ``HuisBatchIdBSV``, not the
    delivery batch. ``index.parquet`` is the registry carrying all three
    beside ``HuisIdBSV``.

    Returns an empty string when the household cannot be resolved. These
    identifiers only decorate a message, so failing to find them must never
    swallow the contradiction the caller is about to raise.
    """
    if household_id is None:
        return ""
    try:
        index_df, _ = read_index()
        rows = index_df[index_df["HuisIdBSV"] == int(household_id)]
    # Broad on purpose: an unreadable, absent or older index must cost the
    # caller nothing more than a less specific message.
    except Exception as exc:
        logging.debug(
            f"[assert_no_delivered_in_declared_absent] no index identifiers "
            f"for HuisIdBSV {household_id}: {exc}"
        )
        return ""
    if rows.empty:
        return ""

    def _named(column: str, label: str) -> str | None:
        if column not in rows.columns:
            return None
        values = rows[column].dropna().unique().tolist()
        if not values:
            return None
        try:
            values = sorted(values)
        except TypeError:
            values = sorted(str(v) for v in values)
        return f"{label} {', '.join(str(v) for v in values)}"

    named = [
        _named("Dataleverancier", "supplier"),
        _named("ProjectIdBSV", "ProjectIdBSV"),
        _named("HuisBatchIdBSV", "HuisBatchIdBSV"),
    ]
    named = [n for n in named if n]
    return (" -- " + ", ".join(named)) if named else ""


def assert_no_delivered_in_declared_absent(
    df: pd.DataFrame,
    declared_absent,
    context: str = "",
    household_id=None,
) -> None:
    """Raise when the data falsifies a device-absence declaration.

    A household's ``ApparaatOntbrekend`` declaration is an explicit metadata
    claim: the device does not exist, its term is zero. A delivered reading
    in a declared-absent column falsifies that claim, and falsified metadata
    is always a hard failure -- the metadata (not the data) must be
    corrected. This replaces the old conditional-default semantics, where a
    supplier-wide zero list silently applied only to households whose column
    happened to be empty and said nothing when it was not.

    Migration started contradiction-free by construction -- only households
    with no delivered reading over their whole span were declared -- so this
    guard exists for FUTURE drift: a device installed later that starts
    reporting raises here until the declaration is removed.

    Checked per declared variable over the whole family of columns the
    device reports through -- see ``_declared_absent_family``: the variable
    itself, its declared complement, and the tariff registers and their
    complements. A reading on any of them is the device reporting.
    Delivered means a non-NA value whose findings carry no INVENTED bit;
    values the pipeline itself wrote (e.g. legacy zero-prefill) do not count
    against the declaration.

    Parameters
    ----------
    df : pd.DataFrame
        The household frame, after mapping renames, before or after fills.
    declared_absent : iterable of str
        The household's declared-absent variables (cumulative names, as in
        the ``ApparaatOntbrekend`` metadata column).
    context : str
        Household/file label for the error message.
    household_id : optional
        HuisIdBSV. When given, the raise also names the supplier, project and
        household batch the declaration belongs to, read from the index --
        the identifiers needed to find and correct it in Grist.

    Raises
    ------
    ValueError
        Naming every falsified declaration: the variable, the column the
        readings sit in, how many delivered readings, and the first/last
        ReadingDate of them.
    """
    ctx = (context + ": ") if context else ""
    violations = []
    for variable in sorted({str(v).strip() for v in declared_absent} - {""}):
        candidates = sorted(_declared_absent_family(variable) & set(df.columns))
        for col in candidates:
            delivered = df[col].notna()
            findings = flag_column(col)
            if findings in df.columns:
                delivered &= (
                    df[findings].fillna(0).astype("Int64")
                    & MappingFlag.INVENTED.value
                ) == 0
            n_delivered = int(delivered.sum())
            if n_delivered == 0:
                continue
            if "ReadingDate" in df.columns:
                dates = df.loc[delivered, "ReadingDate"]
                span = f", first {dates.min()}, last {dates.max()}"
            else:
                span = ""
            violations.append(
                f"'{variable}' is declared absent but '{col}' holds "
                f"{n_delivered} delivered reading(s){span}"
            )
    if violations:
        # Resolved only now, so the normal path never reads the index and the
        # identifiers are current at the moment they are reported.
        raise ValueError(
            f"{ctx}device-absence declaration falsified by delivered data"
            f"{_declaration_owner(household_id)}: "
            + "; ".join(violations)
            + ". Correct the metadata (ApparaatOntbrekend / "
            "GegevensOntbrekend in the household table) -- the data is the "
            "evidence, the declaration is the error."
        )


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
    # The sources are dropped on the next line, so without this the target is
    # indistinguishable from one the supplier delivered whole. Like a combined
    # tariff root, the value carries what the delivered sources reported, so the
    # flag is an action without being INVENTED. The combine is NA-preserving,
    # so a cell that stayed empty is one no source covered and nothing wrote.
    written = combined.notna()
    if bool(written.any()):
        set_flags(df, target, written, MappingFlag.COMBINED_COLUMNS)

    # Where only some sources delivered, the NA-preserving combine yields pd.NA
    # and the next line drops the sources, so a delivered value is destroyed.
    # The signal says a source had data; NULLED says it is gone. Without the
    # pair the cell is indistinguishable from one no source ever covered.
    any_source = df[present[0]].notna()
    for s in present[1:]:
        any_source = any_source | df[s].notna()
    incomplete = any_source & combined.isna()
    if bool(incomplete.any()):
        set_flags(df, target, incomplete, MappingFlag.SOURCES_INCOMPLETE)
        set_flags(df, target, incomplete, MappingFlag.NULLED)
        logging.warning(
            f"[combine_source_columns] {ctx}target '{target}': "
            f"{int(incomplete.sum())} reading(s) had data on some but not all "
            f"of {present}, so the combined value is absent. Flagged "
            f"SOURCES_INCOMPLETE | NULLED."
        )

    df.drop(columns=present, inplace=True)
    logging.info(
        f"[combine_source_columns] {ctx}target '{target}' = {operator}({present}); "
        f"sources dropped."
    )
    return df


def snap_readings_to_grid(
    df: pd.DataFrame,
    date_column: str = 'ReadingDate',
    freq_minutes: float | None = None,
    max_deviation_seconds: float = None,
    context: str = '',
    return_stats: bool = False,
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
    consumption -- a +/-7.3% error per slot that self-cancels over longer windows
    but pollutes 5-minute diff analysis.  Using the per-column characteristic
    offset ensures each cumulative column always draws from its own stream
    across all slots, so diffs always span a consistent physical interval.

    This function does no fill-down or fill-up.  It only snaps and merges.
    Call ensure_intervals afterwards to pad any remaining missing grid slots.

    The grid is required. It is declared per household batch as
    ``Gegevensfrequentie``; resolve it with ``grid_minutes_for_household``.
    Snapping to the wrong grid moves every reading, and snapping to no grid
    leaves the streams unmerged while reporting success.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.  The date_column must already be datetime dtype.
    date_column : str
        Name of the datetime column.  Default 'ReadingDate'.
    freq_minutes : float
        Grid slot size in minutes.  Required.
    max_deviation_seconds : float, optional
        Log a warning if any reading deviates from its nearest slot by more
        than this many seconds.  Default: half the slot size.
    context : str
        Optional prefix for log messages, e.g. a household identifier.
    return_stats : bool, optional
        If True, also return a dict describing the delivered values the slot
        merge discarded. Default False (returns only the DataFrame, so
        existing callers are unaffected).

    Returns
    -------
    pd.DataFrame
        One row per grid slot, columns merged from all input rows that
        mapped to that slot.  The date_column contains exact grid timestamps.
    tuple of (pd.DataFrame, dict)
        When return_stats is True: the frame, and {column: figures} for each
        column that lost a value to the merge. Figures are ``n_discarded``,
        ``n_slots_contested`` and ``max_per_slot``. A column that lost nothing
        is absent, so an empty dict means no delivered value was discarded.

    Raises
    ------
    ValueError
        When *freq_minutes* is not given.
    """
    if freq_minutes is None or pd.isna(freq_minutes):
        raise ValueError(
            "snap_readings_to_grid: freq_minutes was not given, so the grid to "
            "snap onto is unknown. It is declared per household batch as "
            "Gegevensfrequentie."
        )

    ctx = (context + ': ') if context else ''
    freq_seconds = freq_minutes * 60
    if max_deviation_seconds is None:
        max_deviation_seconds = freq_seconds / 2.0

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
    slot = cadence_label(freq_minutes)
    logging.debug(
        f"{ctx}snap_readings_to_grid: max deviation from nearest "
        f"{slot} slot = {max_dev:.2f}s "
        f"(tolerance {max_deviation_seconds:.0f}s)"
    )
    if max_dev > max_deviation_seconds:
        logging.warning(
            f"{ctx}snap_readings_to_grid: some readings deviate by more than "
            f"{max_deviation_seconds:.0f}s from their nearest {slot} "
            f"slot (max={max_dev:.2f}s).  This may indicate irregular data."
        )

    # A findings column is not a sensor stream. Measuring a characteristic
    # offset on one is meaningless -- set_flags creates it non-null on EVERY
    # row, so it resolves to a different offset from the column it describes,
    # lands in a different offset group, and .first() then takes the flags from
    # a different row than the value. Flags are carried per winning row below.
    flag_cols = [c for c in df.columns if c.endswith('_flags')]
    value_cols = [
        c for c in df.columns
        if c not in [date_column, '_SnappedSlot'] and c not in flag_cols
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
    #     actually fall on that offset -- a low percentage (< ~80 %) signals
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
            f"multiple rows (max {int(slot_counts.max())} per slot) -- merging."
        )

    # ------------------------------------------------------------------
    # 5. K-pass vectorized merge
    #    One pass per distinct characteristic offset group (K ~= 2-3).
    #    Within each pass, sort by distance from the group's characteristic
    #    offset so that groupby.first() picks the most precisely-timed
    #    non-NA value per column (evaluated independently per column).
    # ------------------------------------------------------------------
    slot_index = df['_SnappedSlot'].sort_values().unique()
    result = pd.DataFrame({date_column: slot_index})

    collisions = {}
    contested_by_col = {}
    for modal_val, cols in sorted(cols_by_modal.items()):
        dist = (raw_offsets - modal_val).abs()
        subset = df[['_SnappedSlot'] + cols].copy()
        subset['_dist'] = dist.values
        subset = subset.sort_values(['_SnappedSlot', '_dist'])

        # .first() keeps one value per slot per column, so a slot holding two
        # values for one column loses the farther one. The discarded reading is
        # gone and cannot carry a flag, which leaves the count as the only
        # record of it. Skipped entirely when no slot has a second row.
        if multi_row_slots > 0:
            per_slot = subset.groupby('_SnappedSlot', sort=False)[cols].count()
            for col in cols:
                counts = per_slot[col]
                contested = counts > 1
                if bool(contested.any()):
                    contested_by_col[col] = contested
                    collisions[col] = {
                        "n_discarded": int((counts[contested] - 1).sum()),
                        "n_slots_contested": int(contested.sum()),
                        "max_per_slot": int(counts.max()),
                    }

        group_result = (
            subset.groupby('_SnappedSlot', sort=True)[cols]
            .first()
            .reset_index()
            .rename(columns={'_SnappedSlot': date_column})
        )
        result = result.merge(group_result, on=date_column, how='left')

        # A findings column follows the value it describes: the surviving cell's
        # flags are the winning row's, where the winner is the first row of the
        # sorted group holding a value. A losing row is a different reading that
        # never reached the grid, so OR-ing its bits onto the winner would
        # fabricate history -- and an inherited FILLED_DOWN would put a
        # delivered reading into NOT_A_MEASUREMENT. That the losers existed is
        # recorded by SLOT_COLLISION below, not by their bits.
        for col in cols:
            findings = flag_column(col)
            if findings not in df.columns:
                continue
            ordered = df.loc[subset.index, [col, findings]]
            ordered['_slot'] = subset['_SnappedSlot'].values
            # idxmax on the notna mask gives the first row holding a value, or
            # the group's first row where the column is empty throughout.
            winners = ordered[col].notna().groupby(ordered['_slot']).idxmax()
            carried = ordered.loc[winners, findings]
            carried.index = winners.index
            result[findings] = (
                result[date_column].map(carried).astype('Int64')
            )

    # One signal per contested slot, so a reader can tell a value that was
    # selected from candidates from the only reading in its slot. The mask is
    # the one the counts above already built.
    for col, contested in contested_by_col.items():
        set_flags(
            result, col,
            result[date_column].map(contested).fillna(False).astype(bool),
            MappingFlag.SLOT_COLLISION,
        )

    logging.info(
        f"{ctx}snap_readings_to_grid: {len(df)} rows -> {len(result)} grid slots "
        f"({len(cols_by_modal)} offset group(s), {multi_row_slots} slot(s) with "
        f"multiple rows merged)"
    )
    if collisions:
        logging.warning(
            f"{ctx}snap_readings_to_grid: discarded a second delivered value "
            f"on {len(collisions)} column(s): "
            + ", ".join(
                f"{col}={s['n_discarded']}"
                for col, s in sorted(collisions.items())
            )
        )

    if return_stats:
        return result, collisions
    return result


def ensure_intervals(
    df: pd.DataFrame,
    date_column: str = 'ReadingDate',
    freq=None,
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
    freq : str
        The grid to pad to, written as a duration. Required. This is the
        household batch's ``Gegevensfrequentie``; callers that process a
        household resolve it with ``grid_minutes_for_household`` and render it
        with ``cadence_label``. Padding to the wrong grid changes the row count
        of the whole dataset.

    Returns
    -------
    pd.DataFrame
        A DataFrame with consistent time intervals.

    Raises
    ------
    ValueError
        When *freq* is not given.

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
    if freq is None or (not isinstance(freq, str) and pd.isna(freq)):
        raise ValueError(
            "ensure_intervals: freq was not given, so the grid to pad to is "
            "unknown. It is declared per household batch as "
            "Gegevensfrequentie."
        )

    df[date_column] = pd.to_datetime(df[date_column])

    earliest = df[date_column].min()
    latest = df[date_column].max()

    # Counted in the grid being padded to, not in a fixed five minutes: at any
    # other grid the count would be wrong and the padding would be skipped or
    # attempted against the wrong slots.
    expected_num_records = (
        int(
            (latest - earliest) / pd.Timedelta(freq),
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

    # Which slots the source delivered a row into, read before any merge. The
    # complement of this is what the padding creates, and recording the signal
    # per created row rather than per bare flag cell is what makes
    # SLOT_NOT_DELIVERED a fact about the slot instead of a fact about which
    # columns an earlier step happened to flag.
    delivered_slots = set(df[date_column])

    def merge_left(df):
        """Keep only the readings that land on the grid.

        A reading that does not land on a grid slot is dropped by this merge,
        which is data loss, so the count is raised rather than logged. An
        off-grid reading means the data reached this point without being
        snapped; ``snap_readings_to_grid`` is what places readings on slots.
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
        merged = pd.merge(all_dates_df, df, on=date_column, how='left')
        # The grid has a slot the supplier delivered no row into. That absence
        # is a finding in its own right, and recording it also restores the
        # invariant: left as pd.NA, every later finding on those cells would
        # discard silently, since `pd.NA | flag` is pd.NA.
        mark_undelivered_slots(
            merged,
            created=~merged[date_column].isin(delivered_slots),
            columns=_DELIVERY_MARKED_COLUMNS,
        )
        return merged

    if expected_num_records > len(df.index):
        logging.info(f"Adding {freq} intervals.")
        merged_df = pd.merge(all_dates_df, df, on=date_column, how='outer')
        mark_undelivered_slots(
            merged_df,
            created=~merged_df[date_column].isin(delivered_slots),
            columns=_DELIVERY_MARKED_COLUMNS,
        )
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
    cadence=None,
    return_stats=False,
):
    """
    Apply thresholds to columns and update imputation flags.

    This function applies lower and upper bounds to a column in the
    DataFrame. Values outside these bounds are replaced with pd.NA.

    Bounds are scaled to the interval each column reports in. A column
    delivering hourly carries twelve five-minute intervals of consumption in one
    reading, so judging it against the five-minute bound rejects every normal
    reading it produces.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame containing the data.
    cadence : pandas.DataFrame, optional
        Per-column cadence for this household, from
        :func:`detect_column_cadence`. Required for any column whose bound is
        written per interval; without it those columns raise rather than being
        judged against an interval nobody resolved.
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
    minutes_by_column = cadence_minutes_by_column(cadence)
    # A column built during the pipeline was placed on the grid, so that is the
    # interval it reports at -- it was not delivered, so nothing measured it.
    grid = dataset_cadence_minutes(cadence)
    for col in df.columns:
        # A column with no values has nothing to judge, so it needs no bound and
        # no cadence. The model columns added during mapping are empty exactly
        # when the supplier delivered nothing for them.
        if col in thresholds_dict and df[col].notna().any():
            lower_bound, upper_bound = resolve_bounds(
                thresholds_dict[col],
                minutes_by_column.get(col, grid),
                col,
            )
            df, col_stats = apply_threshold_to_col(
                df=df,
                col=col,
                lower_bound=lower_bound,
                upper_bound=upper_bound,
            )
            if col_stats is not None:
                col_stats["cadence_minutes"] = minutes_by_column.get(col, pd.NA)
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
        what was removed: {"n_below", "n_above", "sum_removed_negative",
        "sum_removed_positive", "balance_removed", "min_removed",
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

    below = (flags & MappingFlag.OUT_OF_BOUNDS_LOW.value) != 0
    above = (flags & MappingFlag.OUT_OF_BOUNDS_HIGH.value) != 0
    n_below = int(below.sum())
    n_above = int(above.sum())

    # Record the finding before the value goes, so a removal stays
    # distinguishable from a reading that never arrived.
    set_flags(df, col, below, MappingFlag.OUT_OF_BOUNDS_LOW)
    set_flags(df, col, above, MappingFlag.OUT_OF_BOUNDS_HIGH)

    if n_below or n_above:
        removed = df.loc[mask, col]
        # Signed volumes, so a removal report says how much left the dataset
        # and in which direction. A count alone cannot: one large positive and
        # one large negative removal read the same as two small ones.
        negative = removed[removed < 0]
        positive = removed[removed > 0]
        col_stats = {
            "n_below": n_below,
            "n_above": n_above,
            "sum_removed_negative": float(negative.sum()) if len(negative) else 0.0,
            "sum_removed_positive": float(positive.sum()) if len(positive) else 0.0,
            "balance_removed": float(removed.sum()),
            "min_removed": float(removed.min()),
            "max_removed": float(removed.max()),
            # The bounds actually checked. pd.NA means that side was not set,
            # so its count is "not checked", not "nothing found".
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
        }
    else:
        col_stats = None

    # The signals above say why the value is condemned; this says it was
    # removed. Only rows that actually held a value are marked.
    set_flags(df, col, mask & df[col].notna(), MappingFlag.NULLED)
    df.loc[mask, col] = pd.NA

    return df, col_stats


# Sidecar written next to the per-household mapped parquets (one row per
# HuisIdBSV + column that lost values in the threshold pass).
THRESHOLD_REMOVALS_FILENAME = "mapping_threshold_removals.parquet"

# Nullable dtypes for the sidecar columns.
_THRESHOLD_REMOVAL_DTYPES = {
    "HuisIdBSV": "Int64",
    "column": "string",
    "n_below": "Int64",
    "n_above": "Int64",
    "sum_removed_negative": "Float64",
    "sum_removed_positive": "Float64",
    "balance_removed": "Float64",
    "min_removed": "Float64",
    "max_removed": "Float64",
    "lower_bound": "Float64",
    "upper_bound": "Float64",
    "cadence_minutes": "Int64",
}


def _write_sidecar(rows, mapped_folder_path, filename, dtypes, label):
    """
    Merge accumulated sidecar rows into the typed parquet beside the mapped data.

    Rows are merged on ``HuisIdBSV``: the households this run wrote replace their
    own rows, and every other household's rows are kept. Overwriting the file
    instead would leave a folder mapped by several suppliers holding only the
    last one's rows, since each mapper writes only the households it processed.

    An empty but correctly-typed frame is written when there are no rows and none
    were there before, so downstream readers get a stable schema either way.

    Returns the path written.
    """
    if "HuisIdBSV" not in dtypes:
        raise ValueError(
            f"{label} sidecar declares no HuisIdBSV, so its rows cannot be "
            f"merged per household. Add the id to its dtype map."
        )

    df = pd.DataFrame(rows, columns=list(dtypes))

    # Some suppliers deliver ReadingDate tz-aware and some do not, so a timestamp
    # copied into a sidecar may carry a timezone the data model does not declare.
    # Convert to UTC before dropping it: the instant is preserved, and the result
    # matches the datetime64[ns] the model declares for ReadingDate.
    for col, dtype in dtypes.items():
        if dtype == "datetime64[ns]" and isinstance(
            df[col].dtype, pd.DatetimeTZDtype
        ):
            df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)

    df = df.astype(dtypes)
    path = os.path.join(mapped_folder_path, filename)

    kept = 0
    if os.path.exists(path):
        existing = pd.read_parquet(path, dtype_backend="numpy_nullable")
        if "HuisIdBSV" in existing.columns:
            written_now = set(df["HuisIdBSV"].dropna().unique())
            others = existing[~existing["HuisIdBSV"].isin(written_now)]
            kept = len(others)
            if kept:
                df = pd.concat([others, df], ignore_index=True).astype(dtypes)

    df.to_parquet(path, engine="pyarrow")
    logging.info(
        f"[mapping_helpers] Wrote {len(df)} {label} row(s) to {path}"
        + (f" ({kept} kept from other households)" if kept else "")
    )
    return path


def threshold_stats_to_rows(household_id, threshold_stats):
    """
    Flatten one household's threshold_stats dict into sidecar rows.

    Parameters
    ----------
    household_id : int
        The household's HuisIdBSV.
    threshold_stats : dict
        {column: {"n_below", "n_above", "sum_removed_negative",
        "sum_removed_positive", "balance_removed", "min_removed",
        "max_removed", "lower_bound", "upper_bound"}} as produced by
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
                "sum_removed_negative": s["sum_removed_negative"],
                "sum_removed_positive": s["sum_removed_positive"],
                "balance_removed": s["balance_removed"],
                "min_removed": s["min_removed"],
                "max_removed": s["max_removed"],
                "lower_bound": s["lower_bound"],
                "upper_bound": s["upper_bound"],
                # The bound a removal was judged against is only interpretable
                # with the interval it was scaled to. pd.NA where the column is
                # absent from the cadence table, which is a column the pipeline
                # built rather than one the supplier delivered.
                "cadence_minutes": s.get("cadence_minutes", pd.NA),
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
    return _write_sidecar(
        rows, mapped_folder_path, filename,
        _THRESHOLD_REMOVAL_DTYPES, "threshold-removal",
    )


# Sidecar written next to the per-household mapped parquets (one row per
# HuisIdBSV + cumulative column a decrease was corrected in).
NEGATIVE_DIFF_FILENAME = "mapping_negative_diff_removals.parquet"

_NEGATIVE_DIFF_DTYPES = {
    "HuisIdBSV": "Int64",
    "column": "string",
    "n_dips": "Int64",
    "n_readings_removed": "Int64",
    "sum_dip_magnitude": "Float64",
    "min_dip": "Float64",
    "max_dip": "Float64",
    "n_jump_back": "Int64",
    "n_repeat_negative": "Int64",
    "n_meter_reset": "Int64",
    "n_no_recovery": "Int64",
    "n_zero_total": "Int64",
}


def negative_diff_stats_to_rows(household_id, dip_stats):
    """
    Flatten one household's negative-diff figures into sidecar rows.

    Parameters
    ----------
    household_id : int
        The household's HuisIdBSV.
    dip_stats : dict
        {column: figures} as produced by
        ``add_diff_columns(return_stats=True)``. May be empty or None.

    Returns
    -------
    list of dict
        One row per column a decrease was corrected in; empty when none was.
    """
    rows = []
    for col, s in (dip_stats or {}).items():
        rows.append({"HuisIdBSV": household_id, "column": col, **s})
    return rows


def write_negative_diff_removals(
    rows, mapped_folder_path, filename=NEGATIVE_DIFF_FILENAME
):
    """
    Write accumulated negative-diff figures to a sidecar parquet.

    One row per (HuisIdBSV, column) a decrease was corrected in. The flags on
    the readings say which were condemned and why; this says how far the
    register went back, which no flag carries and which the blanking destroys.

    An empty but correctly-typed frame is written when nothing was corrected,
    so downstream readers get a stable schema either way.

    Returns the path written.
    """
    return _write_sidecar(
        rows, mapped_folder_path, filename,
        _NEGATIVE_DIFF_DTYPES, "negative-diff",
    )


# Sidecar written next to the per-household mapped parquets (one row per
# HuisIdBSV + column that lost a delivered value to the grid-slot merge).
GRID_COLLISIONS_FILENAME = "mapping_grid_collisions.parquet"

_GRID_COLLISION_DTYPES = {
    "HuisIdBSV": "Int64",
    "column": "string",
    "n_discarded": "Int64",
    "n_slots_contested": "Int64",
    "max_per_slot": "Int64",
}


def grid_collisions_to_rows(household_id, collisions):
    """
    Flatten one household's grid-collision figures into sidecar rows.

    Parameters
    ----------
    household_id : int
        The household's HuisIdBSV.
    collisions : dict
        {column: figures} as produced by
        ``snap_readings_to_grid(return_stats=True)``. May be empty or None.

    Returns
    -------
    list of dict
        One row per column that lost a value; empty when none did.
    """
    rows = []
    for col, s in (collisions or {}).items():
        rows.append({"HuisIdBSV": household_id, "column": col, **s})
    return rows


def write_grid_collisions(
    rows, mapped_folder_path, filename=GRID_COLLISIONS_FILENAME
):
    """
    Write accumulated grid-collision figures to a sidecar parquet.

    One row per (HuisIdBSV, column) that lost a delivered value when two
    readings fell in one grid slot. The discarded reading is gone and cannot
    carry a flag, so this count is the only possible record of it.

    An empty but correctly-typed frame is written when nothing was discarded,
    so downstream readers get a stable schema either way.

    Returns the path written.
    """
    return _write_sidecar(
        rows, mapped_folder_path, filename,
        _GRID_COLLISION_DTYPES, "grid-collision",
    )


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
    return _write_sidecar(
        rows, mapped_folder_path, filename,
        {"HuisIdBSV": "Int64", **_CADENCE_TABLE_DTYPES}, "column-cadence",
    )


FROZEN_SPANS_FILENAME = "mapping_frozen_spans.parquet"


def frozen_spans_to_rows(
    household_id, column, spans, reading_dates=None,
    span_kind: str = "derived_interval",
):
    """
    Flatten one column's frozen spans into sidecar rows.

    *reading_dates* is the household's ReadingDate series; when given, each span
    also carries the timestamps it starts and ends at, which is what makes the
    artifact readable without going back to the data.

    *span_kind* records which form the column held, so treatment decides once
    rather than re-deriving it: ``"derived_interval"`` for an interval
    differenced from a delivered cumulative column, ``"delivered_interval"``
    for the supplier's own amounts. The two cannot be treated alike -- blanking
    a delivered span destroys the catch-up's energy, there being no cumulative
    column to give it back to.
    """
    if spans is None or spans.empty:
        return []

    rows = []
    for span in spans.to_dict(orient="records"):
        row = {
            "HuisIdBSV": household_id,
            "column": column,
            "span_kind": span_kind,
            **span,
        }
        if reading_dates is not None:
            row["start_date"] = reading_dates.iloc[int(span["start"])]
            row["catchup_date"] = reading_dates.iloc[int(span["catchup_position"])]
        rows.append(row)
    return rows


def write_frozen_spans(rows, mapped_folder_path, filename=FROZEN_SPANS_FILENAME):
    """
    Write accumulated frozen-span rows to a sidecar parquet.

    One row per (HuisIdBSV, column, span): a stretch where the cumulative variable stood
    still and the reading its accumulation arrived in.

    ``span_kind`` says which form the column held and therefore how the span was
    treated. Additive: a sidecar written before it exists has no such column, so
    a merge yields pd.NA rather than a wrong value.
    """
    dtypes = {
        "HuisIdBSV": "Int64",
        "column": "string",
        "span_kind": "string",
        **_SPAN_DTYPES,
        "start_date": "datetime64[ns]",
        "catchup_date": "datetime64[ns]",
    }
    return _write_sidecar(
        rows, mapped_folder_path, filename, dtypes, "frozen-span",
    )


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


def _accumulates(col: str) -> bool:
    """
    Whether a column holds a running total rather than an instantaneous reading.

    The distinction decides what may be done to a flat stretch. A cumulative
    variable that stops advancing is holding consumption back, and the total it
    reports when it resumes states how much, so that consumption can be
    redistributed across the stretch.

    A momentaan variable is sampled at an instant. A flat run of voltage is the
    voltage being steady and a jump after it is the voltage changing; nothing was
    held back, there is no total to conserve, and spreading a sample across the
    stretch would invent readings that were never taken.

    An interval column accumulates if the cumulative variable it derives from
    does.
    """
    return col in cumulative_columns or cumulative_column(col) is not None


def _detect_frozen_spans_for_household(df, household_id, cadence=None, context=""):
    """
    Find frozen spans in every interval column that has an upper bound.

    A span is a stretch where the cumulative variable did not advance, ending in
    a reading too large to be one interval of consumption. The bound is scaled to
    the cadence the column reports in, since a column reporting hourly delivers
    twelve five-minute intervals of consumption in one reading.

    Returns sidecar rows. Nothing in *df* is changed; this is detection only.

    Parameters
    ----------
    df : pd.DataFrame
        One household's data.
    household_id : int
        HuisIdBSV, recorded on each sidecar row.
    cadence : pd.DataFrame, optional
        Per-column cadence for this household, from
        :func:`detect_column_cadence`: one row per column, ``cadence_minutes``
        holding the interval its readings cover. It is what each column's upper
        bound is scaled by, and it also supplies the grid, for a column the
        pipeline built rather than the supplier delivered. A column it does not
        cover is judged at the grid.
    context : str
        Optional prefix for log messages, e.g. a household identifier.
    """
    ctx = (context + ": ") if context else ""
    # Resolves a derived interval column through its cumulative variable, since
    # cadence is measured on the columns as delivered and a column derived
    # during the pipeline is not there under its own name.
    cadence_minutes = cadence_minutes_by_column(cadence)
    # A column built during the pipeline reports at the grid it was placed on.
    grid = dataset_cadence_minutes(cadence)

    reading_dates = df.get("ReadingDate")
    rows = []
    for col in df.columns:
        entry = thresholds_dict.get(col)
        if entry is None or pd.isna(entry["Max"]):
            continue

        # Which form the column holds, read from the cell findings rather than
        # re-derived: add_diff_columns records an outcome on every cell it
        # wrote, and the branch is per household per column -- the same column
        # is derived for one household and delivered by the next -- so no
        # column-level rule can replace it. Later steps mutate the frame and a
        # second source would drift from it.
        #
        # Tested as the COMPOSITE, not the value member: a column the
        # derivation wrote but which produced only nulls was still derived, and
        # testing one outcome would read it as delivered and take the wrong
        # treatment.
        _findings = df.get(flag_column(col))
        derived = bool(
            _findings is not None
            and (
                (
                    _findings.fillna(MappingFlag.NONE.value)
                    & MappingFlag.INTERVAL_DERIVED_FROM_CUMULATIVE.value
                )
                != 0
            ).any()
        )

        if derived:
            # Only an accumulating variable can freeze and catch up. There is
            # nothing accumulated behind a momentaan reading, so treating a flat
            # run of samples would blank measured values and invent others
            # between them.
            if not _accumulates(col):
                continue

            # Interval columns only. frozen_spans reads its argument as a series
            # of per-interval amounts: it takes a run of zeros as "did not
            # advance" and a reading over the bound as the catch-up. A register
            # carries a running total, where a run of zeros means absent rather
            # than idle and a value over the bound is a reading outside its
            # declared range -- which the threshold pass already reports.
            # _treat_frozen_spans then resolves cumulative_column(register) to
            # None and runs its interval branch on the register, blanking the
            # over-bound reading and stamping it FROZEN_SPAN and CATCHUP for a
            # span that never happened.
            if cumulative_column(col) is None:
                continue
        else:
            # The supplier's own interval amounts. Keeps a delivered cumulative
            # column out -- it is not an interval series whatever its findings
            # say -- and requires a per-interval threshold, which excludes a
            # dimensionless column such as ZelfgebruikPercentage (algemeen)
            # while admitting the interval columns that declare no complement
            # and so never reach the derived side at all.
            if col not in diff_columns:
                continue
            if pd.isna(threshold_base_minutes(entry["ThresholdType"])):
                continue

        # A column with no values has no span to find, so it needs no bound and
        # no cadence.
        if not df[col].notna().any():
            continue

        _, bound = resolve_bounds(
            entry, cadence_minutes.get(col, grid), col
        )
        spans = frozen_spans(
            df[col], upper_bound=bound, missing_counts_as_frozen=derived,
        )
        rows.extend(
            frozen_spans_to_rows(
                household_id, col, spans, reading_dates,
                span_kind="derived_interval" if derived else "delivered_interval",
            )
        )

    if rows:
        logging.info(
            f"{ctx}{len(rows)} frozen span(s) detected across "
            f"{len({r['column'] for r in rows})} column(s)."
        )
    return rows


_INSTALL_PROVENANCE_REPORTED = False


def _spread_catchup_over_span(df, row, context=""):
    """
    Spread a delivered interval column's catch-up evenly over the span it covers.

    Treatment for a column holding the supplier's own interval amounts. There is
    no cumulative column to blank -- it was not delivered, and mapping no longer
    builds one from intervals -- so the derived treatment's blanking would
    simply destroy the catch-up's energy: a ten-interval span catching up 3.0
    would leave the column 3.0 short. These are the supplier's measurements and
    nothing else holds them, so the column's total must come out unchanged.

    The catch-up holds everything accumulated since the column last advanced,
    covering the frozen slots and its own, so the share is over
    ``catchup_position - start + 1`` cells, inclusive of the catch-up. Detection
    on this side matched a run of exact zeros -- an absence ends the run rather
    than extending it -- so every cell in that window is either a reported zero
    or the catch-up, and rewriting them conserves the column's total exactly.
    No fillna: an absent interval is unknown, never a reported zero.

    Nothing is written where the share would still exceed the bound. A catch-up
    too large to be one interval even spread out is not a deferred accumulation,
    and writing it would only have ``apply_thresholds_to_df`` null the spread
    values at step 9 -- losing the volume a second time, and more quietly.

    Modifies *df* in place.
    """
    ctx = (context + ": ") if context else ""
    diff_col = row["column"]
    if diff_col not in df.columns:
        return df

    start = int(row["start"])
    catchup = int(row["catchup_position"])
    positions = pd.Series(range(len(df)), index=df.index)

    # The condition and the arrival, recorded whether or not the spread is
    # written: the span happened either way.
    set_flags(
        df, diff_col,
        (positions >= start) & (positions < catchup),
        MappingFlag.FROZEN_SPAN,
    )
    set_flags(df, diff_col, positions == catchup, MappingFlag.CATCHUP)

    window = (positions >= start) & (positions <= catchup)
    n_cells = catchup - start + 1
    share = float(row["catchup_value"]) / n_cells
    bound = row.get("upper_bound")

    if bound is not None and pd.notna(bound) and share > float(bound):
        logging.warning(
            f"{ctx}'{diff_col}': a catch-up of {row['catchup_value']} over "
            f"{n_cells} interval(s) is {share:.4f} each, still above the "
            f"{float(bound):.4f} bound. Not spread -- a catch-up that large is "
            f"not a deferred accumulation. Delivered values left in place."
        )
        return df

    df.loc[window, diff_col] = share
    set_flags(df, diff_col, window, MappingFlag.FROZEN_DELIVERED_INTERVAL_SPREAD)
    logging.info(
        f"{ctx}'{diff_col}': spread a catch-up of {row['catchup_value']} over "
        f"{n_cells} interval(s) at {share:.4f} each; the column's total is "
        f"unchanged."
    )
    return df


def _treat_frozen_spans(df, rows, context=""):
    """
    Blank a detected span so imputation can spread the energy it holds.

    A frozen span places real consumption in the wrong intervals: the readings
    across it understate and the catch-up overstates. Blanking the cumulative
    variable across the span turns it into an ordinary gap whose end-to-start
    jump is exactly that consumption, which the existing redistribution then
    spreads. No separate spreading step is needed and the volume is conserved.

    The reading before the span is kept. It is the last delivered cumulative
    value and anchors the gap; without it the redistribution has no starting
    value and the volume no longer adds up.

    The interval column is blanked across the span and the catch-up, since all of
    those values are wrong -- zeros that understate, then one reading that
    overstates. Imputation rebuilds them from the cumulative variable.

    Modifies *df* in place.
    """
    if not rows:
        return df

    ctx = (context + ": ") if context else ""
    for row in rows:
        diff_col = row["column"]
        cum_col = cumulative_column(diff_col)
        start, catchup = int(row["start"]), int(row["catchup_position"])

        # A column holding the supplier's own interval amounts has no cumulative
        # column to hand the catch-up's energy back to, so the blanking below
        # would destroy it. Spread it across the span instead, and leave the rest
        # of this function to the derived case it was written for.
        if row.get("span_kind") == "delivered_interval":
            _spread_catchup_over_span(df, row, context=context)
            continue

        # Reached only for an interval differenced from a delivered cumulative
        # column, so that column holds values. Stamping FROZEN_SPAN on an
        # all-NA cumulative column -- the placeholder rearrange_model_columns
        # adds when the supplier sent no register -- would put a finding on a
        # column that never existed.
        if cum_col and cum_col in df.columns:
            assert df[cum_col].notna().any(), (
                f"{ctx}'{diff_col}' was routed to the derived treatment but "
                f"'{cum_col}' holds no values. Only an interval differenced "
                f"from a delivered cumulative column belongs here."
            )

        # Record before blanking. After treatment every row in the block looks
        # alike, and which one carried the delivered reading is exactly what a
        # reconstruction needs to know.
        positions = pd.Series(range(len(df)), index=df.index)
        for target in filter(None, (cum_col, diff_col)):
            if target not in df.columns:
                continue
            set_flags(
                df, target,
                (positions >= start) & (positions < catchup),
                MappingFlag.FROZEN_SPAN,
            )
            set_flags(df, target, positions == catchup, MappingFlag.CATCHUP)

        if cum_col and cum_col in df.columns:
            # Restricted to cells that hold a value, so NULLED records a
            # removal that happened rather than one the span merely covered.
            # Blanking an already-NA cell is a no-op, so the values are the
            # same either way.
            removable = (
                (positions >= start) & (positions < catchup)
            ) & df[cum_col].notna()
            set_flags(df, cum_col, removable, MappingFlag.NULLED)
            df.loc[removable, cum_col] = pd.NA

        if diff_col in df.columns:
            # The span's own values are the zeros the frozen meter reported and
            # the catch-up they arrive in. Anything else in that range is a
            # delivered reading the span does not account for, so it stays.
            positions = pd.Series(range(len(df)), index=df.index)
            in_span = (positions >= start) & (positions < catchup)
            is_catchup = positions == catchup
            removable = (
                (in_span & (df[diff_col].fillna(0) == 0)) | is_catchup
            ) & df[diff_col].notna()
            set_flags(df, diff_col, removable, MappingFlag.NULLED)
            df.loc[removable, diff_col] = pd.NA

    logging.info(
        f"{ctx}Treated {len(rows)} frozen span(s): the cumulative variable reads missing "
        f"across each, so imputation spreads the volume it was holding."
    )
    return df


def _report_etdmap_provenance():
    """
    Say once per run which etdmap is actually being used, and when it was installed.

    etdmap is never installed editable into the other repos -- they carry their own
    non-editable copy so etdmap can be developed independently without breaking
    them mid-change. The cost is that a change to an etdmap checkout does NOT reach
    a mapper until it is reinstalled, and a stale copy fails in confusing ways: an
    ImportError for something that plainly exists, or worse, an older function
    quietly doing the old thing.

    Naming the path and the install time makes that visible in the log rather than
    leaving it to be diagnosed from a traceback.
    """
    global _INSTALL_PROVENANCE_REPORTED
    if _INSTALL_PROVENANCE_REPORTED:
        return
    _INSTALL_PROVENANCE_REPORTED = True

    module_path = os.path.dirname(os.path.abspath(__file__))
    installed = "site-packages" in module_path.replace("\\", "/")
    stamp = datetime.fromtimestamp(os.path.getmtime(__file__)).strftime(
        "%Y-%m-%d %H:%M"
    )

    if installed:
        logging.warning(
            f"[etdmap] Using the INSTALLED etdmap at {module_path} "
            f"(last written {stamp}). Changes made in an etdmap checkout since "
            f"then are NOT in effect here. Reinstall to pick them up: "
            f"pip install --no-deps --force-reinstall <path-to-etdmap>"
        )
    else:
        logging.info(f"[etdmap] Using etdmap from {module_path} (last written {stamp}).")


# Every data column of the model, including the interval columns. Membership is
# performance_column_dtypes for the same reason rearrange_model_columns uses it:
# it spans the whole model, where model_column_order carries no interval column.
# ReadingDate is the index of the readings rather than one of them.
_DELIVERY_MARKED_COLUMNS = frozenset(
    column for column in performance_column_dtypes if column != "ReadingDate"
)


def _lattice_step(cadence_minutes, grid_minutes):
    """
    How many grid rows apart a column's readings fall, or None.

    None where the step cannot be stated: no cadence, no grid, or a cadence that
    is not a whole number of grid rows. A reading covering a fractional number of
    rows has no lattice to sit on, so the caller asserts nothing about its gaps.

    Parameters
    ----------
    cadence_minutes : int, float or pd.NA
        The interval one of the column's readings covers.
    grid_minutes : int, float or pd.NA
        The interval the rows sit on.
    """
    if cadence_minutes is None or pd.isna(cadence_minutes):
        return None
    if grid_minutes is None or pd.isna(grid_minutes) or float(grid_minutes) <= 0:
        return None

    step = float(cadence_minutes) / float(grid_minutes)
    if step < 1 or step != int(step):
        return None
    return int(step)


def mark_delivered(df) -> None:
    """Record what the supplier delivered into each cell of a delivered frame.

    One question with a three-way answer, not three findings:

    ======================== ===============================================
    ``RAW_DELIVERED_VALUE``  a row arrived at this slot and carried a value
    ``DELIVERED_NA``         a row arrived and carried nothing for this column
    ``SLOT_NOT_DELIVERED``   no row arrived at this slot
    ======================== ===============================================

    Exactly one holds of every cell. This writes the first two; the third is
    written by ``ensure_intervals`` when it completes the grid, because those
    cells do not exist until the padding creates them. That is a physical
    necessity, not a division of the question.

    **Call this on the frame as the supplier delivered it** -- after the readings
    are snapped onto the grid and after any merge that brings the columns
    together, but before the grid is padded and before anything is filled. Every
    row is then a row that arrived, which is what makes the two answers here
    distinguishable at all.

    Order matters in one direction only, and it is the one that bites: run this
    after a fill and a filled cell reads as delivered. The previous version
    marked delivery at pipeline step 6b, after
    ``fill_down_infrequent_devices``, and had to subtract ``INVENTED`` to undo
    its own lateness. Marking before any fill is what removes the need for that
    correction.

    A column the supplier never populated is answered too -- ``DELIVERED_NA``
    throughout -- rather than left with an empty findings column, which is
    indistinguishable from a column no step ever reached.

    Only the model's data columns are marked. An identifier the pipeline
    attaches was not delivered into a slot, so the question does not apply
    to it.

    Modifies *df* in place.
    """
    record_delivery(
        df, [c for c in df.columns if c in _DELIVERY_MARKED_COLUMNS]
    )


def mark_cadence(
    df, cadence=None, grid_minutes=None, context: str = ""
) -> None:
    """
    Record, for each empty cell, whether a reading was due there.

    One question, with two answers, read from the column's reporting lattice
    alone: a column reporting every k grid rows is due a reading every k-th row
    from its first one, so a pd.NA on that lattice is marked ON_CADENCE_NA, and
    one between ticks OFF_CADENCE_NA. Neither is a fault on its own, and neither
    says whether a source row arrived -- :func:`etdmap.flags.mark_delivered` and
    ``SLOT_NOT_DELIVERED`` answer that, separately.

    Both forms are marked. A coarsely reporting register can miss a tick as
    easily as a delivered interval can.

    No value is changed and nothing downstream reads these: they are what lets a
    reader, and an analysis, tell an absent reading from a cadence gap.

    **Reads the delivery record rather than recomputing it.** The lattice is
    anchored on the first row the column reported in, and that is already
    recorded as ``RAW_DELIVERED_VALUE`` by :func:`etdmap.flags.mark_delivered`,
    which runs in the mapper on the frame as delivered. Deriving it from the data
    again here would be wrong: ``fill_down_infrequent_devices`` has since put
    values into the gaps, so ``notna()`` no longer means delivered, and the old
    version had to subtract ``INVENTED`` to undo that. A finding is set where it
    is made and read downstream, which is what makes the correction unnecessary
    rather than merely cheaper.

    A column carrying no delivery record is skipped. That is a column created
    after delivery was marked -- the all-NA placeholders
    ``rearrange_model_columns`` adds for model columns the dataset does not have
    -- so there is no first reading to anchor on and nothing to say about its
    gaps.

    A column with no cadence, or one whose cadence is not a whole number of grid
    rows, has no lattice: its delivered values are recorded and neither NA
    signal is, since either would be a guess.

    Parameters
    ----------
    df : pd.DataFrame
        One household's data, sorted by ReadingDate and already padded to the
        grid by ``ensure_intervals``, so a row's position is its grid slot.
    cadence : pd.DataFrame, optional
        The per-column cadence table :func:`detect_column_cadence` produces for
        this household: one row per column, with ``cadence_minutes`` holding the
        interval that column's readings cover. Measured on the timestamps as
        delivered, before any padding or fill.

        Each column is read from **its own row**. A variable delivered in both
        forms has a row for each, and a variable delivered as intervals only
        still has one for its absent register carrying the grid as a fallback,
        so resolving a column through the variable it is paired with would hand
        it the wrong interval.

        Omitted, or empty, means no column has a lattice: delivery is still
        recorded and no absence is classified.
    grid_minutes : float, optional
        The interval the rows sit on, from the household batch's declared
        ``Gegevensfrequentie``. Read from *cadence* when not given. A column's
        readings fall every ``cadence_minutes / grid_minutes`` rows.
    context : str
        Optional prefix for log messages, e.g. a household identifier.

    Modifies *df* in place.
    """
    ctx = (context + ": ") if context else ""
    # Each column's own row, not cadence_minutes_by_column: that reader resolves
    # a column through the variable it is paired with, for the stages that ask
    # about a column derived later and absent from the table under its own name.
    # The question here is what *this* delivered column reported at, and a
    # variable delivered in both forms has a row for each -- resolving would hand
    # the interval column the register's cadence.
    minutes_by_column = {}
    if cadence is not None and not getattr(cadence, "empty", True):
        minutes_by_column = {
            row.column: row.cadence_minutes
            for row in cadence.itertuples()
            if pd.notna(row.cadence_minutes)
        }
    grid = grid_minutes if grid_minutes is not None else dataset_cadence_minutes(cadence)

    # Mapping runs per household on one frame sorted by ReadingDate, and
    # ensure_intervals has given it one row per grid slot, so a row's position is
    # its slot.
    position = pd.Series(range(len(df)), index=df.index, dtype="Int64")
    without_lattice = []

    for column in df.columns:
        if column not in _DELIVERY_MARKED_COLUMNS:
            continue

        # Where this column's own readings start, which is what sets the phase
        # of its lattice. Not the same population as RAW_DELIVERED_VALUE: a
        # combined tariff root and a derived interval hold what the supplier
        # reported without the supplier having delivered that cell, and they
        # anchor a lattice like any other reading. What must be excluded is a
        # value the pipeline INVENTED -- fill_down_infrequent_devices runs
        # ahead of this and leaves no row missing, so anchoring on a filled row
        # would shift the whole lattice.
        anchored = df[column].notna()
        findings = flag_column(column)
        if findings in df.columns:
            anchored &= (
                df[findings].fillna(MappingFlag.NONE.value)
                & MappingFlag.INVENTED.value
            ) == 0
        if not anchored.any():
            continue

        step = _lattice_step(minutes_by_column.get(column), grid)
        if step is None:
            without_lattice.append(column)
            continue

        # Only an empty cell is owed a reading. A cell an earlier step wrote
        # holds a value, whoever put it there, so neither signal applies to it.
        absent = df[column].isna()
        first = int(position[anchored].iloc[0])
        on_lattice = ((position - first) % step) == 0
        set_flags(df, column, absent & on_lattice, MappingFlag.ON_CADENCE_NA)
        set_flags(df, column, absent & ~on_lattice, MappingFlag.OFF_CADENCE_NA)

    if without_lattice:
        logging.debug(
            f"{ctx}No reading lattice for {len(without_lattice)} column(s), so "
            f"their gaps are not classified: {sorted(without_lattice)[:10]}"
        )


def mark_unchanged_values(df) -> None:
    """Record NOT_CHANGED on every value no action was taken on.

    Without it, an empty findings column is ambiguous: it may mean mapping
    looked and left the value alone, or that nothing reached the column at all.
    Derived from the absence of MappingFlag.ACTIONS_TAKEN, so adding an action
    extends this automatically.

    Only a cell that holds a value can be one mapping left alone. An empty cell
    was never examined: ``ensure_intervals`` pads every grid slot the supplier
    did not report in, and a column reporting more slowly than the grid is
    empty between its ticks, so stamping those would claim an inspection of
    cells that hold nothing. The two absences are told apart by
    ``mark_cadence``, as ON_CADENCE_NA and OFF_CADENCE_NA.

    Every data column of the model is marked, not only those that already carry
    a findings column, or a column no step made a finding about would be the one
    case the flag cannot speak for.
    """
    for column in df.columns:
        if column not in _DELIVERY_MARKED_COLUMNS:
            continue
        findings = flag_column(column)
        if findings in df.columns:
            acted = (
                df[findings].fillna(MappingFlag.NONE.value)
                & MappingFlag.ACTIONS_TAKEN.value
            ) != 0
        else:
            acted = pd.Series(False, index=df.index)
        set_flags(df, column, df[column].notna() & ~acted, MappingFlag.NOT_CHANGED)


def run_standard_pipeline(
    df: pd.DataFrame,
    household_id: int,
    household_id_supplier: str,
    mapped_folder_path,
    context: str = "",
    cadence: pd.DataFrame | None = None,
    treat_frozen_spans: bool = True,
    grid_minutes: float | None = None,
) -> dict:
    """
    Standard per-household processing pipeline shared by all ETD supplier mappers.

    Call this after all supplier-specific preprocessing is complete: column rename,
    unit conversion, ReadingDate derived, identifier columns dropped, and any
    supplier-specific grid snapping (e.g. snap_readings_to_grid).

    Steps:
      1. Validate ReadingDate present -- raise KeyError if missing
      2. Coerce ReadingDate to datetime if not already; raise ValueError if any rows fail
      3. Sort by ReadingDate
      4. ensure_intervals
      5. Log model column type mismatches at DEBUG
      6. rearrange_model_columns(add_columns=True)
     6a. add_tariff_root_cadence -- a root combined from delivered splits has
         no measured row of its own
      6b. mark_cadence
      7. add_diff_columns
     7b. Detect frozen spans, and blank them so imputation can spread them
      8. Apply record_flag_conditions (try/except per flag; pd.NA on error)
      9. apply_thresholds_to_df
     10. Save to {mapped_folder_path}/household_{household_id}_table.parquet

    Note: fill_down_infrequent_devices is intentionally NOT part of this pipeline.
    Filling down is a supplier-specific imputation choice -- for some data sources
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
    cadence : pd.DataFrame, required
        Per-column cadence for this household, from ``detect_column_cadence``.
        The mapper measures it, because cadence has to be read from the data as
        delivered: ``fill_down_infrequent_devices`` leaves no row missing, which
        removes the evidence. This pipeline runs after that, so it cannot
        measure the cadence itself.
    treat_frozen_spans : bool
        Whether to blank detected frozen spans so imputation can spread them.
    grid_minutes : float, optional
        The grid to place the data on. Resolved from the household batch's
        declared ``Gegevensfrequentie`` when not given.

    Returns
    -------
    dict
        {'HuisIdLeverancier': household_id_supplier, 'HuisIdBSV': household_id}
    """
    _report_etdmap_provenance()

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
    # 2. Coerce ReadingDate to datetime, and require every value
    # ------------------------------------------------------------------
    if not pd.api.types.is_datetime64_any_dtype(df["ReadingDate"]):
        logging.debug(f"[run_standard_pipeline] {ctx}Coercing 'ReadingDate' to datetime.")
        df = df.copy()
        df["ReadingDate"] = pd.to_datetime(df["ReadingDate"], errors="coerce")

    # Outside the coercion branch: a frame already typed datetime64 can still
    # carry NaT, and NaT sorts last while astype('int64') maps it to INT64_MIN,
    # so the row range the negative-diff correction derives would be taken from
    # an unsorted series.
    n_bad = int(df["ReadingDate"].isna().sum())
    if n_bad > 0:
        raise ValueError(
            f"[run_standard_pipeline] {ctx}{n_bad} ReadingDate value(s) are "
            f"missing or could not be parsed. Every row must have a valid "
            f"ReadingDate: the interval of a cumulative column is the step "
            f"between consecutive readings, so a row that cannot be placed in "
            f"time cannot be differenced."
        )

    # One reading per instant. Two readings at one instant have no step between
    # them, and the negative-diff correction locates the reading a dip recovers
    # at by position in the sorted series.
    duplicated = df["ReadingDate"].duplicated()
    if bool(duplicated.any()):
        n_dup = int(duplicated.sum())
        first = df.loc[duplicated, "ReadingDate"].iloc[0]
        raise ValueError(
            f"[run_standard_pipeline] {ctx}{n_dup} duplicate ReadingDate "
            f"value(s), first at {first}. The interval of a cumulative column "
            f"is the step between consecutive readings, so two readings at one "
            f"instant have no well-defined step and the negative-diff "
            f"correction cannot locate the reading a dip recovers at. Collapse "
            f"them to one row per slot with snap_readings_to_grid first."
        )

    # Checked here rather than where a bound is scaled, so the message names
    # what the caller has to do instead of naming the first column that hit it.
    # After the ReadingDate checks above, so a structural problem with the frame
    # is still reported as itself.
    if cadence is None or getattr(cadence, "empty", True):
        raise ValueError(
            f"[run_standard_pipeline] {ctx}no cadence was given, so every bound "
            f"written per interval would be applied at an interval nobody "
            f"resolved. Measure it with detect_column_cadence on the data as "
            f"delivered, before any fill, and pass the result as cadence."
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
    # 4. Pad to the grid this household batch is recorded on
    # ------------------------------------------------------------------
    if grid_minutes is None:
        grid_minutes = grid_minutes_for_household(household_id)
    logging.info(
        f"[run_standard_pipeline] {ctx}Grid is {cadence_label(grid_minutes)}."
    )
    df = ensure_intervals(df, freq=cadence_label(grid_minutes))

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
                f"not present -- will be added as NaN by rearrange_model_columns."
            )

    # ------------------------------------------------------------------
    # 6. Rearrange columns to match ETD model order; add missing as NaN
    # ------------------------------------------------------------------
    df = rearrange_model_columns(household_df=df, add_columns=True, context=context)

    # ------------------------------------------------------------------
    # 6a. Give a tariff root combined from delivered splits its own cadence
    #     row. Before 6b, so the marking reads the root's cadence rather than
    #     treating it as a column nothing was measured for.
    # ------------------------------------------------------------------
    cadence = add_tariff_root_cadence(df, cadence, context=context)

    # ------------------------------------------------------------------
    # 6b. Record what the supplier delivered, per cell, while the delivered
    #     values are still in place: step 7 replaces a delivered interval with
    #     the register's diff, and the steps below blank values.
    # ------------------------------------------------------------------
    mark_cadence(
        df, cadence=cadence, grid_minutes=grid_minutes, context=context
    )

    # ------------------------------------------------------------------
    # 7. Add diff columns (5-minute differences for cumulative variables)
    # ------------------------------------------------------------------
    df, negative_diff_stats = add_diff_columns(
        df, context=context, return_stats=True,
    )

    # ------------------------------------------------------------------
    # 7b. Detect frozen spans in the interval columns just computed
    # ------------------------------------------------------------------
    frozen_span_rows = _detect_frozen_spans_for_household(
        df, household_id, cadence=cadence, context=context
    )
    if treat_frozen_spans:
        _treat_frozen_spans(df, frozen_span_rows, context=context)

    # ------------------------------------------------------------------
    # 8. Apply record-level validation flags
    #
    #    cadence goes in so a bound written per interval is scaled the same way
    #    step 9 scales it. Without it the two steps disagree about the same
    #    values on any household not reporting at the base interval.
    # ------------------------------------------------------------------
    for flag, condition in record_flag_conditions.items():
        df[flag] = condition(df, **supported_kwargs(condition, cadence=cadence))

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
    df, threshold_stats = apply_thresholds_to_df(
        df, cadence=cadence, return_stats=True
    )
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

    # Every value now carries whatever findings apply to it, so the ones no
    # action touched can be marked as such.
    mark_unchanged_values(df)

    # One scan over every flag column before anything reaches disk. set_flags
    # refuses a write onto a missing history, but only sees the rows it is
    # asked to write, so a column no later step writes to would carry the
    # damage out silently -- which is how 52 columns reached the 2026-10-21
    # artifact while only 4 of them ever saw a write that could complain.
    assert_flag_history_complete(df, context=context or "")

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
        # The cadence the pipeline ran on, which is the measured table plus any
        # tariff-root row step 6a derived. The sidecar is written from this, not
        # from the mapper's own copy, or the derived rows would be dropped.
        "_cadence": cadence,
        "_validation_summary": {
            "flags": flag_counts,
            "thresholds": threshold_counts,
            "threshold_stats": threshold_stats,
            "frozen_spans": frozen_span_rows,
            "negative_diff_stats": negative_diff_stats,
        },
    }
