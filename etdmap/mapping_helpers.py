import logging

import pandas as pd

from etdmap.data_model import cumulative_columns, model_column_order, model_column_type


def rearrange_model_columns(
    household_df: pd.DataFrame,
    add_columns: bool = True,
    context='',
) -> pd.DataFrame:
    if context != '':
        context = context + ': '
    for col, expected_type in model_column_type.items():
        if col in household_df.columns:
            actual_type = household_df[col].dtype
            if actual_type != expected_type:
                logging.warning(
                    f"{context}Column '{col}' has type '{actual_type}' "
                    "but expected type is '{expected_type}'",
                )
    if add_columns:
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
        household_df = household_df[
            [col for col in model_column_order if col in household_df.columns]
            + [col for col in household_df.columns if col not in model_column_order]
        ]
    return household_df

# Check for any gaps greater than one hour
# Check if at least 90% of the values are not NA
def validate_cumulative_variables(
                group: pd.DataFrame,
                timedelta=pd.Timedelta(hours=1),
                available=0.9,
                allow_all_missing=True,
                context='',
            ) -> bool:
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
            filtered_group['ReadingDateDiff'] = filtered_group['ReadingDate'].diff()  # noqa E501
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

            filtered_group['negative_diff'] = (
                round(filtered_group[[col]].diff(), 10) < 0
            )

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
    data,
    id_column: str = None,
    validate_func=validate_cumulative_variables,
    context='',
    drop_unvalidated=False,
) -> pd.DataFrame:
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
            if col in group.columns:
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

                        if not next_value_row.empty:
                            next_value = next_value_row[col + 'Diff_no_gap'].iloc[0]
                            next_value_date = next_value_row['ReadingDate'].iloc[0]
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
                            else:
                                if (
                                    group.loc[
                                        group['ReadingDate'] == rd,
                                        col + 'Diff',
                                    ].iloc[0]
                                    < 0
                                ):
                                    logging.info(
                                        f"{context_string}Negative gap jump "
                                        f"at {rd}. Removing single cumulative "
                                        'value.',
                                    )
                                    group.loc[
                                        group['ReadingDate'] == rd,
                                        col,
                                    ] = pd.NA
                                    # recalculate = True
                                else:
                                    logging.info(
                                        f"{context_string}Negative gap jump "
                                        f"at {rd}. Diff is not negative, not "
                                        'removing any values.',
                                    )
                        else:
                            # recalculate = True
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

                    if any(group[col + 'Diff'] < 0):
                        logging.error(
                            f"{context_string}Removed zeros but "
                            f"diff still has negative values! Check data and "
                            f"consider removing.",
                        )

            else:
                logging.warning(
                    f"{context_string}Cumulative column '{col}' not found. "
                    'No Diff column created.',
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
    df,
    columns=(
        'ElektriciteitsgebruikBoilervat',
        'ElektriciteitsgebruikRadiator',
        'ElektriciteitsgebruikBooster',
    )
):
    # This function is potentially problematic if the data source or
    # devices are misbehaving as the imputation will not be run on it.

    for col in columns:
        if col in df.columns:
            df[col] = df[col].ffill().bfill().fillna(0.0)

    return df


def ensure_intervals(
    df: pd.DataFrame,
    date_column: str = 'ReadingDate',
    freq='5min',
) -> pd.DataFrame:
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
