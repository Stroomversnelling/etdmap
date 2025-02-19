import logging
from typing import Dict, List

import numpy as np
import pandas as pd

"""
Timestamp Alignment and Merging Module

This module provides functions for aligning and merging time series data
from multiple devices. It includes functionality for determining ideal
clock alignments, reporting on tolerance impacts, and aligning timestamps
across multiple dataframes.

Usage Example:
--------------
import pandas as pd
from timestamp_alignment import determine_dynamic_clocks, report_tolerance_impact, align_timestamps, align_and_merge_dataframes

# Assume we have a list of dataframes called 'device_dataframes'
timestamp_col = 'timestamp'
freq = 300  # 5-minute intervals
cumulative_columns = ['energy_consumption', 'water_usage']

# Step 1: Determine ideal clocks
ideal_clocks = determine_dynamic_clocks(device_dataframes, timestamp_col, freq)

# Step 2: Report on tolerance impact
tolerance_report = report_tolerance_impact(device_dataframes, timestamp_col, ideal_clocks, freq=freq)

# Step 3: Align timestamps (using device-specific clocks)
aligned_dfs = []
for i, df in enumerate(device_dataframes):
    start_time = ideal_clocks[f'device_{i}']
    aligned_df = align_timestamps(df, timestamp_col, start_time, freq, 
                                  tolerance=10, method='interpolation', 
                                  cumulative_columns=cumulative_columns)
    aligned_dfs.append(aligned_df)

# Step 4: Align and merge dataframes
final_merged_df = align_and_merge_dataframes(aligned_dfs, timestamp_col='aligned_timestamp', use_first_as_main=False, freq=freq)

# Now 'final_merged_df' contains the aligned and merged data from all devices
"""

def determine_dynamic_clocks(dataframes: List[pd.DataFrame], timestamp_col: str, freq: int = 300) -> Dict[str, pd.Timestamp]:
    """
    ALPHA status (untested code, use at your own risk!). Determines the ideal dynamic clock start for each device and across all devices.

    Parameters
    ----------
    dataframes : list of pandas.DataFrame
        List of dataframes, one per device.
    timestamp_col : str
        Name of the timestamp column.
    freq : int, optional
        Frequency in seconds (default is 300 for 5-minute intervals).

    Returns
    -------
    dict of str: pandas.Timestamp
        Dictionary with ideal clock starts for each device and overall.
    """
    ideal_starts = {}
    all_timestamps = pd.Series(dtype='datetime64[ns]')

    def find_optimal_start(timestamps):
        seconds = timestamps.astype(int) // 1e9
        offsets = np.arange(freq)
        deviations = np.array([np.sum(np.minimum((seconds + offset) % freq, freq - (seconds + offset) % freq)) for offset in offsets])
        optimal_offset = offsets[np.argmin(deviations)]
        optimal_start = pd.Timestamp(seconds.min() - (seconds.min() + optimal_offset) % freq, unit='s')
        return optimal_start

    for i, df in enumerate(dataframes):
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        timestamps = df[timestamp_col]
        ideal_starts[f'device_{i}'] = find_optimal_start(timestamps)
        all_timestamps = all_timestamps.append(timestamps)

    ideal_starts['overall'] = find_optimal_start(all_timestamps)
    return ideal_starts

def report_tolerance_impact(dataframes: List[pd.DataFrame], timestamp_col: str,
                            ideal_starts: Dict[str, pd.Timestamp],
                            tolerances: List[int] = [10, 30, 150],
                            freq: int = 300) -> Dict[str, Dict[str, Dict[int, int]]]:
    """
    ALPHA status (untested code, use at your own risk!). Reports on how different tolerances affect the number of values per column.

    Parameters
    ----------
    dataframes : list of pandas.DataFrame
        List of dataframes, one per device.
    timestamp_col : str
        Name of the timestamp column.
    ideal_starts : dict of str: pandas.Timestamp
        Dictionary of ideal starts from determine_dynamic_clocks.
    tolerances : list of int, optional
        List of tolerances in seconds to check (default is [10, 30, 150]).
    freq : int, optional
        Frequency in seconds (default is 300 for 5-minute intervals).

    Returns
    -------
    dict of str: dict of str: dict of int: int
        Nested dictionary with counts for each device, column, and tolerance.
    """

    results = {}

    for i, df in enumerate(dataframes):
        device_key = f'device_{i}'
        results[device_key] = {}

        for col in df.columns:
            if col != timestamp_col:
                results[device_key][col] = {}

                for tolerance in tolerances:
                    # Count values within tolerance for device-specific clock
                    device_deviations = ((df[timestamp_col] - ideal_starts[device_key]).dt.total_seconds() % freq)
                    device_deviations = np.minimum(device_deviations, freq - device_deviations)
                    device_count = (device_deviations <= tolerance).sum()
                    results[device_key][col][tolerance] = device_count

                    # Count values within tolerance for overall clock
                    overall_deviations = ((df[timestamp_col] - ideal_starts['overall']).dt.total_seconds() % freq)
                    overall_deviations = np.minimum(overall_deviations, freq - overall_deviations)
                    overall_count = (overall_deviations <= tolerance).sum()
                    results[device_key][col][f'{tolerance}_overall'] = overall_count

    return results

def align_and_merge_dataframes(aligned_dfs: List[pd.DataFrame], 
                               timestamp_col: str = 'aligned_timestamp',
                               use_first_as_main: bool = False,
                               freq: int = 300) -> pd.DataFrame:
    """
    Aligns multiple dataframes by adjusting their first timestamps to minimize overall deviation,
    reports the shifts, and merges them into a single dataframe.

    Parameters
    ----------
    aligned_dfs : List[pd.DataFrame]
        List of aligned dataframes to be processed.
    timestamp_col : str, optional
        Name of the aligned timestamp column. Default is 'aligned_timestamp'.
    use_first_as_main : bool, optional
        If True, aligns all dataframes to the first one. Default is False.
    freq : int, optional
        Expected frequency in seconds between timestamps. Default is 300.

    Returns
    -------
    pd.DataFrame
        Merged dataframe with aligned timestamps.

    Notes
    -----
    This function is in ALPHA status (untested code, use at your own risk!).

    Raises
    ------
    ValueError
        If any dataframe has an inconsistent frequency.
    """
    if len(aligned_dfs) == 1:
        logging.info("Only one dataframe provided. No alignment necessary.")
        return aligned_dfs[0]

    # Verify consistent frequency
    for i, df in enumerate(aligned_dfs):
        df_freq = (df[timestamp_col].diff().dt.total_seconds().mode().iloc[0])
        if abs(df_freq - freq) > 1:  # Allow 1 second tolerance for float imprecision
            raise ValueError(f"Dataframe {i} has inconsistent frequency: {df_freq} seconds instead of {freq}")

    # Extract first timestamps
    first_timestamps = [df[timestamp_col].min() for df in aligned_dfs]

    if use_first_as_main:
        reference_time = first_timestamps[0]
    else:
        # Convert timestamps to seconds since epoch
        first_seconds = [ts.timestamp() for ts in first_timestamps]

        # Find the offset that minimizes overall deviation
        offsets = np.arange(freq)
        deviations = [sum(min((s + offset) % freq, freq - (s + offset) % freq) for s in first_seconds) for offset in offsets]
        optimal_offset = offsets[np.argmin(deviations)]

        # Calculate the reference time
        min_timestamp = min(first_timestamps)
        reference_time = min_timestamp - pd.Timedelta(seconds=(min_timestamp.timestamp() + optimal_offset) % freq)

    # Align and merge dataframes
    merged_df = None
    for i, df in enumerate(aligned_dfs):
        df = df.copy()
        shift = (reference_time - first_timestamps[i]).total_seconds() % freq
        if shift > freq / 2:
            shift -= freq

        df[timestamp_col] = df[timestamp_col] + pd.Timedelta(seconds=shift)
        logging.info(f"Dataframe {i} shifted by {shift:.2f} seconds")

        if merged_df is None:
            merged_df = df
        else:
            merged_df = pd.merge(merged_df, df, on=timestamp_col, how='outer', suffixes=(f'_df{i-1}', f'_df{i}'))

    logging.info("All dataframes merged successfully")

    return merged_df


def interpolate_cumulative(series: pd.Series, target_timestamps: pd.DatetimeIndex, tolerance: pd.Timedelta) -> pd.Series:
    """
    ALPHA status (untested code, use at your own risk!). Interpolate cumulative data within tolerance, preserving pd.NA outside of tolerance ranges.

    Parameters
    ----------
    series : pd.Series
        The original cumulative data series.
    target_timestamps : pd.DatetimeIndex
        The target timestamps for alignment.
    tolerance : pd.Timedelta
        The tolerance for considering nearby values.

    Returns
    -------
    pd.Series
        Interpolated series aligned with target_timestamps.

    Notes
    -----
    - Interpolates only when there are at least two values within the tolerance range.
    - Preserves pd.NA for timestamps without nearby values.
    - Raises an error if decreasing cumulative values are detected.
    """
    result = pd.Series(index=target_timestamps, dtype=series.dtype)
    result[:] = pd.NA

    for timestamp in target_timestamps:
        nearby = series[(series.index >= timestamp - tolerance) & 
                        (series.index <= timestamp + tolerance)]

        if len(nearby) >= 2:
            if nearby.is_monotonic_increasing:
                interp_value = np.interp(
                    timestamp.value, 
                    nearby.index.astype(int), 
                    nearby.values
                )
                result[timestamp] = pd.Series([interp_value], dtype=series.dtype)[0]
            elif nearby.is_monotonic:
                result[timestamp] = nearby.iloc[0]
            else:
                raise ValueError(f"Decreasing cumulative values detected near {timestamp}")
        elif len(nearby) == 1:
            result[timestamp] = nearby.iloc[0]

    return result

def align_timestamps(df: pd.DataFrame,
                     timestamp_col: str,
                     start_time: pd.Timestamp,
                     freq: int,
                     tolerance: int = 10,
                     method: str = 'nearest',
                     cumulative_columns: List[str] = None) -> pd.DataFrame:
    """
    ALPHA status (untested code, use at your own risk!). Aligns timestamps in a dataframe based on the specified method.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    timestamp_col : str
        Name of the timestamp column.
    start_time : pd.Timestamp
        Start time for alignment.
    freq : int
        Frequency in seconds.
    tolerance : int, optional
        Tolerance in seconds for alignment (default is 10).
    method : str, optional
        Alignment method ('nearest' or 'interpolation', default is 'nearest').
    cumulative_columns : List[str], optional
        List of cumulative column names.

    Returns
    -------
    pd.DataFrame
        Dataframe with aligned timestamps and adjusted values.

    Notes
    -----
    - For cumulative columns, uses custom interpolation within tolerance.
    - For non-cumulative columns, uses nearest value within tolerance.
    - Preserves pd.NA for values outside of tolerance range.
    - Ensures consistent frequency and handles edge cases.

    Examples
    --------
    >>> import pandas as pd
    >>>
    >>> # Create a sample dataframe
    >>> df = pd.DataFrame({
    ...     'timestamp': pd.date_range('2023-01-01', periods=5, freq='7min'),
    ...     'value': [1, 2, 3, 4, 5],
    ...     'cumulative': [10, 20, 30, 40, 50]
    ... })
    >>>
    >>> # Set alignment parameters
    >>> start_time = pd.Timestamp('2023-01-01')
    >>> freq = 300  # 5 minutes in seconds
    >>>
    >>> # Align timestamps
    >>> aligned_df = align_timestamps(
    ...     df,
    ...     'timestamp',
    ...     start_time,
    ...     freq,
    ...     tolerance=60,
    ...     method='interpolation',
    ...     cumulative_columns=['cumulative']
    ... )
    >>>
    >>> print(aligned_df)

    This will produce an aligned dataframe with 5-minute intervals,
    interpolating the cumulative column and using nearest neighbor
    for the non-cumulative column within a 60-second tolerance.
    """
    df = df.copy()

    timestamps_seconds = df[timestamp_col].astype(int) // 1_000_000_000
    start_time_seconds = start_time.timestamp()
    offsets = (timestamps_seconds - start_time_seconds) % freq

    original_duration = df[timestamp_col].max() - df[timestamp_col].min()
    end_time = start_time + original_duration + pd.Timedelta(seconds=freq)

    target_timestamps = pd.date_range(start=start_time, end=end_time, freq=f'{freq}S')

    if method == 'nearest':
        adjusted_offsets = np.where(offsets <= freq/2, 0, freq)
    elif method == 'interpolation':
        adjusted_offsets = offsets
    else:
        raise ValueError(f"Unknown method: {method}. Use 'nearest' or 'interpolation'.")

    df['aligned_timestamp'] = pd.to_datetime(
        start_time_seconds + 
        ((timestamps_seconds - start_time_seconds) - offsets + adjusted_offsets), 
        unit='s'
    )

    if df['aligned_timestamp'].duplicated().any():
        raise ValueError("Multiple records found for the same aligned timestamp.")

    aligned_df = pd.DataFrame(index=target_timestamps)

    for col in df.columns:
        if col == timestamp_col or col == 'aligned_timestamp':
            continue

        is_cumulative = cumulative_columns and col in cumulative_columns

        if method == 'nearest':
            aligned_df[col] = df.set_index('aligned_timestamp')[col].reindex(
                target_timestamps, method='nearest', tolerance=pd.Timedelta(seconds=tolerance)
            ).fillna(pd.NA)
        elif method == 'interpolation':
            if is_cumulative:
                aligned_df[col] = interpolate_cumulative(
                    df.set_index('aligned_timestamp')[col],
                    target_timestamps,
                    pd.Timedelta(seconds=tolerance)
                )
            else:
                aligned_df[col] = df.set_index('aligned_timestamp')[col].reindex(
                    target_timestamps, method='nearest', tolerance=pd.Timedelta(seconds=tolerance)
                ).fillna(pd.NA)

    aligned_df = aligned_df.dropna(how='all')
    aligned_df.reset_index(inplace=True)
    aligned_df.rename(columns={'index': 'aligned_timestamp'}, inplace=True)

    return aligned_df
