import logging
import os

import pandas as pd
from dataset_validators import dataset_flag_conditions
from mapping_helpers import cumulative_columns

mapped_folder_path = os.getenv('MAPPED_FOLDER_PATH')
bsv_metadata_file = os.getenv('BSV_METADATA_FILE')


def read_metadata(
    metadata_file: str,
    required_columns=['HuisId'],
) -> pd.DataFrame:
    xl = pd.ExcelFile(metadata_file)
    df = xl.parse(sheet_name='Data')

    if all(col in df.columns for col in required_columns):
        # Ensure HuisId is a string
        df['HuisId'] = df['HuisId'].astype(str)
        return df
    else:
        logging.error(
            f'Not all required columns in sheet "Data" in metadata file: {
                metadata_file
            }',
        )
        raise Exception(
            f'Not all required columns in sheet "Data" in metadata file: {
                metadata_file
            }',
        )


bsv_metadata_columns = [
    'HuisId',
    'HuisIdBSV',
    'Meenemen',
    'ProjectId',
    'ProjectIdBSV',
    'Notities',
    'Dataleverancier',
]
bsv_metadata_df = read_metadata(
    bsv_metadata_file,
    required_columns=bsv_metadata_columns,
)


def read_index() -> tuple[pd.DataFrame, str]:
    """
    Reads the index parquet file from the specified folder path.

    Returns:
    tuple: A tuple containing the DataFrame of the index and the path to the
    index file.
    """
    index_path = os.path.join(mapped_folder_path, 'index.parquet')
    if os.path.exists(index_path):
        index_df = pd.read_parquet(index_path)
    else:
        index_df = pd.DataFrame(
            columns=[
                'HuisId',
                'HuisCode',
                'Dataleverancier',
            ],
        )

    # Ensure HuisId is a string
    index_df['HuisId'] = index_df['HuisId'].astype(str)

    return index_df, index_path


def get_household_id_pairs(
    index_df: pd.DataFrame,
    data_folder_path: str,
    data_provider: str,
    list_files_func: callable,
) -> list:
    """
    Generates pairs of HuisCode and filenames for new and existing entries.

    Args:
    index_df (pd.DataFrame): The index DataFrame.
    data_folder_path (str): The path to the folder containing data files.
    data_provider (str): The name of the data provider.
    list_files_func (callable): A function to get a dictionary of id and the
    files in the data folder.

    Returns:
    list: A list of tuples containing HuisCode and filenames.
    """
    existing_ids = (
        index_df[index_df['Dataleverancier'] == data_provider]
        .set_index('HuisId')
        .to_dict()['HuisCode']
    )
    data_files = list_files_func(data_folder_path)

    household_id_pairs = []
    next_id = max(index_df['HuisCode'], default=0) + 1

    for huis_id, file in data_files.items():
        if huis_id in existing_ids:
            household_id_pairs.append((existing_ids[huis_id], file))
        else:
            household_id_pairs.append((next_id, file))
            next_id += 1

    return household_id_pairs


def update_index(
    index_df: pd.DataFrame,
    new_entry: dict,
    data_provider: str,
) -> pd.DataFrame:
    """
    Updates the index with new entries and recalculates or adds flag columns
    for dataset validators.

    Args:
    index_df (pd.DataFrame): The index DataFrame.
    new_entry (dict): The new entry to be added or updated in the index.
    data_provider (str): The name of the data provider.

    Returns:
    pd.DataFrame: The updated index DataFrame.
    """

    index_path = os.path.join(mapped_folder_path, 'index.parquet')

    # Ensure HuisId is a string in new_entry
    new_entry['HuisId'] = str(new_entry['HuisId'])
    new_entry['Dataleverancier'] = data_provider

    if new_entry['HuisId'] in index_df['HuisId'].values:
        index_df.loc[
            index_df['HuisId'] == new_entry['HuisId'],
            ['HuisCode', 'Dataleverancier'],
        ] = (new_entry['HuisCode'], data_provider)
    else:
        new_entry_df = pd.DataFrame([new_entry])
        index_df = pd.concat([index_df, new_entry_df], ignore_index=True)

    # Recalculate or add flag columns
    household_code = new_entry['HuisCode']
    dataset_file = os.path.join(
        mapped_folder_path,
        f"household_{household_code}_table.parquet",
    )
    if os.path.exists(dataset_file):
        df = pd.read_parquet(dataset_file)
        for flag, condition in dataset_flag_conditions.items():
            # Add flag column if it does not exist and ensure it's BooleanDtype
            if flag not in index_df.columns:
                index_df[flag] = pd.Series(
                    pd.NA,
                    dtype='boolean',
                    index=index_df.index,
                )
            try:
                validation_result = condition(df)
                index_df.loc[index_df['HuisCode'] == household_code, flag] = (
                    validation_result
                )
            except Exception as e:
                logging.error(
                    f"Error validating with {flag} for household {
                        household_code
                    }: {e}",
                    exc_info=True,
                )
                index_df.loc[
                    index_df['HuisCode'] == household_code,
                    flag,
                ] = pd.NA

    # Ensure all flag columns are of BooleanDtype
    for flag in dataset_flag_conditions.keys():
        if flag in index_df.columns:
            index_df[flag] = index_df[flag].astype('boolean')

    index_df = update_meta_validators(index_df)

    index_df.to_parquet(index_path, engine='pyarrow')

    return index_df


def update_meta_validators(index_df):
    cols = ['validate_' + col + 'Diff' for col in cumulative_columns]

    if all(col in index_df.columns for col in cols):
        index_df['validate_cumulative_diff_ok'] = index_df[cols].all(axis=1)
    else:
        index_df['validate_cumulative_diff_ok'] = pd.Series(
            pd.NA,
            dtype='boolean',
            index=index_df.index,
        )

    return index_df


def update_meenemen() -> pd.DataFrame:
    logging.info(
        'Updating index with which households to include in column "Meenemen"',
    )

    index_df, index_path = read_index()

    index_df.drop(columns=['Meenemen'], inplace=True)

    global bsv_metadata_df

    bsv_meenemen = bsv_metadata_df[['HuisIdBSV', 'Meenemen']]
    index_df = index_df.merge(bsv_meenemen, on=['HuisIdBSV'])

    index_df.to_parquet(index_path, engine='pyarrow')

    return index_df


def add_metadata_to_index(
    index_df: pd.DataFrame,
    metadata_df: pd.DataFrame,
    data_leverancier=None,
) -> pd.DataFrame:
    """
    Adds metadata columns to the index matching on the HuisId column.

    Args:
    index_df (pd.DataFrame):
        The index DataFrame.
    metadata_df (pd.DataFrame):
        The metadata DataFrame to be added to the index.
    data_leverancier (str):
        The data supplier name, if not present in metadata_df.

    Returns:
    pd.DataFrame: The updated index DataFrame.
    """

    if 'level_0' in index_df.columns:
        index_df.drop(columns=['level_0'], inplace=True)

    if data_leverancier is None:
        raise Exception(
            'Need to provide a supplier explicitly (data_leverancier is None).',  # noqa #E501
        )

    def metadata_format(df: pd.DataFrame):
        df['HuisId'] = df['HuisId'].astype(str)
        return df

    global mapped_folder_path
    index_path = os.path.join(mapped_folder_path, 'index.parquet')

    metadata_df = metadata_format(metadata_df)

    global bsv_metadata_df
    bsv_metadata_filtered_df = metadata_format(
        bsv_metadata_df[bsv_metadata_df['Dataleverancier'] == data_leverancier].copy(),
    )

    # Shared columns
    shared_columns = metadata_df.columns.intersection(bsv_metadata_columns)

    # Check that the combination values of shared columns is the
    # same across the three datasets: index_df, metadata_df, and
    # bsv_metadata_df and add a warning if not
    index_df.reset_index(inplace=True)

    index_df['source'] = 'index_df'
    metadata_df['source'] = 'metadata_df'
    bsv_metadata_filtered_df['source'] = 'bsv_metadata_df'

    shared_columns_source = shared_columns.tolist() + ['source']

    concatenated_df = pd.concat(
        [
            index_df[index_df['Dataleverancier'] == data_leverancier][
                shared_columns_source
            ],
            metadata_df[shared_columns_source],
            bsv_metadata_filtered_df[shared_columns_source],
        ],
    )
    grouped_df = (
        concatenated_df.groupby(shared_columns.tolist())
        .size()
        .reset_index(name='count')
    )
    inconsistent_combinations = grouped_df[grouped_df['count'] != 3]

    if not inconsistent_combinations.empty:
        logging.warning(
            'The following combinations of shared column values are '
            'inconsistent across the datasets:',
        )
        logging.warning(inconsistent_combinations)

    index_df.drop(columns=['source'], inplace=True)
    metadata_df.drop(columns=['source'], inplace=True)
    bsv_metadata_filtered_df.drop(columns=['source'], inplace=True)

    # Make sure data supplier is defined
    if 'Dataleverancier' not in metadata_df.columns:
        if data_leverancier is None:
            raise Exception('Data source not identified. Cannot add metadata.')
        else:
            metadata_df['Dataleverancier'] = data_leverancier

    # Define protected columns and drop them from provider metadata
    protected_columns = ['HuisCode', 'HuisIdBSV', 'ProjectIdBSV']
    metadata_df = metadata_df.drop(
        columns=[col for col in protected_columns if col in metadata_df.columns],
    )

    # add Meenemen, Notities, and protected_columns using data from
    # # bsv_metadata_df (do not change bsv_metadata_df)
    metadata_df = metadata_df.merge(
        bsv_metadata_filtered_df[
            [
                'HuisId',
                'ProjectId',
                'Dataleverancier',
                'HuisIdBSV',
                'ProjectIdBSV',
                'Meenemen',
                'Notities',
            ]
        ],
        on=['HuisId', 'ProjectId', 'Dataleverancier'],
        how='left',
    )

    # Add new columns with pd.NA if they do not already exist in index_df
    for column in metadata_df.columns:
        if column not in index_df.columns:
            index_df[column] = pd.NA

    # Update existing records
    index_df.set_index(
        ['HuisId', 'ProjectId', 'Dataleverancier'],
        inplace=True,
    )
    metadata_df.set_index(
        ['HuisId', 'ProjectId', 'Dataleverancier'],
        inplace=True,
    )

    index_df.update(metadata_df)
    index_df.reset_index(inplace=True)

    # Save the updated index to the parquet file
    index_df.to_parquet(index_path, engine='pyarrow')

    return index_df
