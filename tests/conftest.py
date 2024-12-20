import numpy as np
import pandas as pd
import pytest

from etdmap.index_helpers import bsv_metadata_columns


@pytest.fixture
def valid_metadata_file(tmp_path):
    # tmp_path fixture provides a temp directory unique
    # to the test run
    # Create a valid Excel file with the required columns
    metadata_file = tmp_path / "metadata.xlsx"

    data = {}
    num_rows = 5
    # for testing, make sure there is one numerical column
    data['num_column'] = np.random.randint(1, 100, size=num_rows)
    for column_name in bsv_metadata_columns:
        # Generate random string data
        data[column_name] = [f"{column_name}_val_{i}" for i in range(num_rows)]

    df = pd.DataFrame(data)
    df.to_excel(metadata_file, sheet_name="Data", index=False)
    return str(metadata_file)


@pytest.fixture
def invalid_metadata_file(tmp_path):
    # Create an invalid Excel file missing the required column
    metadata_file = tmp_path / "invalid_metadata.xlsx"
    data = {
        "SomeColumn": [1, 2, 3],
    }
    df = pd.DataFrame(data)
    df.to_excel(metadata_file, sheet_name="Data", index=False)
    return str(metadata_file)
