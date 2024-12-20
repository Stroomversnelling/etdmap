import pandas as pd
import pytest


@pytest.fixture
def valid_metadata_file(tmp_path):
    # Create a valid Excel file with the required columns
    metadata_file = tmp_path / "metadata.xlsx"
    data = {
        "HuisIdLeverancier": [1, 2, 3],
        "OtherColumn": ["A", "B", "C"],
    }
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
