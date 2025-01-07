import pandas as pd
import pytest

import etdmap
from etdmap.index_helpers import (
    bsv_metadata_columns,
    get_bsv_metadata,
    read_metadata,
)


def test_read_metadata(valid_metadata_file, invalid_metadata_file):
    # Test the function with the valid file fixture
    result = read_metadata(valid_metadata_file)
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["num_column", *bsv_metadata_columns]

    # Test the function with an invalid file
    with pytest.raises(Exception) as excinfo:
        read_metadata(invalid_metadata_file, required_columns=["TestCol"])
    assert "Not all required columns" in str(excinfo.value)


def test_get_bsv_metadata(valid_metadata_file, invalid_metadata_file):
    # test valid:
    etdmap.options.bsv_metadata_file = valid_metadata_file
    result = get_bsv_metadata()
    required_columns = set(bsv_metadata_columns)
    assert isinstance(result, pd.DataFrame)
    assert required_columns.issubset(set(result.columns))

    # test if it fails without proper columns
    etdmap.options.bsv_metadata_file = invalid_metadata_file
    with pytest.raises(ValueError) as excinfo:
        get_bsv_metadata()

    assert "Not all required columns in" in str(excinfo.value)


if __name__ == "__main__":
    # Run pytest for debugging the testing
    pytest.main(["-v"])
