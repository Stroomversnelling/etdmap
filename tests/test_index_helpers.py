import pandas as pd
import pytest

from etdmap.index_helpers import read_metadata


def test_read_metadata(tmp_path):
    # tmp_path fixture provides a temp directory unique
    # to the test run

    # Test the function with the valid file fixture
    result = read_metadata(valid_metadata_file)
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["HuisId", "OtherColumn"]
    assert result["HuisId"].dtype == object  # Ensure HuisId is string type

    # Test the function with an invalid file
    with pytest.raises(Exception) as excinfo:
        read_metadata(invalid_metadata_file)
    assert "Not all required columns" in str(excinfo.value)
