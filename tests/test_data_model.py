from pathlib import Path

import pandas as pd
import pytest

from etdmap.data_model import (
    cumulative_columns,
    model_column_type,
    required_model_columns,
)


def test_columns_etdmodelcsv():
    """
    Test if all columns in the required_model_columns in the
    data_model.py are indeed present in the etdmodel.csv in the 
    data folder.
    Additional columns may exist.
    """
    etdmodel_csv = pd.read_csv(Path(r'.\etdmap\data\etdmodel.csv'))
    columns_etdmodel = set(etdmodel_csv.Variabele.values)
    assert set(required_model_columns).issubset(columns_etdmodel)


def test_thresholdscsv():
    """
    Check if thresholds exist for all numeric columns in datamodel.

    The test check if all columns existent in the datamodel that
    are numeric, also have a numeric (not nan) threshold in the
    thresholds.csv
    """
    etdmodel_csv = pd.read_csv(Path(r'.\etdmap\data\etdmodel.csv'))
    # n/a is used to specify that it is not applicable 
    # we want to seperate this value from missing values, 
    # so prevent reading n/a as nan by pandas (default)
    # custom_na_values = ['n/a']
    tresholds_csv = pd.read_csv(
        Path(r'.\etdmap\data\thresholds.csv'),
        # na_values=custom_na_values,
        )
    numeric_cols_etdmodel = set(
        etdmodel_csv[etdmodel_csv['Type variabele']=='number'].Variabele)
    threshold_params = set(tresholds_csv.Variabele)

    # Check if all numeric columns in the datamodel are represented
    # in the thresholds.csv
    assert numeric_cols_etdmodel.issubset(threshold_params)

    # check if all columns have numeric min & max values, or are
    # n/a (not applicable)
    def is_numeric_or_na(val):
        str_value_check = str(val).lower() == 'n.a.'
        numeric_check = pd.notna(pd.to_numeric(val, errors='coerce'))
        return numeric_check or str_value_check

    check_min = tresholds_csv['Min'].apply(is_numeric_or_na)
    check_max = tresholds_csv['Max'].apply(is_numeric_or_na)
    print(check_min, check_max)
    assert check_min.all()
    assert check_max.all()


def test_thresholds_exist():
    """
    Test if all columns in the thresholds.csv have a valid threshold.

    Checks if a threshold is defined, and whether the threshold is
    numerical (and not nan)
    """


if __name__ == "__main__":
    # Run pytest for debugging the testing
    pytest.main(["-v"])

