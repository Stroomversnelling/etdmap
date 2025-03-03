import logging
from pathlib import Path

import pandas as pd
import pytest

from etdmap.data_model import (
    cumulative_columns,
)

required_model_columns = [
    "ProjectIdBSV",
    "ProjectIdLeverancier",
    "HuisIdBSV",
    "HuisIdLeverancier",
    "Weerstation",
    "Oppervlakte",
    "Compactheid",
    "Warmtebehoefte",
    "PrimairFossielGebruik",
    "Bouwjaar",
    "Renovatiejaar",
    "WoningType",
    "WoningTypeDetail",
    "WarmteopwekkerType",
    "WarmteopwekkerCategorie",
    "Warmteopwekker",
    "Ventilatiesysteem",
    "Kookinstallatie",
    "PVJaarbundel",
    "PVMerk",
    "PVType",
    "PVAantalPanelen",
    "PVWattpiekPerPaneel",
    "EPV",
    "GasgebruikVoorRenovatie",
    "ElektriciteitVoorRenovatie",
    "HuisIdBSV",
    "HuisIdLeverancier",
    "ReadingDate",
    "ElektriciteitNetgebruikHoog",
    "ElektriciteitNetgebruikLaag",
    "ElektriciteitTerugleveringHoog",
    "ElektriciteitTerugleveringLaag",
    "ElektriciteitVermogen",
    "Gasgebruik",
    "ElektriciteitsgebruikWTW",
    "ElektriciteitsgebruikWarmtepomp",
    "ElektriciteitsgebruikBooster",
    "ElektriciteitsgebruikBoilervat",
    "ElektriciteitsgebruikHuishoudelijk",
    "TemperatuurWarmTapwater",
    "TemperatuurWoonkamer",
    "TemperatuurSetpointWoonkamer",
    "WarmteproductieWarmtepomp",
    "WatergebruikWarmTapwater",
    "Zon-opwekMomentaan",
    "Zon-opwekTotaal",
    "CO2",
    "Luchtvochtigheid",
    "Ventilatiedebiet",
]

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
    # give warning when more columns are defined in the etdmodel.csv
    # then are required in etdmap.data_model
    if columns_etdmodel - set(required_model_columns):
        logging.warning(
            f"More columns are defined in etdmodel.csv then are"
            f"specified in etdmap.data_model require_columns"
            f"The following columns are found, but not required: "
            f"{columns_etdmodel - set(required_model_columns)}"
            )

def test_thresholdscsv():
    """
    Check thresholds.csv for columns, numeric values and comulatief.

    Check if thresholds exist for all numeric columns in datamodel.
    Check if all thresholds are numeric (not nan) or n.a. in thresholds.csv
    Check if the cummulative types match the cumulative_columns from
    etdmap.data_model
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
    # n.a. (not applicable)
    def is_numeric_or_na(val):
        # Note: originally n.a. was spelled n/a in the csv.
        # these strings are automatically replaced by NAN values
        # Since we want to compare with missing values (also NAN)
        # n/a was renamed to n.a.
        str_value_check = str(val).lower() == 'n.a.'
        numeric_check = pd.notna(pd.to_numeric(val, errors='coerce'))
        return numeric_check or str_value_check

    check_min = tresholds_csv['Min'].apply(is_numeric_or_na)
    check_max = tresholds_csv['Max'].apply(is_numeric_or_na)
    assert check_min.all()
    assert check_max.all()

    # Check cummulative columns:
    cumm_columns_thresholds = set(
        tresholds_csv[tresholds_csv['VariabelType']=='cumulatief'].Variabele
        )
    # Check if all cummulative columns in the etdmap.data_model
    # are also specified in the thresholds.csv
    assert set(cumulative_columns).issubset(cumm_columns_thresholds)
    # Notify if more cummulative columns are specified in the thresholds:
    if cumm_columns_thresholds - set(cumulative_columns):
        logging.warning(
            f"More cummulative columns are defined in thresholds.csv"
            f"then in etdmap.data_model cumulative_columns. "
            f"The following columns are found, but not required: "
            f"{cumm_columns_thresholds - set(cumulative_columns)}"
            )


if __name__ == "__main__":
    # Run pytest for debugging the testing
    pytest.main(["-v"])

