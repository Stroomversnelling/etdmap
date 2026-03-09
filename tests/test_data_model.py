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
    "ElektriciteitsgebruikTotaalHuishoudelijk",
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
    Check thresholds.csv for numeric and cumulative colums:

    - Check if thresholds exist for all numeric columns in datamodel.
    - Check if all thresholds are numeric in thresholds.csv
    - Check if the cumulative types match the cumulative_columns from etdmap.data_model
    """
    etdmodel_csv = pd.read_csv(Path(r'.\etdmap\data\etdmodel.csv'))
    thresholds_csv = pd.read_csv(
        Path(r'.\etdmap\data\thresholds.csv'),
        )
    numeric_cols_etdmodel = set(
        etdmodel_csv[(etdmodel_csv['Type variabele']=='number')&(etdmodel_csv['Entiteit']=='Prestatiedata')].Variabele)
    threshold_params = set(thresholds_csv.Variabele)

    # Check if all numeric columns in the datamodel are represented
    # in the thresholds.csv
    assert numeric_cols_etdmodel.issubset(threshold_params)

    assert pd.to_numeric(thresholds_csv['Min'], errors='coerce').notna().equals(thresholds_csv['Min'].notna()), "Min has non-numeric non-missing values (text?)"
    assert pd.to_numeric(thresholds_csv['Max'], errors='coerce').notna().equals(thresholds_csv['Max'].notna()), "Max has non-numeric non-missing values (text?)"

    # Check cummulative columns:
    cumm_columns_thresholds = set(
        thresholds_csv[thresholds_csv['ThresholdType']=='cumulatief'].Variabele
        )

    # Check if all cumulative columns in the etdmap.data_model
    # are also specified in the thresholds.csv
    assert set(cumulative_columns).issubset(cumm_columns_thresholds)
    # Notify if more cumulative columns are specified in the thresholds:
    if cumm_columns_thresholds - set(cumulative_columns):
        logging.warning(
            f"More cumulative columns are defined in thresholds.csv"
            f"then in etdmap.data_model cumulative_columns. "
            f"The following columns are found, but not required: "
            f"{cumm_columns_thresholds - set(cumulative_columns)}"
            )


if __name__ == "__main__":
    # Run pytest for debugging the testing
    pytest.main(["-v"])

