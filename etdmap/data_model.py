from importlib.resources import files

import pandas as pd

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

cumulative_columns = [
    'ElektriciteitNetgebruikHoog',
    'ElektriciteitNetgebruikLaag',
    'ElektriciteitTerugleveringHoog',
    'ElektriciteitTerugleveringLaag',
    'Gasgebruik',
    'ElektriciteitsgebruikWTW',
    'ElektriciteitsgebruikWarmtepomp',
    'ElektriciteitsgebruikBooster',
    'ElektriciteitsgebruikBoilervat',
    'ElektriciteitsgebruikRadiator',
    # 'ElektriciteitsgebruikHuishoudelijk',
    'WarmteproductieWarmtepomp',
    'WatergebruikWarmTapwater',
    'Zon-opwekTotaal',
]

model_column_order = [
    'ReadingDate',
    'ElektriciteitNetgebruikHoog',
    'ElektriciteitNetgebruikLaag',
    'ElektriciteitTerugleveringHoog',
    'ElektriciteitTerugleveringLaag',
    'ElektriciteitVermogen',
    'Gasgebruik',
    'ElektriciteitsgebruikWTW',
    'ElektriciteitsgebruikWarmtepomp',
    'ElektriciteitsgebruikBooster',
    'ElektriciteitsgebruikBoilervat',
    'ElektriciteitsgebruikRadiator',
    'ElektriciteitsgebruikHuishoudelijk',
    'TemperatuurWarmTapwater',
    'TemperatuurWoonkamer',
    'TemperatuurSetpointWoonkamer',
    'WarmteproductieWarmtepomp',
    'WatergebruikWarmTapwater',
    'Zon-opwekMomentaan',
    'Zon-opwekTotaal',
    'CO2',
    'Luchtvochtigheid',
    'Ventilatiedebiet',
]
model_column_type = {
    'ReadingDate': 'datetime64[ns]',  # pandas datetime column
    'ElektriciteitNetgebruikHoog': 'float64',
    'ElektriciteitNetgebruikLaag': 'float64',
    'ElektriciteitTerugleveringHoog': 'float64',
    'ElektriciteitTerugleveringLaag': 'float64',
    'ElektriciteitVermogen': 'float64',
    'Gasgebruik': 'float64',
    'ElektriciteitsgebruikWTW': 'float64',
    'ElektriciteitsgebruikWarmtepomp': 'float64',
    'ElektriciteitsgebruikBooster': 'float64',
    'ElektriciteitsgebruikBoilervat': 'float64',
    'ElektriciteitsgebruikRadiator': 'float64',
    # 'ElektriciteitsgebruikHuishoudelijk': 'float64',
    'TemperatuurWarmTapwater': 'float64',
    'TemperatuurWoonkamer': 'float64',
    'TemperatuurSetpointWoonkamer': 'float64',
    'WarmteproductieWarmtepomp': 'float64',
    'WatergebruikWarmTapwater': 'float64',
    'Zon-opwekMomentaan': 'float64',
    'Zon-opwekTotaal': 'float64',
    'CO2': 'float64',
    'Luchtvochtigheid': 'float64',
    'Ventilatiedebiet': 'float64',
}

def load_thresholds():
    thresholds_file = files("etdmap.data").joinpath("thresholds.csv")

    dtype_dict = {
        "Variabele": "string",
        "VariabelType": "string",
        "Eenheid": "string",
        "Min": "float",
        "Max": "float",
        "Toelichting": "string"
    }

    df = pd.read_csv(
        thresholds_file,
        dtype=dtype_dict,
        na_values=["n.a.", "NA", "N/A", ""],  # Specify values to be treated as NA
        keep_default_na=True  # Keep pandas' default NA values
    )


    return df

def load_etdmodel():
    etdmodel_file = files("etdmap.data").joinpath("etdmodel.csv")

    dtype_dict = {
        "Entiteit": "string",
        "Variabele": "string",
        "Key": "string",
        "Type variabele": "string",
        "Vereist": "string",
        "Resolutie": "string",
        "Wie vult?": "string",
        "Bron": "string",
        "Definitie": "string",
        "AVG gevoelig": "string"
    }

    df = pd.read_csv(
        etdmodel_file,
        dtype=dtype_dict,
        na_values=["n.a.", "NA", "N/A", ""],  # Specify values to be treated as NA
        keep_default_na=True  # Keep pandas' default NA values
    )

    return df
