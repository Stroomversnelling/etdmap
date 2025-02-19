from importlib.resources import files

import pandas as pd

data_analysis_columns = [
    "ReadingDate",
    "Ventilatiedebiet",
    "CO2",
    "ElektriciteitNetgebruikHoog",
    "ElektriciteitNetgebruikLaag",
    "ElektriciteitTerugleveringHoog",
    "ElektriciteitTerugleveringLaag",
    "ElektriciteitVermogen",
    "ElektriciteitsgebruikWTW",
    "ElektriciteitsgebruikWarmtepomp",
    "ElektriciteitsgebruikBooster",
    "ElektriciteitsgebruikBoilervat",
    "ElektriciteitsgebruikHuishoudelijk",
    "TemperatuurWarmTapwater",
    "TemperatuurWoonkamer",
    "WarmteproductieWarmtepomp",
    "TemperatuurSetpointWoonkamer",
    "Zon-opwekMomentaan",
    "Zon-opwekTotaal",
    "Luchtvochtigheid",
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
    'ElektriciteitNetgebruikHoog': 'Float64',
    'ElektriciteitNetgebruikLaag': 'Float64',
    'ElektriciteitTerugleveringHoog': 'Float64',
    'ElektriciteitTerugleveringLaag': 'Float64',
    'ElektriciteitVermogen': 'Float64',
    'Gasgebruik': 'Float64',
    'ElektriciteitsgebruikWTW': 'Float64',
    'ElektriciteitsgebruikWarmtepomp': 'Float64',
    'ElektriciteitsgebruikBooster': 'Float64',
    'ElektriciteitsgebruikBoilervat': 'Float64',
    'ElektriciteitsgebruikRadiator': 'Float64',
    # 'ElektriciteitsgebruikHuishoudelijk': 'Float64',
    'TemperatuurWarmTapwater': 'Float64',
    'TemperatuurWoonkamer': 'Float64',
    'TemperatuurSetpointWoonkamer': 'Float64',
    'WarmteproductieWarmtepomp': 'Float64',
    'WatergebruikWarmTapwater': 'Float64',
    'Zon-opwekMomentaan': 'Float64',
    'Zon-opwekTotaal': 'Float64',
    'CO2': 'Float64',
    'Luchtvochtigheid': 'Float64',
    'Ventilatiedebiet': 'Float64',
}


allowed_supplier_metadata_columns = [
    "ProjectIdLeverancier", "HuisIdLeverancier", "Weerstation", "Oppervlakte", "PlatOfZadelDak",
    "Compactheid", "Warmtebehoefte", "PrimairFossielGebruik", "Bouwjaar", "Renovatiejaar",
    "WoningType", "WoningTypeDetail", "WarmteopwekkerType", "WarmteopwekkerCategorie", 
    "Warmteopwekker", "Ventilatiesysteem", "Kookinstallatie", "PVJaarbundel", "PVMerk", 
    "PVType", "PVAantalPanelen", "PVWattpiekPerPaneel", "EPV", "GasgebruikVoorRenovatie", 
    "ElektriciteitVoorRenovatie", "Meenemen", "ProjectIdBSV"
]


def load_thresholds():
    thresholds_file = files("etdmap.data").joinpath("thresholds.csv")

    dtype_dict = {
        "Variabele": "string",
        "VariabelType": "string",
        "Eenheid": "string",
        "Min": "Float64",
        "Max": "Float64",
        "Toelichting": "string"
    }

    df = pd.read_csv(
        thresholds_file,
        dtype=dtype_dict,
        na_values=["n.a.", "NA", "N/A", ""],  # Specify values to be treated as NA
        keep_default_na=True  # Keep pandas' default NA values
    )


    return df

def load_thresholds_as_dict() -> dict:
    thresholds_dict = {}
    thresholds_df = load_thresholds()
    for _, row in thresholds_df.iterrows():
        col = row['Variabele']
        thresholds_dict[col] = {}
        thresholds_dict[col]['Min'] = row['Min']
        thresholds_dict[col]['Max'] = row['Max']
    return thresholds_dict

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
