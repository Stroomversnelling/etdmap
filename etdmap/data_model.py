from importlib.resources import files

import pandas as pd

# Required columns
data_analysis_columns = [
    "ReadingDate",
    "ElektriciteitNetgebruikHoog",
    "ElektriciteitNetgebruikLaag",
    "ElektriciteitTerugleveringHoog",
    "ElektriciteitTerugleveringLaag",
    "Zon-opwekTotaal",
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
    #'ElektriciteitsgebruikRadiator',
    'WarmteproductieWarmtepomp',
    'WatergebruikWarmTapwater',
    'Zon-opwekTotaal',

    'ElektriciteitsgebruikWarmtepompIntern',
    'WarmteproductieRuimteverwarming',
    'WarmteproductieTapwater',
    'WatergebruikWarmtepomp',
    'WatergebruikRuimteverwarming',
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
    # 'ElektriciteitsgebruikRadiator',
    # 'ElektriciteitsgebruikHuishoudelijk',
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

    'SlimmemeterVoltageL1',
    'SlimmemeterVoltageL2',
    'SlimmemeterVoltageL3',
    'SlimmemeterStroomsterkteL1',
    'SlimmemeterStroomsterkteL2',
    'SlimmemeterStroomsterkteL3',
    'ElektriciteitsgebruikWarmtepompIntern',
    'TemperatuurBoilervat',
    'TemperatuurBinnenWTW',
    'TemperatuurBuitenWTW',
    'TemperatuurBuitenWarmtepomp',
    'TemperatuurAfgifteAanvoer',
    'TemperatuurAfgifteRetour',
    'WarmteproductieRuimteverwarming',
    'WarmteproductieTapwater',
    'WatergebruikWarmtepomp',
    'WatergebruikRuimteverwarming',
    'WarmtepompModus',
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
    # 'ElektriciteitsgebruikRadiator': 'Float64',
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
    'SlimmemeterVoltageL1': 'Float64',
    'SlimmemeterVoltageL2': 'Float64',
    'SlimmemeterVoltageL3': 'Float64',
    'SlimmemeterStroomsterkteL1': 'Float64',
    'SlimmemeterStroomsterkteL2': 'Float64',
    'SlimmemeterStroomsterkteL3': 'Float64',
    'ElektriciteitsgebruikWarmtepompIntern': 'Float64',
    'TemperatuurBoilervat': 'Float64',
    'TemperatuurBinnenWTW': 'Float64',
    'TemperatuurBuitenWTW': 'Float64',
    'TemperatuurBuitenWarmtepomp': 'Float64',
    'TemperatuurAfgifteAanvoer': 'Float64',
    'TemperatuurAfgifteRetour': 'Float64',
    'WarmteproductieRuimteverwarming': 'Float64',
    'WarmteproductieTapwater': 'Float64',
    'WatergebruikWarmtepomp': 'Float64',
    'WatergebruikRuimteverwarming': 'Float64',
    'WarmtepompModus': 'Float64',
}

allowed_supplier_metadata_columns = [
    "ProjectIdLeverancier", "HuisIdLeverancier", "Weerstation", "Oppervlakte", "PlatOfZadelDak",
    "Compactheid", "Warmtebehoefte", "PrimairFossielGebruik", "Bouwjaar", "Renovatiejaar",
    "WoningType", "WoningTypeDetail", "WarmteopwekkerType", "WarmteopwekkerCategorie",
    "Warmteopwekker", "Ventilatiesysteem", "Kookinstallatie", "PVJaarbundel", "PVMerk",
    "PVType", "PVAantalPanelen", "PVWattpiekPerPaneel", "EPV", "GasgebruikVoorRenovatie",
    "ElektriciteitVoorRenovatie",
    'Eigenaarschap',
    'Nieuwheid',
    'WarmtepompKoudemiddel',
    'WarmtepompVermogenTh',
    'WarmtepompElElement',
    'WarmtepompElAansluiting',
    'WarmtepompBron',
    'BoilervatVolume',
    'AfgiftesysteemCategorie',
    'DakType'
]


def load_thresholds():
    """
    Load thresholds from a CSV file.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the thresholds data.
    """
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
    """
    Load thresholds from the package thresholds CSV file and convert to a dictionary.

    Returns
    -------
    dict
        A dictionary containing the thresholds data.
    """
    thresholds_dict = {}
    thresholds_df = load_thresholds()
    for _, row in thresholds_df.iterrows():
        col = row['Variabele']
        thresholds_dict[col] = {}
        thresholds_dict[col]['Min'] = row['Min']
        thresholds_dict[col]['Max'] = row['Max']
    return thresholds_dict

def load_etdmodel():
    """
    Load ETD model from the package ETD model definition CSV file.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the ETD model data.
    """
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
    