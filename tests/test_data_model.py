import logging
from pathlib import Path

import pandas as pd
import pytest

from etdmap.data_model import (
    cumulative_columns,
    data_analysis_columns,
    get_aggregation_config,
    model_column_order,
    all_performance_data_columns,
    required_performance_data_columns,
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
    "TemperatuurWoonkamerSetpoint",
    "WarmteproductieWarmtepomp",
    "WatergebruikWarmTapwater",
    "Zon-opwekMomentaan",
    "Zon-opwekTotaal",
    "CO2",
    "Luchtvochtigheid",
    "DebietVentilatieAanvoer",
    "DebietVentilatieExtractie",
]

# this test is now wrong - we use the csv as a source of truth - to be fixed later.
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
    numeric_cols_with_range = set(
        etdmodel_csv[
            (etdmodel_csv['Type variabele'] == 'number') &
            (etdmodel_csv['Entiteit'] == 'Prestatiedata') &
            (etdmodel_csv['Min'].notna() | etdmodel_csv['Max'].notna())
        ].Variabele
    )
    threshold_params = set(thresholds_csv.Variabele)

    # Only require threshold entries for number columns that have Min or Max defined in the model.
    # Columns without any range in the model (e.g. categorical codes) do not need threshold entries.
    assert numeric_cols_with_range.issubset(threshold_params)

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


# ---------------------------------------------------------------------------
# get_aggregation_config
# ---------------------------------------------------------------------------

def test_get_aggregation_config_returns_known_columns():
    config = get_aggregation_config()
    assert isinstance(config, dict)
    assert len(config) > 0
    # A known Diff column must be present with the expected method values
    assert "ElektriciteitNetgebruikHoogDiff" in config
    entry = config["ElektriciteitNetgebruikHoogDiff"]
    assert entry["resample_method"] == "sum"
    assert entry["aggregate_method"] == "avg"


def test_get_aggregation_config_raises_on_missing_resample_method(tmp_path, monkeypatch):
    import etdmap
    from pathlib import Path

    model = pd.read_csv(Path("etdmap/data/etdmodel.csv"))
    # Clear ResamplingMethode for a known included variable
    model.loc[model["Variabele"] == "ElektriciteitNetgebruikHoogDiff", "ResamplingMethode"] = None
    patched = tmp_path / "etdmodel_patched.csv"
    model.to_csv(patched, index=False)

    monkeypatch.setattr(etdmap.options, "etdmodel_csv_path", str(patched))
    with pytest.raises(ValueError, match="ResamplingMethode"):
        get_aggregation_config()


def test_get_aggregation_config_raises_on_missing_aggregate_method(tmp_path, monkeypatch):
    import etdmap
    from pathlib import Path

    model = pd.read_csv(Path("etdmap/data/etdmodel.csv"))
    model.loc[model["Variabele"] == "ElektriciteitNetgebruikHoogDiff", "AggregatieMethode"] = None
    patched = tmp_path / "etdmodel_patched.csv"
    model.to_csv(patched, index=False)

    monkeypatch.setattr(etdmap.options, "etdmodel_csv_path", str(patched))
    with pytest.raises(ValueError, match="AggregatieMethode"):
        get_aggregation_config()


# ---------------------------------------------------------------------------
# Regression tests for existing derived variables (etdmap/DECISIONS.md ADR-001)
# ---------------------------------------------------------------------------

def test_cumulative_columns_contains_no_date_types():
    """Guard: the Type variabele != 'date' filter must hold."""
    etdmodel_csv = pd.read_csv(Path(r'.\etdmap\data\etdmodel.csv'))
    date_vars = set(etdmodel_csv[etdmodel_csv["Type variabele"] == "date"]["Variabele"])
    overlap = set(cumulative_columns) & date_vars
    assert not overlap, f"Date-typed columns found in cumulative_columns: {overlap}"


def test_cumulative_columns_are_all_prestatiedata():
    """Guard: every cumulative column must be Prestatiedata (not Berekend).

    This documents the invariant that makes the missing Entiteit filter safe.
    If this test ever fails, either the data model was changed or a computed
    column was incorrectly flagged as cumulative.
    """
    etdmodel_csv = pd.read_csv(Path(r'.\etdmap\data\etdmodel.csv'))
    entiteit_map = dict(zip(etdmodel_csv["Variabele"], etdmodel_csv["Entiteit"]))
    for col in cumulative_columns:
        assert entiteit_map.get(col) == "Prestatiedata", (
            f"{col} is in cumulative_columns but has Entiteit={entiteit_map.get(col)!r}"
        )


def test_model_column_order_is_prestatiedata_only():
    """Regression: model_column_order must not include PrestatiedataBerekend rows.

    Callers such as mapping_helpers and dataset_validators expect only
    provider-supplied (Prestatiedata) columns here.
    """
    etdmodel_csv = pd.read_csv(Path(r'.\etdmap\data\etdmodel.csv'))
    entiteit_map = dict(zip(etdmodel_csv["Variabele"], etdmodel_csv["Entiteit"]))
    for col in model_column_order:
        assert entiteit_map.get(col) == "Prestatiedata", (
            f"{col} is in model_column_order but has Entiteit={entiteit_map.get(col)!r}"
        )


def test_model_column_order_matches_volgorde_sort():
    """Regression: model_column_order must equal Prestatiedata rows sorted by Volgorde."""
    etdmodel_csv = pd.read_csv(Path(r'.\etdmap\data\etdmodel.csv'))
    perf = etdmodel_csv[etdmodel_csv["Entiteit"] == "Prestatiedata"].copy()
    if "Volgorde" in perf.columns and perf["Volgorde"].notna().any():
        perf = perf.sort_values("Volgorde", na_position="last")
    expected = perf["Variabele"].tolist()
    assert model_column_order == expected, (
        "model_column_order does not match Prestatiedata rows sorted by Volgorde"
    )


def test_data_analysis_columns_is_alias_of_model_column_order():
    """Regression: data_analysis_columns is currently an alias of model_column_order.

    If this changes intentionally, update this test and etdmap/DECISIONS.md ADR-001.
    """
    assert data_analysis_columns == model_column_order


# ---------------------------------------------------------------------------
# Tests for new derived variables
# ---------------------------------------------------------------------------

def test_all_performance_data_columns_includes_prestatiedata_berekend():
    """all_performance_data_columns must include at least one PrestatiedataBerekend row."""
    etdmodel_csv = pd.read_csv(Path(r'.\etdmap\data\etdmodel.csv'))
    entiteit_map = dict(zip(etdmodel_csv["Variabele"], etdmodel_csv["Entiteit"]))
    berekend = [c for c in all_performance_data_columns if entiteit_map.get(c) == "PrestatiedataBerekend"]
    assert berekend, "all_performance_data_columns contains no PrestatiedataBerekend columns"


def test_all_performance_data_columns_is_superset_of_model_column_order():
    """all_performance_data_columns must include everything in model_column_order."""
    assert set(model_column_order) <= set(all_performance_data_columns)


def test_required_performance_data_columns_are_subset_of_all():
    """required_performance_data_columns must be a subset of all_performance_data_columns."""
    assert set(required_performance_data_columns) <= set(all_performance_data_columns)


def test_required_cols_match_vereist_ja_in_model():
    """required_performance_data_columns must exactly match Vereist=='ja' Prestatiedata* rows."""
    etdmodel_csv = pd.read_csv(Path(r'.\etdmap\data\etdmodel.csv'))
    perf_all = etdmodel_csv[etdmodel_csv["Entiteit"].str.startswith("Prestatiedata", na=False)]
    expected = set(perf_all[perf_all["Vereist"] == "ja"]["Variabele"].tolist())
    assert set(required_performance_data_columns) == expected


if __name__ == "__main__":
    # Run pytest for debugging the testing
    pytest.main(["-v"])

