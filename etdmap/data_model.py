from importlib.resources import files

import pandas as pd

# Maps Grist/CSV "Type variabele" values to pandas dtype strings.
# This mapping belongs in code, not data — it's a fixed translation layer.
_DTYPE_MAP = {
    "number": "Float64",
    "date": "datetime64[ns]",
    "integer": "Int64",
    "string": "string",
    "boolean": "boolean",
}


def load_etdmodel() -> pd.DataFrame:
    """
    Load the ETD model definition from CSV.

    By default loads the bundled etdmap/data/etdmodel.csv. To use a custom
    model file, set etdmap.options.etdmodel_csv_path before importing this module:

        import etdmap
        etdmap.options.etdmodel_csv_path = "/path/to/my_model.csv"

    The CSV can be updated by any tooling that writes to that path — the
    derived structures (cumulative_columns, model_column_order, etc.) update
    automatically on next import.

    Returns
    -------
    pandas.DataFrame
        Full model DataFrame with all columns present in the CSV.
    """
    from etdmap import options

    if options.etdmodel_csv_path is not None:
        csv_path = options.etdmodel_csv_path
    else:
        csv_path = files("etdmap.data").joinpath("etdmodel.csv")

    return pd.read_csv(csv_path, dtype_backend="numpy_nullable")


def load_thresholds() -> pd.DataFrame:
    """
    Load thresholds from the bundled thresholds.csv.

    This file includes both primary-column thresholds and Diff-column thresholds
    (e.g. ElektriciteitNetgebruikHoogDiff) that are derived from cumulative columns
    and are not rows in etdmodel.csv. It can be regenerated from etdmodel.csv using
    the sync tooling in your workflow repository.

    Returns
    -------
    pandas.DataFrame
        Columns: Variabele, ThresholdType, Eenheid, Min, Max, ThresholdToelichting
    """
    thresholds_file = files("etdmap.data").joinpath("thresholds.csv")

    dtype_dict = {
        "Variabele": "string",
        "ThresholdType": "string",
        "Eenheid": "string",
        "Min": "Float64",
        "Max": "Float64",
        "ThresholdToelichting": "string",
    }

    return pd.read_csv(
        thresholds_file,
        dtype=dtype_dict,
        na_values=["n.a.", "NA", "N/A", ""],
        keep_default_na=True,
    )


def load_thresholds_as_dict() -> dict:
    """
    Load thresholds and return as {variable_name: {"Min": ..., "Max": ...}}.
    """
    thresholds_dict = {}
    for _, row in load_thresholds().iterrows():
        col = row["Variabele"]
        thresholds_dict[col] = {"Min": row["Min"], "Max": row["Max"]}
    return thresholds_dict


# ---------------------------------------------------------------------------
# Derived structures — built once at import time from load_etdmodel().
# Change the CSV (or set etdmap.options.etdmodel_csv_path) to update these.
# ---------------------------------------------------------------------------

def _build_derived_structures():
    df = load_etdmodel()

    # Sort Prestatiedata rows by Volgorde when available, fall back to CSV row order.
    # Volgorde is an explicit integer sequence column that defines the canonical
    # ordering of performance variables. Maintain it in whatever tool manages
    # the CSV so ordering is intentional and stable, not an artifact of row insertion.
    perf = df[df["Entiteit"] == "Prestatiedata"].copy()
    if "Volgorde" in perf.columns and perf["Volgorde"].notna().any():
        perf = perf.sort_values("Volgorde", na_position="last")

    # Cumulative meter-reading columns.
    # Exclude date-type columns (ReadingDate may be flagged Cumulatief in some
    # CSV versions — filtering by type is more robust than filtering by name).
    # The order of this list does not affect correctness.
    cumulative = df[
        (df["Cumulatief"] == "ja") & (df["Type variabele"] != "date")
    ]["Variabele"].tolist()

    # All performance-data columns in Volgorde order — used to validate column
    # presence and to reorder output parquet files.
    col_order = perf["Variabele"].tolist()

    # pandas dtype per performance column, derived from "Type variabele".
    col_types = {
        row["Variabele"]: _DTYPE_MAP.get(str(row["Type variabele"]), "Float64")
        for _, row in perf.iterrows()
        if pd.notna(row["Type variabele"])
    }

    # Columns suppliers are allowed to provide in their metadata files.
    supplier_meta = df[
        (df["Wie vult?"] == "Dataleverancier") & (df["Entiteit"] == "Metadata")
    ]["Variabele"].tolist()

    return cumulative, col_order, col_types, supplier_meta


(
    cumulative_columns,
    model_column_order,
    model_column_type,
    allowed_supplier_metadata_columns,
) = _build_derived_structures()

# data_analysis_columns: the full set of performance columns used to validate
# that required variables are present in a mapped household file.
# Currently equal to model_column_order (all Prestatiedata rows).
# Once the "Vereist" column in etdmodel.csv is correctly populated for all
# relevant columns, this can be narrowed to only Vereist=="ja" rows.
data_analysis_columns = model_column_order

# Preferred variables for aggregation workflows (etdtransform, reporting pipelines).
# Keeps aggregation runs fast by skipping momentaan variables and cumulative base columns
# that are rarely needed in standard project-level analysis.
# This mirrors the active entries in etdtransform/aggregate.py aggregation_variables.
#
# Includes both Diff columns (from raw mapped parquet files) and derived computed totals
# produced by the transform pipeline (e.g. ZonopwekBruto, ElektriciteitsgebruikTotaalNetto).
# Not all column names follow the *Diff pattern — derived totals have their own names.
#
# TODO: replace with a filter on an 'Aggregeren' column in etdmodel.csv once
# that column is added to the Grist data model.
preferred_aggregation_columns: list[str] = [
    # Electricity grid exchange — Diff variables (raw per-interval consumption)
    "ElektriciteitNetgebruikHoogDiff",
    "ElektriciteitNetgebruikLaagDiff",
    "ElektriciteitTerugleveringHoogDiff",
    "ElektriciteitTerugleveringLaagDiff",
    # Heat pump sub-system electricity — Diff variables
    "ElektriciteitsgebruikWTWDiff",
    "ElektriciteitsgebruikWarmtepompDiff",
    "ElektriciteitsgebruikBoosterDiff",
    "ElektriciteitsgebruikBoilervatDiff",
    "ElektriciteitsgebruikRadiatorDiff",
    # Derived computed totals (produced by etdtransform, not present in raw parquet)
    "ZonopwekBruto",
    "TerugleveringTotaalNetto",
    "ElektriciteitsgebruikTotaalNetto",
    "Netuitwisseling",
    "ElektriciteitsgebruikTotaalWarmtepomp",
    "ElektriciteitsgebruikTotaalGebouwgebonden",
    "ElektriciteitsgebruikTotaalHuishoudelijk",
    "Zelfgebruik",
    "ElektriciteitsgebruikTotaalBruto",
]
