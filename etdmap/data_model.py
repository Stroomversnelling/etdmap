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


def get_aggregation_config() -> dict:
    """
    Return per-variable resampling and aggregation config from etdmodel.csv.

    Only includes variables where AggregatieMeenemen is True.

    Returns
    -------
    dict[str, dict[str, str]]
        Mapping of column name to::

            {
                "resample_method": str,   # ResamplingMethode: "sum", "avg", "max", ...
                "aggregate_method": str,  # AggregatieMethode: "avg", "sum", ...
            }

    Raises
    ------
    ValueError
        If any included variable is missing ResamplingMethode or AggregatieMethode
        in the model (model data incomplete -- fail hard per ADR-003).
    """
    df = load_etdmodel()
    included = df[df["AggregatieMeenemen"].fillna(False).astype(bool)]

    config = {}
    missing = []
    for _, row in included.iterrows():
        col = row["Variabele"]
        resample = row.get("ResamplingMethode", None)
        aggregate = row.get("AggregatieMethode", None)
        if pd.isna(resample) or resample == "":
            missing.append(f"{col}: ResamplingMethode")
        if pd.isna(aggregate) or aggregate == "":
            missing.append(f"{col}: AggregatieMethode")
        if missing:
            continue
        config[str(col)] = {
            "resample_method": str(resample),
            "aggregate_method": str(aggregate),
        }

    if missing:
        raise ValueError(
            f"etdmodel.csv is incomplete -- AggregatieMeenemen=True variables"
            f" missing required method columns: {missing}"
        )

    return config


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

    # Raw provider performance columns in Volgorde order — used to validate column
    # presence and to reorder output parquet files for INCOMING mapped data.
    # Scope: Entiteit == "Prestatiedata" ONLY.
    # Do NOT use this as the derivation target universe — use all_perf_cols below.
    # See etdmap/DECISIONS.md ADR-001 for the rationale.
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

    # All performance columns expected after the full pipeline has run:
    # both raw provider columns (Prestatiedata) and ETD-derived columns
    # (PrestatiedataBerekend). Used as the derivation target universe in
    # add_calculated_columns_adaptive(). See etdmap/DECISIONS.md ADR-001.
    perf_all = df[df["Entiteit"].str.startswith("Prestatiedata", na=False)].copy()
    all_perf_cols = perf_all["Variabele"].tolist()

    # Required subset: columns that must be present at end of pipeline.
    # Missing any of these after derivation is a data quality error (logging.error).
    required_perf_cols = perf_all[perf_all["Vereist"] == "ja"]["Variabele"].tolist()

    return cumulative, col_order, col_types, supplier_meta, all_perf_cols, required_perf_cols


(
    cumulative_columns,
    model_column_order,
    model_column_type,
    allowed_supplier_metadata_columns,
    all_performance_data_columns,
    required_performance_data_columns,
) = _build_derived_structures()

# data_analysis_columns: columns validated by dataset_validators.
# Currently an alias of model_column_order (all Prestatiedata rows).
# TODO: narrow to Vereist=="ja" once that column is fully populated in etdmodel.csv.
# A regression test in tests/test_data_model.py guards the current alias.
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
