import logging
import re
from importlib.resources import files

import pandas as pd

logger = logging.getLogger(__name__)

# Maps the data model's "Type variabele" values to pandas dtype strings.
# This mapping belongs in code, not data -- it's a fixed translation layer.
_DTYPE_MAP = {
    "number": "Float64",
    "date": "datetime64[ns]",
    "integer": "Int64",
    "string": "string",
    "boolean": "boolean",
}

_YES_NO_CHOICE = frozenset({"ja", "nee"})

#: Text a declared-boolean value may arrive as. Suppliers write yes/no in their
#: own language and some write 1/0, and the set grows as suppliers do; anything
#: outside it raises rather than becoming pd.NA, so a new spelling surfaces as a
#: question instead of silently emptying a column.
BOOLEAN_SYNONYMS = {
    "ja": True, "yes": True, "true": True, "waar": True, "1": True,
    "nee": False, "no": False, "false": False, "onwaar": False, "0": False,
}

peak_offpeak_substrings = ["Hoog", "Laag"]


def load_etdmodel() -> pd.DataFrame:
    """
    Load the ETD model definition from CSV.

    By default loads the bundled etdmap/data/etdmodel.csv. To use a custom
    model file, set etdmap.options.etdmodel_csv_path before importing this module:

        import etdmap
        etdmap.options.etdmodel_csv_path = "/path/to/my_model.csv"

    The CSV can be updated by any tooling that writes to that path -- the
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


#: Applied when a threshold declares no shape of its own. Zero sustained rate
#: with a square-root exponent: nothing is assumed to be holdable indefinitely,
#: and the plausible excess grows with the square root of the interval.
DEFAULT_UPPER_THRESHOLD_SUSTAINED = 0.0
DEFAULT_UPPER_THRESHOLD_EXPONENT = 0.5


def scale_upper_bound(maximum, n, sustained=pd.NA, exponent=pd.NA):
    """The upper bound for an interval *n* times the threshold's own unit.

        B(n) = n * sustained + (maximum - sustained) * n ** exponent

    *sustained* is the rate that may be held for the whole interval and so
    scales with it; the remainder is a peak and grows more slowly. B(1) is
    *maximum* for any parameters, so a column reporting at the declared unit is
    unaffected.

    Raises
    ------
    ValueError
        If *sustained* exceeds *maximum*, which would make the bound shrink as
        the interval grows, or if *exponent* falls outside [0, 1].
    """
    if pd.isna(maximum):
        return maximum
    sustained = (
        DEFAULT_UPPER_THRESHOLD_SUSTAINED if pd.isna(sustained) else float(sustained)
    )
    exponent = (
        DEFAULT_UPPER_THRESHOLD_EXPONENT if pd.isna(exponent) else float(exponent)
    )

    if sustained > float(maximum):
        raise ValueError(
            f"UpperThresholdSustained {sustained} exceeds Max {maximum}: the "
            f"bound would fall as the interval grows"
        )
    if not 0.0 <= exponent <= 1.0:
        raise ValueError(
            f"UpperThresholdExponent {exponent} is outside [0, 1]"
        )

    return n * sustained + (float(maximum) - sustained) * n ** exponent


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
        Columns: Variabele, ThresholdType, Eenheid, Min, Max,
        UpperThresholdSustained, UpperThresholdExponent, ThresholdToelichting
    """
    thresholds_file = files("etdmap.data").joinpath("thresholds.csv")

    dtype_dict = {
        "Variabele": "string",
        "ThresholdType": "string",
        "Eenheid": "string",
        "Min": "Float64",
        "Max": "Float64",
        "UpperThresholdSustained": "Float64",
        "UpperThresholdExponent": "Float64",
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
    Load thresholds as {variable_name: {"Min", "Max", "ThresholdType"}}.

    A bound is declared against a unit of time -- "momentaan", "5-minute",
    "cumulatief" or "algemeen" -- and ThresholdType names it. Min or Max may be
    pd.NA, meaning no bound is declared on that side.
    """
    thresholds_dict = {}
    for _, row in load_thresholds().iterrows():
        col = row["Variabele"]
        thresholds_dict[col] = {
            "Min": row["Min"],
            "Max": row["Max"],
            "ThresholdType": row["ThresholdType"],
            # Shape the upper bound takes when scaled to a coarser cadence.
            # Absent means the defaults in scale_upper_bound apply.
            "UpperThresholdSustained": row.get("UpperThresholdSustained", pd.NA),
            "UpperThresholdExponent": row.get("UpperThresholdExponent", pd.NA),
        }
    return thresholds_dict


def _validate_yes_no_column(df: pd.DataFrame, column: str) -> None:
    """Raise ``ValueError`` if ``df[column]`` contains anything other than
    ``"ja"``, ``"nee"``, or empty/NA.

    ja/nee columns in etdmodel.csv are fixed-choice fields, not free
    text. Any other content (capitalisation, whitespace, typos) is a
    sync bug -- normalising it away here would hide the bug. Loud
    failure at import time is the right behaviour.
    """
    series = df[column].dropna()
    bad_mask = ~series.astype(str).isin(_YES_NO_CHOICE)
    if bad_mask.any():
        offenders = (
            df.loc[series.index[bad_mask], ["Variabele", column]]
              .to_dict(orient="records")
        )
        raise ValueError(
            f"etdmodel.csv: {column} is a choice field and must be one of "
            f"{sorted(_YES_NO_CHOICE)} (or empty). Offending rows: {offenders}. "
            f"Fix the source data model and re-sync."
        )


def load_unit_map() -> dict:
    """
    Return {Variabele: Eenheid} from the data model.

    Flat ``{column_name: unit_string}`` derived from explicit rows in
    ``etdmodel.csv``. Variables with no declared unit (NA Eenheid) are
    omitted so callers can use ``.get(col, "")`` semantics.

    No suffix-based Diff inheritance: every variable referenced
    downstream must have an explicit row in ``etdmodel.csv`` (synced
    from the source data model). Missing rows surface as a clean lookup
    miss at the call site, pointing callers at the source data model
    instead of papering over the gap with a heuristic.
    """
    df = load_etdmodel()
    unit_map: dict[str, str] = {}
    for variable, unit in zip(df["Variabele"], df["Eenheid"]):
        if pd.isna(variable) or pd.isna(unit):
            continue
        unit_map[str(variable)] = str(unit)
    return unit_map


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
    inconsistent_derive = []
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
        resample = str(resample)
        aggregate = str(aggregate)
        # 'derive' marks a variable that is recomputed from its catalog rule
        # AFTER its components are aggregated (e.g. a ratio like
        # ZelfgebruikPercentage, which cannot be summed or averaged as a stored
        # value). That is only meaningful when BOTH axes derive -- a mix is a
        # modelling error, so fail loud (ADR-003).
        if (resample == "derive") != (aggregate == "derive"):
            inconsistent_derive.append(
                f"{col}: ResamplingMethode={resample!r}, "
                f"AggregatieMethode={aggregate!r}"
            )
        config[str(col)] = {
            "resample_method": resample,
            "aggregate_method": aggregate,
        }

    if missing:
        raise ValueError(
            f"etdmodel.csv is incomplete -- AggregatieMeenemen=True variables"
            f" missing required method columns: {missing}"
        )

    if inconsistent_derive:
        raise ValueError(
            "etdmodel.csv has inconsistent 'derive' methods -- a variable must "
            "use 'derive' for BOTH ResamplingMethode and AggregatieMethode or "
            f"for neither: {inconsistent_derive}"
        )

    return config


# ---------------------------------------------------------------------------
# Derived structures -- built once at import time from load_etdmodel().
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

    # Strict-choice validation for the ja/nee classifier columns. These are
    # fixed-choice fields; any other content is a sync bug, not
    # something to normalise away. Fail loud at import time so the gap
    # surfaces immediately.
    for _classifier_col in ("Cumulatief", "Momentaan", "Diff", "Monotoon"):
        _validate_yes_no_column(df, _classifier_col)

    # Cumulative meter-reading columns.
    # Exclude date-type columns (ReadingDate may be flagged Cumulatief in some
    # CSV versions -- filtering by type is more robust than filtering by name).
    # The order of this list does not affect correctness.
    cumulative = df[
        (df["Cumulatief"] == "ja") & (df["Type variabele"] != "date")
    ]["Variabele"].tolist()

    # Cumulative variables that only ever increase (Monotoon="ja"). The
    # averaging and the negative-diff corrections apply to these and to nothing
    # else. A meter that can legitimately dip is Cumulatief=ja, Monotoon=nee.
    monotonic = df[
        (df["Monotoon"] == "ja") & (df["Type variabele"] != "date")
    ]["Variabele"].tolist()

    # Instantaneous-reading columns (Momentaan="ja"). Profile / timeline
    # plots gate on membership of this list (see etdanalyze ADR-003).
    # Date-type rows excluded for symmetry with `cumulative_columns`.
    momentaan = df[
        (df["Momentaan"] == "ja") & (df["Type variabele"] != "date")
    ]["Variabele"].tolist()

    # Dataset-cadence delta columns (Diff="ja"). These are interval
    # deltas of cumulative meter readings; their raw values depend on
    # row cadence, so profile plots require conversion to an
    # instantaneous sibling (e.g. kWh-per-row -> kW) before rendering.
    diff_cols = df[
        (df["Diff"] == "ja") & (df["Type variabele"] != "date")
    ]["Variabele"].tolist()

    # Performance columns that carry a declared Volgorde, in that order.
    # Scope: Entiteit == "Prestatiedata" ONLY. This is an ordering convenience,
    # NOT a statement about which columns a supplier may deliver -- any data
    # model column is valid raw data. Do NOT use this as a membership test
    # (use all_col_types) or as the derivation target universe (use all_perf_cols).
    # See etdmap/DECISIONS.md ADR-001 for the rationale.
    col_order = perf["Variabele"].tolist()

    # pandas dtype per performance column, derived from "Type variabele".
    col_types = {
        row["Variabele"]: _DTYPE_MAP.get(str(row["Type variabele"]), "Float64")
        for _, row in perf.iterrows()
        if pd.notna(row["Type variabele"])
    }

    # Columns suppliers are allowed to provide in their metadata files.
    supplier_meta_rows = df[
        (df["Wie vult?"] == "Dataleverancier") & (df["Entiteit"] == "Metadata")
    ]
    supplier_meta = supplier_meta_rows["Variabele"].tolist()

    # pandas dtype per metadata column, derived from "Type variabele" exactly as
    # the performance columns are. Declared rather than inferred: a metadata
    # column read from one supplier's file whose values happen to be whole
    # numbers infers an integer dtype, and writing a fractional value from
    # another supplier into it then fails.
    supplier_meta_types = {
        str(row["Variabele"]): _DTYPE_MAP.get(str(row["Type variabele"]), "Float64")
        for _, row in supplier_meta_rows.iterrows()
        if pd.notna(row["Variabele"]) and pd.notna(row["Type variabele"])
    }

    # Every performance column in the model (Prestatiedata and
    # PrestatiedataBerekend alike). The Berekend suffix names a variable's role
    # in the derivation rules; it does not mean the column cannot be delivered.
    # Used as the derivation target universe in add_calculated_columns_adaptive().
    # See etdmap/DECISIONS.md ADR-001.
    perf_all = df[df["Entiteit"].str.startswith("Prestatiedata", na=False)].copy()
    all_perf_cols = perf_all["Variabele"].tolist()

    # Every performance column in the data model, mapped to its dtype.
    # Insertion order IS the column order: col_order first (declared Volgorde),
    # then the remaining performance columns in model order.
    all_col_types = dict(col_types)
    for _, row in perf_all.iterrows():
        variable = row["Variabele"]
        if pd.isna(variable) or str(variable) in all_col_types:
            continue
        all_col_types[str(variable)] = _DTYPE_MAP.get(
            str(row["Type variabele"]), "Float64"
        )

    # Required subset: columns that must be present at end of pipeline.
    # Missing any of these after derivation is a data quality error (logging.error).
    required_perf_cols = perf_all[perf_all["Vereist"] == "ja"]["Variabele"].tolist()

    return (
        cumulative, momentaan, diff_cols, monotonic,
        col_order, col_types, supplier_meta, supplier_meta_types,
        all_perf_cols, required_perf_cols, all_col_types,
    )


(
    cumulative_columns,
    momentaan_columns,
    diff_columns,
    monotonic_columns,
    model_column_order,
    model_column_type,
    allowed_supplier_metadata_columns,
    supplier_metadata_dtypes,
    all_performance_data_columns,
    required_performance_data_columns,
    performance_column_dtypes,
) = _build_derived_structures()

# data_analysis_columns: columns validated by dataset_validators.
# Currently an alias of model_column_order (all Prestatiedata rows).
# TODO: narrow to Vereist=="ja" once that column is fully populated in etdmodel.csv.
# A regression test in tests/test_data_model.py guards the current alias.
data_analysis_columns = model_column_order

# The thresholds as declared in the model, each against its own unit of time.
# Static: this is the model, so it is loaded once. Anything cadence-aware or
# per-household is derived from it and kept separate -- it does not replace
# these values or write back into this dict.
thresholds_dict = load_thresholds_as_dict()


def load_projects() -> pd.DataFrame:
    """
    Load the project registry.

    Returns an empty frame when the file has not been synced yet. A consumer
    that needs a project's physical model raises then, naming the projects it
    could not resolve -- an unsynced registry must be loud, never a default.
    """
    projects_file = files("etdmap.data").joinpath("projects.csv")
    if not projects_file.is_file():
        return pd.DataFrame(
            columns=["Dataleverancier", "ProjectIdBSV", "FysischModel"]
        )
    return pd.read_csv(
        projects_file,
        dtype={
            "Dataleverancier": "string",
            "ProjectIdBSV": "Int64",
            "FysischModel": "string",
        },
        na_values=["n.a.", "NA", "N/A", ""],
        keep_default_na=True,
    )


# The physical model each project's households follow, keyed by ProjectIdBSV.
# Built once at import like the threshold tables: rule selection reads it per
# row, and a per-row lookup cannot afford a file read.
physical_model_by_project = {
    int(row.ProjectIdBSV): str(row.FysischModel)
    for row in load_projects().itertuples()
    if pd.notna(row.ProjectIdBSV) and pd.notna(row.FysischModel)
}


# ---------------------------------------------------------------------------
# Tariff register pairs (Hoog/Laag) -- derived from etdmodel.csv variable names.
# Hoog = peak tariff register, Laag = off-peak. Suppliers report either the
# combined root (single-tariff meter) or the split pair (multi-tariff meter).
# Consumers use these structures to reason about which side of a pair has data.
# In the future we expect to replace this with explicit metadata about pairs.
# ---------------------------------------------------------------------------

_TARIFF_RE = re.compile(
    r"^(?P<root>.+?)(?P<tariff>" +
    "|".join(peak_offpeak_substrings) +
    r")(?P<suffix>Diff)?$"
)


def _build_tariff_register_pairs() -> tuple[dict[str, str], dict[str, tuple[str, str]]]:
    """
    Derive peak/off-peak tariff register pair structures from etdmodel.csv.

    Returns
    -------
    (partners, root_to_splits)
        partners : dict[str, str]
            Symmetric flat partner map. Each Hoog or Laag variable name maps
            to its complementary partner (e.g. ElektriciteitNetgebruikHoog ->
            ElektriciteitNetgebruikLaag and vice versa). Includes Diff variants.
        root_to_splits : dict[str, tuple[str, str]]
            Maps the implied root variable name (Hoog/Laag suffix removed) to
            its (Hoog variant, Laag variant) tuple. The root is the conceptual
            combined-tariff column name; it may or may not exist as a separate
            variable in etdmodel.csv -- consumers typically check whether it
            appears in the data they are processing.
    """
    df = load_etdmodel()
    variables = set(df["Variabele"].dropna().astype(str))

    partners: dict[str, str] = {}
    root_to_splits: dict[str, tuple[str, str]] = {}

    for var in variables:
        m = _TARIFF_RE.match(var)
        if not m or m.group("tariff") != "Hoog":
            continue
        root = m.group("root")
        suffix = m.group("suffix") or ""
        off_peak_var = f"{root}{peak_offpeak_substrings[1]}{suffix}"
        if off_peak_var not in variables:
            continue
        partners[var] = off_peak_var
        partners[off_peak_var] = var
        root_to_splits[root + suffix] = (var, off_peak_var)

    return partners, root_to_splits


tariff_partner_columns, tariff_root_to_splits = _build_tariff_register_pairs()


# ---------------------------------------------------------------------------
# Cumulative / interval column pairs.
#
# The model states two independent facts -- Cumulatief="ja" and Diff="ja" -- and
# ComplementVariabele names the link between them. The link is stated in the
# model and nowhere else: a variable name is a label, not a relationship, and
# deriving the pair from a shared stem would silently mispair any variable whose
# two forms are named differently.
#
# The relation is one to one, and declaring it on either row states it in both
# directions.
#
# This is committed data and is read as given. sync_data_model checks the link
# against Grist before writing it, and the etdmap suite checks the committed
# file; neither check belongs here.
# ---------------------------------------------------------------------------

COMPLEMENT_COLUMN = "ComplementVariabele"


def _build_cumulative_interval_pairs() -> tuple[dict[str, str], dict[str, str]]:
    """
    Read the cumulative <-> interval link from ``ComplementVariabele``.

    Returns
    -------
    (interval_by_cumulative, cumulative_by_interval)
        Each maps one side of a pair to the other. A variable with no declared
        complement appears in neither, and resolves to None.

    Raises
    ------
    ValueError
        If a declared pair is not one Cumulatief=ja variable and one Diff=ja
        variable. The link is then unreadable rather than merely wrong.
    """
    df = load_etdmodel()
    if COMPLEMENT_COLUMN not in df.columns:
        return {}, {}

    cumulative = set(cumulative_columns)
    intervals = set(diff_columns)

    forward, backward = {}, {}
    for _, row in df.iterrows():
        variable, complement = row.get("Variabele"), row.get(COMPLEMENT_COLUMN)
        if pd.isna(variable) or pd.isna(complement):
            continue
        variable, complement = str(variable), str(complement).strip()
        if not complement:
            continue

        if variable in cumulative and complement in intervals:
            forward[variable] = complement
            backward[complement] = variable
        elif variable in intervals and complement in cumulative:
            forward[complement] = variable
            backward[variable] = complement
        else:
            raise ValueError(
                f"etdmodel.csv: {COMPLEMENT_COLUMN} links '{variable}' to "
                f"'{complement}', which is not one Cumulatief=ja variable and "
                f"one Diff=ja variable."
            )

    return forward, backward


interval_by_cumulative, cumulative_by_interval = _build_cumulative_interval_pairs()


def interval_column(column: str) -> str | None:
    """
    The interval column derived from cumulative variable *column*.

    Returns None when *column* is not a cumulative variable.
    """
    return interval_by_cumulative.get(column)


# The interval columns of the monotonic cumulative variables: what the project
# average and the gap fill operate on. Resolved through ComplementVariabele, so
# a pair that cannot be resolved is absent rather than guessed at.
monotonic_interval_columns = [
    _interval
    for _cumulative in monotonic_columns
    if (_interval := interval_column(_cumulative)) is not None
]

def cumulative_column(column: str) -> str | None:
    """
    The cumulative variable that interval column *column* is derived from.

    Returns None when no link is known: either the variable is a calculated
    interval quantity with no cumulative form, or its pair is not expressible in
    the names and ``ComplementVariabele`` does not declare it yet.
    """
    return cumulative_by_interval.get(column)


def complement_column(column: str) -> str | None:
    """
    The other half of *column*'s cumulative/interval pair, whichever side it is.

    One lookup for callers that hold a column without knowing which form it is,
    which is the common case: a cadence recorded under the delivered name, a
    declaration naming either form, a guard checking a pair is complete. The
    direction-specific lookups remain for callers that do know.

    Returns None when no link is declared -- a calculated interval quantity
    with no cumulative form, or a pair ``ComplementVariabele`` does not state.
    """
    return interval_column(column) or cumulative_column(column)


def parse_device_declaration(cell) -> set:
    """Parse a household's device or variable declaration cell into a set.

    Metadata tables holds comma-separated performance
    variable names (cumulative device forms, e.g.
    ``ElektriciteitsgebruikRadiator``). Shared by the mappers and the
    index/metadata path so the two never parse differently.

    NA, empty and whitespace-only cells are an empty declaration. Every
    parsed name must be a performance variable the data model knows --
    an unknown name is inconsistent metadata, which is always a hard
    failure (same contract as the sync's completeness check), so it
    raises rather than passing a typo through as a silently-ignored
    declaration.
    """
    if cell is None or pd.isna(cell) or str(cell).strip() == "":
        return set()
    names = {part.strip() for part in str(cell).split(",") if part.strip()}
    unknown = names - set(performance_column_dtypes)
    if unknown:
        raise ValueError(
            f"Device declaration names unknown to the data model: "
            f"{sorted(unknown)}. Correct the household metadata "
            f"(ApparaatOntbrekend / GegevensOntbrekend)."
        )
    return names


def cadence_lookup_names(column: str) -> list[str]:
    """
    The names *column*'s cadence may be recorded under.

    Cadence is measured on the columns as delivered, so a column derived during
    the pipeline is recorded under the name it was derived from. Returning both
    forms lets one lookup find either, whichever side of the pair the caller
    holds.
    """
    partner = complement_column(column)
    return [column] if partner is None else [column, partner]


def cadence_minutes_by_column(cadence) -> dict:
    """
    Map each column to the interval its readings cover, in minutes.

    Reads the table :func:`etdmap.mapping_helpers.detect_column_cadence`
    produces for one household. A column is found under its own name or under
    the name of the variable it is paired with, since cadence is measured on the
    columns as delivered and a derived column is not there under its own name.

    Returns an empty dict when no cadence is available. A caller that scales a
    bound treats a column missing from the result as unresolved rather than as
    reporting at the interval its threshold was written against.
    """
    if cadence is None or getattr(cadence, "empty", True):
        return {}

    measured = {
        row.column: row.cadence_minutes
        for row in cadence.itertuples()
        if pd.notna(row.cadence_minutes)
    }
    resolved = {}
    for column in list(measured):
        for name in cadence_lookup_names(column):
            resolved.setdefault(name, measured[column])
    return resolved


def dataset_cadence_minutes(cadence, date_column: str = "ReadingDate"):
    """
    The interval the dataset itself is recorded on, from its cadence table.

    :func:`etdmap.mapping_helpers.detect_column_cadence` measures the timestamp
    column alongside the data columns and resolves it the same way, so this row
    carries the grid the household batch declares.

    This is the interval a column created during the pipeline reports at. A
    synthesised tariff register, or an interval column derived from one, is
    built row by row on the grid, so the grid is its cadence -- it was not
    delivered, so there is nothing to have measured.

    Returns pd.NA when there is no cadence table or no row for *date_column*.
    """
    if cadence is None or getattr(cadence, "empty", True):
        return pd.NA

    rows = cadence[cadence["column"] == date_column]
    if rows.empty:
        return pd.NA
    return rows["cadence_minutes"].iloc[0]


def scale_bounds(declared: dict, cadence_minutes=None) -> tuple:
    """
    Both bounds from a ``thresholds_dict`` entry, scaled to a reporting interval.

    A ``5-minute`` bound states what one five-minute interval may hold, so a
    column reporting hourly is judged against twelve times that. Called without
    a cadence it returns the declared bounds, which is the unscaled pair; a
    caller judging real readings resolves the cadence first, since the declared
    pair belongs to the interval it was written against and to no other.

    Takes the entry rather than the column name so the caller decides which
    threshold table is in force. That matters: a corrected threshold has to take
    effect through the dict the caller holds.

    Returns
    -------
    tuple
        ``(lower, upper)``. ``pd.NA`` on either side means that side is not
        declared, which is unbounded rather than zero.
    """
    from etdmap.flags import scale_bound_to_cadence, threshold_base_minutes

    kind = declared.get("ThresholdType")
    lower = scale_bound_to_cadence(declared.get("Min", pd.NA), kind, cadence_minutes)

    # The lower bound scales in proportion -- a floor of zero stays zero, and a
    # signed floor is a rate. The upper bound takes a declared shape, because a
    # peak plausible for one interval is not plausible sustained across twelve.
    maximum = declared.get("Max", pd.NA)
    base_minutes = threshold_base_minutes(kind)
    if pd.isna(base_minutes) or pd.isna(maximum) or pd.isna(cadence_minutes):
        return lower, maximum

    return lower, scale_upper_bound(
        maximum,
        float(cadence_minutes) / float(base_minutes),
        declared.get("UpperThresholdSustained", pd.NA),
        declared.get("UpperThresholdExponent", pd.NA),
    )


def resolve_bounds(declared: dict, cadence_minutes, column: str = "") -> tuple:
    """
    :func:`scale_bounds`, but an unresolved cadence is an error.

    Returning the declared pair when the cadence is unknown multiplies the bound
    by one, which is the same arithmetic as deciding the column reports at the
    interval the bound was written against. That decision belongs to whoever
    resolved the cadence, not to this function, and it is wrong for every
    household recorded on anything else.

    Use this when judging real readings. :func:`scale_bounds` stays lenient for
    callers inspecting what a threshold declares rather than applying it.

    A caller holding a column with no values at all has nothing to judge and no
    need for a bound, so it skips this rather than resolving a cadence for an
    empty column. Columns derived after the cadence was measured -- the model
    columns added during mapping, and their interval columns -- are empty
    exactly when their source delivered nothing.

    Raises
    ------
    ValueError
        When the bound is written per interval and *cadence_minutes* is unknown.
    """
    from etdmap.flags import threshold_base_minutes

    kind = declared.get("ThresholdType")
    unresolved = cadence_minutes is None or pd.isna(cadence_minutes)
    if unresolved and pd.notna(threshold_base_minutes(kind)):
        named = f"'{column}' " if column else ""
        raise ValueError(
            f"{named}has a '{kind}' bound, which is written per interval, but "
            f"no cadence was resolved for it. Cadence is settled by "
            f"detect_column_cadence and read from its table."
        )

    return scale_bounds(declared, cadence_minutes)


# TEST-ONLY: hand-curated subset of aggregation variables that test fixtures
# carry meaningful synthetic values for. Tests filter both the stored fixture
# and the freshly-generated pipeline output to this subset before comparing,
# so data-model expansion outside the subset does not break tests.
#
# Production code paths must NOT import this. Production aggregation pulls
# from get_aggregation_config() (sourced from etdmodel.csv via
# AggregatieMeenemen=True), and consumer entry points filter by what is
# actually present in the dataset via active_aggregation_variables(df) in
# etdtransform/aggregate.py. Using this list in production would lock the
# scope to whatever was last manually curated here, not what the data model
# currently says.
#
# Updating this list is a deliberate code change: extend it when you have
# added plausible synthetic ranges in the test fixture for the new variable
# and want the comparison surface to grow. The data model sync impact report
# highlights candidates from the data model that could be added here -- those
# candidates are informational only.
test_aggregation_columns: list[str] = [
    # Electricity grid exchange -- Diff variables (raw per-interval consumption)
    "ElektriciteitNetgebruikHoogDiff",
    "ElektriciteitNetgebruikLaagDiff",
    "ElektriciteitTerugleveringHoogDiff",
    "ElektriciteitTerugleveringLaagDiff",
    # Heat pump sub-system electricity -- Diff variables
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


# ---------------------------------------------------------------------------
# Indexed lookup dicts -- variables grouped by data-model column.
#
# Built once at import from etdmodel.csv. Use direct dict access at the call
# site; compose set operations there for combined filters rather than adding
# per-filter helpers here. Examples:
#
#     from etdmap.data_model import variables_by_categorie, variables_by_eenheid
#     temp_outdoor = sorted(
#         set(variables_by_eenheid.get("graden C", []))
#         & {v for v in variables_by_categorie.get("Temperatuur", []) if "Buiten" in v}
#     )
#     gj_heat = variables_by_eenheid.get("GJ", [])
#
# Values are sorted unique lists; keys are the raw Categorie / Eenheid
# strings as they appear in etdmodel.csv. Missing keys raise KeyError on
# bracket access -- use .get(key, []) for permissive callers.
# ---------------------------------------------------------------------------

def _build_indexed_lookups() -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """
    Return ({Categorie: [Variabele,...]}, {Eenheid: [Variabele,...]}) from
    etdmodel.csv. Rows with NA in the index column are skipped.
    """
    df = load_etdmodel()
    by_cat: dict[str, set[str]] = {}
    by_unit: dict[str, set[str]] = {}
    for _, row in df.iterrows():
        var = row.get("Variabele")
        if pd.isna(var):
            continue
        var = str(var)
        cat = row.get("Categorie")
        if pd.notna(cat):
            by_cat.setdefault(str(cat), set()).add(var)
        unit = row.get("Eenheid")
        if pd.notna(unit):
            by_unit.setdefault(str(unit), set()).add(var)
    return (
        {k: sorted(v) for k, v in by_cat.items()},
        {k: sorted(v) for k, v in by_unit.items()},
    )


variables_by_categorie, variables_by_eenheid = _build_indexed_lookups()
