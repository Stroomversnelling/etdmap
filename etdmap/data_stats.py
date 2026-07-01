"""
Per-entity (HuisIdBSV-keyed mapped mode, source-file-keyed raw mode)
data-stats producer for etdmap. Mirrors ``etdtransform.data_stats`` so
both repos expose the same public name (``get_data_stats``) at the
same module path; ``etdmap`` handles the raw and mapped stages
(per-household or per-file fan-out), ``etdtransform`` handles the
wide-parquet stages (default / imputed / calculated).

Schema contract: ``_STATS_DTYPES`` and ``NUMERIC_STATS_SCHEMA`` (see
parent ADR-018). The schema is defined here and consumed by every
stats producer in the project. Schema-parity tests in
``etdmap/tests/test_mapping_helpers.py`` and
``etdtransform/tests/test_data_stats.py`` enforce that the
per-Series kernel, the per-group pandas/ibis kernels, and the
wide-parquet producer all agree on shape and values.

Historical note: these definitions previously lived in
``etdmap/mapping_helpers.py``. They were relocated here for
module-path symmetry with ``etdtransform.data_stats``. ``mapping_helpers``
re-exports the same names for backward compatibility -- existing
``from etdmap.mapping_helpers import get_data_stats`` imports keep
working. New code should prefer ``from etdmap.data_stats import ...``.
"""

import json
import logging
import os
from concurrent.futures import ProcessPoolExecutor
from functools import partial

import pandas as pd

from etdmap.data_model import tariff_root_to_splits
from etdmap.index_helpers import get_mapped_data, read_index
from etdmap.timestamp_helpers import derive_and_normalize_reading_date


# ---------------------------------------------------------------------------
# Schema constants
# ---------------------------------------------------------------------------

# Schema contract for the DataFrame returned by get_data_stats(). Applied
# via .astype() so the output is fully typed (no object-dtype min/max
# columns, no dict-as-string top5). min_datetime / max_datetime are cast
# separately via pd.to_datetime to preserve datetime64[ns] dtype.
_STATS_DTYPES = {
    "column": "string",
    "type": "string",
    "count": "Int64",
    "missing": "Int64",
    "errors": "Int64",
    "min": "Float64",
    "max": "Float64",
    "mean": "Float64",
    "std": "Float64",
    "median": "Float64",
    "iqr": "Float64",
    "p01": "Float64",
    "p25": "Float64",
    "p75": "Float64",
    "p99": "Float64",
    "top5": "string",
    "season": "string",
}

# Default seasonal partition for get_data_stats(seasonal=True). The keys are
# season names that appear in the output 'season' column; the values are
# sets of month numbers (1-12) that belong to each season. The "annual"
# slice (full year) is always emitted; the dict configures the additional
# slices.
#
# This default reflects the Netherlands' verwarmingsperiode (heating-period)
# convention -- October through April for cold, May through September for
# warm. Other regions / studies will want different splits (e.g. four
# meteorological seasons; or coupled to local climate or HVAC switch-over
# rules). Callers pass `seasons=` to override; see get_data_stats.
DEFAULT_SEASON_MONTHS = {
    "cold": {10, 11, 12, 1, 2, 3, 4},
    "warm": {5, 6, 7, 8, 9},
}

# Backward-compat aliases (kept for any direct importers).
_COLD_MONTHS = DEFAULT_SEASON_MONTHS["cold"]
_WARM_MONTHS = DEFAULT_SEASON_MONTHS["warm"]


NUMERIC_STATS_SCHEMA = (
    "count", "missing", "min", "max", "mean", "std", "median",
    "p01", "p25", "p75", "p99", "iqr",
)
"""Canonical numeric-stats column names. See parent ADR-018.

Single source of truth for the per-column numeric-stats schema across
``compute_numeric_column_stats`` (per-Series kernel) and
``etdanalyze.analysis_helpers.compute_stats_table`` (per-group
vectorised path). Any consumer adding or removing a stat column must
update this tuple and both paths in the same change; the parity test
in ``etdmap/tests/test_mapping_helpers.py`` enforces it.

``count`` is the number of non-NA observations; ``missing`` is the
number of NA observations. Their sum is the group/column total -- the
denominator a reader needs to judge whether ``count`` is large relative
to the population. A statistic reported without ``missing`` hides that
denominator (e.g. n=3 could be 3-of-3 or 3-of-3000), so ``missing``
travels with the stats rather than only in a separate coverage table.
"""


# ---------------------------------------------------------------------------
# Tariff-pair root synthesis (stats-collection helper)
# ---------------------------------------------------------------------------

def _synthesise_tariff_roots(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add synthesised root columns for Hoog/Laag tariff register pairs
    where the root is missing or all-NA.

    Some suppliers / households report energy as a single combined
    register (e.g. ``ElektriciteitNetgebruik``) while others split it
    into a high-tariff and a low-tariff register
    (``ElektriciteitNetgebruikHoog``, ``ElektriciteitNetgebruikLaag``).
    To allow the downstream stats / report to compare projects
    apples-to-apples on the *root* column, this helper synthesises
    ``df[root] = df[hoog].fillna(0) + df[laag].fillna(0)`` whenever
    the root column has no usable data and at least one of the
    Hoog/Laag splits has data.

    Pair mapping is sourced from
    ``etdmap.data_model.tariff_root_to_splits``. The same logic
    applies to Diff variants (``...HoogDiff`` /  ``...LaagDiff``
    -> ``...Diff``).

    Operates on a copy of the input frame; never overwrites a root
    that already carries data.
    """
    out = df
    copied = False
    for root, (hoog, laag) in tariff_root_to_splits.items():
        root_has_data = (
            root in out.columns
            and not pd.to_numeric(out[root], errors="coerce").isna().all()
        )
        hoog_present = hoog in out.columns
        laag_present = laag in out.columns
        if not (hoog_present or laag_present):
            continue
        if root_has_data:
            continue
        if not copied:
            out = out.copy()
            copied = True
        hoog_num = (
            pd.to_numeric(out[hoog], errors="coerce")
            if hoog_present
            else pd.Series(pd.NA, index=out.index, dtype="Float64")
        )
        laag_num = (
            pd.to_numeric(out[laag], errors="coerce")
            if laag_present
            else pd.Series(pd.NA, index=out.index, dtype="Float64")
        )
        # Sum with NA->0 substitution, but preserve NA in rows where BOTH
        # tariff registers are missing -- otherwise the synthesised root
        # would inflate its row count by counting "no data" timestamps as
        # zero readings.
        combined = hoog_num.fillna(0) + laag_num.fillna(0)
        both_na = hoog_num.isna() & laag_num.isna()
        combined = combined.mask(both_na, other=pd.NA)
        out[root] = combined.astype("Float64")
    return out


# ---------------------------------------------------------------------------
# Seasonal slicing
# ---------------------------------------------------------------------------

def _seasonal_slices(df: pd.DataFrame, seasonal: bool, seasons=None):
    """
    Yield (season_name, sliced_df) tuples for the seasonal-split modes.

    Returns just the annual slice when seasonal=False, or when the df has no
    valid datetime ReadingDate column to split on. When seasonal=True and a
    valid ReadingDate exists, yields the 'annual' slice plus one slice per
    entry in `seasons`.

    Parameters
    ----------
    df : pd.DataFrame
    seasonal : bool
    seasons : dict[str, set[int]] | None, optional
        Mapping of season name -> set of month numbers (1..12). When None,
        falls back to DEFAULT_SEASON_MONTHS (Netherlands verwarmingsperiode
        cold/warm split). Pass an arbitrary dict for other partitions
        (e.g. four meteorological seasons). Each season name appears as a
        value in the output 'season' column; the name 'annual' is reserved
        and added automatically.
    """
    if not (
        seasonal
        and "ReadingDate" in df.columns
        and pd.api.types.is_datetime64_any_dtype(df["ReadingDate"])
    ):
        return [("annual", df)]

    season_map = DEFAULT_SEASON_MONTHS if seasons is None else seasons
    months = df["ReadingDate"].dt.month
    slices = [("annual", df)]
    for name, month_set in season_map.items():
        if name == "annual":
            # Reserved; skip user redefinitions of the annual slice.
            continue
        slices.append((name, df[months.isin(set(month_set))]))
    return slices


# ---------------------------------------------------------------------------
# Per-Series kernels
# ---------------------------------------------------------------------------

def compute_numeric_column_stats(series: pd.Series) -> dict:
    """
    Per-column numeric statistics with the project's standard names.

    Returns a dict with keys equal to ``NUMERIC_STATS_SCHEMA``:
      ``count``, ``missing``, ``min``, ``max``, ``mean``, ``std``,
      ``median``, ``p01``, ``p25``, ``p75``, ``p99``, ``iqr``.

    ``count`` is the non-NA observation count; ``missing`` is the NA
    count. ``count + missing`` is the series length -- the denominator a
    reader needs to judge ``count`` against the population.

    Centralises the stat names + math so any per-Series consumer
    (mapping reports, analysis scripts, ad-hoc tooling) emits the same
    columns. Group-aware callers (e.g. ``etdanalyze.compute_stats_table``)
    do their own ``groupby().agg()`` for speed but match the same names
    (see ADR-018).

    Non-numeric or all-NA inputs return a dict with ``count`` and
    ``missing`` set and every other key as ``pd.NA``.

    Notes
    -----
    Boolean columns are coerced to Float64 (False=0, True=1) before
    reduction so min/max/mean populate. Datetime / object columns are
    out of scope; use ``collect_column_stats`` for the typed-routing
    behaviour (datetime min/max, object top5).
    """
    out = {
        "count": series.count(),
        "missing": series.isna().sum(),
        "min": pd.NA, "max": pd.NA, "mean": pd.NA,
        "std": pd.NA, "median": pd.NA,
        "p01": pd.NA, "p25": pd.NA, "p75": pd.NA, "p99": pd.NA, "iqr": pd.NA,
    }
    if series.isna().all():
        return out
    if pd.api.types.is_bool_dtype(series):
        numeric = series.astype("Float64")
        out["min"] = numeric.min()
        out["max"] = numeric.max()
        out["mean"] = numeric.mean()
        return out
    if not pd.api.types.is_numeric_dtype(series):
        return out
    out["min"] = series.min()
    out["max"] = series.max()
    out["mean"] = series.mean()
    out["std"] = series.std()
    out["median"] = series.median()
    p25 = series.quantile(0.25)
    p75 = series.quantile(0.75)
    out["p01"] = series.quantile(0.01)
    out["p25"] = p25
    out["p75"] = p75
    out["p99"] = series.quantile(0.99)
    out["iqr"] = p75 - p25
    return out


def collect_column_stats(identifier, column_data):
    """
    Collect typed summary statistics for a single column.

    The returned dict's keys map 1:1 to the columns of the DataFrame
    produced by get_data_stats(); the schema contract is enforced
    downstream via _STATS_DTYPES.

    Type routing:
      - bool  -> coerced to Float64 (False=0, True=1) so min/max/mean
                 populate. This makes "ever fired" filterable as max == 1.
      - numeric -> min/max/mean/std/median/iqr/p25/p75.
      - datetime64 (any tz) -> min_datetime / max_datetime, vectorised
                 to UTC-naive. Numeric min/max remain pd.NA. Object
                 columns are NOT promoted here -- per-row varying
                 tzinfo would require per-cell handling and is dealt
                 with separately by expand_tz_columns().
      - object -> top5 only (JSON-serialised string for CSV safety).

    All math uses pandas vectorised reductions; no per-cell Python
    loops on the hot path.
    """
    dtype = column_data.dtype

    stats = {
        "Identifier": identifier,
        "column": column_data.name,
        "type": str(dtype),
        "count": column_data.count(),
        "missing": column_data.isna().sum(),
        "errors": column_data.isna().sum(),
        "min": pd.NA,
        "max": pd.NA,
        "mean": pd.NA,
        "std": pd.NA,
        "median": pd.NA,
        "iqr": pd.NA,
        "p01": pd.NA,
        "p25": pd.NA,
        "p75": pd.NA,
        "p99": pd.NA,
        "min_datetime": pd.NaT,
        "max_datetime": pd.NaT,
        "top5": pd.NA,
    }

    if column_data.isna().all():
        return stats

    if pd.api.types.is_bool_dtype(column_data) or pd.api.types.is_numeric_dtype(column_data):
        # Delegate the numeric / bool math to the canonical kernel (ADR-018).
        # The kernel handles the bool->Float64 coercion internally and emits
        # exactly NUMERIC_STATS_SCHEMA. Datetime / object branches stay below.
        stats.update(compute_numeric_column_stats(column_data))
    elif pd.api.types.is_datetime64_any_dtype(column_data):
        # datetime64 dtype is uniform-tz by construction; safe to vectorise.
        if getattr(column_data.dt, "tz", None) is not None:
            normalised = column_data.dt.tz_convert("UTC").dt.tz_localize(None)
        else:
            normalised = column_data
        stats["min_datetime"] = normalised.min()
        stats["max_datetime"] = normalised.max()
    elif pd.api.types.is_object_dtype(column_data):
        top5 = column_data.value_counts().head(5).to_dict()
        stats["top5"] = json.dumps(top5, default=str)

    return stats


# ---------------------------------------------------------------------------
# Schema enforcement
# ---------------------------------------------------------------------------

def _cast_stats_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply the _STATS_DTYPES contract to a stats DataFrame; cast
    min_datetime / max_datetime to UTC-naive datetime64[ns] separately.

    Defensive: only casts columns that actually appear in df. If a future
    field is added to collect_column_stats but missed in _STATS_DTYPES, it
    survives as object until the contract is updated.
    """
    if df.empty:
        return df
    cast_map = {col: dtype for col, dtype in _STATS_DTYPES.items() if col in df.columns}
    df = df.astype(cast_map)
    for dt_col in ("min_datetime", "max_datetime"):
        if dt_col in df.columns:
            converted = pd.to_datetime(df[dt_col], errors="raise")
            tz = getattr(converted.dt, "tz", None) if hasattr(converted, "dt") else None
            if tz is not None:
                converted = converted.dt.tz_localize(None)
            df[dt_col] = converted
    return df


# ---------------------------------------------------------------------------
# Per-entity collectors
# ---------------------------------------------------------------------------

def collect_mapped_data_stats(huis_id_bsv, seasonal=False, seasons=None):
    """
    Collect statistics for each column in the DataFrame corresponding to a
    specific HuisIdBSV.

    Parameters
    ----------
    huis_id_bsv : str or int
        The identifier for the household to process.
    seasonal : bool, optional
        If True, emit one row per (HuisIdBSV, numeric column, season) where
        season includes 'annual' plus each entry in `seasons`. Non-numeric /
        non-bool columns only get the 'annual' row regardless. Default False
        (annual only).
    seasons : dict[str, set[int]] | None, optional
        Mapping season name -> set of month numbers (1..12). When None and
        seasonal=True, falls back to DEFAULT_SEASON_MONTHS (Netherlands
        verwarmingsperiode cold/warm split). Pass a custom dict for any
        other partition.

    Returns
    -------
    list of dict
        Stats dicts produced by collect_column_stats, each with an extra
        'season' key.
    """
    logging.info(f"Processing stats from columns where HuisIdBSV = {huis_id_bsv}")
    file_summary_data = []
    try:
        df = get_mapped_data(huis_id_bsv)
        # Fill in any Hoog/Laag tariff-pair root columns that are missing
        # or empty so downstream stats compare projects on the root name
        # apples-to-apples regardless of how a supplier reports the data.
        df = _synthesise_tariff_roots(df)
        for season, sliced in _seasonal_slices(df, seasonal, seasons):
            for column in sliced.columns:
                col_data = sliced[column]
                if season != "annual" and not (
                    pd.api.types.is_numeric_dtype(col_data)
                    or pd.api.types.is_bool_dtype(col_data)
                ):
                    continue
                stats = collect_column_stats(huis_id_bsv, col_data)
                stats["season"] = season
                file_summary_data.append(stats)
    except Exception as e:
        logging.error(
            f"Failed to process stats from columns where HuisIdBSV = {huis_id_bsv}: {str(e)}",
            exc_info=True,
        )

    return file_summary_data


# Raw supplier files name the reading timestamp differently per project
# (e.g. a supplier may use 'Datum' or 'readingDate'); mirror the supplier mappers'
# candidate-list pattern so the raw-stats path derives ReadingDate the SAME way
# the mapping pipeline does (via derive_and_normalize_reading_date).
_RAW_TIMESTAMP_CANDIDATES = ("ReadingDate", "Datum", "readingDate")


def process_raw_data_file(args, seasonal=False, seasons=None, timestamp_candidates=None):
    file, raw_data_folder_path = args

    file_path = os.path.join(raw_data_folder_path, file)
    logging.info(f"Opening {file_path}")

    df = pd.read_parquet(file_path)
    df = _synthesise_tariff_roots(df)
    # Derive a proper datetime ReadingDate using the canonical mapper helper, so
    # the per-project timestamp column name + format (incl. tz-aware object
    # strings like a supplier's 'Datum') are handled exactly as in the mapping
    # pipeline and min_datetime/max_datetime populate. If no timestamp column is
    # recognised, fall back gracefully -- stats still emit, without a ReadingDate.
    candidates = list(timestamp_candidates or _RAW_TIMESTAMP_CANDIDATES)
    try:
        df = derive_and_normalize_reading_date(df, candidates, context=file)
    except ValueError as exc:
        logging.warning(
            f"[process_raw_data_file] {file}: no usable timestamp column "
            f"({exc}); continuing without ReadingDate."
        )
    summary_data = []

    for season, sliced in _seasonal_slices(df, seasonal, seasons):
        for column in sliced.columns:
            col_data = sliced[column]
            if season != "annual" and not (
                pd.api.types.is_numeric_dtype(col_data)
                or pd.api.types.is_bool_dtype(col_data)
            ):
                continue
            stats = collect_column_stats(file, col_data)
            stats["season"] = season
            summary_data.append(stats)
    return summary_data


# ---------------------------------------------------------------------------
# Top-level entry
# ---------------------------------------------------------------------------

def get_data_stats(raw_data_folder_path=None, multi=False, max_workers=2,
                   seasonal=False, seasons=None, timestamp_candidates=None):
    """
    Collect typed per-column summary statistics, either for mapped
    household data or for a folder of raw parquet files.

    Returned DataFrame schema is enforced via _STATS_DTYPES so all
    numeric stat columns are nullable Float64, counts are Int64,
    min_datetime / max_datetime are datetime64[ns] (UTC-naive), top5
    is a JSON-serialised string, and type is the column dtype as a
    string. CSV / Excel exports round-trip cleanly without any
    post-processing.

    Modes:
      raw_data_folder_path is None (default): mapped mode. Iterates
        the HuisIdBSV values from read_index(), collecting stats per
        household via collect_mapped_data_stats. The mode-driver
        identifier is HuisIdBSV; the index DataFrame is merged in
        (Dataleverancier, ProjectIdBSV, etc).
      raw_data_folder_path is a path-like: raw mode. Iterates the
        *.parquet files in the folder, collecting stats per file via
        process_raw_data_file. The mode-driver identifier is the
        source filename. No index merge.

    Output schema (shape-stable across modes):
      Identifier  -- always present; the mode-driver value as string
                     (HuisIdBSV in mapped mode, filename in raw mode).
                     Use this column for mode-agnostic consumers.
      HuisIdBSV   -- always present; populated in mapped mode, all NA
                     (Int64 dtype) in raw mode.
      source_file -- always present; populated in raw mode, all NA
                     (string dtype) in mapped mode.

    A consumer that is mode-agnostic should use `Identifier`. A consumer
    that requires a specific identifier type should use the typed
    column (`HuisIdBSV` or `source_file`) and treat all-NA as "not
    available in this mode" -- not silently fall back to row counts or
    other proxies.

    Parameters
    ----------
    raw_data_folder_path : str | os.PathLike | None, optional
        Folder of raw parquets to inspect. None for mapped mode.
    multi : bool, optional
        If True, run workers via ProcessPoolExecutor. Default False.
    max_workers : int, optional
        Worker count when multi=True. Default 2.
    seasonal : bool, optional
        If True, every numeric/bool column produces one row per season
        (the 'annual' slice plus one row per entry in `seasons`).
        Non-numeric columns only get the annual row. Default False
        (annual only). The output DataFrame always carries a 'season'
        column for downstream filtering.
    seasons : dict[str, set[int]] | None, optional
        Mapping season name -> set of month numbers (1..12) used when
        seasonal=True. None falls back to DEFAULT_SEASON_MONTHS (the
        Netherlands verwarmingsperiode cold/warm split). Pass a custom
        dict for region-specific or four-season analyses, e.g.
        ``{"spring": {3, 4, 5}, "summer": {6, 7, 8},
           "fall": {9, 10, 11}, "winter": {12, 1, 2}}``.
        The 'annual' slice (full year) is always emitted regardless;
        the dict configures the additional slices.
    timestamp_candidates : list[str] | tuple[str] | None, optional
        Raw mode only. Ordered candidate names for the reading timestamp
        column (it varies per supplier/project, e.g. 'Datum' vs
        'readingDate'). The first found is derived into a datetime
        'ReadingDate' via the canonical derive_and_normalize_reading_date,
        so min_datetime/max_datetime populate. None falls back to
        _RAW_TIMESTAMP_CANDIDATES. Pass the supplier mapper's own
        candidate list to mirror its handling exactly.

    Returns
    -------
    pd.DataFrame
        One row per (identifier, column, season) tuple, fully typed.
    """
    summary_data = []

    if raw_data_folder_path is None:
        index_df, _ = read_index()
        worker = partial(collect_mapped_data_stats, seasonal=seasonal, seasons=seasons)
        if multi:
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                results = executor.map(worker, index_df["HuisIdBSV"])
                summary_data = [item for sublist in results for item in sublist]
        else:
            for huis_id in index_df["HuisIdBSV"]:
                logging.info(f"Collecting stats for HuisIdBSV = {huis_id}")
                summary_data.extend(worker(huis_id))
        df = pd.DataFrame(summary_data)
        df = _cast_stats_dtypes(df)
        # Shape-stable schema (see docstring): copy the mode-driver
        # identifier into both a generic `Identifier` column and the
        # typed `HuisIdBSV`. The raw-mode column `source_file` is
        # always present too, all-NA in this mode, with the same
        # string dtype it carries in raw mode.
        df["HuisIdBSV"] = df["Identifier"]
        df["source_file"] = pd.Series([pd.NA] * len(df), dtype="string")
        df = pd.merge(df, index_df, how="left", on="HuisIdBSV")
        return df

    file_extension = "parquet"
    files = os.listdir(raw_data_folder_path)
    worker = partial(process_raw_data_file, seasonal=seasonal, seasons=seasons,
                     timestamp_candidates=timestamp_candidates)
    if multi:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            args_list = [
                (file, raw_data_folder_path) for file in files
                if file.endswith(f".{file_extension}")
            ]
            results = executor.map(worker, args_list)
            summary_data = [item for sublist in results for item in sublist]
    else:
        for file in files:
            if not file.endswith(f".{file_extension}"):
                continue
            summary_data.extend(worker((file, raw_data_folder_path)))
    df = pd.DataFrame(summary_data)
    df = _cast_stats_dtypes(df)
    # Shape-stable schema (see docstring): copy the mode-driver
    # identifier into both a generic `Identifier` column and the typed
    # `source_file`. The mapped-mode column `HuisIdBSV` is always
    # present too, all-NA in this mode, with Int64 dtype to match its
    # populated dtype in mapped mode.
    df["source_file"] = df["Identifier"]
    df["HuisIdBSV"] = pd.Series([pd.NA] * len(df), dtype="Int64")
    return df


# ---------------------------------------------------------------------------
# Mapping annotation
# ---------------------------------------------------------------------------

def annotate_mapped_bsv_variable(stats_df, mapping_dict, raw_col_field="column"):
    """
    Add a ``mapped_bsv_variable`` column to a raw-data-stats DataFrame.

    Lets raw stats be analysed across suppliers without looking up each raw
    column's BSV variable by hand.

    Parameters
    ----------
    stats_df : pandas.DataFrame
        A raw-data-stats table (one row per raw column), e.g. the output of
        get_data_stats(raw_data_folder_path=...).
    mapping_dict : dict
        {raw_column_name: bsv_variable_name} for the supplier -- the {raw: bsv}
        dict returned by load_supplier_pipeline_config, or a supplier's
        hardcoded mapping dict.
    raw_col_field : str, optional
        Column in stats_df holding the raw column name to look up. Default
        "column". For multi-device suppliers whose mapping keys are prefixed
        (e.g. a multi-device supplier's ``sheetName_columnName``), pass a field that
        already holds the prefixed key.

    Returns
    -------
    pandas.DataFrame
        A copy with a nullable-string ``mapped_bsv_variable`` column. Raw
        columns with no mapping (timestamps, Mapping_Meenemen=0, debug columns)
        get pd.NA. Where several raw columns combine into one BSV variable
        (e.g. ``_binnen`` + ``_buiten`` parts summed into one variable), each raw column shows its own
        staging name from the mapping dict; the combination happens later in
        the mapper and is not represented here.
    """
    out = stats_df.copy()
    out["mapped_bsv_variable"] = (
        out[raw_col_field].map(mapping_dict).astype("string")
    )
    return out
