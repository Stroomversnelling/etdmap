"""Generation of the synthetic mapped dataset the test suites run against.

etdmap owns mapping, so it owns the fixture dataset that represents mapped
output. Downstream suites call :func:`generate_mapped_dataset` rather than
reading a folder a previous run left behind, so they run against what this
package currently emits.

Install with the ``testing`` extra to use this module.
"""

import hashlib
import logging
import os
from contextlib import contextmanager
from pathlib import Path
from typing import NamedTuple

import pandas as pd
from numpy.random import PCG64, Generator

import etdmap
import etdmap.data_model as data_model
import etdmap.index_helpers as index_helpers
import etdmap.mapping_helpers as mapping
from etdmap.index_helpers import bsv_metadata_columns, read_metadata

__all__ = [
    "CADENCE_DTYPES",
    "SyntheticModel",
    "cadence_sidecar",
    "cumulative_pair",
    "household_batch_of",
    "household_frame",
    "households_frame",
    "declare_synthetic_model",
    "HOUSEHOLD_BATCH_OFFSET",
    "generate_mapped_dataset",
    "generate_raw_dataset",
    "installed_fingerprint",
    "interval_draw_range",
    "model_row",
    "model_rows",
    "supplier_label",
    "synthetic_model",
]


class SyntheticModel(NamedTuple):
    """The column names, relations and thresholds a test declares for itself.

    Built by :func:`synthetic_model`, installed by
    :func:`declare_synthetic_model`.

    ``device`` is the cumulative variable and ``device_complement`` its
    interval form, so a test wanting the ordinary pair reads those two.
    """

    complements: dict
    tariff_splits: dict
    thresholds: dict
    monotonic_intervals: list
    device: str
    device_complement: str
    high: str
    low: str
    high_complement: str
    low_complement: str
    plain: str
    plain_complement: str


def synthetic_model(
    stem: str = "A",
    plain: str = "B",
    maximum: float = 1.0,
    minimum: float = 0.0,
    threshold_type: str = "5-minute",
) -> SyntheticModel:
    """Build a two-device model that names nothing the real model names.

    The live model is derived from Grist and its names change, so a test
    reaching for a real variable is pinned to today's metadata rather than to
    the behaviour it means to check. This builds the same *shape* from names
    nothing else uses:

    - *stem* is a tariff device -- a combined variable, its complement, and
      the two tariff splits (each with its own complement) that mapping sums
      into it;
    - *plain* is an ordinary device, with a complement and no tariff splits.
      Its presence is what distinguishes a declared device family from every
      column on the frame.

    Every interval column is declared monotonic, and every column gets the
    same threshold entry -- *minimum*, *maximum*, *threshold_type*, with no
    sustained level or exponent, so a bound scales by a plain multiply.

    Pass the result to :func:`declare_synthetic_model`.
    """
    # Tariff registers are paired by name: _build_tariff_register_pairs
    # matches the Hoog/Laag substrings and the one interval suffix its pattern
    # accepts, so the names below carry both. The cumulative-to-interval link
    # is declared through ComplementVariabele and needs no such convention.
    device, device_complement = stem, f"{stem}Diff"
    high, low = f"{stem}Hoog", f"{stem}Laag"
    high_complement, low_complement = f"{high}Diff", f"{low}Diff"
    plain_complement = f"{plain}Diff"

    pairs = (
        (device, device_complement),
        (high, high_complement),
        (low, low_complement),
        (plain, plain_complement),
    )
    complements: dict = {}
    for cumulative, interval in pairs:
        complements[cumulative] = interval
        complements[interval] = cumulative

    entry = {
        "Min": minimum,
        "Max": maximum,
        "ThresholdType": threshold_type,
        "UpperThresholdSustained": pd.NA,
        "UpperThresholdExponent": pd.NA,
    }

    return SyntheticModel(
        complements=complements,
        tariff_splits={
            device: (high, low),
            device_complement: (high_complement, low_complement),
        },
        thresholds={column: dict(entry) for column in complements},
        monotonic_intervals=[interval for _, interval in pairs],
        device=device,
        device_complement=device_complement,
        high=high,
        low=low,
        high_complement=high_complement,
        low_complement=low_complement,
        plain=plain,
        plain_complement=plain_complement,
    )


#: Distance between a household id and the household batch id a test frame
#: declares for it. The two are never equal in a frame: code reaching for the
#: household where the dataset is meant then finds no rows rather than the
#: right ones.
HOUSEHOLD_BATCH_OFFSET = 1000


def household_batch_of(household_id: int) -> int:
    """The household batch id a test frame gives *household_id*."""
    return int(household_id) + HOUSEHOLD_BATCH_OFFSET


def household_frame(
    n_rows: int = 4,
    start="2024-01-01",
    project: int = 1,
    freq: str = "5min",
    **columns,
):
    """One household batch's rows: the id columns, plus *columns*, plus the
    cumulative counterpart of every interval column given.

    Each value may be a scalar (repeated) or a list of length *n_rows*. The
    pairing is completed from the data model rather than spelled out per test,
    because a frame holding one half of a pair is not a household artifact and
    the stages refuse it.

    ``HuisIdBSV`` and ``HuisBatchIdBSV`` are deliberately absent: the shard
    path carries them, which is where a reader expects them. Use
    :func:`households_frame` for a frame that carries the ids itself.
    """
    from etdmap.data_model import cumulative_column

    df = pd.DataFrame({
        "ProjectIdBSV": pd.array([project] * n_rows, dtype="Int64"),
        "ReadingDate": pd.date_range(start, periods=n_rows, freq=freq, tz="UTC"),
    })
    for name, value in columns.items():
        values = value if isinstance(value, (list, tuple)) else [value] * n_rows
        df[name] = pd.array(list(values), dtype="Float64")
    for name in list(columns):
        counterpart = cumulative_column(name)
        if counterpart and counterpart not in df.columns:
            # A monotonic cumulative column under the interval one. The value is
            # not the subject of any test that needs the pair present; its being
            # there is.
            df[counterpart] = pd.array(
                [float(i) for i in range(n_rows)], dtype="Float64"
            )
    return df


def households_frame(
    per_household: dict,
    *,
    start="2024-01-01",
    freq: str = "5min",
    project: int = 1,
    project_of: dict | None = None,
    tz: str | None = "UTC",
    **columns,
):
    """Several households' rows in one frame, carrying both id columns.

    What a stage receives once the shard reader has injected the ids, so a test
    that works on a frame rather than a shard tree -- a project average, a
    chunk, a per-dataset groupby -- builds it here instead of assembling the id
    columns itself.

    *per_household* maps a household id to that household's values for the
    columns in *columns*: either ``{household: [values]}`` when one column is
    given, or ``{household: {column: [values]}}`` for several. The row count per
    household is the length of its values. Every interval column's cumulative
    counterpart is completed as :func:`household_frame` does.

    ``HuisBatchIdBSV`` is :func:`household_batch_of` the household, so the two
    ids are never equal.

    *project_of* gives a household its own project where they differ; *project*
    is the one they share otherwise. *tz* may be None for a tz-naive frame,
    which some stages' inputs are.
    """
    from etdmap.data_model import cumulative_column

    names = list(columns)
    parts = []
    for household_id, value in per_household.items():
        per_column = (
            dict(value) if isinstance(value, dict)
            else {names[0]: value} if names
            else {}
        )
        for name, declared in columns.items():
            per_column.setdefault(name, declared)
        n_rows = max(
            (len(v) for v in per_column.values() if isinstance(v, (list, tuple))),
            default=1,
        )
        part = pd.DataFrame({
            "HuisIdBSV": pd.array([int(household_id)] * n_rows, dtype="Int64"),
            "HuisBatchIdBSV": pd.array(
                [household_batch_of(household_id)] * n_rows, dtype="Int64"
            ),
            "ProjectIdBSV": pd.array(
                [int((project_of or {}).get(household_id, project))] * n_rows,
                dtype="Int64",
            ),
            "ReadingDate": pd.date_range(
                start, periods=n_rows, freq=freq, tz=tz
            ),
        })
        for name, values in per_column.items():
            if not isinstance(values, (list, tuple)):
                values = [values] * n_rows
            part[name] = pd.array(list(values), dtype="Float64")
        for name in per_column:
            counterpart = cumulative_column(name)
            if counterpart and counterpart not in part.columns:
                part[counterpart] = pd.array(
                    [float(i) for i in range(n_rows)], dtype="Float64"
                )
        parts.append(part)
    if not parts:
        raise ValueError(
            "households_frame: no households given, so there is no frame to "
            "build. Name at least one."
        )
    return pd.concat(parts, ignore_index=True)


def model_row(variable: str, **fields) -> dict:
    """One row of etdmodel.csv, for a variable a test declares.

    The classifiers default to "nee" and the entity to the delivered one, so a
    row states only what it is about and a test reads as the property it
    declares. Any field of etdmodel.csv may be passed; one the model does not
    have is refused by :func:`etdmap.data_model.rebuild_model`.

    :func:`model_rows` is the whole-declaration form, for a
    :class:`SyntheticModel` rather than a single row.
    """
    return {
        "Variabele": variable,
        "Entiteit": "Prestatiedata",
        "Type variabele": "number",
        "Cumulatief": "nee",
        "Monotoon": "nee",
        "Momentaan": "nee",
        "Diff": "nee",
        **fields,
    }


def cumulative_pair(variable: str, **fields) -> tuple[list, list]:
    """A cumulative variable, its interval column, and the thresholds of both.

    Declaring a cumulative variable means declaring all four. The model refuses
    one with no ``ComplementVariabele``, whose interval column -- and so the
    outlier check built for it -- is unknown, and one with no threshold row,
    whose whole-file bounds are.

    *fields* go on the cumulative row. The bounds are arbitrary; a test whose
    subject is a bound states its own through :func:`synthetic_model`.

    Returns
    -------
    tuple
        ``(etdmodel rows, thresholds rows)``, to pass to
        :func:`etdmap.data_model.rebuild_model`.
    """
    interval = f"{variable}Diff"
    return (
        [
            model_row(
                variable, Cumulatief="ja", ComplementVariabele=interval, **fields
            ),
            model_row(interval, Diff="ja", ComplementVariabele=variable),
        ],
        [
            {"Variabele": variable, "Min": 0.0, "Max": 10.0,
             "ThresholdType": "cumulatief"},
            {"Variabele": interval, "Min": 0.0, "Max": 1.0,
             "ThresholdType": "5-minute"},
        ],
    )


def model_rows(model: SyntheticModel) -> tuple[list, list]:
    """The etdmodel and thresholds rows *model* declares.

    The translation from a declaration to the two tables the data model is
    built from, so a declared model goes through the same derivation as the
    shipped one.

    Returns
    -------
    tuple
        ``(etdmodel rows, thresholds rows)``, both lists of mappings.
    """
    intervals = set(model.monotonic_intervals)
    rows = []
    for column, complement in model.complements.items():
        is_interval = column in intervals
        rows.append({
            "Variabele": column,
            "Entiteit": "Prestatiedata",
            "Type variabele": "number",
            "Cumulatief": "nee" if is_interval else "ja",
            # The cumulative side carries Monotoon, which is what puts its
            # interval column in monotonic_interval_columns.
            "Monotoon": "nee" if is_interval else "ja",
            "Momentaan": "nee",
            "Diff": "ja" if is_interval else "nee",
            "ComplementVariabele": complement,
        })

    threshold_rows = [
        {"Variabele": column, **entry}
        for column, entry in model.thresholds.items()
    ]
    return rows, threshold_rows


@contextmanager
def declare_synthetic_model(model: SyntheticModel = None):
    """Install *model* as the model every loaded etd module reads.

    Consumers bind these names with ``from etdmap.data_model import ...``,
    which copies the object into their own namespace. The declaration is
    therefore merged into the model tables and every derived structure rebuilt
    in place: a module reading one of them reads the declaration without being
    patched.

    The declaration supplies rows, and everything answering "which columns
    belong to one variable, and what may it read" is derived from them -- the
    complement and tariff pairings, the monotonic interval columns, the
    thresholds, and the per-column checks.

    It is merged into the shipped model rather than replacing it, so a column
    it does not mention keeps whatever the model says about it. State in
    ``model.thresholds`` the bound of every column the code under test looks
    up, so the test does not rest on a shipped number.

    On exit the model is rebuilt from the shipped tables, which is the restore:
    a declared column reaches more structures than its own row names.

    Yields the model, so a test can read its column names from it.
    """
    model = model if model is not None else synthetic_model()
    rows, threshold_rows = model_rows(model)
    data_model.rebuild_model(
        extra_model_rows=rows, extra_threshold_rows=threshold_rows
    )
    try:
        yield model
    finally:
        data_model.rebuild_model()


#: The sidecar's dtypes, as :func:`etdmap.mapping_helpers.detect_column_cadence`
#: returns them. A hand-built table that gets one of these wrong is accepted by
#: the frame and then read differently by whatever consumes it.
CADENCE_DTYPES = {
    "column": "string",
    "cadence_minutes": "Int64",
    "cadence_source": "string",
    "cadence_detected": "Int64",
    "confidence": "Float64",
    "n_readings": "Int64",
}


def cadence_sidecar(
    columns,
    *,
    minutes=5,
    grid_minutes=None,
    source="declared",
    detected=pd.NA,
    confidence=pd.NA,
    n_readings=pd.NA,
    date_column="ReadingDate",
) -> pd.DataFrame:
    """
    The per-column cadence table for one household, as a test needs it.

    What :func:`etdmap.mapping_helpers.detect_column_cadence` produces, without
    running a detection: a test stating the cadence it means to exercise says so
    rather than building a frame whose timestamps imply it.

    *columns* is either names, all at *minutes*, or a mapping of name to its own
    cadence. The *date_column* row is always present and carries *grid_minutes*,
    defaulting to *minutes*, because the grid is read from that row and a table
    without it resolves no grid at all.

    Parameters
    ----------
    columns : iterable of str, or mapping of str to float
        The columns to record, and their cadence where it differs.
    minutes : float
        Cadence for any column whose own value is not given.
    grid_minutes : float, optional
        The interval the dataset is recorded on. Defaults to *minutes*.
    source : str
        ``'declared'``, ``'detected'`` or ``'dataset'``.
    detected, confidence, n_readings
        What a detection would have measured. Default to pd.NA, which is what a
        declared cadence carries.
    date_column : str
        Name of the timestamp column, whose row carries the grid.
    """
    per_column = (
        dict(columns) if hasattr(columns, "items")
        else {column: minutes for column in columns}
    )
    rows = [{
        "column": date_column,
        "cadence_minutes": minutes if grid_minutes is None else grid_minutes,
        "cadence_source": source,
        "cadence_detected": detected,
        "confidence": confidence,
        "n_readings": n_readings,
    }]
    rows += [
        {
            "column": column,
            "cadence_minutes": column_minutes,
            "cadence_source": source,
            "cadence_detected": detected,
            "confidence": confidence,
            "n_readings": n_readings,
        }
        for column, column_minutes in per_column.items()
        if column != date_column
    ]
    return pd.DataFrame(rows).astype(CADENCE_DTYPES)


def installed_fingerprint(*extra_files) -> str:
    """A key that changes when this installed package's code changes.

    Hashes the contents of every module in the installed package, not its
    version: a version number does not move during development, but a
    reinstall of changed code does. Any *extra_files* given are hashed too, so
    a caller can include the metadata its dataset is generated from.
    """
    digest = hashlib.sha256()
    root = Path(__file__).parent
    for path in sorted(root.rglob("*.py")):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(path.read_bytes())
    for path in extra_files:
        path = Path(path)
        if path.is_file():
            digest.update(path.name.encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()[:16]


def supplier_label(household_number: int) -> str:
    """The HuisIdLeverancier the fixture gives household *household_number*.

    The generator writes it onto the raw data and the mapping reads it back off
    the filename, so both sides spell it through here.
    """
    return f"Huis{household_number:02}"


#: The devices this fixture fills down, and how much sparser than the gap cycle
#: below each one reports. Each supplier mapper declares its own set: which
#: devices report infrequently is a fact about a supplier's export, not about
#: the model.
#:
#: A column carrying a value on every row leaves
#: ``fill_down_infrequent_devices`` nothing to fill, so these are generated
#: sparse.
INFREQUENT_DEVICE_SPARSITY = {
    "ElektriciteitsgebruikBoilervat": 1,
    "ElektriciteitsgebruikRadiator": 2,
    "ElektriciteitsgebruikBooster": 5,
}

INFREQUENT_DEVICE_COLUMNS = tuple(INFREQUENT_DEVICE_SPARSITY)

#: The gaps between an infrequent device's readings, in grid rows, cycled over
#: the span and multiplied by the device's sparsity.
#:
#: Uneven, with no gap repeating within a cycle: these devices report when they
#: run rather than on a schedule, so no cadence can be measured off them.
#: ``detect_column_cadence`` reports the most common gap's share as its
#: confidence, and distinct gaps keep that share under the threshold at which a
#: measurement is trusted, so the column falls back to the grid and
#: ``fill_down_infrequent_devices`` holds each reading until the next arrives.
_INFREQUENT_GAPS = (7, 23, 53, 97)

#: Fewest readings an infrequent device may deliver over the span. One reading
#: fills to a flat line, with no accumulation anywhere.
_MIN_INFREQUENT_READINGS = 2

#: Drawn against where a column declares no upper bound, since a per-interval
#: reading needs some magnitude and the model states none for it.
UNBOUNDED_INTERVAL_DRAW = 1.0


def interval_draw_range(column: str, grid_minutes: float) -> tuple:
    """
    The range a plausible per-interval reading of *column* is drawn from.

    The bound the model declares for the column's interval form, scaled to the
    grid the fixture is generated at. A reading drawn inside its own bound
    cannot violate it, so a test whose subject is a violation declares the
    bound and the value itself through :func:`declare_synthetic_model`.

    Raises
    ------
    KeyError
        When *column* has no interval form, or that form declares no
        threshold. Either means the fixture is generating a column the model
        says nothing about.
    """
    from etdmap.data_model import interval_column, scale_bounds, thresholds_dict

    interval = interval_column(column)
    if interval is None or interval not in thresholds_dict:
        raise KeyError(
            f"interval_draw_range: '{column}' has no declared interval "
            f"threshold to draw from. Either the model needs one, or the "
            f"column is not a cumulative variable of the model."
        )
    lower, upper = scale_bounds(thresholds_dict[interval], grid_minutes)
    lower = 0.0 if pd.isna(lower) else float(lower)
    upper = UNBOUNDED_INTERVAL_DRAW if pd.isna(upper) else float(upper)
    return lower, upper


#: The dataset's shape, where a caller states none. Two projects, because a
#: project aggregate needs more than one; enough households per project to meet
#: the averaging floor; and three days, because the coarsest resample is daily
#: and a single day gives it no interior slot.
DEFAULT_HOUSEHOLDS_PER_PROJECT = 10
DEFAULT_PROJECTS = 2
DEFAULT_DAYS = 3
DEFAULT_GRID = "5min"
DEFAULT_BASE_DATE = pd.Timestamp("2023-01-01")

#: One gap, in one column of one household: with no gap anywhere, imputation
#: writes its artifacts having filled nothing, and the gap accumulator records
#: nothing. Declared by household number and span fraction, so it lands inside
#: whatever shape the caller asked for.
GAP_HOUSEHOLD = 1
GAP_COLUMN = "Gasgebruik"
GAP_START_FRACTION = 0.5
GAP_SLOTS = 12


def _blank_a_span(household_df, column, start_fraction, slots):
    """*household_df* with *slots* readings of *column* set to pd.NA."""
    start = int(len(household_df) * start_fraction)
    household_df.loc[start:start + slots - 1, column] = pd.NA
    return household_df


#: One value per declared supplier metadata column, by dtype. The values are
#: not the subject of any test; a column being declared and typed is, since the
#: index join takes its dtypes from here.
_SUPPLIER_METADATA_VALUES = {
    "string": "synthetic",
    "Float64": 1.0,
    "Int64": 1,
    "boolean": True,
}


def synthetic_supplier_metadata(household_keys) -> pd.DataFrame:
    """
    The supplier metadata frame for *household_keys*, from the model.

    What :func:`etdmap.index_helpers.read_metadata` returns for a workbook,
    without one: a row per household over exactly the columns the model lets a
    supplier declare. Generated rather than read, so the household count is
    the caller's: a workbook covers the households it was written for.

    Typed through :func:`etdmap.index_helpers.set_metadata_dtypes`, which is
    where both declaration styles and the boolean synonyms are applied.

    Parameters
    ----------
    household_keys : iterable of (str, str)
        ``(HuisIdLeverancier, ProjectIdLeverancier)`` per household.
    """
    from etdmap.data_model import (
        allowed_supplier_metadata_columns,
        supplier_metadata_dtypes,
    )
    from etdmap.index_helpers import set_metadata_dtypes

    keys = list(household_keys)
    frame = pd.DataFrame({
        "HuisIdLeverancier": [household for household, _ in keys],
        "ProjectIdLeverancier": [project for _, project in keys],
    })
    for column in allowed_supplier_metadata_columns:
        if column in frame.columns:
            continue
        declared = str(supplier_metadata_dtypes.get(column, "string"))
        if declared not in _SUPPLIER_METADATA_VALUES:
            raise ValueError(
                f"synthetic_supplier_metadata: '{column}' is declared "
                f"'{declared}', which has no value here. Add one, so a model "
                f"column cannot be silently absent from the fixture."
            )
        frame[column] = [_SUPPLIER_METADATA_VALUES[declared]] * len(keys)
    return set_metadata_dtypes(metadata_df=frame[allowed_supplier_metadata_columns])


def _infrequent_reading_rows(column, num_records):
    """The rows an infrequent device delivers a reading on."""
    scale = INFREQUENT_DEVICE_SPARSITY[column]
    rows = []
    row = 0
    while row < num_records:
        rows.append(row)
        row += _INFREQUENT_GAPS[len(rows) % len(_INFREQUENT_GAPS)] * scale
    if len(rows) < _MIN_INFREQUENT_READINGS:
        raise ValueError(
            f"generate_raw_dataset: the span is {num_records} rows, which "
            f"gives '{column}' {len(rows)} reading(s) at its sparsity of "
            f"{scale}. Generate a longer span, or lower the sparsity."
        )
    return rows


def _cumulative_column(column, reading_rows, num_records, grid_minutes, household_id):
    """One cumulative column, carrying a reading on each row in *reading_rows*.

    The rows in between are empty: a device that did not report delivered
    nothing. Increments are drawn against the bound for one grid interval
    wherever the readings land, so a reading is inside its own bound however
    long the device was idle for.
    """
    min_diff, max_diff = interval_draw_range(column, grid_minutes)
    # Seeded per household and column, so a column's values do not move when
    # the column list does.
    column_seed = int(
        hashlib.md5(f"{household_id}_{column}".encode()).hexdigest(), 16
    ) % (2**32)
    draw = Generator(PCG64(seed=column_seed))
    diffs = pd.Series(
        draw.uniform(min_diff, max_diff, size=len(reading_rows) - 1),
        dtype="Float64",
    )
    readings = pd.concat(
        [pd.Series([0], dtype="Float64"), diffs.cumsum()], ignore_index=True
    )
    readings.index = reading_rows
    return readings.reindex(range(num_records))


def generate_raw_dataset(
    output_dir,
    *,
    households_per_project=DEFAULT_HOUSEHOLDS_PER_PROJECT,
    projects=DEFAULT_PROJECTS,
    days=DEFAULT_DAYS,
    base_date=DEFAULT_BASE_DATE,
    time_interval=DEFAULT_GRID,
):
    """
    Raw supplier-shaped household data, with a minimal index file.

    A clean, complete dataset: every cumulative column present, non-decreasing
    from zero, on an unbroken grid, with every value inside its declared bound.
    No fault is injected; a stage's handling of one is tested on a frame that
    test builds.

    The infrequent devices (:data:`INFREQUENT_DEVICE_COLUMNS`) deliver a few
    unevenly spaced readings and are empty between them, as a supplier delivers
    them. Every other column reports on every row.

    The shape is the caller's, and it decides which behaviours a run can reach:
    a project below the averaging floor publishes no average, and a span
    shorter than an interval gives that interval no slot.

    Returns:
        str: Path to the directory containing the generated fixture data.
    """
    per_interval = pd.Timedelta(time_interval)
    num_records = int(pd.Timedelta(days=days) / per_interval)
    grid_minutes = per_interval.total_seconds() / 60
    project_ids = list(range(1, projects + 1))

    # The rows each column carries a reading on. The same for every household,
    # so it is resolved once -- and raises here rather than per household if the
    # span is too short for a device to report twice.
    reading_rows = {
        column: (
            _infrequent_reading_rows(column, num_records)
            if column in INFREQUENT_DEVICE_COLUMNS
            else list(range(num_records))
        )
        for column in data_model.cumulative_columns
    }

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Metadata index
    index_raw = []
    index_bsv = []

    # Generate data for each household
    for project_id in project_ids:
        for household_idx in range(1, households_per_project + 1):
            # Generate unique HuisIdBSV
            household_id = (project_id - 1) * households_per_project + household_idx

            # Preconstruct the prefixed strings
            household_prefixed = supplier_label(household_id)
            project_prefixed = f"Project{project_id:02}"

            # Generate time series
            timestamps = pd.date_range(start=base_date, periods=num_records, freq=time_interval)
            household_data = {
                "HuisIdLeverancier": pd.Series([household_prefixed] * num_records, dtype="string"),
                "ProjectIdLeverancier": pd.Series([project_prefixed] * num_records, dtype="string"),
                "ReadingDate": timestamps,
            }

            for col in sorted(data_model.cumulative_columns):
                household_data[col] = _cumulative_column(
                    col, reading_rows[col], num_records, grid_minutes, household_id
                )

            household_df = pd.DataFrame(household_data)
            if household_id == GAP_HOUSEHOLD:
                household_df = _blank_a_span(
                    household_df, GAP_COLUMN, GAP_START_FRACTION, GAP_SLOTS
                )

            file_path = os.path.join(output_dir, f"household_{household_id}_table.parquet")
            household_df.to_parquet(file_path, index=False)

            # Add metadata
            index_bsv.append({"HuisIdLeverancier": household_prefixed, "ProjectIdLeverancier": project_prefixed, "HuisIdBSV": household_id, "ProjectIdBSV": project_id})
            index_raw.append({"HuisIdLeverancier": household_prefixed, "ProjectIdLeverancier": project_prefixed})

    index_raw_df = pd.DataFrame(index_raw)
    index_raw_file_path = os.path.join(output_dir, "index_raw.parquet")
    index_raw_df.to_parquet(index_raw_file_path, index=False)

    index_bsv_df = pd.DataFrame(index_bsv)
    index_bsv_file_path = os.path.join(output_dir, "index_bsv.parquet")
    index_bsv_df.to_parquet(index_bsv_file_path, index=False)

    logging.info(f"Fixture generated in directory: {output_dir}")
    return output_dir


def _list_files_data_fixture(folder_path):
    """The raw household files, keyed by stem, in household-number order.

    HuisIdBSV is assigned in the order this yields. Sorting on the number in
    the name rather than on the name keeps household_10 after household_1 and
    makes the assignment the same on any platform.
    """
    names = [
        name for name in os.listdir(folder_path)
        if name.endswith(".parquet") and "index" not in name
    ]
    return {
        name[:-8]: name
        for name in sorted(names, key=lambda name: int(name.split("_")[1]))
    }


def _process_data_fixture_file(
    household_id, file_name, etd_test_fixture_path, mapped_folder_path, grid_minutes
):
    """Map one raw fixture file, returning its index entry and its cadence.

    The pipeline writes the household batch's file, so nothing here names its
    path.
    """
    # The dataset being mapped. It keys the output shard and every sidecar row;
    # the index entry the pipeline returns is the household's.
    household_batch_id = index_helpers.household_batch_for_household(
        int(household_id)
    )
    file_path = os.path.join(etd_test_fixture_path, file_name)
    household_id_supplier = supplier_label(
        int(file_name.replace("household_", "").replace("_table.parquet", ""))
    )

    data_fixture_df = pd.read_parquet(file_path, dtype_backend="numpy_nullable")

    # Identifier columns belong to the index, not the household data, and are
    # not part of the data model -- read them before the mapping steps.
    project_id = (
        str(data_fixture_df["ProjectIdLeverancier"].iloc[0])
        if "ProjectIdLeverancier" in data_fixture_df.columns
        else "unknown"
    )

    # Cadence is measured on the timestamps as delivered. Padding to the grid
    # first would report the grid rather than what each column sends.
    cadence = mapping.detect_column_cadence(
        data_fixture_df,
        context=f"{household_id}/{file_name}",
        grid_minutes=grid_minutes,
    )

    # What the supplier delivered, on the frame as delivered: every row here is
    # a row that arrived, so an empty cell is a field left empty rather than a
    # slot no row came for. Must precede the fill, or a filled cell reads as
    # delivered.
    mapping.mark_delivered(data_fixture_df)

    # Which devices report infrequently is per supplier, so the fill sits
    # outside run_standard_pipeline and a mapper applies it before calling.
    data_fixture_df = mapping.fill_down_infrequent_devices(
        data_fixture_df,
        columns=INFREQUENT_DEVICE_COLUMNS,
        cadence=cadence,
        grid_minutes=grid_minutes,
    )

    # The production mapping path, as the supplier mappers call it: the flags a
    # stage downstream reads are the ones this path writes.
    new_entry = mapping.run_standard_pipeline(
        data_fixture_df,
        int(household_id),
        household_id_supplier,
        mapped_folder_path,
        context=f"{household_id}/{file_name}",
        cadence=cadence,
        grid_minutes=grid_minutes,
        household_batch_id=household_batch_id,
    )

    # From the pipeline rather than the frame measured above: it adds the
    # cadence row for a tariff root built from delivered splits, which has none
    # of its own. Both extras are popped; the index takes no other fields.
    cadence = new_entry.pop("_cadence", None)
    new_entry.pop("_validation_summary", None)
    new_entry["ProjectIdLeverancier"] = project_id
    return new_entry, cadence, household_batch_id


def generate_mapped_dataset(
    target_folder,
    *,
    bsv_metadata_file,
    supplier_metadata_xlsx_file=None,
    project_mapping_csv_path=None,
    raw_folder=None,
    households_per_project=DEFAULT_HOUSEHOLDS_PER_PROJECT,
    projects=DEFAULT_PROJECTS,
    days=DEFAULT_DAYS,
    base_date=DEFAULT_BASE_DATE,
):
    """Generate the synthetic mapped dataset into *target_folder*.

    Raw supplier-shaped input is generated first -- into *raw_folder* when
    given, a sibling of the target otherwise -- and mapped through
    ``run_standard_pipeline``. The result is ``index.parquet``, the per-column
    cadence sidecar, and the household data in whichever layout
    ``etdmap.options.mapped_output_format`` names; read it with
    :func:`etdmap.storage.mapped_household_paths`.

    The caller supplies the metadata paths, so each suite uses its own
    configuration and this module needs none.

    Leaves ``etdmap.options`` as it found it, with one exception the etdmap
    suite relies on: ``bsv_metadata_file`` is left pointing at the reviewed
    metadata, since generation runs against a first-mapping variant of it.
    """
    target_folder = Path(target_folder)
    target_folder.mkdir(parents=True, exist_ok=True)
    raw_folder = Path(raw_folder) if raw_folder else target_folder.parent / "raw_fixture"
    project_mapping_csv_path = project_mapping_csv_path or bsv_metadata_file

    raw_folder = generate_raw_dataset(
        raw_folder,
        households_per_project=households_per_project,
        projects=projects,
        days=days,
        base_date=base_date,
    )

    etdmap.options.mapped_folder_path = target_folder
    etdmap.options.bsv_metadata_file = Path(bsv_metadata_file)

    # The generated metadata sits inside the generated tree. The real index
    # write path populates index.parquet's batch columns from the sync CSV, so
    # fixtures exercise production code rather than a fixture-only derivation.
    # Writing it beside the supplied metadata instead would put it in a folder
    # the caller owns and two suites configured alike would share one copy.
    # Meenemen is empty: the researcher adds the rows and reviews later.
    fixtures_dir = target_folder / "_generated_metadata"
    fixtures_dir.mkdir(parents=True, exist_ok=True)
    households = households_per_project * projects
    household_ids = list(range(1, households + 1))
    # The project each household was generated into, by the same arithmetic
    # generate_raw_dataset assigns it. Declaring one project for all of them
    # would contradict the index, which splits them.
    project_of = [
        ((household_id - 1) // households_per_project) + 1
        for household_id in household_ids
    ]
    # The span the data actually covers. A window that does not contain the
    # readings would make the batch disagree with the household files.
    start = pd.Timestamp(base_date)
    end = start + pd.Timedelta(days=days)
    pd.DataFrame({
        "HuisIdBSV": pd.array(household_ids, dtype="Int64"),
        # One household batch per household, numbered with the household id,
        # which is what save_mapped_household writes. A household can have
        # several household batches; the mapping cannot produce them yet.
        "HuisBatchIdBSV": pd.array(household_ids, dtype="Int64"),
        "BatchIdBSV": pd.array([1] * households, dtype="Int64"),
        "ProjectIdBSV": pd.array(project_of, dtype="Int64"),
        "Meenemen": pd.array([pd.NA] * households, dtype="boolean"),
        "Gegevensfrequentie": pd.array([DEFAULT_GRID] * households, dtype="string"),
        "Leverancierfrequentie": pd.array([pd.NA] * households, dtype="string"),
        "Startdatum": [f"{start:%Y-%m-%d %H:%M:%S} UTC"] * households,
        "Einddatum": [f"{end:%Y-%m-%d %H:%M:%S} UTC"] * households,
    }).to_csv(fixtures_dir / "household_batch_sync.csv", index=False)
    etdmap.options.household_batch_csv_path = fixtures_dir / "household_batch_sync.csv"

    # Generation runs in the FIRST-MAPPING state: Meenemen is not reviewed yet,
    # so the Meenemen stamp reads a variant of the metadata with those values
    # blanked. The option is restored to the reviewed metadata on the way out.
    reviewed_metadata = Path(bsv_metadata_file)
    first_mapping_metadata = fixtures_dir / "metadata_first_mapping.csv"
    meta_df = pd.read_csv(reviewed_metadata, dtype_backend="numpy_nullable")
    meta_df["Meenemen"] = pd.array([pd.NA] * len(meta_df), dtype="boolean")
    meta_df.to_csv(first_mapping_metadata, index=False)
    etdmap.options.bsv_metadata_file = first_mapping_metadata

    # Bootstrap: this generates a brand-new synthetic dataset into an empty
    # folder, so there is no index to look household ids up from and the
    # sequential assignment is the intended behaviour here.
    index_df, _ = index_helpers.read_index(allow_missing=True)
    if "Dataleverancier" not in index_df.columns:
        index_df.loc[:, "Dataleverancier"] = "etdmap"
    household_id_pairs = index_helpers.get_household_id_pairs(
        index_df, raw_folder, data_provider="etdmap",
        list_files_func=_list_files_data_fixture,
    )

    cadence_rows = []
    for household_id, file_name in household_id_pairs:
        logging.info(f"Starting {file_name}")
        # Read from the batch CSV written just above, the same way a mapper
        # resolves it, rather than repeating the fixture's own interval here.
        new_entry, cadence, household_batch_id = _process_data_fixture_file(
            household_id, file_name, raw_folder,
            etdmap.options.mapped_folder_path,
            index_helpers.grid_minutes_for_household(household_id),
        )
        cadence_rows.extend(
            mapping.cadence_to_rows(household_batch_id, cadence)
        )
        index_df = index_helpers.update_index(
            index_df, new_entry, data_provider="etdmap"
        )

    # The sidecar every later stage scales a threshold from. Without it the
    # fixture is not something the pipeline can be run against.
    mapping.write_column_cadence(cadence_rows, str(etdmap.options.mapped_folder_path))

    # The project mapping covers every project this dataset generated. A
    # supplied file covers whatever it was written for, so a dataset with more
    # projects than that leaves ProjectIdBSV unresolved on the rest.
    generated_project_mapping = fixtures_dir / "project_mapping.csv"
    pd.DataFrame({
        "Dataleverancier": pd.array(["etdmap"] * projects, dtype="string"),
        "ProjectIdLeverancier": pd.array(
            [f"Project{project_id:02}" for project_id in range(1, projects + 1)],
            dtype="string",
        ),
        "ProjectIdBSV": pd.array(range(1, projects + 1), dtype="Int64"),
    }).to_csv(generated_project_mapping, index=False)
    etdmap.options.project_mapping_csv_path = (
        project_mapping_csv_path or generated_project_mapping
    )

    # Generated for the households this run made, so the join resolves all
    # of them whatever shape the caller asked for.
    metadata_df = (
        read_metadata(Path(supplier_metadata_xlsx_file))
        if supplier_metadata_xlsx_file is not None
        else synthetic_supplier_metadata(
            zip(index_df["HuisIdLeverancier"], index_df["ProjectIdLeverancier"])
        )
    )
    index_helpers.add_supplier_metadata_to_index(
        index_df, metadata_df, data_supplier="etdmap"
    )

    # The reviewed metadata, derived from the index this run just built rather
    # than from the file the caller supplied. A supplied file fixes the
    # household count, so the shape could not be the caller's and a dataset
    # shaped for a behaviour -- a project large enough to meet the averaging
    # floor, say -- would fail the household-key comparison in update_include.
    # Meenemen is True throughout: the review has happened by this point, and a
    # household it excluded would be absent from every stage downstream.
    reviewed = index_df.loc[:, bsv_metadata_columns].copy()
    reviewed["Meenemen"] = pd.array([True] * len(reviewed), dtype="boolean")
    # Declared empty rather than absent: the device-absence reader refuses
    # metadata with no such column at all.
    reviewed["ApparaatOntbrekend"] = pd.array(
        [pd.NA] * len(reviewed), dtype="string"
    )
    generated_reviewed = fixtures_dir / "metadata_reviewed.csv"
    reviewed.to_csv(generated_reviewed, index=False)

    etdmap.options.bsv_metadata_file = generated_reviewed
    return etdmap.options.mapped_folder_path
