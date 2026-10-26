"""Generation of the synthetic mapped dataset the test suites run against.

etdmap owns mapping, so it owns the fixture dataset that represents mapped
output. Downstream suites call :func:`generate_mapped_dataset` rather than
reading a folder a previous run left behind, which keeps them aligned with what
this package actually emits.

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
from etdmap.record_validators import record_flag_conditions

__all__ = [
    "CADENCE_DTYPES",
    "SYNTHETIC_DIFF_RANGES",
    "SyntheticModel",
    "cadence_sidecar",
    "declare_synthetic_model",
    "generate_mapped_dataset",
    "generate_raw_dataset",
    "installed_fingerprint",
    "model_rows",
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

    The live model is derived from Grist and its names change, so a
    behavioural test reaching for a real variable is pinned to today's
    metadata rather than to the behaviour it means to check. This builds the
    same *shape* from names nothing else uses:

    - *stem* is a tariff device -- a combined variable, its complement, and
      the two registers (each with its own complement) that mapping sums
      into it;
    - *plain* is an ordinary device, with a complement and no registers.
      Having one in the model is what keeps a test honest about a device
      family being the declared one rather than every column on the frame.

    Every interval column is declared monotonic, and every column gets the
    same threshold entry -- *minimum*, *maximum*, *threshold_type*, with no
    sustained level or exponent, so a bound scales by a plain multiply. A
    test whose subject is a bound states the *maximum* it needs rather than
    inheriting whatever the metadata happens to declare this week.

    Pass the result to :func:`declare_synthetic_model`.
    """
    # Tariff registers are paired by name, so a declared model gets tariff
    # splits only if its registers carry the substrings
    # _build_tariff_register_pairs matches, and its interval columns the one
    # suffix that pattern's suffix group accepts. The cumulative-to-interval
    # link is declared through ComplementVariabele and needs no convention, so
    # the suffix is here for the tariff pattern alone. The stems name nothing
    # the real model names.
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


def model_rows(model: SyntheticModel) -> tuple[list, list]:
    """The etdmodel and thresholds rows *model* declares.

    The translation from a declaration to the two tables the data model is
    built from, so a declared model goes through the same derivation the
    shipped one does rather than through lookups assembled here.

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
    which copies the object into their own namespace. Rather than patch each
    of those bindings, the declaration is merged into the model tables and
    every derived structure is rebuilt in place, so a module reading one of
    them reads the declaration without being patched, and a module that starts
    reading one later is covered without this helper being updated.

    The declaration supplies rows; everything that answers "which columns
    belong to one variable, and what may it read" is then derived from them --
    the complement and tariff pairings, the monotonic interval columns, the
    thresholds, and the per-column checks.

    The declaration is merged into the shipped model rather than replacing it,
    so a column the declaration does not mention keeps whatever the model says
    about it. State in ``model.thresholds`` the bound of every column a code
    path under test looks up, so the test does not rest on a shipped number.

    On exit the model is rebuilt from the shipped tables. A declared column
    reaches more structures than its own row names, so the restore is a
    rebuild rather than an undo of the entries that were written.

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


# Frozen synthetic generation ranges for the raw-data test fixture:
# cumulative column name -> (min_diff, max_diff) for its per-interval Diff.
#
# PROVENANCE: seeded 2026-06-19 from the then-current thresholds.csv effective
# values (each "{col}Diff" row's Min/Max, with a missing Max collapsed to 1.0).
# These are INTENTIONALLY INDEPENDENT of thresholds.csv from here on: test input
# must exercise behaviour, never be derived from the artifact under test
# (generating from thresholds is circular -- data drawn inside [Min, Max] can
# never violate its own thresholds, so the validators/clipping are never
# exercised and a wrong threshold cannot be caught). Editing thresholds.csv no
# longer rewrites this fixture.
#
# This dict is also the LIST OF COLUMNS the fixture generates: the generator
# iterates it, not cumulative_columns. Driving generation off the declared model
# made every new model variable a fixture error -- adding
# ElektriciteitsgebruikTotaalWarmtepompCumulatief to the model broke four tests
# that have nothing to do with it. A fixture declares its own columns; what the
# model declares is tested as behaviour, not as an inventory.
SYNTHETIC_DIFF_RANGES: dict[str, tuple[float, float]] = {
    "ElektriciteitNetgebruik": (0.0, 1.5),
    "ElektriciteitNetgebruikHoog": (0.0, 1.5),
    "ElektriciteitNetgebruikLaag": (0.0, 1.5),
    "ElektriciteitTeruglevering": (0.0, 1.5),
    "ElektriciteitTerugleveringHoog": (0.0, 1.5),
    "ElektriciteitTerugleveringLaag": (0.0, 1.5),
    "ElektriciteitsgebruikBoilervat": (0.0, 0.32),
    "ElektriciteitsgebruikBooster": (0.0, 0.32),
    "ElektriciteitsgebruikOverigGG": (0.0, 1.0),
    "ElektriciteitsgebruikRadiator": (0.0, 0.32),
    "ElektriciteitsgebruikWTW": (0.0, 0.32),
    "ElektriciteitsgebruikWarmtepomp": (0.0, 0.96),
    "ElektriciteitsgebruikWarmtepompIntern": (0.0, 1.0),
    "Gasgebruik": (0.0, 0.8333),
    "WarmteproductieRuimteverwarming": (0.0, 0.0576),
    "WarmteproductieWarmTapwater": (0.0, 0.0576),
    "WarmteproductieWarmtepomp": (0.0, 0.0576),
    "WatergebruikRuimteverwarming": (0.0, 1.0),
    "WatergebruikWarmTapwater": (0.0, 100.0),
    "WatergebruikWarmtepomp": (0.0, 1.0),
    "ZonopwekTotaal": (0.0, 0.96),
    "ZonopwekVerbruik": (0.0, 1.0),
}


#: The dataset's shape, where a caller states none. Two projects because a
#: project aggregate needs more than one, and enough households per project to
#: meet the averaging floor, so a run exercises the production constant instead
#: of a lowered one. Three days because the coarsest resample is daily and a
#: single day gives it no interior slot.
DEFAULT_HOUSEHOLDS_PER_PROJECT = 10
DEFAULT_PROJECTS = 2
DEFAULT_DAYS = 3
DEFAULT_GRID = "5min"
DEFAULT_BASE_DATE = pd.Timestamp("2023-01-01")

#: One gap, in one column of one household. Not realism: with no gap anywhere
#: imputation produces its artifacts having filled nothing, so a run cannot
#: show the fill placing volume or the gap accumulator recording any. What the
#: fill does with a gap is asserted per behaviour, on frames those tests build.
#: Declared by household number and span fraction so it lands inside whatever
#: shape the caller asked for.
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
    supplier declare. Generated rather than read so the household count is the
    caller's -- a workbook fixes it, and a dataset shaped for a behaviour then
    has households the metadata does not cover.

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

    A clean, complete dataset: every cumulative column present and populated,
    non-decreasing from zero, on an unbroken grid, with every value inside its
    declared bound. What a stage does with a fault is asserted per fault, on a
    frame the test builds, so nothing is injected here.

    The shape is the caller's. It decides which behaviours a run can reach -- a
    project below the averaging floor cannot publish an average, and a span
    shorter than an interval gives that interval no slot -- so a caller testing
    one of those states its own shape rather than inheriting this one.

    Returns:
        str: Path to the directory containing the generated fixture data.
    """
    per_interval = pd.Timedelta(time_interval)
    num_records = int(pd.Timedelta(days=days) / per_interval)
    project_ids = list(range(1, projects + 1))

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
            household_prefixed = f"Huis{household_id:02}"
            project_prefixed = f"Project{project_id:02}"

            # Generate time series
            timestamps = pd.date_range(start=base_date, periods=num_records, freq=time_interval)
            household_data = {
                "HuisIdLeverancier": pd.Series([household_prefixed] * num_records, dtype="string"),
                "ProjectIdLeverancier": pd.Series([project_prefixed] * num_records, dtype="string"),
                "ReadingDate": timestamps,
            }

            # Generate cumulative column data.
            # Each column uses an independent seed derived from household + column name
            # so fixture data is stable regardless of the order of cumulative_columns.
            for col in sorted(SYNTHETIC_DIFF_RANGES):
                min_diff, max_diff = SYNTHETIC_DIFF_RANGES[col]
                col_seed = int(hashlib.md5(f"{household_id}_{col}".encode()).hexdigest(), 16) % (2**32)
                col_rng = Generator(PCG64(seed=col_seed))
                diffs = pd.Series(col_rng.uniform(min_diff, max_diff, size=num_records - 1), dtype="float64")
                cumulative = pd.concat([pd.Series([0]), diffs.cumsum()], ignore_index=True)
                household_data[col] = cumulative

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
    return {f[:-8]: f for f in os.listdir(folder_path) if f.endswith(".parquet") and "index" not in f}


def _process_data_fixture_file(
    household_id, file_name, etd_test_fixture_path, mapped_folder_path, grid_minutes
):
    """Map one raw fixture file, returning its index entry and its cadence."""
    file_path = os.path.join(etd_test_fixture_path, file_name)
    new_file_path = os.path.join(
        mapped_folder_path, f"household_{int(household_id)}_table.parquet"
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

    # What the supplier delivered, recorded on the frame as delivered: every row
    # here is a row that arrived, so an empty cell is a field the supplier left
    # empty rather than a slot no row came for. The padding below answers the
    # third case. Must precede the fill, or a filled cell reads as delivered.
    mapping.mark_delivered(data_fixture_df)

    data_fixture_df = mapping.ensure_intervals(
        data_fixture_df, freq=mapping.cadence_label(grid_minutes)
    )

    data_fixture_df = mapping.rearrange_model_columns(
        household_df=data_fixture_df, add_columns=True, context=f"{household_id}/{file_name}"
    )

    # Before the fill-down and before add_diff_columns, as in
    # run_standard_pipeline: both write values, and the cadence lattice is
    # anchored on the delivery record taken above.
    mapping.mark_cadence(
        data_fixture_df,
        cadence=cadence,
        grid_minutes=grid_minutes,
        context=f"{household_id}/{file_name}",
    )

    data_fixture_df = mapping.fill_down_infrequent_devices(
        df=data_fixture_df,
        columns=("ElektriciteitsgebruikBoilervat", "ElektriciteitsgebruikRadiator", "ElektriciteitsgebruikBooster"),
        grid_minutes=grid_minutes,
    )

    data_fixture_df = mapping.add_diff_columns(data_fixture_df, context=f"{household_id}/{file_name}")

    for flag, condition in record_flag_conditions.items():
        try:
            data_fixture_df[flag] = condition(
                data_fixture_df,
                **index_helpers.supported_kwargs(condition, cadence=cadence),
            )
        except Exception as e:
            logging.error(
                f"Error validating with {flag} for household {household_id} / {file_name}: {e}",
                exc_info=True,
            )
            data_fixture_df[flag] = pd.NA

    data_fixture_df.to_parquet(new_file_path, engine="pyarrow")

    return {
        "HuisIdLeverancier": f'Huis{int(file_name.replace("household_", "").replace("_table.parquet", "")):02}',
        "ProjectIdLeverancier": project_id,
        "HuisIdBSV": household_id,
    }, cadence


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

    Writes ``household_<id>_table.parquet`` per household, ``index.parquet`` and
    the per-column cadence sidecar -- the shape downstream stages read. Raw supplier-shaped input is generated
    first, into *raw_folder* when given and a sibling of the target otherwise.

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
        new_entry, cadence = _process_data_fixture_file(
            household_id, file_name, raw_folder,
            etdmap.options.mapped_folder_path,
            index_helpers.grid_minutes_for_household(household_id),
        )
        cadence_rows.extend(mapping.cadence_to_rows(household_id, cadence))
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

    # Generated for the households this run made, for the same reason.
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
