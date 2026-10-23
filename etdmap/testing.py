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
import sys
from pathlib import Path
from typing import NamedTuple

import pandas as pd
from numpy.random import PCG64, Generator

import etdmap
import etdmap.index_helpers as index_helpers
import etdmap.mapping_helpers as mapping
from etdmap.data_model import (
    cumulative_columns,
    interval_column,
    scale_bounds,
    thresholds_dict,
)
from etdmap.index_helpers import read_metadata
from etdmap.record_validators import record_flag_conditions

__all__ = [
    "SYNTHETIC_DIFF_RANGES",
    "SyntheticModel",
    "declare_synthetic_model",
    "generate_mapped_dataset",
    "generate_raw_dataset",
    "installed_fingerprint",
    "synthetic_model",
]


class SyntheticModel(NamedTuple):
    """The column names and relations a test declares for itself.

    Built by :func:`synthetic_model`, installed by
    :func:`declare_synthetic_model`.
    """

    complements: dict
    tariff_splits: dict
    device: str
    device_complement: str
    high: str
    low: str
    high_complement: str
    low_complement: str
    plain: str
    plain_complement: str


def synthetic_model(stem: str = "A", plain: str = "B") -> SyntheticModel:
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

    The relations are the two the model declares: a symmetric complement map
    and root-to-registers. Pass the result to :func:`declare_synthetic_model`.
    """
    device, device_complement = stem, f"{stem}_Complement"
    high, low = f"{stem}_High", f"{stem}_Low"
    high_complement, low_complement = f"{high}_Complement", f"{low}_Complement"
    plain_complement = f"{plain}_Complement"

    complements: dict = {}
    for one, other in (
        (device, device_complement),
        (high, high_complement),
        (low, low_complement),
        (plain, plain_complement),
    ):
        complements[one] = other
        complements[other] = one

    return SyntheticModel(
        complements=complements,
        tariff_splits={
            device: (high, low),
            device_complement: (high_complement, low_complement),
        },
        device=device,
        device_complement=device_complement,
        high=high,
        low=low,
        high_complement=high_complement,
        low_complement=low_complement,
        plain=plain,
        plain_complement=plain_complement,
    )


def declare_synthetic_model(monkeypatch, model: SyntheticModel = None):
    """Install *model* as the model every loaded etdmap module reads.

    Consumers bind the relations with ``from etdmap.data_model import ...``,
    which copies the object into their own namespace, so patching
    ``data_model`` alone would leave them reading the real model. The
    declaration is therefore applied to every loaded etdmap module holding
    either name -- which also means a module that starts consuming them
    later is covered without this helper being updated.

    Scope is the two relations that say which columns belong to one device:
    ``complement_column`` and ``tariff_root_to_splits``. Nothing else about
    the model is replaced.

    Returns the model, so a test can read its column names from it.
    """
    model = model if model is not None else synthetic_model()

    def _complement(column):
        return model.complements.get(column)

    patched = 0
    for module in list(sys.modules.values()):
        if not getattr(module, "__name__", "").startswith("etdmap"):
            continue
        if callable(getattr(module, "complement_column", None)):
            monkeypatch.setattr(module, "complement_column", _complement)
            patched += 1
        if isinstance(getattr(module, "tariff_root_to_splits", None), dict):
            monkeypatch.setattr(
                module, "tariff_root_to_splits", model.tariff_splits
            )
            patched += 1
    if not patched:
        raise ValueError(
            "[declare_synthetic_model] no loaded etdmap module exposes "
            "complement_column or tariff_root_to_splits, so the declared "
            "model would not be read by anything. Import the module under "
            "test before calling this."
        )
    return model


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
# longer rewrites this fixture. tests/test_threshold_fixture_coverage.py warns
# if thresholds.csv grows variables/columns/units this dict no longer covers.
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


def generate_raw_dataset(output_dir):
    """
    Pytest fixture to generate raw household data for 2 projects with 5 households each with a minimal index file.
    Saves the generated files in a temporary directory.

    It will use household 1 and some variables to generate some expected mistakes/problems so we can test our validators and mapping process.

    Returns:
        str: Path to the directory containing the generated fixture data.
    """
    # Settings
    num_households_per_project = 5
    projects = [1, 2]
    num_records = 105120  # 1 year of data at 5-minute intervals
    base_date = pd.Timestamp("2023-01-01")
    time_interval = "5min"  # 5-minute intervals

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Metadata index
    index_raw = []
    index_bsv = []

    # Explicitly define the bit generator to ensure the algorithm/seed don't change
    bit_generator = PCG64(seed=42)
    rng = Generator(bit_generator)

    # Generate data for each household
    for project_id in projects:
        for household_idx in range(1, num_households_per_project + 1):
            # Generate unique HuisIdBSV
            household_id = (project_id - 1) * num_households_per_project + household_idx

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
            for col in sorted(cumulative_columns):
                if col not in SYNTHETIC_DIFF_RANGES:
                    raise ValueError(
                        f"Cannot generate raw data test fixture: no synthetic range for "
                        f"cumulative column '{col}' in SYNTHETIC_DIFF_RANGES (tests/conftest.py). "
                        f"Add it -- see tests/test_threshold_fixture_coverage.py."
                    )
                min_diff, max_diff = SYNTHETIC_DIFF_RANGES[col]
                col_seed = int(hashlib.md5(f"{household_id}_{col}".encode()).hexdigest(), 16) % (2**32)
                col_rng = Generator(PCG64(seed=col_seed))
                diffs = pd.Series(col_rng.uniform(min_diff, max_diff, size=num_records - 1), dtype="float64")
                cumulative = pd.concat([pd.Series([0]), diffs.cumsum()], ignore_index=True)
                household_data[col] = cumulative

            household_df = pd.DataFrame(household_data)
            # The interval these rows were built at, passed rather than
            # defaulted again: the test cases place gaps and shifts relative to
            # it, so a second copy of the figure could silently disagree.
            household_df = add_raw_data_test_case(
                base_date, household_df, household_id_raw=household_prefixed,
                project_id_raw=project_prefixed, rng=rng,
                interval=pd.Timedelta(time_interval),
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


# cases to add:
#    mixed data types in a single column - are they reported and coerced correctly?
#    drifting clocks - are they detected and corrected?
#    less records than expected - are they reported?
def add_raw_data_test_case(
    base_date, household_df, household_id_raw, project_id_raw, rng, interval
):
    """Inject one household's deliberate fault into the generated raw data.

    *interval* is the spacing the rows were generated at. It is required: the
    cases below place gaps, shifts and duplicate readings relative to it, so a
    default here would be a second copy of a figure the caller already holds and
    could silently disagree with it.
    """
    if interval is None or pd.isna(interval):
        raise ValueError(
            "add_raw_data_test_case: interval was not given, so the spacing the "
            "injected test cases are placed at is unknown."
        )

    if (household_id_raw == "Huis1") and (project_id_raw == "Project1"):
        # There is a gap in data but after gap it continues (shift data down 24h)
        # Define conditions
        gap_start = base_date + pd.Timedelta(days=30)
        gap_length = pd.Timedelta(hours=24)
        var = cumulative_columns[0]
        household_df = introduce_gap(household_df=household_df, columns=[var], gap_start=gap_start, gap_length=gap_length, shift = True)

    elif (household_id_raw == "Huis2") and (project_id_raw == "Project1"):
        # there is a gap in data but after gap it continues with the same starting value (shift data down 24h + copying the last value)
        # Define conditions
        gap_start = base_date + pd.Timedelta(days=60)
        gap_length_adjusted = pd.Timedelta(hours=24) + interval
        gap_end = gap_start + pd.Timedelta(hours=24)
        var = cumulative_columns[1]
        last_value = household_df.loc[household_df['ReadingDate'] == gap_start - interval, var]
        if pd.isna(last_value).any():
            raise ValueError(f"Last value before gap at {gap_start - interval} is missing for {var}")
        household_df = introduce_gap(household_df=household_df, columns=[var], gap_start=gap_start, gap_length=gap_length_adjusted, shift = True)
        # fill value at gap_end time with the last value before the gap
        household_df.loc[household_df['ReadingDate'] == gap_end, var] = last_value

    elif (household_id_raw == "Huis3") and (project_id_raw == "Project1"):
        # there is a gap in data and after the gap, the meter was reset to 0. Subtract the last value before the gap from all values after the gap.
        # Define conditions
        gap_start = base_date + pd.Timedelta(days=90)
        gap_length = pd.Timedelta(hours=24)
        gap_end = gap_start + gap_length
        var = cumulative_columns[2]
        household_df = introduce_gap(household_df=household_df, columns=[var], gap_start=gap_start, gap_length=gap_length, shift = True)
        household_df = reset_cumulative_column(household_df=household_df, columns=[var], reset_time=gap_end, check_negative = True)

    elif (household_id_raw == "Huis4") and (project_id_raw == "Project1"):
        # there is no gap in the data but the meter was reset to 0 at some point. Subtract the last value before the reset from all values after the reset.
        meter_reset_date = base_date + pd.Timedelta(days=120)
        var = cumulative_columns[3]
        household_df = reset_cumulative_column(household_df=household_df, columns=[var], reset_time=meter_reset_date, check_negative = True)

    elif (household_id_raw == "Huis5") and (project_id_raw == "Project1"):
        # there is 24 hour gap in the data. Delete 24hrs of data and do not shift it down. There should be a big jump in the value as a result.
        gap_start = base_date + pd.Timedelta(days=150)
        gap_length = pd.Timedelta(hours=24)
        var = cumulative_columns[4]
        household_df = introduce_gap(household_df=household_df, columns=[var], gap_start=gap_start, gap_length=gap_length, shift = False)

    elif (household_id_raw in ["Huis6","Huis7", "Huis8", "Huis9", "Huis10"]) and (project_id_raw == "Project2"):
        # at the same time for each household there is 24h gap in all data. Delete 24hs of data and do not shift it down. There should be a big jump in the value as a result.
        gap_start = base_date + pd.Timedelta(days=180)
        gap_length = pd.Timedelta(hours=24)
        var = cumulative_columns[0]
        household_df = introduce_gap(household_df=household_df, columns=[var], gap_start=gap_start, gap_length=gap_length, shift = False)

    # There is a single variable that only has data every hour - insert pd.NA unless at the hour
    hourly_var = cumulative_columns[5]
    mask_hourly = (household_df['ReadingDate'].dt.minute != 0)
    household_df.loc[mask_hourly, hourly_var] = pd.NA

    # There is a single variable that only has data every 15 minutes - insert pd.NA unless at the 15 minute mark
    fifteen_min_var = cumulative_columns[6]
    mask_fifteen_min = (household_df['ReadingDate'].dt.minute % 15 != 0)
    household_df.loc[mask_fifteen_min, fifteen_min_var] = pd.NA

    # There is a single variable the supplier delivers as intervals only: the
    # interval column carries hourly readings and the register is never sent, so
    # nothing downstream has a running total to read for it. One tick is dropped
    # so an absent reading can be told apart from the gaps between them.
    #
    # The variable is outside every sum validator and outside the infrequent-
    # device fill-down, so the scenario adds a delivery form without changing
    # what any existing check or case covers.
    interval_only_var = "WarmteproductieRuimteverwarming"
    interval_only_col = interval_column(interval_only_var)
    hourly_ticks = household_df["ReadingDate"].dt.minute == 0
    tick_seed = int(
        hashlib.md5(f"{household_id_raw}_{interval_only_col}".encode()).hexdigest(), 16
    ) % (2**32)
    tick_rng = Generator(PCG64(seed=tick_seed))
    # Drawn against the column's own hourly bound rather than twelve times its
    # 5-minute one: a '5-minute' threshold scales sub-linearly, since an hour
    # cannot sustain a five-minute peak. Readings land below the bound, so this
    # is a plausible coarse delivery and not a second out-of-bounds case.
    _, hourly_max = scale_bounds(thresholds_dict[interval_only_col], 60)
    hourly_values = tick_rng.uniform(
        0.0, float(hourly_max) * 0.8, size=int(hourly_ticks.sum())
    )
    delivered = pd.Series(pd.NA, index=household_df.index, dtype="Float64")
    delivered.loc[hourly_ticks] = hourly_values
    missed_tick = household_df.index[hourly_ticks][24]
    delivered.loc[missed_tick] = pd.NA
    household_df[interval_only_col] = delivered
    household_df[interval_only_var] = pd.NA

    ## There is a single variable that exceeds the upper limits
    household_df[cumulative_columns[7]] = household_df[cumulative_columns[7]]*2

    ## There is a single variable that is negative (some of the time)
    # Define the probability of flipping a value to negative (e.g., 0.1 means 10% chance)
    flip_probability = 0.005
    # Create a boolean mask with True where we want to flip the values to negative
    mask = rng.random(size=household_df.shape[0]) < flip_probability
    # Apply the mask to flip some of the values in the specified column to their negatives
    household_df.loc[mask, cumulative_columns[8]] *= -1

    return household_df


def shift_values(household_df, columns, shift_amount, gap_start=None):
    """
    Shift values in a column by a specific time delta.

    Args:
        household_df (pd.DataFrame): The household data.
        columns (list): List of columns to shift values in.
        shift_amount (pd.Timedelta): The amount of time to shift.
        gap_start (pd.Timestamp, optional): If provided, apply the shift only for rows after this time.

    Returns:
        pd.DataFrame: Modified DataFrame with shifted values.
    """
    shifted_df = household_df[["ReadingDate", *columns]].copy()
    if gap_start:
        mask = household_df['ReadingDate'] >= gap_start
        shifted_df.loc[mask, 'ReadingDate'] += shift_amount
    else:
        shifted_df['ReadingDate'] += shift_amount

    # Merge shifted values back into the original DataFrame
    household_df = household_df.drop(columns=columns)
    household_df = household_df.merge(shifted_df, on='ReadingDate', how='left')

    # Make sure the gap is filled with pd.NA
    if gap_start:
        mask_gap = (household_df['ReadingDate'] >= gap_start) & (household_df['ReadingDate'] < gap_start + shift_amount)
        household_df.loc[mask_gap, columns] = pd.NA

    return household_df


def introduce_gap(household_df, columns, gap_start, gap_length, shift=True):
    """
    Introduce a gap in the specified columns by setting values to pd.NA in the gap range.

    Args:
        household_df (pd.DataFrame): The household data.
        columns (list): List of columns to apply the gap.
        gap_start (pd.Timestamp): Start of the gap.
        gap_length (pd.Timedelta): The length of the gap.
        shift (bool): If True, shift existing values. If False, overwrite with pd.NA.

    Returns:
        pd.DataFrame: Modified DataFrame with the gap.
    """

    if shift:
        return shift_values(household_df=household_df, columns=columns, shift_amount=gap_length, gap_start=gap_start)
    else:
        gap_end = gap_start + gap_length
        mask = (household_df['ReadingDate'] >= gap_start) & (household_df['ReadingDate'] < gap_end)
        household_df.loc[mask, columns] = pd.NA
        return household_df


def reset_cumulative_column(household_df, columns, reset_time, check_negative = True):
    """
    Resets the cumulative column to 0 after a given time

    Args:
        household_df (pd.DataFrame): The household data.
        columns (list): List of columns to reset.
        reset_time (pd.Timestamp): The time at which the meter reset occurs.
        check_negative (bool): If True, asserts there should be no negative values after reset. Default is True.

    Returns:
        pd.DataFrame: Modified DataFrame with reset cumulative values.
    """
    for col in columns:
        reset_value = pd.Series(household_df.loc[household_df['ReadingDate'] == reset_time, col].values).iloc[0]
        if pd.isna(reset_value):
            raise ValueError(f"No data available at reset time {reset_time} for column: {col}")
        mask_reset = household_df['ReadingDate'] > reset_time
        household_df.loc[mask_reset, col] -= reset_value

        if check_negative:
            assert all(household_df.loc[mask_reset, col] >= 0), "Values of column `{col}` after reset should not be negative"

    return household_df


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
    supplier_metadata_xlsx_file,
    project_mapping_csv_path=None,
    raw_folder=None,
    households=10,
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

    raw_folder = generate_raw_dataset(raw_folder)

    etdmap.options.mapped_folder_path = target_folder
    etdmap.options.bsv_metadata_file = Path(bsv_metadata_file)

    # The household-batch sync CSV sits beside the metadata. The real index
    # write path populates index.parquet's batch columns from it, so fixtures
    # exercise production code rather than a fixture-only derivation.
    # Meenemen is empty: the researcher adds the rows and reviews later.
    fixtures_dir = Path(bsv_metadata_file).parent
    pd.DataFrame({
        "HuisIdBSV": pd.array(range(1, households + 1), dtype="Int64"),
        "HuisBatchIdBSV": pd.array(range(1, households + 1), dtype="Int64"),
        "BatchIdBSV": pd.array([1] * households, dtype="Int64"),
        "ProjectIdBSV": pd.array([1] * households, dtype="Int64"),
        "Meenemen": pd.array([pd.NA] * households, dtype="boolean"),
        "Gegevensfrequentie": pd.array(["5min"] * households, dtype="string"),
        "Leverancierfrequentie": pd.array([pd.NA] * households, dtype="string"),
        "Startdatum": ["2024-01-01 00:00:00 UTC"] * households,
        "Einddatum": ["2025-01-01 00:00:00 UTC"] * households,
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
    for count, (household_id, file_name) in enumerate(household_id_pairs):
        if count >= households:
            break
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

    etdmap.options.project_mapping_csv_path = project_mapping_csv_path
    metadata_df = read_metadata(Path(supplier_metadata_xlsx_file))
    index_helpers.add_supplier_metadata_to_index(
        index_df, metadata_df, data_supplier="etdmap"
    )

    etdmap.options.bsv_metadata_file = reviewed_metadata
    return etdmap.options.mapped_folder_path
