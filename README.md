# About etdmap

This package provides the required data model and helpers to map raw data to the `Energietransitie Dataset` (ETD). The ETD is a model defining important variables for energy in the built environment, which are used to inform policy and planning decisions in the Netherlands. For an overview of the ETD and all documentation, see <a href="https://energietransitiedataset.nl/">https://energietransitiedataset.nl/</a>.

# License

The package may only be used for open processing. You agree to publicly share the intended application, obtained insights, and applied calculation methods under the same license.

# Citation

If you use this package or any code in this package or refer to output, please use the following citations for attribution:

_Witkamp, Dickinson, Izeboud (2024). etdmap: A Python package for mapping raw data to the Energietransitie Dataset (ETD) model._


# The data model

The dataset includes two primary types of variables:  

1. **Metadata Variables**: Descriptive information about the project or house, such as identifiers, physical attributes, or installed HVAC systems.  
2. **Performance Variables**: Time-series data describing the energy performance or environmental conditions.

## Examples of Variables  

| **Category**     | **Variable Name**             | **Type**   | **Description**                                              |
| ---------------- | ----------------------------- | ---------- | ------------------------------------------------------------ |
| **Metadata**     | `ProjectIdLeverancier`        | String     | Unique identifier for the project assigned by the data provider. |
|                  | `Bouwjaar`                    | Integer    | Construction year of the house.                              |
|                  | `WarmteopwekkerCategorie`     | String     | Type of heating system (e.g., hybrid heat pump).             |
|                  | `Oppervlakte`                 | Integer    | Surface area in square meters.                               |
| **Performance**  | `ReadingDate`                 | Date       | Timestamp of the reading (Format: YYYY-MM-DD HH:MM:SS).      |
|                  | `ElektriciteitNetgebruikHoog` | Number     | Electricity consumption at high tariff (kWh, cumulative).    |
|                  | `TemperatuurWoonkamer`        | Number     | Current living room temperature in degrees Celsius.          |
|                  | `Zon-opwekTotaal`             | Number     | Total solar energy generated (kWh, cumulative).              |

This categorization ensures the dataset is comprehensive for energy-related policy and planning while maintaining clarity for data contributors and users. Future data models are likely to add further category breakdowns, perhaps including district heating, weather data and other categories and data types.

See the section on data processing and mapping to learn more about how to access this metadata in a python script.

There are two levels of data ultimately used in analysis:

- data for a connected unit in the built environment, such as a household or, perhaps in the future, a charging point, and
- project level data, an aggregated collection of network connected units with similar characteristics.

Each data set is provided under a "batch" to be able to differentiate different types of datasets and also to be able to submit data at different times from the same source and about the same unit, e.g. households. These datasets may or may not have overlapping data ranges and may share some or all columns. Within each "batch", there will be one or more unique datasets, e.g. one per household. The household batch table contains one record per unit/household in each batch. 

# Installation and quick start

_Note: If only using ETD datasets for analysis, one would use the `etdanalyze` and `etdtranform` packages and install their requirements, which will automatically install `etdmap`._

To use this package on its own and map raw data in a workflow, you can install it with pip:

```bash
git clone https://github.com/Stroomversnelling/etdmap.git
cd etdmap
pip install .
```

# Configuration

In some cases, you may be using this package on its own and may have to configure some options:

```python
import etdmap

# Required:
etdmap.options.mapped_folder_path = 'mapped_folder_path'  # folder where mapped files are stored
etdmap.options.bsv_metadata_file = 'filepath_of_bsv_metadata'  # combined BSV/ETD metadata file (not supplier metadata)
etdmap.options.aggregate_folder_path = 'aggregate_folder_path'  # folder where aggregated files are stored

# Enables the household-batch table and fields - in the future this will be required due to the new data model - it is optional at the moment to still allow legacy datasets :
etdmap.options.household_batch_csv_path = 'filepath_of_household_batch_csv'  # synced household-batch table
```

The first time raw files are mapped, a new index is generated (or an existing one is updated) and every household receives an auto-generated  id, e.g. `HuisIdBSV`. The BSV metadata file links those ids to additional metadata, such as the `ProjectIdBSV` that assigns households to projects and the `Meenemen` boolean that marks whether a delivered batch of data from a particular household/unit should be included in analysis (`True`) or excluded (`False`). `Meenemen` is a property of the household batch, not the household (see "Households, batches, and the index" below). It starts **empty** when a household is first mapped: a researcher reviews the mapped data afterwards and records the decision in the project's metadata administration, from where it reaches the metadata file at the next sync. Aggregation steps only include data with `Meenemen = True`, so an unreviewed delivery is never silently analysed. Some reasons to exclude a household may be:

- By request from the data supplier
- Due to missing data that could skew analyses or is insufficient
- Misbehaving devices and poor quality data

# Developing and contributing

If you would like to contribute to the package code, create an environment and
install the package into it:

```bash	
git clone https://github.com/Stroomversnelling/etdmap.git
cd etdmap
python -m venv .venv
source .venv/bin/activate  # On Windows use `.venv\Scripts\activate`
pip install .
```

Install normally -- never `pip install -e`. Editable installs are not used
anywhere in this project. Each environment holds its own private copy of the
package, which is what allows different folders to run different development
versions at the same time. That is the intended arrangement, not a workaround:
after changing the source, reinstall into whichever environment should pick the
change up, and leave the others on the version they were on.

## Running the tests

Run everything from the repo root, using this repo's own virtual environment.
On Linux/macOS use `.venv/bin/python` in place of `.venv/Scripts/python`.

```bash
# The whole suite
.venv/Scripts/python -m pytest

# A single file
.venv/Scripts/python -m pytest tests/test_mapping_helpers.py

# A single class, or a single test
.venv/Scripts/python -m pytest tests/test_mapping_helpers.py::TestRunStandardPipeline
.venv/Scripts/python -m pytest tests/test_mapping_helpers.py::TestRunStandardPipeline::test_output_has_model_columns

# Everything whose name matches an expression, across all files
.venv/Scripts/python -m pytest -k "interval and not cumulative"
```

The suite reads `config_test.yaml` from the repo root. Copy
`config_test_template.yaml` and fill in the local fixture paths before the
first run. If it is missing, `tests/conftest.py` raises
`FileNotFoundError: no file named 'config_test.yaml'` while pytest is
collecting, so the run stops immediately instead of quietly skipping the tests
that needed it (ADR-003).

### Narrowing a run is fine; skipping is not

Selecting a subset of tests -- by path, by node ID, by `-k`, or by marker
deselection (`-m "not slow"`) -- is normal and encouraged while developing.
Those tests are simply not selected for this run.

Skipping is a different mechanism and is not allowed. A skipped test was
selected, then declined to run, and pytest reports it in a way that reads as
green. `tests/conftest.py` contains a `pytest_runtest_makereport` hook that
turns **any** skip -- `pytest.skip()`, `skip`/`skipif` markers,
`importorskip` -- into a hard failure:

```
SKIP NOT ALLOWED (ADR-007).
  reason given: ...
  skip class  : ...
  allowed     : (none -- the allowlist is empty)
```

`ALLOWED_SKIP_CLASSES` in that file is an allowlist of skip classes, keyed on a
`"<TOKEN>: "` prefix in the skip reason. **It is empty on purpose** -- every
skip examined in this code base turned out to be a missing prerequisite that
should have failed loudly. If a skip is ever genuinely warranted, its class is
added to the allowlist in review, never in the moment.

So: if a test exists, make the prerequisite available, or let it fail so the
gap is visible. Do not reach for `pytest.skip`.

### Working quickly

Most of the wall time is a session-scoped fixture in `tests/conftest.py` that
generates the synthetic dataset and runs the mapping pipeline once. That cost
is paid the first time any test needing it runs, so narrowing to one such file
saves less than you would expect - while narrowing to a file that does not use
the fixture is near-instant. `--durations` tells you which case you are in.

| Flag | What it does |
|---|---|
| `-x` | Stop at the first failure |
| `--lf` | Re-run only what failed last time |
| `--ff` | Run last-failed first, then everything else |
| `-q` | One character per test instead of one line |
| `-k EXPR` | Select by name; supports `and`, `or`, `not` |
| `--durations=10` | Report the 10 slowest tests |
| `-p no:cacheprovider` | Do not write a `.pytest_cache` directory |

A good loop is `--lf -x` until green, then the whole file, then the suite.

### When to run what

Per ADR-006 and ADR-007 in `DECISIONS.md`:

- **Record a baseline before you start.** Run the suite before your first edit
  so you know what was already green. Anything that fails afterwards is a
  regression from your change until proven otherwise.
- **Run the whole suite before any commit that leaves your machine** - a push,
  a pull request, a release, or any change to the data model.
- **During a development iteration**, small local commits may run only the
  focused tests for the area you are touching. Run everything again before you
  put the change down.
- **Never leave an unexpected failure uninvestigated.** Every red test is
  either a bug in the code, a stale fixture, or a wrong test -- and you do not
  know which until you look. "Pre-existing" is not an explanation; it is a
  thing to check.
- **A red test may be legitimate, but only when it is known and understood.**
  A test written ahead of the feature it describes starts red by design. That
  is acceptable when the reason is understood and stated. What is never
  acceptable is a failure nobody has looked at, or making a suite look green by
  removing, skipping, or deselecting the test that exposed the gap.

Do not sit watching a long run. Start it, keep writing code, and come back to
it - block only when the next thing you want to do depends on the result, or
when something is genuinely broken.

Prefer accuracy over speed. Where the two conflict, slow down.

This package sits at the bottom of the dependency chain (ADR-004), so a green
suite here does not clear the packages built on it. They test against their own
installed copy of this package, so they need their own runs after it has been
reinstalled for them - at the usual points, not continuously.

The module-level structures in `data_model.py` (`cumulative_columns`,
`model_column_order`, and the rest) are built at import time from
`etdmodel.csv`. That file is generated by the data model sync tooling from the
upstream source of truth -- which is the sanctioned way the model changes.
Editing the model there and syncing it in *is* how these variables change;
there is no separate category of "metadata change" that leaves them untouched.

Because downstream code reads those structures directly, a change to them
carries additional risk, and per ADR-004 needs a regression test in
`tests/test_data_model.py` plus a check that dependent packages still behave.
That is one extra check at the points already defined above -- not a reason to
run everything all the time.

## Writing a new test

**[docs/WRITING_TESTS.md](docs/WRITING_TESTS.md) names the builder to call for
every artifact a test needs, and the idioms that go with them.** Read it first;
what follows is the file layout and the house style only.

Tests live in `tests/`, one file per module under test, named
`tests/test_<module>.py`. pytest collects any function named `test_*`; group
related cases in a `class Test<Thing>` when a file covers several behaviours.

```python
import pandas as pd
import pytest

from etdmap.mapping_helpers import add_diff_columns


def test_diff_is_the_change_since_the_previous_reading():
    """A cumulative meter column gains a Diff column holding per-interval change."""
    df = pd.DataFrame(
        {
            "ReadingDate": pd.date_range("2026-01-01", periods=4, freq="5min", tz="UTC"),
            "ElektriciteitNetgebruikHoog": pd.array([0.0, 1.5, 3.0, 4.5], dtype="Float64"),
        }
    )

    out = add_diff_columns(df, context="example")

    assert out["ElektriciteitNetgebruikHoogDiff"].dtype == "Float64"
    assert out["ElektriciteitNetgebruikHoogDiff"].tolist()[1:] == [1.5, 1.5, 1.5]
```

What the suite expects, per ADR-005 and ADR-015 in `DECISIONS.md`:

- Build fixture arrays with `pd.array([...], dtype="Float64")` (or `"Int64"`,
  `"boolean"`). Never `np.array(...)`, and never a bare Python list that pandas
  will infer into a numpy dtype.
- The missing-value sentinel is `pd.NA` - not `np.nan`, not `None`.
- Compare nullable Series with `pd.testing.assert_series_equal`, or cast with
  `.astype(float)` first. `pytest.approx` does not work against a `Float64`
  Series via `==`.
- Write files only under `tmp_path` (a pytest built-in fixture). Never write
  into the repo, and never point a test at production data.

The frame above is handmade because it is three columns wide. Anything that
carries ids, a schema or a whole artifact comes from a builder - see
[docs/WRITING_TESTS.md](docs/WRITING_TESTS.md), which also covers why the
timezone of `ReadingDate` is a choice a test has to make rather than a default.

Write the docstring as the behaviour being locked in, not as a description of
the code. When a test fails a year from now, that sentence is what tells the
next person whether the test or the code is wrong.

Prefer parametrising over copy-pasting near-identical cases:

```python
@pytest.mark.parametrize("supplied,expected", [(0.0, True), (-1.0, False)])
def test_threshold(supplied, expected):
    ...
```

When a test needs the full synthetic dataset rather than a handmade frame,
request the existing session fixtures from `tests/conftest.py` instead of
building your own - they are seeded and deterministic, and the rest of the
suite has already paid for them.

## Test fixtures

Tests build the data they assert on. Nothing is committed under `tests/data/`
and no test compares output against a stored copy of a previous run.

`etdmap.testing` holds a builder per artifact.
[docs/WRITING_TESTS.md](docs/WRITING_TESTS.md) lists them with the artifact each
one produces; this section covers only the whole-dataset generators.

### A mapped dataset

`etdmap.testing.generate_mapped_dataset` writes a mapped dataset, generated per
session into a temporary folder. Raw supplier-shaped input is generated first
and then mapped through `run_standard_pipeline`, so the fixture is the output of
the production mapping path and lands in whichever layout
`mapped_output_format` names -- the sharded one by default. Read it with
`etdmap.storage.mapped_household_paths` rather than globbing for a filename.

The data is deterministic: a per-column seed derived from the household and the
column name, so values do not move when columns are reordered.

```python
from etdmap.testing import generate_mapped_dataset

generate_mapped_dataset(
    target_folder,
    bsv_metadata_file=...,
    households_per_project=10,   # defaults, see etdmap.testing
    projects=2,
    days=3,
)
```

The dataset is clean: every cumulative column the model declares is present,
non-decreasing from zero, on an unbroken five-minute grid, every value inside
the bound the model declares for it. One column of one household carries a gap
(`GAP_HOUSEHOLD`, `GAP_COLUMN`, `GAP_SLOTS`).

The columns in `INFREQUENT_DEVICE_COLUMNS` deliver a few unevenly spaced
readings and are empty between them, as a supplier delivers a device that
reports when it runs. No cadence can be measured off them, so the mapping's
`fill_down_infrequent_devices` holds each reading until the next one arrives.
Every other column reports on every row.

The shape decides which behaviours a run can reach. A project below
`etdtransform.presence.MIN_ELIGIBLE_HOUSEHOLDS` publishes no average; a span
shorter than an interval gives that interval no slot. State the shape a test
needs rather than taking the default. At the default span the dataset does not
satisfy the checks written against a production year of data -- see
`tests/test_index_verdicts_from_a_run.py` for which hold and which do not.

Three metadata files are generated alongside, for the households of that run:
the reviewed BSV metadata, the project mapping, and the household-batch sync
CSV, all under `<target>/_generated_metadata/`. Passing
`supplier_metadata_xlsx_file` or `project_mapping_csv_path` uses that file
instead of generating one.

### A model a test declares

`declare_synthetic_model` installs a model for the duration of a test, for
columns and bounds the shipped model need not carry:

```python
from etdmap.testing import declare_synthetic_model, synthetic_model

with declare_synthetic_model(synthetic_model(maximum=0.5)) as model:
    ...  # model.device, model.device_complement, model.high, model.low
```

The declaration is merged into the model tables and every derived structure is
rebuilt in place, so a module that imported one by name sees it. The shipped
model is restored on exit.

`etdmap.data_model.rebuild_model(extra_model_rows=..., extra_threshold_rows=...)`
is the same mechanism for rows a test writes itself -- one field of one variable,
where `synthetic_model` declares a whole device. Everything is derived before
anything is written, and a failure in the phase that cannot be derived ahead is
followed by a rebuild from the shipped tables, so a declaration that will not
load leaves the model loadable.

### Other builders

- `model_row(variable, **fields)` -- one row of etdmodel.csv, with the
  classifiers defaulted, so a test states only the field it is about.
- `cumulative_pair(variable, **fields)` -- that row, its interval column and
  both threshold rows. The model refuses a cumulative variable missing either,
  so declaring one means declaring all four.
- `cadence_sidecar(columns, minutes=..., ...)` -- the per-column cadence table
  `detect_column_cadence` produces, for a test stating a cadence rather than
  implying one through timestamps. `CADENCE_DTYPES` is its schema.
- `generate_raw_dataset` -- supplier-shaped input, if a test needs the stage
  before mapping.

# Overview

`etdmap` is a package that provides data mapping functionalities for energy-related datasets. It includes functions to map and transform data according to specific schemas, ensuring consistency across different sources. It also includes some utility variables like `cumulative_columns`, which can be used.

The process of mapping raw data files to the ETD model involves several key functions. Here's an overview of the main functions used in this process.

## Data model and column definitions and thresholds

_Note: This API is relatively stable but subject to change as it is still under development. Please follow our repository or, if necessary, use a tagged releases to ensure stability._

From `data_model.py`:

1. `load_etdmodel()`: 
   - **Purpose**: Loads the ETD model specification.
   - **Description**: Reads a CSV file into a Pandas dataframe that defines the structure and requirements of the ETD model.

2. `load_thresholds()`: 
   - **Purpose**: Loads predefined thresholds for data validation. These include thresholds for cumulative (annual) values as well as those for 5 minute intervals and instantaneous measurements.
   - **Description**: Reads a CSV file into a Pandas dataframe containing threshold values for various variables used in data validation.


```python
from etdmap.record_validators import load_thresholds
thresholds_df = load_thresholds()
```

### Derived variables in data_model.py

`data_model.py` exposes several module-level variables derived from `etdmodel.csv` at
import time. Each is scoped to a specific subset of the model — see `DECISIONS.md` for
the rationale.

| Variable | Entity scope | Purpose |
|---|---|---|
| `cumulative_columns` | any `Entiteit`, via `Cumulatief=="ja"` and `Type variabele != "date"` | Columns representing cumulative meter readings. Used to compute Diff columns during mapping. |
| `model_column_order` | `Prestatiedata` only, sorted by `Volgorde` | Canonical column order for provider-supplied raw files. Use this to validate and reorder incoming mapped parquet files. |
| `model_column_type` | `Prestatiedata` only | Pandas dtype for each provider-supplied column. |
| `data_analysis_columns` | `Prestatiedata` only (currently alias of `model_column_order`) | Columns validated by dataset validators. Will narrow to `Vereist=="ja"` when that column is fully populated. |
| `allowed_supplier_metadata_columns` | `Metadata` where `Wie vult?=="Dataleverancier"` | Metadata columns suppliers are permitted to include in their files. |
| `all_performance_data_columns` | `Prestatiedata` **and** `PrestatiedataBerekend` | Every column that may be present at any point in the pipeline, however it got there. Use this — not `model_column_order` — when defining derivation targets. |
| `required_performance_data_columns` | Subset of above, `Vereist=="ja"` | Columns that must be present at end of pipeline. Missing any is a data quality error. |

```python
from etdmap.data_model import (
    cumulative_columns,
    model_column_order,
    model_column_type,
    all_performance_data_columns,
    required_performance_data_columns,
)
```

The catalog module exposes the Rule.csv vocabulary for cross-checks:

```python
from etdmap.catalog import (
    load_catalog,
    load_rules,
    rule_lhs_variables,   # set of all derivation target names
    rule_rhs_variables,   # set of all input variable names used in rules
    rule_all_variables,   # union of the above
)
```

## Managing the mapped data files

`index_helpers.py` contains the functions used to manage metadata and add files to the mapped data file index. It also has functions for managing household, project metadata, and BSV (aka ETD) metadata.

From `index_helpers.py`:

1. `read_index()`: 
   - **Purpose**: Reads the current index file.
   - **Description**: Loads the existing index from a parquet file or creates a new one if it doesn't exist.

2. `get_household_id_pairs()`: 
   - **Purpose**: Generates pairs of HuisIdBSV and filenames in order to maintain unique ids in the dataset.
   - **Description**: Creates a list of tuples containing HuisIdBSV and corresponding filenames for new and existing entries.

3. `update_index()`: 
   - **Purpose**: Updates the index with new entries.
   - **Description**: Adds new entries to the index and recalculates or adds flag columns for dataset validators.

4. `add_supplier_metadata_to_index()`: 
   - **Purpose**: Adds metadata columns to the index.
   - **Description**: Updates the index with additional metadata from the supplier, matching on the HuisIdLeverancier column.

## Households, batches, and the index

Batches hold projects, which hold household datasets.

A **batch** is one delivery of data, covering a real period (`Startdatum` to
`Einddatum`). A **household dataset** (`HuisBatch`) describes one household
over that period, scoped to its project and its batch. The same household is
delivered more than once over time -- a second year of data, a re-delivery at a
different measurement frequency, a period after a renovation -- and each of
those is its own dataset with its own period, frequency
(`Gegevensfrequentie`) and quality review.

**The household dataset is the primary key**, of the index and of every mapped
artifact: the shard, the rows in each mapping sidecar, and every computation
that runs along one time series. The household is an *attribute* of a dataset,
not a key; reach for it to count households or to join household-level
metadata, and nowhere else.

### Naming the id columns in code

`etdmap.ids` holds the column names, so code names the id it means rather than
repeating a string, and a rename in the data model is one edit:

```python
from etdmap import ids

df.groupby(ids.DATASET_AXIS)              # one running total per time series
df[ids.ACCOUNTING_AXIS].nunique()         # how many households
```

`ids.DATASET_AXIS` and `ids.ACCOUNTING_AXIS` carry the same strings as
`ids.HOUSEHOLD_BATCH` and `ids.HOUSEHOLD`; writing the axis says which question
the call is asking, so "is this keyed on the right thing?" is a grep rather
than a judgement per site. `ids.DTYPES` gives each one its nullable dtype.

**This section is the only place the raw column names are written.** Elsewhere
-- code, comments, docstrings, messages -- use the `etdmap.ids` names. A
comment that names a raw column, or that says what a column used to key, is a
defect.

The index is a single file, `index.parquet`, written by
`save_index_to_parquet()`. It holds one row per household batch
(`HuisBatchIdBSV`), carrying the household columns (`HuisIdBSV`,
`HuisIdLeverancier`, `ProjectIdBSV`, `Meenemen`, validation flags, ...) plus
the batch fields: `BatchIdBSV`, `Gegevensfrequentie`, `Leverancierfrequentie`,
`Startdatum` and `Einddatum`.

Ids are always assigned locally by this library, not by external tooling except in exceptional carefully checked fashion.
The batch fields come from a synced household-batch table (a CSV whose path
is configured as `etdmap.options.household_batch_csv_path`); households that do
not have a row in that table yet are *pending*: they stay in the index with
empty batch fields, and a paste-ready `pending_household_batch_additions.csv`
is written to help add them.

In the data model households are not unique across batches, and `Meenemen`
and the other batch fields belong to the household batch, not the household.
`HuisBatchIdBSV` is therefore the key of a dataset, and `HuisIdBSV` is not.

Two things are still catching up with that. The mapping numbers each household
batch with its household's id (`save_mapped_household`), so it cannot yet write
a household's second delivery. And the code paths that can only answer for one
-- a single file for a household, a single index row -- raise
`HouseBatchOverlapError` when they meet a household with several, rather than
mixing data from different deliveries. Read a household's data with
`mapped_household_files`, which answers per household batch.

## Storage layouts

Mapped household data is stored in one of two layouts:

- **flat (deprecated)**: one parquet file per household,
  `household_<HuisIdBSV>_table.parquet`. It has no household-batch level, so
  it cannot represent a household delivered more than once; reads of it
  report `HuisBatchIdBSV` as the household id.
- **sharded** (the default for new output): a partitioned folder tree in
  which the directory names carry the ids --
  `sharded/HuisIdBSV=<n>/HuisBatchIdBSV=<p>/part.parquet`. The ids live in
  the folder names, not inside the files. This layout keeps individual files
  small as the dataset grows, allows households to be written and read
  independently, and gives each household batch its own file.

Reading code never needs to know the layout: the helpers in `etdmap.storage`
detect it (`detect_mapped_format`, `read_mapped_households`,
`mapped_household_files`) and work on both. Writing flat output remains
possible as an explicit per-run choice via the mapper scripts.

## Mapping raw data to the model specification

From `mapping_helpers.py`:

1. `rearrange_model_columns()`: 
   - **Purpose**: Ensures all required columns are present and in the correct order.
   - **Description**: Adds missing columns, fills them with NA values, and arranges columns according to the model specification.

2. `add_diff_columns()`: 
   - **Purpose**: Calculates difference columns for cumulative variables.
   - **Description**: Computes the difference between consecutive readings for cumulative variables and adds these as new columns.

3. `ensure_intervals()`: 
   - **Purpose**: Ensures consistent time intervals in the data. Defaults are `date_column = 'ReadingDate', freq='5min'`
   - **Description**: Adds missing timestamps to ensure a consistent time series, typically with 5-minute or 15-minute intervals.

4. `fill_down_infrequent_devices()`: 
   - **Purpose**: Fills gaps in infrequently updated device data. At the moment, this defaults to: 'ElektriciteitsgebruikBoilervat', 'ElektriciteitsgebruikRadiator', 'ElektriciteitsgebruikBooster'. This can be changed based on data source requirements.
   - **Description**: For certain columns that update infrequently, this function fills down the last known value.

5. `validate_cumulative_variables()`:
   - **Purpose**: Validates cumulative variables in a DataFrame for various quality checks.
   - **Description**:

   - *Time gap check*: Ensures there are no gaps in readings greater than a specified time delta (default is 1 hour).
   - *Negative difference check*: Verifies that cumulative values are not decreasing over time.
   - *Unexpected zero check*: Identifies and flags unexpected zero values in cumulative readings.
   - *Data availability check*: Ensures that at least a specified percentage of values (default 90%) are not NA.

## Alignment of clocks from different devices and merging data

`snap_readings_to_grid` in `mapping_helpers.py` aligns clocks from multiple
devices and handles readings that arrive at different offsets within the same
grid period. It takes the grid to snap onto, which is declared per household
batch as `Gegevensfrequentie`. Call `ensure_intervals` afterwards to pad any
grid slot that received no reading.

## Validators

There are various validators available. There are two major categories:

1. **Dataset validators**: These are used to validate the entire dataset, including checks on cumulative columns and differences between consecutive records.
2. **Record validators**: These are used to validate individual records, including checks on instantaneous measurements and 5-minute intervals.

# Guidance for suppliers of data and for data preparation

## Prepping raw data

Use the `Vereist` (`required`) column in the data model in order to check if your metadata and your raw data from device readings are complete and correct. These are the currently required columns (as of 20 Feb 2025):

| Entiteit      | Variabele                       | Key         | Definitie                                                                                                                          | Type variabele | Vereist     | Resolutie    | Wie vult?       | Bron                                  | AVG gevoelig |
| ------------- | ------------------------------- | ----------- | ---------------------------------------------------------------------------------------------------------------------------------- | -------------- | ----------- | ------------ | --------------- | ------------------------------------- | ------------ |
| Metadata      | ProjectIdLeverancier            | nee         | code toegekend door dataleverancier                                                                                                | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | ja           |
| Metadata      | HuisIdLeverancier               | ja          | code toegekend door dataleverancier                                                                                                | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | ja           |
| Metadata      | Weerstation                     | nee         | dichtstbijzijnde KNMI weerstation                                                                                                  | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | nee          |
| Metadata      | Oppervlakte                     | nee         | Ag, oftewel m2 gebruiksoppervlak                                                                                                   | integer        | ja          | vaste waarde | Dataleverancier | BAG / EP-online / Energielabel / Vabi | nee          |
| Metadata      | Compactheid                     | nee         | Als/Ag, oftewel m2 verliesoppervlak / m2 gebruiksoppervlak                                                                         | number         | ja          | vaste waarde | Dataleverancier | EP-online / Energielabel              | nee          |
| Metadata      | Warmtebehoefte                  | nee         | kWh_th/m2/jr, volgens NTA8800                                                                                                      | number         | ja          | vaste waarde | Dataleverancier | EP-online / Energielabel              | nee          |
| Metadata      | PrimairFossielGebruik           | nee         | kWh/m2/jr, volgens NTA8800                                                                                                         | number         | ja          | vaste waarde | Dataleverancier | EP-online / Energielabel              | nee          |
| Metadata      | Bouwjaar                        | nee         | Oorspronkelijk bouwjaar                                                                                                            | integer        | ja          | vaste waarde | Dataleverancier | BAG/Vabi                              | nee          |
| Metadata      | Renovatiejaar                   | nee         | Jaar van renovatie                                                                                                                 | integer        | ja          | vaste waarde | Dataleverancier | Vabi                                  | nee          |
| Metadata      | WoningType                      | nee         | Wat voor woningbouw is het? Kiezen uit: EGW, MGW                                                                                   | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | nee          |
| Metadata      | WarmteopwekkerCategorie         | nee         | Hoe wordt verwarmd? Kiezen uit: Hybride warmtepomp, Lucht-water warmtepomp, Water-water warmtepomp, Lucht-lucht warmtepomp, Anders | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | nee          |
| Metadata      | Warmteopwekker                  | nee         | Hoe wordt verwarmd? Kiezen uit: Collectief, Individueel                                                                            | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | nee          |
| Metadata      | Ventilatiesysteem               | nee         | Hoe wordt geventileerd? Kiezen uit: Natuurlijke ventilatie, Type C, Type D, Type E                                                 | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | nee          |
| Metadata      | Kookinstallatie                 | nee         | Hoe wordt gekookt? Kiezen uit: Gas, Inductie, Anders                                                                               | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | nee          |
| Metadata      | EPV                             | nee         | Wordt EPV geïnd? Kiezen uit: EPV 1.0, EPV 2.0 Basis, EPV 2.0 Hoogwaardig, Nee, Niet bekend                                         | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | nee          |
| Prestatiedata | HuisIdLeverancier               | ja          | code toegekend door dataleverancier                                                                                                | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | ja           |
| Prestatiedata | ReadingDate                     | nee         | Format: YYYY-MM-DD HH:MM:SS                                                                                                        | date           | ja          | 5 minuten    | Dataleverancier | Gateway / Portal                      | ja           |
| Prestatiedata | ElektriciteitNetgebruikHoog     | nee         | kWh (cumulatief)                                                                                                                   | number         | ja          | 5 minuten    | Dataleverancier | Slimme meter                          | ja           |
| Prestatiedata | ElektriciteitNetgebruikLaag     | nee         | kWh (cumulatief)                                                                                                                   | number         | ja          | 5 minuten    | Dataleverancier | Slimme meter                          | ja           |
| Prestatiedata | ElektriciteitTerugleveringHoog  | nee         | kWh (cumulatief)                                                                                                                   | number         | ja          | 5 minuten    | Dataleverancier | Slimme meter                          | ja           |
| Prestatiedata | ElektriciteitTerugleveringLaag  | nee         | kWh (cumulatief)                                                                                                                   | number         | ja          | 5 minuten    | Dataleverancier | Slimme meter                          | ja           |
| Prestatiedata | ElektriciteitsgebruikWarmtepomp | nee         | kWh (cumulatief)                                                                                                                   | number         | ja          | 5 minuten    | Dataleverancier | Gateway / Portal                      | ja           |
| Prestatiedata | Zon-opwekTotaal                 | nee         | kWh (cumulatief)                                                                                                                   | number         | ja          | 5 minuten    | Dataleverancier | Gateway / Portal                      | ja           |

## Validation of columns

One should check the stats for each raw data column using the `collect_column_stats()` function to ensure that the data meets expected statistical properties. The statistics include:

   - 'Identifier': The identifier for the dataset.
   - 'column': The name of the column.
   - 'type': The data type of the column.
   - 'count': The number of non-null values in the column.
   - 'missing': The number of missing values in the column.
   - 'min': The minimum value in the column, if applicable.
   - 'max': The maximum value in the column, if applicable.
   - 'mean': The mean value in the column, if applicable.
   - 'median': The median value in the column, if applicable.
   - 'iqr': The interquartile range (IQR) of the column, if applicable.
   - 'quantile_25': The 25th percentile value in the column, if applicable.
   - 'quantile_75': The 75th percentile value in the column, if applicable.
   - 'top5': A dictionary with the top 5 most frequent values and their counts, if applicable.

This helps to validate whether or not a particular household/unit has sufficient data of sufficient quality to include in the mapped datasets.

## Validation of cumulative columns

The `etdmap` package provides helpers in `mapping_helpers.py` to enable the processing and validation of columns. 
Cumulative columns are processsed and also validated using the `add_diff_columns()` function. It produces a `diff`, a variable with the incremental increase in the cumulative variable and expects cumulative variables to monotonically increase. In other words, they should never decrease. This assumption might be incorrect in cases where meter readings naturally decrease and in those cases, custom logic may need to be written.

By default, `add_diff_columns()` relies on the function `validate_cumulative_variables()` to ensure that cumulative variables meet specific conditions:
- 90% available data
- Max gap of 1 hour of consecutive missing data
- No "unexpected" zero values in cumulative data
- No decreasing cumulative values

The `validate_cumulative_variables` function checks for logical consistency in cumulative variables but may not cover all edge cases or handle complex data scenarios effectively. To customize this validation behaviour, one may write a custom wrapper function or new function with different checks and pass it to `add_diff_columns()` using the `validate_func` parameter. If this validation is not correctly implemented, problems may not be reported. If `drop_unvalidated` is `True` then function may drop valid data or keep invalid data. By default, `drop_unvalidated` is `False`: data which does not pass these tests is kept but problems are reported in the log. 

_It is important to manually check the logs when adding new datasets. In addition, one should check the stats for each mapped column using the `get_data_stats()` function to ensure that the data meets the expected statistical properties. Pass `raw_data_folder_path=<path>` to inspect a folder of raw parquets instead of mapped households._

## Filling data values for devices which report infrequently

Sometimes devices are simply not reporting because they are inactive.  In order to ensure the column is not seen as mostly 'missing' data and rather as an unchanging value, the `fill_down_infrequent_devices()` function uses forward fill followed by backward fill to impute missing values for specified columns. 

_This approach may not be suitable if devices if there are underlying issues with the device data, which would be amplified or ignored, especially if the data source is misbehaving. We recommend visually inspecting these columns / data before applying the function._

## Time Series Consistency

Each household batch declares the interval its data is recorded on, as
`Gegevensfrequentie` in the HuisBatch table. Read it with
`grid_minutes_for_household()`, which raises rather than assuming an interval
when the batch declares none. `snap_readings_to_grid()` puts readings onto that
grid; a data source delivering at irregular offsets is handled there rather than
by custom logic per source.

Once the data is on the declared grid, the `ensure_intervals()` function
attempts to ensure a consistent time series by adding missing intervals and
removing excess records for the same moment. This process can lead to incorrect
data if the raw data does not naturally fit into the expected frequency.

# Specific function notes

## `add_diff_columns()`

1. **Negative differences**:
   - Assumes that negative differences in cumulative columns are erroneous and attempts to correct them by setting values to `pd.NA`. This assumption might not hold true if the meter readings naturally decrease or if there are legitimate zero jumps.
   
2. **Error handling**:
   - Logs errors when it encounters negative differences after corrections but does not address underlying issues in the data collection process.

3. **Handling of large gaps without rates of change**:
   - Does not consider the time elapsed between readings, which can lead to incorrect assumptions about the nature of the data.
   - When there are larger gaps in the `diff` data (missing intermediate readings), the function may incorrectly assume that the next available reading is a continuation of the meter reading when perhaps a meter reset has taken place and no cumulative values have been collected for some time (bringing the reading back to the original reading level). If there is a suspicion of these errors, custom logic will need to be added and some assumption about the rate of change in the cumulative variable.

4. **Known edge cases**:
   - Edge cases where multiple negative dips occur consecutively or where there is a significant pause in data collection are not fully addressed.

## `fill_down_infrequent_devices()`

1. **Imputation Strategy**:
   - Uses forward fill followed by backward fill to impute missing values, which can lead to inaccuracies if the device reports irregularly or if there are underlying issues with data collection.
   
2. **Assumption of continuity**:
   - Assumes that missing values should be filled based on the last available reading, which might not be appropriate for devices that report infrequently or have non-continuous usage patterns.

3. **Final filling with zeroes**:
   - Fills column with zeros if all values are NA. This assumption may not be correct if the device simply was not reporting.

4. **Column specificity**:
   - Processes specified variables by default: `ElektriciteitsgebruikBoilervat`, `ElektriciteitsgebruikRadiator`, `ElektriciteitsgebruikBooster`. Can handle other columns if passed as an argument. Some datasets may require different processing.

## `ensure_intervals()`

1. **Frequency assumption**:
   - Assumes a fixed frequency for the time series data (e.g., 5-minute intervals) and the only check it does is for the number of expected records. If the raw data does not naturally fit into this frequency the errors may occur as additional pre-processing is required.
   
2. **Data source issues**:
   - Does not handle cases where the data source is misbehaving, such as reporting data at irregular intervals or with incorrect timestamps.

3. **Adding vs. removing records**:
   - Adds missing records to ensure consistent intervals but does not verify the correctness of these retained records where removed. It will report inconsistency with respect to the number of records expected.
   
4. **Excess records**:
   - Attempts to reduce excess records by performing a left merge, which might lead to data loss if the excess records contain valid information.

## `rearrange_model_columns()`

1. **Column order**:
   - Ensures that the DataFrame columns are in a specific order with additional columns left at the end/right of the dataset.





