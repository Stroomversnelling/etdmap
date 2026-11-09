# Writing a behavioural test

Everything a test needs to build already exists in `etdmap.testing`. Build it
from there. A test that assembles its own frames, tables or ids ends up
encoding a model of the data rather than exercising the one the library has.

This guide is for both `etdmap/tests` and `etdtransform/tests`. It is the only
copy; other documents link to it rather than restating it.

## The data model, in three sentences

Batches hold projects, which hold household datasets.

A **household dataset** (`ids.HOUSEHOLD_BATCH`) describes one household over one
period, scoped to its project and its batch. It is the primary key: of the
index, of the shard, of each mapping sidecar, and of every computation that runs
along one time series.

A **household** (`ids.HOUSEHOLD`) is the dwelling a dataset describes. It is an
attribute, not a key. Reach for it only to count households.

## What to call

| to build | call |
|---|---|
| several datasets' rows in one frame | `etdmap.testing.datasets_frame` |
| one dataset's rows, for a shard tree | `etdmap.testing.dataset_frame` |
| a frame shaped as a supplier delivered it | `dataset_frame(tz=None, project=None)` |
| any of the five mapping sidecars | `etdmap.testing.sidecar_frame` |
| a sidecar's column list | `etdmap.testing.sidecar_columns` |
| the per-column cadence table | `etdmap.testing.cadence_sidecar` |
| the index | `etdmap.testing.index_frame` |
| the synced HuisBatch table | `etdmap.testing.batch_table_frame` |
| the project mapping | `etdmap.testing.project_mapping_frame` |
| all three metadata files at once | `etdmap.testing.metadata_set` |
| supplier metadata, as a workbook read gives it | `etdmap.testing.synthetic_supplier_metadata` |
| a shard tree | `conftest.write_shard_tree` -> `mapping_helpers.save_household_shard` |
| a model the test declares | `synthetic_model` + `declare_synthetic_model` |
| one model row, or a cumulative/interval pair | `model_row`, `cumulative_pair` |
| a whole raw or mapped dataset | `generate_raw_dataset`, `generate_mapped_dataset` |

Read the builder's docstring before using it. Each one says what it produces
and which library function produces the same shape in production.

## The idioms

**Never write an id.** The builders assign them. Name the dataset you want and
the builder derives its household. Map each dataset to its columns and values:

```python
frame = datasets_frame({
    1001: {"ElektriciteitNetgebruikHoogDiff": [1.0, 2.0]},
    1002: {"ElektriciteitNetgebruikHoogDiff": [3.0, 4.0]},
})
```

There is a shorter form for one column -- `{dataset: [values]}` with the column
named as a keyword -- but the keyword then needs a placeholder value that the
per-dataset values replace, so prefer the mapping above unless every dataset
shares the keyword's value.

Two datasets of ONE household -- the case that tells a right key from a wrong
one -- is `household_of`:

```python
datasets_frame(
    {1001: {"ElektriciteitNetgebruikHoogDiff": [1.0]},
     2001: {"ElektriciteitNetgebruikHoogDiff": [2.0]}},
    household_of={2001: 1},
)
```

A frame of nothing but ids and `ReadingDate` has no values to count rows from,
so say how many: `datasets_frame({1001: {}}, n_rows=2)`.

A frame's two ids are never equal, so code reaching for the household where the
dataset was meant finds no rows instead of the right ones. Computing an id
(`household_id + 1000`) defeats that; `one_to_one_test_household_batch_of` and
`household_of_batch` are the conversion.

**Never restate a schema** when you are BUILDING data. Take the columns and
dtypes from the library's own map. `sidecar_frame` and `index_frame` do this for
you and refuse a column the artifact does not have, so a typo fails instead of
becoming an extra column that every reader ignores.

The exception is a test whose subject IS the schema -- one that pins the
columns an artifact must have on disk. There the expectation has to be stated
independently, because reading it from the same map the writer uses makes the
assertion a tautology that cannot fail. Write the column list out, and say in
one line that it is the on-disk contract.

**Build the whole shape.** The readers tolerate a missing column by design -- a
sidecar written before that column existed still reads -- so a partial table
answers "nothing recorded" rather than raising. That is how a test comes to
compare 24 missing values against 24 missing values and pass. Ask for a partial
shape explicitly (`sidecar_frame(..., partial=[...])`) when a missing column is
the test's subject.

One partial shape is legitimate and named: the cadence table as
`detect_column_cadence` returns it carries no `HuisBatchIdBSV`, because
`cadence_to_rows` adds the dataset afterwards -- and that function now raises
if handed a table that already carries one. Build it with `cadence_sidecar(...)`
and no `household_batch_id`, or, where each row states its own measurements,
with `sidecar_frame("cadence", rows, partial=sidecar_columns("cadence",
with_dataset=False))`.

**Decide what a completed pair holds.** An interval column's cumulative
counterpart is completed for you, because a frame holding half a pair is not a
household artifact and the stages refuse it. What it holds is a choice, and it
is load-bearing: the averaging engine takes a dataset's volume from the
cumulative column whenever that column holds any value. So the default
`pairs=PAIRS_RAMP` -- a usable total -- becomes the volume source in place of
the interval values the test stated, and the averages, the `n_delivered` counts
and the `source` label all come from the builder instead.

```python
datasets_frame({1001: [...]}, pairs=PAIRS_EMPTY, SomethingDiff=0.0)
```

`PAIRS_EMPTY` keeps the pair present and the interval column the source.
`pairs=False` omits the counterpart entirely. Supply your own cumulative values
and the builder leaves them alone.

**Nullable dtypes only.** `Float64`, `Int64`, `boolean`, `string`; the missing
value is `pd.NA`. Never `float64`, `int64`, `bool`, `np.nan` or `None` as a
sentinel -- a numpy dtype cannot hold `pd.NA`, so a test about missing data
built on one is testing numpy's semantics instead of the library's. Compare
with `pd.testing.assert_series_equal` or `.astype(float)`; `pytest.approx` does
not work through `==` on a `Float64` Series.

**Leave the timezone alone.** `ReadingDate` occurs both tz-aware and tz-naive
in production: `timestamp_helpers` parses supplier timestamps to UTC, the
coercion downstream does not always strip the zone, and the library is written
to tolerate either. So the shape a test builds decides which branch it
exercises. Pass `tz=None` for naive, leave the default for aware, and say which
stage the test stands at if it is not obvious. Never parse or shift a timestamp
in a test -- that is `timestamp_helpers` and `expand_tz_columns`.

**Declare the model you judge against.** `synthetic_model` +
`declare_synthetic_model` give a test its own variables and bounds, so it is
pinned to a behaviour rather than to this week's metadata. Monkeypatching
`thresholds_dict` for a real variable pins it to the data model instead, and
the test changes meaning when the model does.

**Reuse the loaders and writers.** Read an artifact through `etdmap.storage`
(`read_source_frame`, `mapped_household_paths`, `resolve_source_path`) and write
one through its own writer. A naive `pd.read_parquet` of a shard directory
raises, and a hand-rolled write produces a layout nothing else reads. Every
`pd.read_parquet` needs `dtype_backend="numpy_nullable"`.

**A local builder is legitimate only when the values or the shape are the
test's subject** -- a malformed frame a guard must reject, say. Then it says so
in one line of comment, and that line is the whole justification.

## A shared helper's shape is part of its contract

A fixture or helper in `conftest.py` is depended on by every test that calls
it, including for what it leaves OUT. `conftest.cadence_table` carries no
`n_readings`, and that absence is the contract:
`bounds.delivered_by_household_batch` answers "nothing is known about
delivery" for a table without the field and "delivered nothing" for one that
has it empty, and the two route down different paths. Filling the column in
looks like tidying and silently moves every caller.

So: completing a shared helper's shape is a change to every test that uses it.
Add the column at the call site that needs it, or give the helper an argument,
and run every file that calls it -- not just the one you are working in.

A test should also not depend on what another test leaves behind. If a file is
green on its own and fails in a full run, that is state leaking between tests;
the full run found a real defect and the fix belongs in whichever test mutates
shared state without restoring it.

## Comments

A comment says what the code does and what constraint it is under. It does not
record history, a decision, a date or an issue number, and it never explains a
change. "Removing X" needs no comment at all.

## Running the suite

Activate the repo's own `.venv` and install before running, never with
`pip install -e`:

```bash
cd etdmap       && ./.venv/Scripts/python.exe -m pip install . && \
                   ./.venv/Scripts/python.exe -m pytest tests -q
cd etdtransform && ./.venv/Scripts/python.exe -m pip install . && \
                   ./.venv/Scripts/python.exe -m pytest tests --full-suite -q
```

Select tests positively: the default suite, `--full-suite`, or a named file or
class. Never deselect with a negating `-m` or `-k`, and never skip a test to
make a run green.
