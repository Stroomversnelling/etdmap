"""The identifier columns of the data model, and what each one identifies.

One place the names are written. A call site then reads the id it means instead
of a string literal, a rename in the data model is one edit here, and a
mechanical pass can rewrite literals into these names.

Imports nothing from the rest of the package beyond pandas, so any module may
use it.

The data model
--------------
Batches hold projects, which hold household datasets. A **household dataset**
(:data:`HOUSEHOLD_BATCH`) describes one household over a particular period,
scoped to its project and its batch. It is the primary key of the index and of
every mapped artifact: the shard, the rows in every mapping sidecar, and every
computation that runs along one time series.

A **household** (:data:`HOUSEHOLD`) is the dwelling a dataset describes. It is
an ATTRIBUTE of a dataset, not a key: one household has as many datasets as it
has periods.

**The household batch is the unit of analysis; the household is not a unit of
anything.** Every count, grouping, statistic, average and eligibility floor
keys on the household batch -- a household delivered as two datasets
contributes two series, and counting the dwelling would count it once. There is
no count that keys on the household.

Four things name it, all about attribution or storage rather than analysis: the
index retains it so a dataset can be attributed to a dwelling, the guards whose
subject is a household having more than one dataset, the shard path and reader
which carry both levels, and the index entry a mapping run returns.

Writing the axis rather than the column
---------------------------------------
:data:`DATASET_AXIS` holds the same string as :data:`HOUSEHOLD_BATCH` and
exists so a call site can say that it is working along one series rather than
naming a column::

    df.groupby(ids.DATASET_AXIS)                 # one running total per series

Written that way, "is this keyed on the right thing?" is a grep rather than a
judgement per site.
"""

import pandas as pd

#: The household dataset: one household over one period, scoped to a project
#: and a batch. Primary key of the index and of every mapped artifact.
HOUSEHOLD_BATCH = "HuisBatchIdBSV"

#: The household a dataset describes -- an attribute of the dataset, not a key.
#: For counting households and for joining household-level metadata.
HOUSEHOLD = "HuisIdBSV"

#: The delivery. A batch holds projects, which hold household datasets.
BATCH = "BatchIdBSV"

#: The project a household dataset belongs to. A household's datasets in
#: different periods may sit in different projects.
PROJECT = "ProjectIdBSV"

#: The supplier's own identifiers, as delivered, before any are resolved to a
#: BSV id.
HOUSEHOLD_SUPPLIER = "HuisIdLeverancier"
PROJECT_SUPPLIER = "ProjectIdLeverancier"
SUPPLIER = "Dataleverancier"

#: The timestamp every time series is ordered by.
READING_DATE = "ReadingDate"

#: The axis a time series is grouped, ordered, windowed, diffed or accumulated
#: on. Two household batches of one household are two series, possibly over the
#: same period, so keying any of those on the household pools them.
DATASET_AXIS = HOUSEHOLD_BATCH

#: The hive partition keys of a sharded dataset, outermost first.
SHARD_PATH_KEYS = (HOUSEHOLD, HOUSEHOLD_BATCH)

#: The dtype each identifier column carries, as the pandas dtype object the
#: index maps use (`pd.Int64Dtype()`, not the `"Int64"` alias), so one map can
#: be read by `astype` and compared against another without normalising first.
#:
#: Nullable throughout (ADR-005): a BSV id is a whole number that may be absent
#: -- not yet assigned, or pending its row in the household batch table --
#: which float64 cannot say and int64 cannot represent.
#:
#: `READING_DATE` is absent on purpose. It is not an identifier, and its dtype
#: depends on whether a supplier delivered it tz-aware.
DTYPES = {
    HOUSEHOLD: pd.Int64Dtype(),
    HOUSEHOLD_BATCH: pd.Int64Dtype(),
    BATCH: pd.Int64Dtype(),
    PROJECT: pd.Int64Dtype(),
    HOUSEHOLD_SUPPLIER: pd.StringDtype(),
    PROJECT_SUPPLIER: pd.StringDtype(),
    SUPPLIER: pd.StringDtype(),
}

#: Every identifier column, for a caller that needs to tell an id from a
#: measured variable -- dropping them before per-column statistics, say.
ALL = (
    HOUSEHOLD,
    HOUSEHOLD_BATCH,
    BATCH,
    PROJECT,
    HOUSEHOLD_SUPPLIER,
    PROJECT_SUPPLIER,
    SUPPLIER,
)
