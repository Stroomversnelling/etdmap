"""The identifier columns of the data model, and what each one identifies.

One place the names are written. A call site then reads the id it means instead
of a string literal, a rename in the data model is one edit here, and a
mechanical pass can rewrite literals into these names.

Imports nothing from the rest of the package beyond pandas, so any module may
use it.

The data model
--------------
Stated once, in ``data/model.yaml``: the entities, their relations, which key
to write, and how to name each one in prose. Read that file. What follows says
only enough to use the names below.

Delivery batches hold projects, which hold household batch datasets. A
**household batch dataset** (:data:`HOUSEHOLD_BATCH`) is one household over one
period, scoped to its project and its delivery batch. It is the unit of
analysis and the primary key of the index and of every mapped artifact: the
shard, the rows in every mapping sidecar, and every computation that runs along
one time series. Every count, grouping, statistic, average and eligibility
floor keys on it.

A **household** (:data:`HOUSEHOLD`) is the dwelling a household batch
describes. It is an ATTRIBUTE, carried and reported, and **the unit of
nothing** -- no count keys on it. A household delivered twice contributes two
series, and counting the dwelling would count it once. ``data/model.yaml``
names the only two places it is still a key: the guard whose subject is a
household with more than one household batch, and the outer level of the shard
path.

"""

import pandas as pd

#: The household batch dataset: one household over one period, scoped to a
#: project and a delivery batch. The unit of analysis, and the primary key of
#: the index and of every mapped artifact.
HOUSEHOLD_BATCH = "HuisBatchIdBSV"

#: The dwelling a household batch describes -- an attribute of it, never a key.
#: For joining household-level metadata onto a household batch.
HOUSEHOLD = "HuisIdBSV"

#: The delivery batch: one delivery of household batch datasets from a supplier. Holds
#: projects, which hold household batches. Never means a household batch.
BATCH = "BatchIdBSV"

#: The project a household batch belongs to. A household's household batches
#: may sit in different projects, which is how a renovation is represented:
#: the project defines the physical model.
PROJECT = "ProjectIdBSV"

#: The supplier's own identifiers, as delivered, before any are resolved to a
#: BSV id.
HOUSEHOLD_SUPPLIER = "HuisIdLeverancier"
PROJECT_SUPPLIER = "ProjectIdLeverancier"
SUPPLIER = "Dataleverancier"

#: The timestamp every time series is ordered by.
READING_DATE = "ReadingDate"

#: The hive partition keys of the sharded layout, outermost first. The layout
#: on disk, not a key: a shard holds one household batch, under a directory
#: level for its household.
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
