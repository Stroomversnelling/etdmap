"""
TDD (red-first) contract tests for the batch-registry guard.

These define the behaviour of the shared validators that protect the parallel
per-HuisBatchIdBSV registry (`batch_index.parquet`) while it coexists with the
legacy per-HuisIdBSV `index.parquet`. The implementation does NOT exist yet --
this file is written first and is expected to fail at import (red) until the
guard lands in etdmap.index_helpers.

Contracts (see docs/refactoring/NATIVE_RESOLUTION_DESIGN.md in etdworkflow):

1. Perfect 1:1 correspondence. While the legacy and batch registries coexist,
   every HuisIdBSV maps to exactly one HuisBatchIdBSV and the two registries
   agree row-for-row. The guard RAISES the moment that breaks -- a household
   appearing in more than one batch (the forced-switch trigger), or any row-set
   mismatch between the two registries. A loud, early stop -- never a silent
   degrade into 1:many join corruption.

2. Gegevensfrequentie is required. The batch's finest mapped cadence is declared
   externally (synced) and must be present on every included batch row.

3. A mapped column is never finer than its batch's Gegevensfrequentie. Columns
   may deviate only coarser (O-Nexus electricity 15-min inside a 5-min batch);
   a detected cadence finer than the declared batch cadence is a fatal metadata
   bug -> RAISE.

All fixtures use pandas nullable dtypes (ADR-005). ASCII-only messages.
"""

import pandas as pd
import pytest

from etdmap.index_helpers import (
    HuisBatchOverlapError,
    validate_batch_index_correspondence,
    validate_gegevensfrequentie_present,
)


# ---------------------------------------------------------------------------
# Builders (synthetic; no fixture dataset needed)
# ---------------------------------------------------------------------------

def _index_df(huis_ids):
    """Legacy per-HuisIdBSV registry: one row per household."""
    return pd.DataFrame({"HuisIdBSV": pd.array(huis_ids, dtype="Int64")})


def _batch_index_df(rows, gegevensfrequentie="5-minute"):
    """Per-HuisBatchIdBSV registry. `rows` is a list of (HuisIdBSV, HuisBatchIdBSV)."""
    huis = [r[0] for r in rows]
    batch = [r[1] for r in rows]
    return pd.DataFrame({
        "HuisIdBSV": pd.array(huis, dtype="Int64"),
        "HuisBatchIdBSV": pd.array(batch, dtype="Int64"),
        "Gegevensfrequentie": pd.array([gegevensfrequentie] * len(rows), dtype="string"),
    })


# ---------------------------------------------------------------------------
# 1. Perfect 1:1 correspondence
# ---------------------------------------------------------------------------

class TestOneToOneCorrespondence:
    def test_perfect_1to1_passes(self):
        # Initial state: HuisBatchIdBSV duplicates HuisIdBSV.
        index = _index_df([1, 2, 3])
        batch = _batch_index_df([(1, 1), (2, 2), (3, 3)])
        # Must NOT raise.
        validate_batch_index_correspondence(index, batch)

    def test_household_in_two_batches_raises(self):
        # The forced-switch trigger: HuisIdBSV 2 recurs in a second batch.
        # Raises the specific HuisBatchOverlapError (whose docstring lists the
        # code that must be batch-aware by this point).
        index = _index_df([1, 2, 3])
        batch = _batch_index_df([(1, 1), (2, 2), (3, 3), (2, 4)])
        with pytest.raises(HuisBatchOverlapError) as exc:
            validate_batch_index_correspondence(index, batch)
        assert "2" in str(exc.value)

    def test_household_in_batch_not_in_index_raises(self):
        # batch_index has a household the legacy index does not: impossible data
        # (ids are minted from the index) -> hard error.
        index = _index_df([1, 2])
        batch = _batch_index_df([(1, 1), (2, 2), (3, 3)])
        with pytest.raises(Exception):
            validate_batch_index_correspondence(index, batch)

    def test_household_in_index_not_in_batch_warns_pending(self, caplog):
        # legacy index has a household the batch_index does not: PENDING its
        # manual addition to the metadata (normal transient state) -> warning.
        index = _index_df([1, 2, 3])
        batch = _batch_index_df([(1, 1), (2, 2)])
        validate_batch_index_correspondence(index, batch)  # must NOT raise
        warnings = [r.message for r in caplog.records if r.levelname == "WARNING"]
        assert any("3" in w for w in warnings)


# ---------------------------------------------------------------------------
# 2. Gegevensfrequentie required
# ---------------------------------------------------------------------------

class TestGegevensfrequentieRequired:
    def test_all_present_passes(self):
        batch = _batch_index_df([(1, 1), (2, 2)], gegevensfrequentie="5-minute")
        validate_gegevensfrequentie_present(batch)

    def test_missing_na_raises(self):
        batch = _batch_index_df([(1, 1), (2, 2)])
        batch.loc[1, "Gegevensfrequentie"] = pd.NA
        with pytest.raises(Exception):
            validate_gegevensfrequentie_present(batch)

    def test_empty_string_raises(self):
        batch = _batch_index_df([(1, 1)])
        batch.loc[0, "Gegevensfrequentie"] = ""
        with pytest.raises(Exception):
            validate_gegevensfrequentie_present(batch)


# ---------------------------------------------------------------------------
# NOTE on column cadence (removed check):
# A per-column "detected finer than declared -> raise" check was removed. On a
# single mapped dataset the rows ARE the grid at Gegevensfrequentie, so a column
# cannot genuinely report finer than the row frequency -- the check was vacuous
# as framed, and where non-vacuous (declared-vs-detected) it should be a DQ
# WARNING report, not a raise. Per-column cadence variation is handled by
# batch-splitting (mechanism) + a data-form warning report summarised at end of
# run. See etdworkflow/docs/refactoring/NATIVE_RESOLUTION_DESIGN.md.
# ---------------------------------------------------------------------------
