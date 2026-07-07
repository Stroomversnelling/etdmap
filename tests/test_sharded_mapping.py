"""
TDD (red-first) contract for the sharded-mapping WRITE primitive.

Sharding originates at mapping (etdmap), not in a downstream etdtransform "default"
step (see etdworkflow/docs/refactoring/NATIVE_RESOLUTION_DESIGN.md, Draft ADR F).
`save_household_shard` writes ONE household's mapped data as a hive-partitioned
shard -- the atomic, per-household, idempotent unit the parallel standardized
mapping pipeline builds on. It does NOT exist yet -> red at import until it lands.

Contract:
- Writes to `<root>/HuisIdBSV=<n>/HuisBatchIdBSV=<p>/part.parquet`.
- Partition columns (the ids) live in the PATH, not the file -- hive convention,
  and consistent with the flat mapped file which also carries no id columns.
- Round-trips the mapped DataFrame identically (nullable dtypes preserved, ADR-005).
- Per-household + idempotent: re-running one household overwrites only its shard
  (no stale duplicate part files); different batches of the same household coexist.
"""

import os

import pandas as pd
import pytest

from etdmap.mapping_helpers import save_household_shard


def _mapped_df():
    """A minimal mapped-household frame: ReadingDate (UTC) + nullable data cols,
    no id columns -- exactly what run_standard_pipeline writes today."""
    idx = pd.to_datetime(
        ["2024-01-01 00:00", "2024-01-01 00:05", "2024-01-01 00:10"], utc=True
    )
    return pd.DataFrame({
        "ReadingDate": idx,
        "ElektriciteitNetgebruik": pd.array([1.0, 2.0, pd.NA], dtype="Float64"),
        "SomeCount": pd.array([10, 20, 30], dtype="Int64"),
    })


class TestSaveHouseholdShard:
    def test_roundtrip_and_hive_path(self, tmp_path):
        df = _mapped_df()
        out = save_household_shard(df, 42, 7, tmp_path)
        expected = os.path.join(
            str(tmp_path), "HuisIdBSV=42", "HuisBatchIdBSV=7", "part.parquet"
        )
        assert os.path.normpath(str(out)) == os.path.normpath(expected)
        assert os.path.exists(out)

        back = pd.read_parquet(out, dtype_backend="numpy_nullable")
        # ids live in the path, not the file
        assert "HuisIdBSV" not in back.columns
        assert "HuisBatchIdBSV" not in back.columns
        pd.testing.assert_frame_equal(back, df)

    def test_idempotent_single_part_file(self, tmp_path):
        df = _mapped_df()
        save_household_shard(df, 42, 7, tmp_path)
        save_household_shard(df, 42, 7, tmp_path)  # re-run one household
        part_dir = tmp_path / "HuisIdBSV=42" / "HuisBatchIdBSV=7"
        assert len(list(part_dir.glob("*.parquet"))) == 1

    def test_two_batches_same_household_coexist(self, tmp_path):
        df = _mapped_df()
        save_household_shard(df, 42, 7, tmp_path)
        save_household_shard(df, 42, 9, tmp_path)
        assert (tmp_path / "HuisIdBSV=42" / "HuisBatchIdBSV=7" / "part.parquet").exists()
        assert (tmp_path / "HuisIdBSV=42" / "HuisBatchIdBSV=9" / "part.parquet").exists()
