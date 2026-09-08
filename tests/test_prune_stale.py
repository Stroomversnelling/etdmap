"""
Contract tests for the known-stale household prune helpers.

"Known stale" = a household output whose id is NOT in the current
authoritative set (from the index). Only those are
removed; everything in the set is kept, and anything that does not match the
household naming pattern is never touched. Each removal is returned in a
manifest -- delete-with-manifest, not a blanket glob-delete.

Two helpers: flat mapped files and sharded shards.
"""

import pandas as pd  # noqa: F401  (ensures etdmap import env is consistent)
import pytest

from etdmap.mapping_helpers import (
    prune_stale_household_files,
    prune_stale_household_shards,
)


def _touch(p):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"x")


class TestPruneStaleFlat:
    def test_removes_only_unknown_ids(self, tmp_path):
        for n in (1, 2, 3):
            _touch(tmp_path / f"household_{n}_table.parquet")
        _touch(tmp_path / "index.parquet")      # must NOT be touched
        _touch(tmp_path / "notes.txt")          # unrelated; must NOT be touched

        removed = prune_stale_household_files(tmp_path, keep_household_ids={1, 2})

        assert not (tmp_path / "household_3_table.parquet").exists()
        assert (tmp_path / "household_1_table.parquet").exists()
        assert (tmp_path / "household_2_table.parquet").exists()
        assert (tmp_path / "index.parquet").exists()
        assert (tmp_path / "notes.txt").exists()
        assert sorted(r["HuisIdBSV"] for r in removed) == [3]

    def test_nothing_removed_when_all_kept(self, tmp_path):
        for n in (1, 2):
            _touch(tmp_path / f"household_{n}_table.parquet")
        removed = prune_stale_household_files(tmp_path, keep_household_ids={1, 2})
        assert removed == []


class TestPruneStaleShards:
    def _make_shard(self, root, household_id, household_batch_id):
        _touch(root / f"HuisIdBSV={household_id}" / f"HuisBatchIdBSV={household_batch_id}" / "part.parquet")

    def test_removes_only_unknown_pairs(self, tmp_path):
        for n in (1, 2, 3):
            self._make_shard(tmp_path, n, n)

        removed = prune_stale_household_shards(tmp_path, keep_pairs={(1, 1), (2, 2)})

        assert not (tmp_path / "HuisIdBSV=3").exists()
        assert (tmp_path / "HuisIdBSV=1" / "HuisBatchIdBSV=1" / "part.parquet").exists()
        assert (tmp_path / "HuisIdBSV=2" / "HuisBatchIdBSV=2" / "part.parquet").exists()
        assert sorted((r["HuisIdBSV"], r["HuisBatchIdBSV"]) for r in removed) == [(3, 3)]

    def test_prunes_stale_batch_keeps_other_batch_same_household(self, tmp_path):
        # Household 5 has two batches; keep only (5, 5) -> the (5, 9) shard is stale.
        self._make_shard(tmp_path, 5, 5)
        self._make_shard(tmp_path, 5, 9)

        removed = prune_stale_household_shards(tmp_path, keep_pairs={(5, 5)})

        assert (tmp_path / "HuisIdBSV=5" / "HuisBatchIdBSV=5" / "part.parquet").exists()
        assert not (tmp_path / "HuisIdBSV=5" / "HuisBatchIdBSV=9").exists()
        assert sorted((r["HuisIdBSV"], r["HuisBatchIdBSV"]) for r in removed) == [(5, 9)]
