"""
read_source_frame: reading a hive shard directory into one DataFrame.

Regression focus: a household's id may live ONLY in the shard folder name
(HuisIdBSV=<n>) and not inside the parquet file. Requesting a column subset
that resolves to only such partition-path ids must still return every row --
an earlier version read zero in-file columns, got a zero-length frame, and
silently returned 0 rows per shard.
"""

import pandas as pd
import pytest

from etdmap.storage import read_source_frame


def _write_shard(root, household_id, batch_id, n_rows, store_id_in_file):
    """One HuisIdBSV=<h>/HuisBatchIdBSV=<b>/part.parquet with n_rows rows.

    When store_id_in_file is False the ids live ONLY in the folder names
    (as the imputed/resampled stage artifacts do); otherwise they are also
    columns in the file (as the calculated stage stores them)."""
    d = root / f"HuisIdBSV={household_id}" / f"HuisBatchIdBSV={batch_id}"
    d.mkdir(parents=True)
    data = {"value": pd.array(range(n_rows), dtype="Int64")}
    if store_id_in_file:
        data["HuisIdBSV"] = pd.array([household_id] * n_rows, dtype="Int64")
        data["HuisBatchIdBSV"] = pd.array([batch_id] * n_rows, dtype="Int64")
    pd.DataFrame(data).to_parquet(d / "part.parquet", engine="pyarrow")


class TestPartitionOnlyColumnSubset:
    """The bug: ids only in the path + a subset request for just those ids."""

    def test_huisidbsv_only_subset_returns_all_rows(self, tmp_path):
        _write_shard(tmp_path, 1, 1, 10, store_id_in_file=False)
        _write_shard(tmp_path, 2, 2, 7, store_id_in_file=False)

        out = read_source_frame(tmp_path, columns=["HuisIdBSV"])

        assert len(out) == 17
        assert list(out.columns) == ["HuisIdBSV"]
        assert out["HuisIdBSV"].value_counts().to_dict() == {1: 10, 2: 7}

    def test_huisbatchidbsv_only_subset_returns_all_rows(self, tmp_path):
        _write_shard(tmp_path, 5, 9, 4, store_id_in_file=False)
        out = read_source_frame(tmp_path, columns=["HuisBatchIdBSV"])
        assert len(out) == 4
        assert out["HuisBatchIdBSV"].tolist() == [9, 9, 9, 9]

    def test_partition_ids_are_int64(self, tmp_path):
        _write_shard(tmp_path, 1, 1, 3, store_id_in_file=False)
        out = read_source_frame(tmp_path, columns=["HuisIdBSV"])
        assert str(out["HuisIdBSV"].dtype) == "Int64"


class TestMixedAndFullReads:
    def test_partition_id_plus_infile_column(self, tmp_path):
        _write_shard(tmp_path, 1, 1, 10, store_id_in_file=False)
        _write_shard(tmp_path, 2, 2, 7, store_id_in_file=False)
        out = read_source_frame(tmp_path, columns=["HuisIdBSV", "value"])
        assert len(out) == 17
        assert list(out.columns) == ["HuisIdBSV", "value"]

    def test_full_read_injects_ids(self, tmp_path):
        _write_shard(tmp_path, 3, 3, 5, store_id_in_file=False)
        out = read_source_frame(tmp_path)
        assert len(out) == 5
        assert {"HuisIdBSV", "HuisBatchIdBSV", "value"} <= set(out.columns)
        assert out["HuisIdBSV"].tolist() == [3] * 5

    def test_ids_stored_in_file_are_not_double_injected(self, tmp_path):
        _write_shard(tmp_path, 4, 4, 6, store_id_in_file=True)
        out = read_source_frame(tmp_path, columns=["HuisIdBSV"])
        assert len(out) == 6
        assert list(out.columns) == ["HuisIdBSV"]
        assert out["HuisIdBSV"].tolist() == [4] * 6

    def test_unparseable_shard_path_raises(self, tmp_path):
        # A parquet not under a HuisIdBSV=/HuisBatchIdBSV= path -> never guess.
        bad = tmp_path / "loose"
        bad.mkdir()
        pd.DataFrame({"value": [1]}).to_parquet(bad / "x.parquet")
        with pytest.raises(ValueError):
            read_source_frame(tmp_path, columns=["HuisIdBSV"])


class TestSourceReadTargets:
    """Selecting households must happen by PATH, not by filtering after the
    read: the data is one directory per household, so naming them up front is
    what lets the reader skip the rest."""

    def _artifact(self, root, household_ids):
        for h in household_ids:
            _write_shard(root, h, h, n_rows=2, store_id_in_file=False)
        return root

    def test_all_households_is_a_single_glob(self, tmp_path):
        from etdmap.storage import source_read_targets
        self._artifact(tmp_path, [1, 2])
        target = source_read_targets(tmp_path)
        assert isinstance(target, str)

    def test_subset_returns_one_glob_per_household(self, tmp_path):
        from etdmap.storage import source_read_targets
        self._artifact(tmp_path, [1, 2, 3])
        targets = source_read_targets(tmp_path, household_ids=[1, 3])
        assert len(targets) == 2
        assert all("HuisIdBSV=" in t for t in targets)
        assert not any("HuisIdBSV=2" in t for t in targets)

    def test_absent_households_are_skipped_not_fatal(self, tmp_path):
        """An artifact holds only the households that reached that stage, so a
        index-derived request can name households with no data here."""
        from etdmap.storage import source_read_targets
        self._artifact(tmp_path, [1])
        targets = source_read_targets(tmp_path, household_ids=[1, 99999])
        assert len(targets) == 1
        assert "HuisIdBSV=1" in targets[0]

    def test_raises_when_no_requested_household_has_data(self, tmp_path):
        from etdmap.storage import source_read_targets
        self._artifact(tmp_path, [1])
        with pytest.raises(FileNotFoundError):
            source_read_targets(tmp_path, household_ids=[42])

    def test_empty_selection_is_an_error_not_everything(self, tmp_path):
        from etdmap.storage import source_read_targets
        self._artifact(tmp_path, [1])
        with pytest.raises(ValueError):
            source_read_targets(tmp_path, household_ids=[])

    def test_single_file_source_cannot_be_pruned_by_path(self, tmp_path):
        from etdmap.storage import source_read_targets
        f = tmp_path / "household_60min.parquet"
        pd.DataFrame({"value": [1]}).to_parquet(f)
        assert source_read_targets(f, household_ids=[1]) == str(f)
