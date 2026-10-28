"""Establishing a source's layout instead of assuming it from `isdir`.

`source_is_sharded` used to answer "is this a directory" while every caller used
it as "is this a hive-partitioned shard root". Those differ, and a directory of
flat per-household files answers yes to the first and no to the second, which
produced three failures from one cause:

1. A recursive glob from a mixed folder's root matched both the flat files and
   the partitioned ones, and the reader rejected the mixture.
2. Selecting households looked for `HuisIdBSV=` directories at the root, found
   none, and raised as though the households had no data.
3. A purely flat directory read as shards returned rows with no `HuisIdBSV` at
   all, and raised nothing. That one is the dangerous case.

`data/mapped` is the mixed shape: flat files at the root, partitions under
`sharded/`. The fixtures here reproduce it.
"""

import pandas as pd
import pytest

from etdmap import ids
from etdmap.storage import (
    flat_glob,
    read_source_frame,
    shard_glob,
    shard_root,
    source_is_directory,
    source_is_sharded,
    source_layout,
    source_read_targets,
    source_schema_names,
    source_sql,
)


def _rows(household_id, n=3):
    return pd.DataFrame({
        "ReadingDate": pd.date_range("2026-01-01", periods=n, freq="5min"),
        "value": pd.array([float(household_id)] * n, dtype="Float64"),
    })


def _flat_file(root, household_id, n=3):
    _rows(household_id, n).to_parquet(
        root / f"household_{household_id}_table.parquet", engine="pyarrow"
    )


def _shard(root, household_id, n=3):
    d = root / "sharded" / f"HuisIdBSV={household_id}" / f"HuisBatchIdBSV={household_id}"
    d.mkdir(parents=True)
    _rows(household_id, n).to_parquet(d / "part.parquet", engine="pyarrow")


@pytest.fixture()
def mixed(tmp_path):
    """The `data/mapped` shape: flat at the root, partitions under sharded/."""
    for household_id in (1, 2):
        _flat_file(tmp_path, household_id)
        _shard(tmp_path, household_id)
    return tmp_path


@pytest.fixture()
def flat(tmp_path):
    """Flat only, no partitions anywhere."""
    for household_id in (7, 9):
        _flat_file(tmp_path, household_id)
    return tmp_path


@pytest.fixture()
def one_file(tmp_path):
    path = tmp_path / "household_default.parquet"
    _rows(1).to_parquet(path, engine="pyarrow")
    return path


class TestTheLayoutIsEstablished:
    def test_a_single_file_is_a_file(self, one_file):
        assert source_layout(one_file) == "file"
        assert not source_is_directory(one_file)

    def test_partitions_under_sharded_make_it_sharded(self, mixed):
        assert source_layout(mixed) == "sharded"

    def test_partitions_at_the_root_make_it_sharded(self, tmp_path):
        d = tmp_path / "HuisIdBSV=1" / "HuisBatchIdBSV=1"
        d.mkdir(parents=True)
        _rows(1).to_parquet(d / "part.parquet", engine="pyarrow")
        assert source_layout(tmp_path) == "sharded"
        assert shard_root(tmp_path) == str(tmp_path)

    def test_a_directory_of_household_files_is_flat(self, flat):
        assert source_layout(flat) == "flat"

    def test_flat_is_a_directory_but_not_sharded(self, flat):
        """The distinction the old predicate could not make."""
        assert source_is_directory(flat)
        assert not source_is_sharded(flat)

    def test_an_empty_directory_is_flat_not_sharded(self, tmp_path):
        assert source_layout(tmp_path) == "flat"
        assert shard_root(tmp_path) is None


class TestTheGlobComesFromTheResolvedRoot:
    def test_it_points_at_the_sharded_subdirectory(self, mixed):
        assert "sharded" in shard_glob(mixed)

    def test_the_flat_files_beside_it_are_not_matched(self, mixed):
        """Failure 1: a recursive glob from the root swept in both layouts."""
        import glob

        matched = glob.glob(shard_glob(mixed), recursive=True)
        assert matched
        assert not any("household_" in m and "HuisIdBSV=" not in m for m in matched)

    def test_asking_a_flat_directory_for_a_shard_glob_raises(self, flat):
        with pytest.raises(ValueError, match="no HuisIdBSV"):
            shard_glob(flat)

    def test_the_flat_glob_matches_the_household_files(self, flat):
        import glob

        assert len(glob.glob(flat_glob(flat))) == 2


class TestSelectingHouseholds:
    def test_a_mixed_folder_resolves_the_requested_shard(self, mixed):
        """Failure 2: it looked for HuisIdBSV= at the root and blamed the data."""
        targets = source_read_targets(mixed, household_ids=[1])
        assert len(targets) == 1
        assert "HuisIdBSV=1" in targets[0]

    def test_a_flat_folder_resolves_the_household_file(self, flat):
        targets = source_read_targets(flat, household_ids=[7])
        assert len(targets) == 1
        assert targets[0].endswith("household_7_table.parquet")

    def test_a_household_with_no_data_still_raises(self, mixed):
        with pytest.raises(FileNotFoundError):
            source_read_targets(mixed, household_ids=[999])

    def test_a_single_file_is_returned_unchanged(self, one_file):
        assert source_read_targets(one_file, household_ids=[1]) == str(one_file)


class TestAFlatDirectoryKeepsItsHouseholdIdentity:
    """Failure 3: the identity was silently absent and nothing raised."""

    def test_the_frame_carries_the_ids_from_the_file_names(self, flat):
        out = read_source_frame(flat)
        assert ids.HOUSEHOLD in out.columns
        assert sorted(out[ids.HOUSEHOLD].unique().tolist()) == [7, 9]

    def test_the_batch_id_equals_the_household_id(self, flat):
        """One file per household, the same equivalence the mapped reader uses."""
        out = read_source_frame(flat)
        assert (out[ids.HOUSEHOLD] == out[ids.HOUSEHOLD_BATCH]).all()

    def test_every_row_is_read(self, flat):
        assert len(read_source_frame(flat)) == 6

    def test_the_ids_are_reported_in_the_schema(self, flat):
        assert {ids.HOUSEHOLD, ids.HOUSEHOLD_BATCH} <= source_schema_names(flat)

    def test_sql_over_it_refuses_rather_than_dropping_the_id(self, flat):
        """A fragment with no HuisIdBSV and no error is how failure 3 hid."""
        with pytest.raises(ValueError, match="flat directory"):
            source_sql(flat)


class TestTheTwoWaysIntoAMixedFolderAgree:
    def test_the_root_and_the_subdirectory_read_the_same_rows(self, mixed):
        root = read_source_frame(mixed)
        nested = read_source_frame(mixed / "sharded")
        assert len(root) == len(nested) == 6

    def test_both_carry_the_household_ids(self, mixed):
        for path in (mixed, mixed / "sharded"):
            out = read_source_frame(path)
            assert sorted(out[ids.HOUSEHOLD].unique().tolist()) == [1, 2]

    def test_the_sql_fragments_are_equivalent(self, mixed):
        assert source_sql(mixed) == source_sql(mixed / "sharded")


class TestResolveSourcePath:
    def test_a_sharded_area_artifact_resolves(self, tmp_path):
        """The sharded pipeline writes stage outputs under <folder>/sharded/,
        so an artifact there resolves before the legacy flat file is
        assumed."""
        from etdmap.storage import resolve_source_path

        target = tmp_path / "sharded" / "household_calculated"
        target.mkdir(parents=True)
        assert resolve_source_path(tmp_path, "household_calculated") == str(target)

    def test_a_direct_directory_wins_over_the_sharded_area(self, tmp_path):
        from etdmap.storage import resolve_source_path

        direct = tmp_path / "household_imputed"
        direct.mkdir()
        (tmp_path / "sharded" / "household_imputed").mkdir(parents=True)
        assert resolve_source_path(tmp_path, "household_imputed") == str(direct)

    def test_nothing_on_disk_falls_back_to_the_legacy_file(self, tmp_path):
        from etdmap.storage import resolve_source_path

        assert resolve_source_path(tmp_path, "household_imputed").endswith(
            "household_imputed.parquet"
        )
