"""Reading ONE household batch dataset, and statistics keyed on it.

A household delivered twice is two datasets: two series over two periods,
possibly overlapping. ``get_mapped_data`` returns both concatenated, which is
right for a question about the dwelling's files and wrong for every statistic
-- a min, max, mean or percentile over the concatenation describes neither
period. ``get_mapped_dataset`` reads one.

Every test here uses a household with TWO datasets, because that is the only
shape in which the two keys differ. On a household with one, the pooled and
the per-dataset read return the same frame, which is why keying on the
dwelling went unnoticed.
"""

import pandas as pd
import pytest

import etdmap
from etdmap import ids
from etdmap.data_stats import get_data_stats
from etdmap.index_helpers import (
    get_mapped_data,
    get_mapped_dataset,
    households_of_household_batches,
)
from etdmap.mapping_helpers import save_household_shard
from etdmap.storage import read_mapped_households
from etdmap.testing import index_frame

#: One household, two datasets. The values differ per dataset so a pooled read
#: is visible in any statistic rather than only in the row count.
HOUSEHOLD = 42
EARLY, LATE = 7, 9
VALUES = {EARLY: [1.0, 2.0, 3.0], LATE: [100.0, 200.0, 300.0]}


def _dataset_frame(values, start):
    """A mapped dataset: timestamps and one measured column, no id columns.

    Written as the shard writer writes them -- the ids live in the path, so a
    frame carrying one would be counted as a measured variable by anything
    that iterates columns.
    """
    return pd.DataFrame({
        "ReadingDate": pd.to_datetime(
            [f"{start} 00:00", f"{start} 00:05", f"{start} 00:10"], utc=True
        ),
        "ElektriciteitNetgebruikHoog": pd.array(values, dtype="Float64"),
    })


@pytest.fixture
def mapped_folder(tmp_path):
    """A mapped folder holding one household's two datasets, plus its index."""
    root = tmp_path / "mapped"
    shard_root = root / "sharded"
    save_household_shard(
        _dataset_frame(VALUES[EARLY], "2024-01-01"), HOUSEHOLD, EARLY, shard_root
    )
    save_household_shard(
        _dataset_frame(VALUES[LATE], "2024-06-01"), HOUSEHOLD, LATE, shard_root
    )
    index_frame(
        [EARLY, LATE], household_of={EARLY: HOUSEHOLD, LATE: HOUSEHOLD}
    ).to_parquet(root / "index.parquet", engine="pyarrow", index=False)

    previous = etdmap.options.mapped_folder_path
    etdmap.options.mapped_folder_path = str(root)
    yield root
    etdmap.options.mapped_folder_path = previous


class TestReadMappedHouseholds:
    """The reader prunes directories by household and selects datasets
    exactly, because the shard path has a household level and the dataset is
    the key."""

    def test_without_a_dataset_filter_every_dataset_is_read(self, mapped_folder):
        frame = read_mapped_households([HOUSEHOLD])

        assert set(frame[ids.HOUSEHOLD_BATCH]) == {EARLY, LATE}
        assert len(frame) == 6

    def test_a_dataset_filter_selects_exactly_that_dataset(self, mapped_folder):
        frame = read_mapped_households([HOUSEHOLD], household_batch_ids=[LATE])

        assert set(frame[ids.HOUSEHOLD_BATCH]) == {LATE}
        assert frame["ElektriciteitNetgebruikHoog"].tolist() == VALUES[LATE]

    def test_a_dataset_the_households_do_not_hold_raises(self, mapped_folder):
        """Rather than returning the other datasets, which reads as success."""
        with pytest.raises(FileNotFoundError, match=str(ids.HOUSEHOLD_BATCH)):
            read_mapped_households([HOUSEHOLD], household_batch_ids=[LATE, 999])


class TestGetMappedDataset:
    def test_returns_one_dataset_where_the_pooled_read_returns_both(
        self, mapped_folder
    ):
        pooled = get_mapped_data(HOUSEHOLD)
        early = get_mapped_dataset(EARLY)
        late = get_mapped_dataset(LATE)

        assert len(pooled) == 6
        assert early["ElektriciteitNetgebruikHoog"].tolist() == VALUES[EARLY]
        assert late["ElektriciteitNetgebruikHoog"].tolist() == VALUES[LATE]

    def test_carries_no_identifier_column(self, mapped_folder):
        """A caller iterates its columns, so an id would be a variable."""
        frame = get_mapped_dataset(EARLY)

        assert ids.HOUSEHOLD not in frame.columns
        assert ids.HOUSEHOLD_BATCH not in frame.columns

    def test_a_dataset_the_index_does_not_place_raises(self, mapped_folder):
        with pytest.raises(FileNotFoundError, match="does not place"):
            get_mapped_dataset(999)

    def test_the_households_of_datasets_come_from_the_index(self, mapped_folder):
        assert households_of_household_batches([EARLY]) == [HOUSEHOLD]
        assert households_of_household_batches([EARLY, LATE]) == [HOUSEHOLD]
        assert households_of_household_batches([999]) == []


class TestStatisticsAreKeyedOnTheDataset:
    def test_each_dataset_gets_its_own_statistics(self, mapped_folder):
        stats = get_data_stats()
        measured = stats[stats["column"] == "ElektriciteitNetgebruikHoog"]

        assert set(measured[ids.HOUSEHOLD_BATCH]) == {EARLY, LATE}
        by_dataset = measured.set_index(ids.HOUSEHOLD_BATCH)
        assert float(by_dataset.loc[EARLY, "max"]) == 3.0
        assert float(by_dataset.loc[LATE, "max"]) == 300.0
        # Pooled, the max of both would be 300 for each row, and the count 6.
        assert set(measured["count"]) == {3}

    def test_the_household_arrives_as_an_attribute(self, mapped_folder):
        """Keyed on the dataset; the dwelling comes in with the index row."""
        stats = get_data_stats()

        assert set(stats[ids.HOUSEHOLD].dropna()) == {HOUSEHOLD}
