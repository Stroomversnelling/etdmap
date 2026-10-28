"""Reading ONE household batch dataset, and statistics keyed on it.

A household delivered twice is two household batches: two series over two periods,
possibly overlapping. ``get_mapped_data`` returns both concatenated, which is
right for a question about the dwelling's files and wrong for every statistic
-- a min, max, mean or percentile over the concatenation describes neither
period. ``get_mapped_household_batch`` reads one.

Every test here uses a household with TWO household batches, because that is the only
shape in which the two keys differ. On a household with one, the pooled and
the per-household-batch read return the same frame, which is why keying on the
dwelling went unnoticed.
"""

import pandas as pd
import pytest

import etdmap
from etdmap import ids
from etdmap.data_stats import get_data_stats
from etdmap.index_helpers import (
    get_mapped_data,
    get_mapped_household_batch,
    households_of_household_batches,
)
from etdmap.mapping_helpers import save_household_shard
from etdmap.storage import read_mapped_households
from etdmap.testing import index_frame

#: One household, two household batches. The values differ per household batch so a pooled read
#: is visible in any statistic rather than only in the row count.
HOUSEHOLD = 42
EARLY, LATE = 7, 9
VALUES = {EARLY: [1.0, 2.0, 3.0], LATE: [100.0, 200.0, 300.0]}


def _household_batch_frame(values, start):
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
    """A mapped folder holding one household's two household batches, plus its index."""
    root = tmp_path / "mapped"
    shard_root = root / "sharded"
    save_household_shard(
        _household_batch_frame(VALUES[EARLY], "2024-01-01"), HOUSEHOLD, EARLY, shard_root
    )
    save_household_shard(
        _household_batch_frame(VALUES[LATE], "2024-06-01"), HOUSEHOLD, LATE, shard_root
    )
    index_frame(
        [EARLY, LATE], household_of={EARLY: HOUSEHOLD, LATE: HOUSEHOLD}
    ).to_parquet(root / "index.parquet", engine="pyarrow", index=False)

    previous = etdmap.options.mapped_folder_path
    etdmap.options.mapped_folder_path = str(root)
    yield root
    etdmap.options.mapped_folder_path = previous


class TestReadMappedHouseholds:
    """The reader prunes directories by household and selects household batches
    exactly, because the shard path has a household level and the household batch is
    the key."""

    def test_without_a_household_batch_filter_every_household_batch_is_read(self, mapped_folder):
        frame = read_mapped_households([HOUSEHOLD])

        assert set(frame[ids.HOUSEHOLD_BATCH]) == {EARLY, LATE}
        assert len(frame) == 6

    def test_a_household_batch_filter_selects_exactly_that_household_batch(self, mapped_folder):
        frame = read_mapped_households([HOUSEHOLD], household_batch_ids=[LATE])

        assert set(frame[ids.HOUSEHOLD_BATCH]) == {LATE}
        assert frame["ElektriciteitNetgebruikHoog"].tolist() == VALUES[LATE]

    def test_a_household_batch_the_households_do_not_hold_raises(self, mapped_folder):
        """Rather than returning the other household batches, which reads as success."""
        with pytest.raises(FileNotFoundError, match=str(ids.HOUSEHOLD_BATCH)):
            read_mapped_households([HOUSEHOLD], household_batch_ids=[LATE, 999])


class TestGetMappedHouseholdBatch:
    def test_returns_one_household_batch_where_the_pooled_read_returns_both(
        self, mapped_folder
    ):
        pooled = get_mapped_data(HOUSEHOLD)
        early = get_mapped_household_batch(EARLY)
        late = get_mapped_household_batch(LATE)

        assert len(pooled) == 6
        assert early["ElektriciteitNetgebruikHoog"].tolist() == VALUES[EARLY]
        assert late["ElektriciteitNetgebruikHoog"].tolist() == VALUES[LATE]

    def test_carries_no_identifier_column(self, mapped_folder):
        """A caller iterates its columns, so an id would be a variable."""
        frame = get_mapped_household_batch(EARLY)

        assert ids.HOUSEHOLD not in frame.columns
        assert ids.HOUSEHOLD_BATCH not in frame.columns

    def test_a_household_batch_the_index_does_not_place_raises(self, mapped_folder):
        with pytest.raises(FileNotFoundError, match="does not place"):
            get_mapped_household_batch(999)

    def test_the_households_of_household_batches_come_from_the_index(self, mapped_folder):
        assert households_of_household_batches([EARLY]) == [HOUSEHOLD]
        assert households_of_household_batches([EARLY, LATE]) == [HOUSEHOLD]
        assert households_of_household_batches([999]) == []


class TestStatisticsAreKeyedOnTheHouseholdBatch:
    def test_each_household_batch_gets_its_own_statistics(self, mapped_folder):
        stats = get_data_stats()
        measured = stats[stats["column"] == "ElektriciteitNetgebruikHoog"]

        assert set(measured[ids.HOUSEHOLD_BATCH]) == {EARLY, LATE}
        by_household_batch = measured.set_index(ids.HOUSEHOLD_BATCH)
        assert float(by_household_batch.loc[EARLY, "max"]) == 3.0
        assert float(by_household_batch.loc[LATE, "max"]) == 300.0
        # Pooled, the max of both would be 300 for each row, and the count 6.
        assert set(measured["count"]) == {3}

    def test_the_household_arrives_as_an_attribute(self, mapped_folder):
        """Keyed on the household batch; the dwelling comes in with the index row."""
        stats = get_data_stats()

        assert set(stats[ids.HOUSEHOLD].dropna()) == {HOUSEHOLD}
