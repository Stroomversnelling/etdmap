"""The metadata the generator writes agrees with the household batch it wrote.

Three files are generated beside the mapped households: the household-batch sync
CSV the index's household-batch columns are read from, the reviewed BSV metadata the
Meenemen stamp is read from, and the project mapping ids.PROJECT is resolved
through. Each states something about the household batch, and these compare them
against it.

They are not interchangeable with a supplied file: one written for a given shape
covers the households and projects it names, and leaves the rest of a
differently shaped household batch unresolved.
"""

import pandas as pd
import pytest

from etdmap import ids
from etdmap.storage import mapped_household_paths

GENERATED_METADATA = "_generated_metadata"


@pytest.fixture(scope="module")
def generated(mapped_fixtures):
    """The index, and the three metadata files written beside it."""
    folder = mapped_fixtures / GENERATED_METADATA

    def read(name):
        return pd.read_csv(folder / name, dtype_backend="numpy_nullable")

    return {
        "index": pd.read_parquet(
            mapped_fixtures / "index.parquet", dtype_backend="numpy_nullable"
        ),
        "batch": read("household_batch_sync.csv"),
        "reviewed": read("metadata_reviewed.csv"),
        "projects": read("project_mapping.csv"),
        "folder": mapped_fixtures,
    }


class TestTheBatchSyncDescribesTheDataset:
    def test_it_covers_exactly_the_households_the_index_holds(self, generated):
        assert set(generated["batch"][ids.HOUSEHOLD]) == set(
            generated["index"][ids.HOUSEHOLD]
        )

    def test_the_project_of_each_household_matches_the_index(self, generated):
        """Declaring one project for every household would contradict the
        index, which splits them, and nothing downstream would notice."""
        batch = generated["batch"].set_index(ids.HOUSEHOLD)[ids.PROJECT]
        index = generated["index"].set_index(ids.HOUSEHOLD)[ids.PROJECT]

        assert batch.sort_index().tolist() == index.sort_index().tolist()
        assert index.nunique() > 1, (
            "the dataset has one project, so this check cannot tell a correct "
            "mapping from a constant"
        )

    def test_its_window_contains_the_readings(self, generated):
        """A household batch whose window excludes the readings describes a different
        household batch from the one on disk."""
        paths = mapped_household_paths(generated["folder"])
        readings = pd.read_parquet(
            paths[min(paths)], columns=["ReadingDate"],
            dtype_backend="numpy_nullable",
        )["ReadingDate"]
        start = pd.to_datetime(generated["batch"]["Startdatum"]).min()
        end = pd.to_datetime(generated["batch"]["Einddatum"]).max()

        assert start.tz_localize(None) <= readings.min()
        assert end.tz_localize(None) >= readings.max()


class TestTheReviewedMetadataDescribesTheDataset:
    def test_it_covers_exactly_the_households_the_index_holds(self, generated):
        """update_include compares the two household key sets and raises on a
        difference, so a metadata file covering a different set stops the
        downstream stages outright."""
        assert set(generated["reviewed"][ids.HOUSEHOLD]) == set(
            generated["index"][ids.HOUSEHOLD]
        )

    def test_every_household_is_included(self, generated):
        """The stages filter on Meenemen, so a household left out here is
        absent from every artifact after mapping."""
        assert generated["reviewed"]["Meenemen"].all()


class TestTheProjectMappingCoversEveryProject:
    def test_each_project_in_the_index_can_be_resolved(self, generated):
        assert set(generated["index"][ids.PROJECT].dropna()) <= set(
            generated["projects"][ids.PROJECT]
        )

    def test_no_household_is_left_without_a_project(self, generated):
        """ids.PROJECT is joined through this mapping, so a project missing
        from it leaves its households unresolved rather than failing."""
        assert generated["index"][ids.PROJECT].notna().all()
