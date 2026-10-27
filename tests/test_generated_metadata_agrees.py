"""The metadata the generator writes agrees with the dataset it wrote.

Three files are generated beside the mapped households: the household-batch sync
CSV the index's batch columns are read from, the reviewed BSV metadata the
Meenemen stamp is read from, and the project mapping ProjectIdBSV is resolved
through. Each states something about the dataset, and these compare them
against it.

They are not interchangeable with a supplied file: one written for a given shape
covers the households and projects it names, and leaves the rest of a
differently shaped dataset unresolved.
"""

import pandas as pd
import pytest

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
        assert set(generated["batch"]["HuisIdBSV"]) == set(
            generated["index"]["HuisIdBSV"]
        )

    def test_the_project_of_each_household_matches_the_index(self, generated):
        """Declaring one project for every household would contradict the
        index, which splits them, and nothing downstream would notice."""
        batch = generated["batch"].set_index("HuisIdBSV")["ProjectIdBSV"]
        index = generated["index"].set_index("HuisIdBSV")["ProjectIdBSV"]

        assert batch.sort_index().tolist() == index.sort_index().tolist()
        assert index.nunique() > 1, (
            "the dataset has one project, so this check cannot tell a correct "
            "mapping from a constant"
        )

    def test_its_window_contains_the_readings(self, generated):
        """A batch whose window excludes the readings describes a different
        dataset from the one on disk."""
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
        assert set(generated["reviewed"]["HuisIdBSV"]) == set(
            generated["index"]["HuisIdBSV"]
        )

    def test_every_household_is_included(self, generated):
        """The stages filter on Meenemen, so a household left out here is
        absent from every artifact after mapping."""
        assert generated["reviewed"]["Meenemen"].all()


class TestTheProjectMappingCoversEveryProject:
    def test_each_project_in_the_index_can_be_resolved(self, generated):
        assert set(generated["index"]["ProjectIdBSV"].dropna()) <= set(
            generated["projects"]["ProjectIdBSV"]
        )

    def test_no_household_is_left_without_a_project(self, generated):
        """ProjectIdBSV is joined through this mapping, so a project missing
        from it leaves its households unresolved rather than failing."""
        assert generated["index"]["ProjectIdBSV"].notna().all()
