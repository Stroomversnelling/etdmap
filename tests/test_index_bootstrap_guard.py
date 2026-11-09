"""read_index refuses an absent index unless asked to bootstrap.

The index is the household-id registry: assign_ids looks ids.HOUSEHOLD up by
(ids.SUPPLIER, ids.PROJECT_SUPPLIER, ids.HOUSEHOLD_SUPPLIER) and invents an
id only for a key it cannot find. An empty index therefore makes every
household look new, so mapping into a folder without one renumbers the
whole dataset in mapper execution order -- silently, while ids.HOUSEHOLD is the
join key of every comparison against an earlier run.

Returning an empty frame is still correct for a genuinely new dataset, so
the behaviour is kept behind an explicit opt-in rather than removed.

These tests assert the REFUSAL and that the index-present path is
untouched. They deliberately assert nothing about how new ids are
allocated -- that logic is not changed here.
"""

import pandas as pd
import pytest

import etdmap
from etdmap import ids
from etdmap.index_helpers import bsv_metadata_columns, read_index


def _write_index(folder, rows):
    frame = pd.DataFrame(rows)
    frame.to_parquet(folder / "index.parquet", engine="pyarrow", index=False)
    return folder / "index.parquet"


@pytest.fixture
def mapped_folder(tmp_path):
    previous = etdmap.options.mapped_folder_path
    etdmap.options.mapped_folder_path = tmp_path
    yield tmp_path
    etdmap.options.mapped_folder_path = previous


_ROWS = [
    {
        ids.HOUSEHOLD_SUPPLIER: "DV-VDA-1",
        ids.HOUSEHOLD: 1,
        ids.PROJECT_SUPPLIER: "P1",
        ids.PROJECT: 1,
        ids.SUPPLIER: "Factory Zero",
        "Meenemen": pd.NA,
        "Notities": pd.NA,
    },
    {
        ids.HOUSEHOLD_SUPPLIER: "Address 1",
        ids.HOUSEHOLD: 298,
        ids.PROJECT_SUPPLIER: "P8",
        ids.PROJECT: 8,
        ids.SUPPLIER: "O-Nexus",
        "Meenemen": pd.NA,
        "Notities": pd.NA,
    },
]


class TestMissingIndexIsRefused:
    def test_absent_index_raises(self, mapped_folder):
        with pytest.raises(FileNotFoundError):
            read_index()

    def test_message_names_the_path_it_looked_for(self, mapped_folder):
        with pytest.raises(FileNotFoundError) as excinfo:
            read_index()
        assert "index.parquet" in str(excinfo.value)
        assert str(mapped_folder) in str(excinfo.value)

    def test_message_explains_the_consequence_and_both_remedies(self, mapped_folder):
        with pytest.raises(FileNotFoundError) as excinfo:
            read_index()
        message = str(excinfo.value)
        # The reader has to learn WHY this matters, not just that a file is
        # missing: ids get reassigned and comparisons silently mispair.
        assert "reassigned" in message
        # Remedy 1: seed the canonical index (the remap case).
        assert "copy the canonical index.parquet" in message
        # Remedy 2: the explicit opt-in (the new-household-batch case).
        assert "allow_missing=True" in message
        assert "--allow-new-index" in message

    def test_explicit_folder_argument_is_also_guarded(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            read_index(tmp_path / "nowhere")


class TestBootstrapStillWorks:
    def test_allow_missing_returns_the_empty_index(self, mapped_folder):
        index_df, index_path = read_index(allow_missing=True)
        assert index_df.empty
        assert str(index_path).endswith("index.parquet")

    def test_empty_index_carries_the_declared_columns(self, mapped_folder):
        index_df, _ = read_index(allow_missing=True)
        for column in bsv_metadata_columns:
            assert column in index_df.columns


class TestIndexPresentPathUnchanged:
    def test_existing_index_is_read_without_the_opt_in(self, mapped_folder):
        _write_index(mapped_folder, _ROWS)
        index_df, index_path = read_index()
        assert len(index_df) == len(_ROWS)
        assert str(index_path) == str(mapped_folder / "index.parquet")

    def test_opt_in_makes_no_difference_when_the_index_exists(self, mapped_folder):
        _write_index(mapped_folder, _ROWS)
        guarded, guarded_path = read_index()
        permissive, permissive_path = read_index(allow_missing=True)
        pd.testing.assert_frame_equal(guarded, permissive)
        assert guarded_path == permissive_path

    def test_existing_index_keeps_the_ids_it_was_written_with(self, mapped_folder):
        """Read-only: the registry a mapping run would look ids up from still
        holds them. Says nothing about how a NEW id would be allocated."""
        _write_index(mapped_folder, _ROWS)
        index_df, _ = read_index()
        by_key = {
            (row[ids.SUPPLIER], row[ids.PROJECT_SUPPLIER], row[ids.HOUSEHOLD_SUPPLIER]):
                int(row[ids.HOUSEHOLD])
            for _, row in index_df.iterrows()
        }
        assert by_key[("Factory Zero", "P1", "DV-VDA-1")] == 1
        assert by_key[("O-Nexus", "P8", "Address 1")] == 298
