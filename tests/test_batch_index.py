"""
Contract for the batch_index registry files: save / read guards.

batch_index.parquet is written together with index.parquet by
save_index_to_parquet (the single registry write; see test_registry_save.py
for that contract). This file pins the SAVE/READ guards in isolation:

- `save_batch_index(df, folder)` validates before writing (Gegevensfrequentie
  present on rows with Meenemen == True; HuisBatchIdBSV unique).
- `read_batch_index(folder)` returns (df, path) like read_index, and WIRES THE
  GUARDS ON LOAD: cadence check, and -- while a legacy index.parquet exists in
  the same folder -- the 1:1 correspondence (HuisBatchOverlapError on a
  household in >1 batch; ValueError on a batch row no index knows). With no
  index.parquet present (the post-switch state), the correspondence check is
  skipped by design.
- The etdmap test fixture (mapped_fixtures) produces batch_index.parquet via
  the REAL write path (a fixture HuisBatch sync CSV next to the fixture BSV
  metadata), so downstream suites see both registries.
"""

import pandas as pd
import pytest

import etdmap
from etdmap.index_helpers import (
    HuisBatchOverlapError,
    read_batch_index,
    save_batch_index,
)


def _index_df(huis_ids, meenemen=True):
    return pd.DataFrame({
        "HuisIdBSV": pd.array(huis_ids, dtype="Int64"),
        "ProjectIdBSV": pd.array([1] * len(huis_ids), dtype="Int64"),
        "Meenemen": pd.array([meenemen] * len(huis_ids), dtype="boolean"),
    })


def _bi(huis_ids, meenemen=True, freq="5-minute"):
    """A batch_index frame in the on-disk schema (1:1 ids, nullable dtypes)."""
    n = len(huis_ids)
    return pd.DataFrame({
        "HuisIdBSV": pd.array(huis_ids, dtype="Int64"),
        "HuisBatchIdBSV": pd.array(huis_ids, dtype="Int64"),
        "BatchIdBSV": pd.array([1] * n, dtype="Int64"),
        "ProjectIdBSV": pd.array([1] * n, dtype="Int64"),
        "Meenemen": pd.array([meenemen] * n, dtype="boolean"),
        "Gegevensfrequentie": pd.array([freq] * n, dtype="string"),
        "Leverancierfrequentie": pd.array([pd.NA] * n, dtype="string"),
    })


class TestSaveAndReadBatchIndex:
    def test_roundtrip(self, tmp_path):
        bi = _bi([1, 2])
        path = save_batch_index(bi, tmp_path)
        assert path.endswith("batch_index.parquet")
        back, back_path = read_batch_index(tmp_path)
        assert back_path == path
        pd.testing.assert_frame_equal(
            back[["HuisIdBSV", "HuisBatchIdBSV", "Gegevensfrequentie"]],
            bi[["HuisIdBSV", "HuisBatchIdBSV", "Gegevensfrequentie"]],
        )

    def test_save_rejects_missing_gegevensfrequentie_for_included_row(self, tmp_path):
        bi = _bi([1, 2])  # Meenemen True on both
        bi.loc[1, "Gegevensfrequentie"] = pd.NA
        with pytest.raises(ValueError):
            save_batch_index(bi, tmp_path)

    def test_save_tolerates_missing_gegevensfrequentie_when_not_included(self, tmp_path):
        bi = _bi([1, 2], meenemen=False)
        bi.loc[1, "Gegevensfrequentie"] = pd.NA
        save_batch_index(bi, tmp_path)  # must not raise

    def test_save_rejects_duplicate_huisbatchid(self, tmp_path):
        bi = _bi([1, 2])
        bi.loc[1, "HuisBatchIdBSV"] = 1  # duplicate primary key
        with pytest.raises(ValueError):
            save_batch_index(bi, tmp_path)

    def test_read_missing_file_raises_filenotfound(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            read_batch_index(tmp_path)


class TestGuardsWiredOnRead:
    def _write_index(self, folder, huis_ids):
        _index_df(huis_ids).to_parquet(folder / "index.parquet")

    def test_overlap_with_legacy_index_raises(self, tmp_path):
        # A household in two batches while index.parquet coexists -> forced switch.
        self._write_index(tmp_path, [1, 2])
        bi = pd.DataFrame({
            "HuisIdBSV": pd.array([1, 2, 2], dtype="Int64"),
            "HuisBatchIdBSV": pd.array([1, 2, 9], dtype="Int64"),
            "Gegevensfrequentie": pd.array(["5-minute"] * 3, dtype="string"),
        })
        bi.to_parquet(tmp_path / "batch_index.parquet")  # bypass save's checks
        with pytest.raises(HuisBatchOverlapError):
            read_batch_index(tmp_path)

    def test_index_only_household_is_pending_warning_not_error(self, tmp_path, caplog):
        # Index has HH3 but the batch registry does not (an older registry
        # file): HH3 is PENDING -- warned about, never an error on read.
        self._write_index(tmp_path, [1, 2, 3])
        save_batch_index(_bi([1, 2]), tmp_path)
        back, _ = read_batch_index(tmp_path)  # must NOT raise
        assert len(back) == 2
        warnings = [r.message for r in caplog.records if r.levelname == "WARNING"]
        assert any("3" in w for w in warnings)

    def test_batch_only_household_raises(self, tmp_path):
        # A batch row for a household that exists in NO index is impossible data
        # (ids are minted from the index) -> hard error, unchanged.
        self._write_index(tmp_path, [1])
        save_batch_index(_bi([1, 2]), tmp_path)
        with pytest.raises(ValueError):
            read_batch_index(tmp_path)

    def test_no_legacy_index_skips_correspondence(self, tmp_path):
        # Post-switch state: index.parquet retired; overlap is then legal.
        bi = pd.DataFrame({
            "HuisIdBSV": pd.array([1, 1], dtype="Int64"),
            "HuisBatchIdBSV": pd.array([1, 9], dtype="Int64"),
            "Gegevensfrequentie": pd.array(["5-minute", "15-minute"], dtype="string"),
        })
        bi.to_parquet(tmp_path / "batch_index.parquet")
        back, _ = read_batch_index(tmp_path)
        assert len(back) == 2

    def test_read_rejects_missing_gegevensfrequentie_for_included_row(self, tmp_path):
        bi = _bi([1])  # Meenemen True
        bi["Gegevensfrequentie"] = pd.array([pd.NA], dtype="string")
        bi.to_parquet(tmp_path / "batch_index.parquet")
        with pytest.raises(ValueError):
            read_batch_index(tmp_path)


class TestFixtureEmitsBatchIndex:
    def test_mapped_fixture_has_guarded_batch_index(self, mapped_fixtures):
        """The 10-HH fixture produces batch_index.parquet via the REAL write
        path, and it loads through the guarded reader (1:1 with the index)."""
        bi, _ = read_batch_index(mapped_fixtures)  # guards run on load
        index_df, _ = etdmap.index_helpers.read_index()
        assert set(bi["HuisIdBSV"].tolist()) == set(index_df["HuisIdBSV"].tolist())
        assert (bi["HuisBatchIdBSV"] == bi["HuisIdBSV"]).all()
        assert (bi["Gegevensfrequentie"] == "5-minute").all()
        # First-mapping state: Meenemen is EMPTY in both registry files until
        # the researcher reviews the households (the update_meenemen tests
        # exercise that transition later in the suite).
        assert bi["Meenemen"].isna().all()
        assert index_df["Meenemen"].isna().all()
