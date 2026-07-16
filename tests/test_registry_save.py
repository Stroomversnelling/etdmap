"""
Contract for save_index_to_parquet as THE single registry write.

One path, one function: saving the index IS saving the registry. There is no
separate batch-registry update step. One call, one state, both files:

1. Meenemen is stamped onto the index from the combined BSV metadata (the one
   intermediate for Meenemen -- see the canonical authority chain). Households
   not in the metadata get NA: Meenemen starts empty when a household is first
   mapped; the researcher fills it in afterwards in the metadata
administration.
2. index.parquet is written.
3. batch_index.parquet is written FROM THE SAME index rows (HuisBatchIdBSV =
   HuisIdBSV; ids are assigned locally -- the external HuisBatch table is a
   hand-maintained copy of them, never a source of new rows). Batch fields
   (BatchIdBSV, Gegevensfrequentie, Leverancierfrequentie,
   Startdatum/Einddatum) are joined from the synced HuisBatch CSV;
   Meenemen is carried over from step 1, so the two files cannot disagree
   (they once diverged when separate code paths updated them at different
   times).

Pending households (no synced row yet) are present in batch_index with NA batch
fields, warned about, and listed in a paste-ready proposal CSV. Cadence
(Gegevensfrequentie) is required only for rows with Meenemen == True.
"""

import pandas as pd
import pytest

import etdmap
from etdmap.index_helpers import (
    HuisBatchOverlapError,
    read_batch_index,
    save_index_to_parquet,
)

_START = "2024-05-01 00:00:00 UTC"
_END = "2025-05-01 00:00:00 UTC"
_START_EPOCH = 1714521600
_END_EPOCH = 1746057600


def _index_df(huis_ids, meenemen=None):
    n = len(huis_ids)
    return pd.DataFrame({
        "HuisIdLeverancier": pd.array([f"L{h}" for h in huis_ids], dtype="string"),
        "HuisIdBSV": pd.array(huis_ids, dtype="Int64"),
        "Meenemen": pd.array(meenemen if meenemen is not None else [pd.NA] * n,
                             dtype="boolean"),
        "ProjectIdLeverancier": pd.array(["P1"] * n, dtype="string"),
        "ProjectIdBSV": pd.array([1] * n, dtype="Int64"),
        "Notities": pd.array([""] * n, dtype="string"),
        "Dataleverancier": pd.array(["etdmap"] * n, dtype="string"),
    })


def _write_bsv_metadata(folder, rows):
    """rows: list of (HuisIdBSV, Meenemen). Written as the combined BSV
    metadata CSV (only the columns the Meenemen stamp reads)."""
    path = folder / "metadata.csv"
    pd.DataFrame({
        "HuisIdBSV": pd.array([r[0] for r in rows], dtype="Int64"),
        "Meenemen": pd.array([r[1] for r in rows], dtype="boolean"),
    }).to_csv(path, index=False)
    return path


def _sync_row(hid, hbid=None, batch=1, freq="5-minute", start=_START, end=_END):
    return {
        "HuisIdBSV": hid,
        "HuisBatchIdBSV": hbid if hbid is not None else hid,
        "BatchIdBSV": batch,
        "ProjectIdBSV": 1,
        "Meenemen": True,  # ignored: Meenemen comes via the BSV metadata only
        "Gegevensfrequentie": freq,
        "Leverancierfrequentie": "Nog fijner",
        "Startdatum": start,
        "Einddatum": end,
    }


def _write_sync_csv(folder, rows):
    path = folder / "huisbatch_sync.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


@pytest.fixture
def registry_options(tmp_path):
    prev = (
        etdmap.options.mapped_folder_path,
        etdmap.options.bsv_metadata_file,
        etdmap.options.huisbatch_csv_path,
    )
    etdmap.options.mapped_folder_path = tmp_path
    etdmap.options.bsv_metadata_file = tmp_path / "metadata.csv"
    etdmap.options.huisbatch_csv_path = tmp_path / "huisbatch_sync.csv"
    yield tmp_path
    (
        etdmap.options.mapped_folder_path,
        etdmap.options.bsv_metadata_file,
        etdmap.options.huisbatch_csv_path,
    ) = prev


class TestSingleRegistryWrite:
    def test_one_save_writes_both_files_consistently(self, registry_options):
        tmp = registry_options
        _write_bsv_metadata(tmp, [(1, True), (2, False)])
        _write_sync_csv(tmp, [_sync_row(1), _sync_row(2)])
        # index carries STALE Meenemen; the save must stamp from the metadata
        save_index_to_parquet(_index_df([1, 2], meenemen=[False, True]))

        idx = pd.read_parquet(tmp / "index.parquet", dtype_backend="numpy_nullable")
        bi, _ = read_batch_index(tmp)
        assert idx.set_index("HuisIdBSV")["Meenemen"].to_dict() == {1: True, 2: False}
        assert bi.set_index("HuisIdBSV")["Meenemen"].to_dict() == {1: True, 2: False}
        # rows from the index; ids locally minted
        assert (bi["HuisBatchIdBSV"] == bi["HuisIdBSV"]).all()
        assert (bi["BatchIdBSV"] == 1).all()
        assert (bi["Gegevensfrequentie"] == "5-minute").all()
        assert bi["Startdatum"].iloc[0] == pd.Timestamp(_START_EPOCH, unit="s", tz="UTC")
        assert bi["Einddatum"].iloc[0] == pd.Timestamp(_END_EPOCH, unit="s", tz="UTC")

    def test_epoch_second_dates_also_accepted(self, registry_options):
        tmp = registry_options
        _write_bsv_metadata(tmp, [(1, True)])
        _write_sync_csv(tmp, [_sync_row(1, start=_START_EPOCH, end=_END_EPOCH)])
        save_index_to_parquet(_index_df([1]))
        bi, _ = read_batch_index(tmp)
        assert bi["Startdatum"].iloc[0] == pd.Timestamp(_START_EPOCH, unit="s", tz="UTC")

    def test_meenemen_empty_until_reviewed(self, registry_options):
        """Meenemen starts empty when a household is first mapped -- a
        household absent from the BSV metadata gets NA in BOTH files and
        nothing raises."""
        tmp = registry_options
        _write_bsv_metadata(tmp, [(1, True)])  # HH2 not reviewed yet
        _write_sync_csv(tmp, [_sync_row(1), _sync_row(2, freq=None)])
        save_index_to_parquet(_index_df([1, 2]))
        idx = pd.read_parquet(tmp / "index.parquet", dtype_backend="numpy_nullable")
        bi, _ = read_batch_index(tmp)
        assert idx.set_index("HuisIdBSV")["Meenemen"].isna().to_dict() == {1: False, 2: True}
        assert bi.set_index("HuisIdBSV")["Meenemen"].isna().to_dict() == {1: False, 2: True}

    def test_pending_household_present_with_na_fields(self, registry_options, caplog):
        """No synced HuisBatch row yet: the household is still IN the registry
        (rows come from the index), with NA batch fields, a warning, and a
        paste-ready proposal CSV."""
        tmp = registry_options
        _write_bsv_metadata(tmp, [(1, True)])
        _write_sync_csv(tmp, [_sync_row(1)])  # HH2 + HH3 pending
        save_index_to_parquet(_index_df([1, 2, 3]))
        bi, _ = read_batch_index(tmp)
        assert set(int(x) for x in bi["HuisIdBSV"]) == {1, 2, 3}
        assert bi.set_index("HuisIdBSV")["BatchIdBSV"].isna().to_dict() == {
            1: False, 2: True, 3: True}
        warnings = [r.message for r in caplog.records if r.levelname == "WARNING"]
        assert any("2" in w and "pending" in w.lower() for w in warnings)
        prop = pd.read_csv(tmp / "pending_huisbatch_additions.csv",
                           dtype_backend="numpy_nullable")
        assert sorted(prop["HuisIdBSV"].tolist()) == [2, 3]
        assert (prop["HuisBatchIdBSV"] == prop["HuisIdBSV"]).all()

    def test_pending_file_removed_when_resolved(self, registry_options):
        tmp = registry_options
        # HH2 pending: absent from BOTH intermediates (Meenemen lives on the
        # external HuisBatch table, so a household without its row cannot have
        # a Meenemen value either).
        _write_bsv_metadata(tmp, [(1, True)])
        _write_sync_csv(tmp, [_sync_row(1)])
        save_index_to_parquet(_index_df([1, 2]))
        assert (tmp / "pending_huisbatch_additions.csv").exists()
        # researcher adds the row + Meenemen externally; the next sync updates both
        _write_bsv_metadata(tmp, [(1, True), (2, True)])
        _write_sync_csv(tmp, [_sync_row(1), _sync_row(2)])
        save_index_to_parquet(_index_df([1, 2]))
        assert not (tmp / "pending_huisbatch_additions.csv").exists()

    def test_synced_only_households_warned(self, registry_options, caplog):
        tmp = registry_options
        _write_bsv_metadata(tmp, [(1, True)])
        _write_sync_csv(tmp, [_sync_row(1), _sync_row(99)])
        save_index_to_parquet(_index_df([1]))
        warnings = [r.message for r in caplog.records if r.levelname == "WARNING"]
        assert any("99" in w for w in warnings)
        bi, _ = read_batch_index(tmp)
        assert set(int(x) for x in bi["HuisIdBSV"]) == {1}

    def test_two_batches_in_synced_table_raises(self, registry_options):
        tmp = registry_options
        _write_bsv_metadata(tmp, [(1, True), (2, True)])
        _write_sync_csv(tmp, [
            _sync_row(1), _sync_row(2), _sync_row(1, hbid=9, batch=2),
        ])
        with pytest.raises(HuisBatchOverlapError):
            save_index_to_parquet(_index_df([1, 2]))

    def test_missing_cadence_for_included_household_raises(self, registry_options):
        tmp = registry_options
        _write_bsv_metadata(tmp, [(1, True)])  # included -> cadence required
        _write_sync_csv(tmp, [_sync_row(1, freq=None)])
        with pytest.raises(ValueError):
            save_index_to_parquet(_index_df([1]))

    def test_missing_cadence_tolerated_when_not_included(self, registry_options):
        tmp = registry_options
        _write_bsv_metadata(tmp, [(1, False)])  # reviewed and excluded
        _write_sync_csv(tmp, [_sync_row(1, freq=None)])
        save_index_to_parquet(_index_df([1]))  # must not raise
        bi, _ = read_batch_index(tmp)
        assert bi["Gegevensfrequentie"].isna().all()

    def test_no_sync_csv_skips_batch_file_with_warning(self, registry_options, caplog):
        tmp = registry_options
        _write_bsv_metadata(tmp, [(1, True)])
        save_index_to_parquet(_index_df([1]))  # must not raise
        assert (tmp / "index.parquet").exists()
        assert not (tmp / "batch_index.parquet").exists()
        assert any("batch_index" in r.message for r in caplog.records
                   if r.levelname == "WARNING")

    def test_no_bsv_metadata_keeps_index_meenemen(self, registry_options):
        """Legacy / fixture datasets without a BSV metadata file: the index
        Meenemen values pass through unchanged and are still carried into
        the batch file."""
        tmp = registry_options
        _write_sync_csv(tmp, [_sync_row(1), _sync_row(2)])
        save_index_to_parquet(_index_df([1, 2], meenemen=[True, False]))
        idx = pd.read_parquet(tmp / "index.parquet", dtype_backend="numpy_nullable")
        bi, _ = read_batch_index(tmp)
        assert idx.set_index("HuisIdBSV")["Meenemen"].to_dict() == {1: True, 2: False}
        assert bi.set_index("HuisIdBSV")["Meenemen"].to_dict() == {1: True, 2: False}

    def test_idempotent_rerun(self, registry_options):
        tmp = registry_options
        _write_bsv_metadata(tmp, [(1, True)])
        _write_sync_csv(tmp, [_sync_row(1)])
        save_index_to_parquet(_index_df([1]))
        save_index_to_parquet(_index_df([1]))
        bi, _ = read_batch_index(tmp)
        assert len(bi) == 1
