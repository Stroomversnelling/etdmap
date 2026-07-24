"""
Contract for save_index_to_parquet as THE single registry write.

One path, one function, one file: saving the index IS saving the registry.
index.parquet holds one row per household batch (HuisBatchIdBSV).

1. Meenemen is stamped onto the index from the combined BSV metadata (the one
   intermediate for Meenemen). Households not in the metadata get NA:
   Meenemen starts empty when a household is first mapped; the researcher
   fills it in afterwards in the metadata administration.
2. The household-batch columns are added: HuisBatchIdBSV (assigned locally --
   the external HuisBatch table is a hand-maintained copy of these ids,
   never a source of new rows) plus the batch fields (BatchIdBSV,
   Gegevensfrequentie, Leverancierfrequentie, Startdatum/Einddatum) joined
   from the synced HuisBatch CSV.
3. index.parquet is written.

Pending households (no synced row yet) keep NA batch fields, are warned
about, and listed in a paste-ready proposal CSV. Cadence
(Gegevensfrequentie) is required only for rows with Meenemen == True.
"""

import pandas as pd
import pytest

import etdmap
from etdmap.index_helpers import (
    HuisBatchOverlapError,
    read_index,
    save_index_to_parquet,
    validate_gegevensfrequentie_present,
)

_START = "2024-05-01 00:00:00 UTC"
_END = "2025-05-01 00:00:00 UTC"
_START_EPOCH = 1714521600
_END_EPOCH = 1746057600


def _index_df(household_ids, include=None):
    n = len(household_ids)
    return pd.DataFrame({
        "HuisIdLeverancier": pd.array([f"L{h}" for h in household_ids], dtype="string"),
        "HuisIdBSV": pd.array(household_ids, dtype="Int64"),
        "Meenemen": pd.array(include if include is not None else [pd.NA] * n,
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


def _sync_row(hid, hbid=None, batch=1, freq="5min", start=_START, end=_END):
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
    path = folder / "household_batch_sync.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _read_registry(folder):
    return pd.read_parquet(folder / "index.parquet",
                           dtype_backend="numpy_nullable")


@pytest.fixture
def registry_options(tmp_path):
    prev = (
        etdmap.options.mapped_folder_path,
        etdmap.options.bsv_metadata_file,
        etdmap.options.household_batch_csv_path,
    )
    etdmap.options.mapped_folder_path = tmp_path
    etdmap.options.bsv_metadata_file = tmp_path / "metadata.csv"
    etdmap.options.household_batch_csv_path = tmp_path / "household_batch_sync.csv"
    yield tmp_path
    (
        etdmap.options.mapped_folder_path,
        etdmap.options.bsv_metadata_file,
        etdmap.options.household_batch_csv_path,
    ) = prev


class TestSingleRegistryWrite:
    def test_one_save_writes_registry_with_batch_columns(self, registry_options):
        tmp = registry_options
        _write_bsv_metadata(tmp, [(1, True), (2, False)])
        _write_sync_csv(tmp, [_sync_row(1), _sync_row(2)])
        # index carries STALE Meenemen; the save must stamp from the metadata
        save_index_to_parquet(_index_df([1, 2], include=[False, True]))

        idx = _read_registry(tmp)
        assert idx.set_index("HuisIdBSV")["Meenemen"].to_dict() == {1: True, 2: False}
        # ids locally assigned; batch fields joined from the synced CSV
        assert (idx["HuisBatchIdBSV"] == idx["HuisIdBSV"]).all()
        assert (idx["BatchIdBSV"] == 1).all()
        assert (idx["Gegevensfrequentie"] == "5min").all()
        assert idx["Startdatum"].iloc[0] == pd.Timestamp(_START_EPOCH, unit="s", tz="UTC")
        assert idx["Einddatum"].iloc[0] == pd.Timestamp(_END_EPOCH, unit="s", tz="UTC")

    def test_epoch_second_dates_also_accepted(self, registry_options):
        tmp = registry_options
        _write_bsv_metadata(tmp, [(1, True)])
        _write_sync_csv(tmp, [_sync_row(1, start=_START_EPOCH, end=_END_EPOCH)])
        save_index_to_parquet(_index_df([1]))
        idx = _read_registry(tmp)
        assert idx["Startdatum"].iloc[0] == pd.Timestamp(_START_EPOCH, unit="s", tz="UTC")

    def test_meenemen_empty_until_reviewed(self, registry_options):
        """Meenemen starts empty when a household is first mapped -- a
        household absent from the BSV metadata gets NA and nothing raises."""
        tmp = registry_options
        _write_bsv_metadata(tmp, [(1, True)])  # HH2 not reviewed yet
        _write_sync_csv(tmp, [_sync_row(1), _sync_row(2, freq=None)])
        save_index_to_parquet(_index_df([1, 2]))
        idx = _read_registry(tmp)
        assert idx.set_index("HuisIdBSV")["Meenemen"].isna().to_dict() == {1: False, 2: True}

    def test_pending_household_present_with_na_fields(self, registry_options, caplog):
        """No synced HuisBatch row yet: the household is still IN the registry
        (rows come from the index), with NA batch fields, a warning, and a
        paste-ready proposal CSV."""
        tmp = registry_options
        _write_bsv_metadata(tmp, [(1, True)])
        _write_sync_csv(tmp, [_sync_row(1)])  # HH2 + HH3 pending
        save_index_to_parquet(_index_df([1, 2, 3]))
        idx = _read_registry(tmp)
        assert set(int(x) for x in idx["HuisIdBSV"]) == {1, 2, 3}
        assert idx.set_index("HuisIdBSV")["BatchIdBSV"].isna().to_dict() == {
            1: False, 2: True, 3: True}
        warnings = [r.message for r in caplog.records if r.levelname == "WARNING"]
        assert any("2" in w and "pending" in w.lower() for w in warnings)
        prop = pd.read_csv(tmp / "pending_household_batch_additions.csv",
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
        assert (tmp / "pending_household_batch_additions.csv").exists()
        # researcher adds the row + Meenemen externally; the next sync updates both
        _write_bsv_metadata(tmp, [(1, True), (2, True)])
        _write_sync_csv(tmp, [_sync_row(1), _sync_row(2)])
        save_index_to_parquet(_index_df([1, 2]))
        assert not (tmp / "pending_household_batch_additions.csv").exists()

    def test_synced_only_households_warned(self, registry_options, caplog):
        tmp = registry_options
        _write_bsv_metadata(tmp, [(1, True)])
        _write_sync_csv(tmp, [_sync_row(1), _sync_row(99)])
        save_index_to_parquet(_index_df([1]))
        warnings = [r.message for r in caplog.records if r.levelname == "WARNING"]
        assert any("99" in w for w in warnings)
        idx = _read_registry(tmp)
        assert set(int(x) for x in idx["HuisIdBSV"]) == {1}

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
        idx = _read_registry(tmp)
        assert idx["Gegevensfrequentie"].isna().all()

    def test_no_sync_csv_omits_batch_fields_with_warning(self, registry_options, caplog):
        tmp = registry_options
        _write_bsv_metadata(tmp, [(1, True)])
        save_index_to_parquet(_index_df([1]))  # must not raise
        idx = _read_registry(tmp)
        # HuisBatchIdBSV is always assigned; the synced batch fields are not
        assert (idx["HuisBatchIdBSV"] == idx["HuisIdBSV"]).all()
        assert "Gegevensfrequentie" not in idx.columns
        assert any("household-batch fields" in r.message for r in caplog.records
                   if r.levelname == "WARNING")

    def test_no_bsv_metadata_keeps_index_meenemen(self, registry_options):
        """Legacy / fixture datasets without a BSV metadata file: the index
        Meenemen values pass through unchanged."""
        tmp = registry_options
        _write_sync_csv(tmp, [_sync_row(1), _sync_row(2)])
        save_index_to_parquet(_index_df([1, 2], include=[True, False]))
        idx = _read_registry(tmp)
        assert idx.set_index("HuisIdBSV")["Meenemen"].to_dict() == {1: True, 2: False}

    def test_idempotent_rerun(self, registry_options):
        tmp = registry_options
        _write_bsv_metadata(tmp, [(1, True)])
        _write_sync_csv(tmp, [_sync_row(1)])
        save_index_to_parquet(_index_df([1]))
        save_index_to_parquet(_index_df([1]))
        idx = _read_registry(tmp)
        assert len(idx) == 1


class TestGegevensfrequentieValidator:
    def _registry_df(self, rows, gegevensfrequentie="5min"):
        """rows: list of (HuisIdBSV, HuisBatchIdBSV, Meenemen)."""
        return pd.DataFrame({
            "HuisIdBSV": pd.array([r[0] for r in rows], dtype="Int64"),
            "HuisBatchIdBSV": pd.array([r[1] for r in rows], dtype="Int64"),
            "Meenemen": pd.array([r[2] for r in rows], dtype="boolean"),
            "Gegevensfrequentie": pd.array(
                [gegevensfrequentie] * len(rows), dtype="string"),
        })

    def test_all_present_passes(self):
        validate_gegevensfrequentie_present(
            self._registry_df([(1, 1, True), (2, 2, True)]))

    def test_missing_na_for_included_raises(self):
        df = self._registry_df([(1, 1, True), (2, 2, True)])
        df.loc[1, "Gegevensfrequentie"] = pd.NA
        with pytest.raises(ValueError):
            validate_gegevensfrequentie_present(df)

    def test_empty_string_for_included_raises(self):
        df = self._registry_df([(1, 1, True)])
        df.loc[0, "Gegevensfrequentie"] = ""
        with pytest.raises(ValueError):
            validate_gegevensfrequentie_present(df)

    def test_missing_tolerated_when_not_included(self):
        df = self._registry_df([(1, 1, False), (2, 2, pd.NA)],
                               gegevensfrequentie=None)
        validate_gegevensfrequentie_present(df)  # must not raise

    def test_absent_column_passes(self):
        # An older dataset's registry without batch fields passes unchecked.
        df = pd.DataFrame({
            "HuisIdBSV": pd.array([1], dtype="Int64"),
            "Meenemen": pd.array([True], dtype="boolean"),
        })
        validate_gegevensfrequentie_present(df)


class TestReadIndexGuard:
    def test_read_raises_on_included_row_without_cadence(self, registry_options):
        tmp = registry_options
        # Bypass the save-time guard: write a corrupt registry directly.
        df = _index_df([1], include=[True])
        df["HuisBatchIdBSV"] = pd.array([1], dtype="Int64")
        df["Gegevensfrequentie"] = pd.array([pd.NA], dtype="string")
        df.to_parquet(tmp / "index.parquet")
        with pytest.raises(ValueError):
            read_index(tmp)

    def test_read_accepts_registry_without_batch_columns(self, registry_options):
        tmp = registry_options
        _index_df([1, 2], include=[True, False]).to_parquet(tmp / "index.parquet")
        idx, path = read_index(tmp)
        assert len(idx) == 2
        assert str(tmp) in path
