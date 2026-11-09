"""
Contract for save_index_to_parquet as THE single index write.

One path, one function, one file: index.parquet is the whole index.
index.parquet holds one row per household batch (ids.HOUSEHOLD_BATCH).

1. Meenemen is stamped onto the index from the combined BSV metadata (the one
   intermediate for Meenemen). Households not in the metadata get NA:
   Meenemen starts empty when a household is first mapped; the researcher
   fills it in afterwards in the metadata administration.
2. The household-batch columns are added: ids.HOUSEHOLD_BATCH (assigned locally --
   the external HuisBatch table is a hand-maintained copy of these ids,
   never a source of new rows) plus the household-batch fields (ids.BATCH,
   Gegevensfrequentie, Leverancierfrequentie, Startdatum/Einddatum) joined
   from the synced HuisBatch CSV.
3. index.parquet is written.

Pending households (no synced row yet) keep NA household-batch fields, are warned
about, and listed in a paste-ready proposal CSV. Cadence
(Gegevensfrequentie) is required only for rows with Meenemen == True.
"""

import pandas as pd
import pytest

import etdmap
from etdmap import ids
from etdmap.index_helpers import (
    HouseBatchOverlapError,
    read_index,
    save_index_to_parquet,
    validate_data_frequency_present,
)
from etdmap.testing import (
    index_frame,
    one_to_one_test_household_batch_of,
    one_to_one_test_household_of_household_batch,
)

_START = "2024-05-01 00:00:00 UTC"
_END = "2025-05-01 00:00:00 UTC"
_START_EPOCH = 1714521600
_END_EPOCH = 1746057600

#: The household batch each household here is indexed through. This write resolves
#: everything by household -- the Meenemen stamp, the synced join, the pending
#: list and the synced-only warning -- so the household batches are derived from the
#: household rather than stated beside it.
_HOUSEHOLD_BATCHES = {
    household_id: one_to_one_test_household_batch_of(household_id)
    for household_id in (1, 2, 3, 99)
}

#: A second household batch of household 1, for a synced table holding two deliveries
#: of one household.
_SECOND_HOUSEHOLD_BATCH = 2001


def _write_bsv_metadata(folder, household_batch_ids, include):
    """Write the combined BSV metadata CSV. The Meenemen stamp reads
    ids.HOUSEHOLD and Meenemen from it."""
    path = folder / "metadata.csv"
    index_frame(
        household_batch_ids, batch_fields=False, Meenemen=include
    ).to_csv(path, index=False)
    return path


# Local: the synced DateTime columns in both forms the write accepts --
# formatted strings and epoch seconds -- are the subject of these tests.
def _sync_row(household_batch_id, household_id=None, delivery_batch=1, freq="5min",
              start=_START, end=_END):
    return {
        ids.HOUSEHOLD: (
            one_to_one_test_household_of_household_batch(household_batch_id)
            if household_id is None
            else household_id
        ),
        ids.HOUSEHOLD_BATCH: household_batch_id,
        ids.BATCH: delivery_batch,
        ids.PROJECT: 1,
        "Meenemen": True,  # ignored: Meenemen comes via the BSV metadata only
        "Gegevensfrequentie": freq,
        "Leverancierfrequentie": "Nog fijner",
        "Startdatum": start,
        "Einddatum": end,
    }


def _write_sync_csv(folder, rows):
    path = folder / "household_batch_sync.csv"
    pd.DataFrame(rows).convert_dtypes().to_csv(path, index=False)
    return path


def _read_index(folder):
    return pd.read_parquet(folder / "index.parquet",
                           dtype_backend="numpy_nullable")


@pytest.fixture
def index_options(tmp_path):
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


class TestSingleIndexWrite:
    def test_one_save_writes_index_with_batch_columns(self, index_options):
        tmp = index_options
        _write_bsv_metadata(tmp, [_HOUSEHOLD_BATCHES[1], _HOUSEHOLD_BATCHES[2]], [True, False])
        _write_sync_csv(tmp, [_sync_row(_HOUSEHOLD_BATCHES[1]), _sync_row(_HOUSEHOLD_BATCHES[2])])
        # index carries STALE Meenemen; the save must stamp from the metadata
        save_index_to_parquet(index_frame(
            [_HOUSEHOLD_BATCHES[1], _HOUSEHOLD_BATCHES[2]],
            batch_fields=False,
            Meenemen=[False, True],
        ))

        idx = _read_index(tmp)
        assert idx.set_index(ids.HOUSEHOLD_BATCH)["Meenemen"].to_dict() == {
            _HOUSEHOLD_BATCHES[1]: True, _HOUSEHOLD_BATCHES[2]: False}
        # The household batch id comes from the synced CSV, not from the household: the
        # two differ here, so a derived key would fail this.
        assert sorted(idx[ids.HOUSEHOLD_BATCH]) == [_HOUSEHOLD_BATCHES[1], _HOUSEHOLD_BATCHES[2]]
        assert not (idx[ids.HOUSEHOLD_BATCH] == idx[ids.HOUSEHOLD]).any()
        assert (idx[ids.BATCH] == 1).all()
        assert (idx["Gegevensfrequentie"] == "5min").all()
        assert idx["Startdatum"].iloc[0] == pd.Timestamp(_START_EPOCH, unit="s", tz="UTC")
        assert idx["Einddatum"].iloc[0] == pd.Timestamp(_END_EPOCH, unit="s", tz="UTC")

    def test_epoch_second_dates_also_accepted(self, index_options):
        tmp = index_options
        _write_bsv_metadata(tmp, [_HOUSEHOLD_BATCHES[1]], True)
        _write_sync_csv(tmp, [
            _sync_row(_HOUSEHOLD_BATCHES[1], start=_START_EPOCH, end=_END_EPOCH),
        ])
        save_index_to_parquet(index_frame([_HOUSEHOLD_BATCHES[1]], batch_fields=False))
        idx = _read_index(tmp)
        assert idx["Startdatum"].iloc[0] == pd.Timestamp(_START_EPOCH, unit="s", tz="UTC")

    def test_meenemen_empty_until_reviewed(self, index_options):
        """Meenemen starts empty when a household is first mapped -- a
        household absent from the BSV metadata gets NA and nothing raises."""
        tmp = index_options
        _write_bsv_metadata(tmp, [_HOUSEHOLD_BATCHES[1]], True)  # HH2 not reviewed yet
        _write_sync_csv(tmp, [
            _sync_row(_HOUSEHOLD_BATCHES[1]), _sync_row(_HOUSEHOLD_BATCHES[2], freq=pd.NA),
        ])
        save_index_to_parquet(index_frame(
            [_HOUSEHOLD_BATCHES[1], _HOUSEHOLD_BATCHES[2]], batch_fields=False
        ))
        idx = _read_index(tmp)
        assert idx.set_index(ids.HOUSEHOLD_BATCH)["Meenemen"].isna().to_dict() == {
            _HOUSEHOLD_BATCHES[1]: False, _HOUSEHOLD_BATCHES[2]: True}

    def test_pending_household_present_with_na_fields(self, index_options, caplog):
        """No synced HuisBatch row yet: the household is still IN the index
        (rows come from the index), with NA household-batch fields, a warning, and a
        paste-ready proposal CSV."""
        tmp = index_options
        _write_bsv_metadata(tmp, [_HOUSEHOLD_BATCHES[1]], True)
        _write_sync_csv(tmp, [_sync_row(_HOUSEHOLD_BATCHES[1])])  # HH2 + HH3 pending
        save_index_to_parquet(index_frame(
            [_HOUSEHOLD_BATCHES[1], _HOUSEHOLD_BATCHES[2], _HOUSEHOLD_BATCHES[3]], batch_fields=False
        ))
        idx = _read_index(tmp)
        assert set(int(x) for x in idx[ids.HOUSEHOLD]) == {1, 2, 3}
        # Keyed on the HOUSEHOLD here, and only here: a pending household has
        # no synced row, so its household batch id is exactly what is absent.
        # Keying on it would collapse every pending household into one pd.NA.
        assert idx.set_index(ids.HOUSEHOLD)[ids.BATCH].isna().to_dict() == {
            1: False, 2: True, 3: True}
        warnings = [r.message for r in caplog.records if r.levelname == "WARNING"]
        assert any("2" in w and "pending" in w.lower() for w in warnings)
        prop = pd.read_csv(tmp / "pending_household_batch_additions.csv",
                           dtype_backend="numpy_nullable")
        assert sorted(prop[ids.HOUSEHOLD].tolist()) == [2, 3]
        assert (prop[ids.HOUSEHOLD_BATCH] == prop[ids.HOUSEHOLD]).all()

    def test_pending_file_removed_when_resolved(self, index_options):
        tmp = index_options
        # HH2 pending: absent from BOTH intermediates (Meenemen lives on the
        # external HuisBatch table, so a household without its row cannot have
        # a Meenemen value either).
        _write_bsv_metadata(tmp, [_HOUSEHOLD_BATCHES[1]], True)
        _write_sync_csv(tmp, [_sync_row(_HOUSEHOLD_BATCHES[1])])
        save_index_to_parquet(index_frame(
            [_HOUSEHOLD_BATCHES[1], _HOUSEHOLD_BATCHES[2]], batch_fields=False
        ))
        assert (tmp / "pending_household_batch_additions.csv").exists()
        # researcher adds the row + Meenemen externally; the next sync updates both
        _write_bsv_metadata(tmp, [_HOUSEHOLD_BATCHES[1], _HOUSEHOLD_BATCHES[2]], True)
        _write_sync_csv(tmp, [_sync_row(_HOUSEHOLD_BATCHES[1]), _sync_row(_HOUSEHOLD_BATCHES[2])])
        save_index_to_parquet(index_frame(
            [_HOUSEHOLD_BATCHES[1], _HOUSEHOLD_BATCHES[2]], batch_fields=False
        ))
        assert not (tmp / "pending_household_batch_additions.csv").exists()

    def test_synced_only_households_warned(self, index_options, caplog):
        tmp = index_options
        _write_bsv_metadata(tmp, [_HOUSEHOLD_BATCHES[1]], True)
        _write_sync_csv(tmp, [_sync_row(_HOUSEHOLD_BATCHES[1]), _sync_row(_HOUSEHOLD_BATCHES[99])])
        save_index_to_parquet(index_frame([_HOUSEHOLD_BATCHES[1]], batch_fields=False))
        warnings = [r.message for r in caplog.records if r.levelname == "WARNING"]
        assert any("99" in w for w in warnings)
        idx = _read_index(tmp)
        assert set(int(x) for x in idx[ids.HOUSEHOLD]) == {1}

    def test_two_batches_in_synced_table_raises(self, index_options):
        tmp = index_options
        _write_bsv_metadata(tmp, [_HOUSEHOLD_BATCHES[1], _HOUSEHOLD_BATCHES[2]], True)
        _write_sync_csv(tmp, [
            _sync_row(_HOUSEHOLD_BATCHES[1]),
            _sync_row(_HOUSEHOLD_BATCHES[2]),
            _sync_row(
                _SECOND_HOUSEHOLD_BATCH,
                household_id=one_to_one_test_household_of_household_batch(_HOUSEHOLD_BATCHES[1]),
                delivery_batch=2,
            ),
        ])
        with pytest.raises(HouseBatchOverlapError):
            save_index_to_parquet(index_frame(
                [_HOUSEHOLD_BATCHES[1], _HOUSEHOLD_BATCHES[2]], batch_fields=False
            ))

    def test_missing_cadence_for_included_household_raises(self, index_options):
        tmp = index_options
        # included -> cadence required
        _write_bsv_metadata(tmp, [_HOUSEHOLD_BATCHES[1]], True)
        _write_sync_csv(tmp, [_sync_row(_HOUSEHOLD_BATCHES[1], freq=pd.NA)])
        with pytest.raises(ValueError):
            save_index_to_parquet(index_frame([_HOUSEHOLD_BATCHES[1]], batch_fields=False))

    def test_missing_cadence_tolerated_when_not_included(self, index_options):
        tmp = index_options
        # reviewed and excluded
        _write_bsv_metadata(tmp, [_HOUSEHOLD_BATCHES[1]], False)
        _write_sync_csv(tmp, [_sync_row(_HOUSEHOLD_BATCHES[1], freq=pd.NA)])
        # must not raise
        save_index_to_parquet(index_frame([_HOUSEHOLD_BATCHES[1]], batch_fields=False))
        idx = _read_index(tmp)
        assert idx["Gegevensfrequentie"].isna().all()

    def test_no_sync_csv_omits_batch_fields_with_warning(self, index_options, caplog):
        tmp = index_options
        _write_bsv_metadata(tmp, [_HOUSEHOLD_BATCHES[1]], True)
        # must not raise
        save_index_to_parquet(index_frame([_HOUSEHOLD_BATCHES[1]], batch_fields=False))
        idx = _read_index(tmp)
        # The household batch id is a synced household-batch field, so without the synced table it
        # is omitted with the rest rather than invented from the household.
        assert ids.HOUSEHOLD_BATCH not in idx.columns
        assert "Gegevensfrequentie" not in idx.columns
        assert any("household-batch fields" in r.message for r in caplog.records
                   if r.levelname == "WARNING")

    def test_no_bsv_metadata_keeps_index_meenemen(self, index_options):
        """Legacy / fixture datasets without a BSV metadata file: the index
        Meenemen values pass through unchanged."""
        tmp = index_options
        _write_sync_csv(tmp, [_sync_row(_HOUSEHOLD_BATCHES[1]), _sync_row(_HOUSEHOLD_BATCHES[2])])
        save_index_to_parquet(index_frame(
            [_HOUSEHOLD_BATCHES[1], _HOUSEHOLD_BATCHES[2]],
            batch_fields=False,
            Meenemen=[True, False],
        ))
        idx = _read_index(tmp)
        assert idx.set_index(ids.HOUSEHOLD_BATCH)["Meenemen"].to_dict() == {
            _HOUSEHOLD_BATCHES[1]: True, _HOUSEHOLD_BATCHES[2]: False}

    def test_idempotent_rerun(self, index_options):
        tmp = index_options
        _write_bsv_metadata(tmp, [_HOUSEHOLD_BATCHES[1]], True)
        _write_sync_csv(tmp, [_sync_row(_HOUSEHOLD_BATCHES[1])])
        save_index_to_parquet(index_frame([_HOUSEHOLD_BATCHES[1]], batch_fields=False))
        save_index_to_parquet(index_frame([_HOUSEHOLD_BATCHES[1]], batch_fields=False))
        idx = _read_index(tmp)
        assert len(idx) == 1


class TestDatasetFrequencyValidator:
    def test_all_present_passes(self):
        validate_data_frequency_present(
            index_frame([_HOUSEHOLD_BATCHES[1], _HOUSEHOLD_BATCHES[2]], Meenemen=True))

    def test_missing_na_for_included_raises(self):
        df = index_frame([_HOUSEHOLD_BATCHES[1], _HOUSEHOLD_BATCHES[2]], Meenemen=True)
        df.loc[1, "Gegevensfrequentie"] = pd.NA
        with pytest.raises(ValueError):
            validate_data_frequency_present(df)

    def test_empty_string_for_included_raises(self):
        df = index_frame([_HOUSEHOLD_BATCHES[1]], Meenemen=True)
        df.loc[0, "Gegevensfrequentie"] = ""
        with pytest.raises(ValueError):
            validate_data_frequency_present(df)

    def test_missing_tolerated_when_not_included(self):
        df = index_frame([_HOUSEHOLD_BATCHES[1], _HOUSEHOLD_BATCHES[2]],
                         Meenemen=[False, pd.NA], Gegevensfrequentie=pd.NA)
        validate_data_frequency_present(df)  # must not raise

    def test_absent_column_passes(self):
        # An older household batch's index without household-batch fields passes unchecked.
        validate_data_frequency_present(
            index_frame([_HOUSEHOLD_BATCHES[1]], batch_fields=False, Meenemen=True))


class TestReadIndexGuard:
    def test_read_raises_on_included_row_without_cadence(self, index_options):
        tmp = index_options
        # Bypass the save-time guard: write a corrupt index directly.
        index_frame(
            [_HOUSEHOLD_BATCHES[1]], Meenemen=True, Gegevensfrequentie=pd.NA
        ).to_parquet(tmp / "index.parquet")
        with pytest.raises(ValueError):
            read_index(tmp)

    def test_read_accepts_index_without_batch_columns(self, index_options):
        tmp = index_options
        index_frame(
            [_HOUSEHOLD_BATCHES[1], _HOUSEHOLD_BATCHES[2]],
            batch_fields=False,
            Meenemen=[True, False],
        ).to_parquet(tmp / "index.parquet")
        idx, path = read_index(tmp)
        assert len(idx) == 2
        assert str(tmp) in path
