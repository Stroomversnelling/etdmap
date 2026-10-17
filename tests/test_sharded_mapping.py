"""
Contract tests for the sharded-mapping WRITE primitive.

Sharding originates at mapping: `save_household_shard` writes ONE household's
mapped data as a hive-partitioned shard -- the atomic, per-household,
idempotent unit the mapping pipeline builds on.

Contract:
- Writes to `<root>/HuisIdBSV=<n>/HuisBatchIdBSV=<p>/part.parquet`.
- Partition columns (the ids) live in the PATH, not the file -- hive convention,
  and consistent with the flat mapped file which also carries no id columns.
- Round-trips the mapped DataFrame identically (nullable dtypes preserved, ADR-005).
- Per-household + idempotent: re-running one household overwrites only its shard
  (no stale duplicate part files); different batches of the same household coexist.
"""

import os

import pandas as pd
import pytest

import etdmap
from etdmap.mapping_helpers import run_standard_pipeline, save_household_shard


#: The grid these frames are built on and declared to sit on. Passed
#: explicitly: there is no default grid.
GRID = 5


def _pipeline_df(n=12, freq="5min"):
    """Tiny valid input for run_standard_pipeline (mirrors _minimal_pipeline_df)."""
    from etdmap.data_model import cumulative_columns
    col = cumulative_columns[0]
    dates = pd.date_range("2023-01-01", periods=n, freq=freq)
    return pd.DataFrame({"ReadingDate": dates, col: range(n)})


def _pipeline_cadence(df):
    """The cadence table a mapper measures before calling the pipeline."""
    from etdmap.mapping_helpers import detect_column_cadence
    return detect_column_cadence(df, grid_minutes=GRID)


class _output_format:
    """Temporarily set etdmap.options.mapped_output_format (global option)."""

    def __init__(self, value):
        self.value = value

    def __enter__(self):
        self.prev = etdmap.options.mapped_output_format
        etdmap.options.mapped_output_format = self.value

    def __exit__(self, *exc):
        etdmap.options.mapped_output_format = self.prev


def _mapped_df():
    """A minimal mapped-household frame: ReadingDate (UTC) + nullable data cols,
    no id columns -- exactly what run_standard_pipeline writes today."""
    idx = pd.to_datetime(
        ["2024-01-01 00:00", "2024-01-01 00:05", "2024-01-01 00:10"], utc=True
    )
    return pd.DataFrame({
        "ReadingDate": idx,
        "ElektriciteitNetgebruik": pd.array([1.0, 2.0, pd.NA], dtype="Float64"),
        "SomeCount": pd.array([10, 20, 30], dtype="Int64"),
    })


class TestSaveHouseholdShard:
    def test_roundtrip_and_hive_path(self, tmp_path):
        df = _mapped_df()
        out = save_household_shard(df, 42, 7, tmp_path)
        expected = os.path.join(
            str(tmp_path), "HuisIdBSV=42", "HuisBatchIdBSV=7", "part.parquet"
        )
        assert os.path.normpath(str(out)) == os.path.normpath(expected)
        assert os.path.exists(out)

        back = pd.read_parquet(out, dtype_backend="numpy_nullable")
        # ids live in the path, not the file
        assert "HuisIdBSV" not in back.columns
        assert "HuisBatchIdBSV" not in back.columns
        pd.testing.assert_frame_equal(back, df)

    def test_idempotent_single_part_file(self, tmp_path):
        df = _mapped_df()
        save_household_shard(df, 42, 7, tmp_path)
        save_household_shard(df, 42, 7, tmp_path)  # re-run one household
        part_dir = tmp_path / "HuisIdBSV=42" / "HuisBatchIdBSV=7"
        assert len(list(part_dir.glob("*.parquet"))) == 1

    def test_two_batches_same_household_coexist(self, tmp_path):
        df = _mapped_df()
        save_household_shard(df, 42, 7, tmp_path)
        save_household_shard(df, 42, 9, tmp_path)
        assert (tmp_path / "HuisIdBSV=42" / "HuisBatchIdBSV=7" / "part.parquet").exists()
        assert (tmp_path / "HuisIdBSV=42" / "HuisBatchIdBSV=9" / "part.parquet").exists()


class TestSaveMappedHousehold:
    """The SINGLE save primitive every mapper must use (run_standard_pipeline
    internally; older scripts like map_watch_e that write directly must call it
    too -- Watch-E wrote zero shards on the first real run because its direct
    to_parquet bypassed the format branch)."""

    def test_sharded_mode(self, tmp_path):
        from etdmap.mapping_helpers import save_mapped_household
        with _output_format("sharded"):
            path = save_mapped_household(_mapped_df(), 42, tmp_path)
        assert (tmp_path / "sharded" / "HuisIdBSV=42" / "HuisBatchIdBSV=42" / "part.parquet").exists()
        assert not (tmp_path / "household_42_table.parquet").exists()
        assert "HuisIdBSV=42" in str(path)

    def test_flat_mode(self, tmp_path):
        from etdmap.mapping_helpers import save_mapped_household
        with _output_format("flat"):
            path = save_mapped_household(_mapped_df(), 42, tmp_path)
        assert (tmp_path / "household_42_table.parquet").exists()
        assert not (tmp_path / "sharded").exists()
        assert str(path).endswith("household_42_table.parquet")

    def test_invalid_format_raises(self, tmp_path):
        from etdmap.mapping_helpers import save_mapped_household
        with _output_format("banana"):
            with pytest.raises(ValueError):
                save_mapped_household(_mapped_df(), 1, tmp_path)


class TestPipelineOutputFormat:
    """run_standard_pipeline honours etdmap.options.mapped_output_format --
    the same existing API, no new caller-facing parameter. Sharded is the
    default; "flat" writes the legacy one-file-per-household layout. Sharded
    output goes to <mapped_folder_path>/sharded/; the writer assigns
    HuisBatchIdBSV equal to HuisIdBSV (a household in more than one batch is
    not yet supported)."""

    def test_default_output_format_is_sharded(self):
        assert etdmap.options.mapped_output_format == "sharded"

    def test_sharded_mode_writes_shard_not_flat(self, tmp_path):
        with _output_format("sharded"):
            run_standard_pipeline(
                _pipeline_df(), household_id=42, household_id_supplier="HuisX",
                mapped_folder_path=tmp_path, grid_minutes=GRID,
                cadence=_pipeline_cadence(_pipeline_df()))
        shard = tmp_path / "sharded" / "HuisIdBSV=42" / "HuisBatchIdBSV=42" / "part.parquet"
        assert shard.exists()
        assert not (tmp_path / "household_42_table.parquet").exists()

    def test_flat_mode_unchanged_legacy(self, tmp_path):
        with _output_format("flat"):
            run_standard_pipeline(
                _pipeline_df(), household_id=42, household_id_supplier="HuisX",
                mapped_folder_path=tmp_path, grid_minutes=GRID,
                cadence=_pipeline_cadence(_pipeline_df()))
        assert (tmp_path / "household_42_table.parquet").exists()
        assert not (tmp_path / "sharded").exists()

    def test_sharded_equals_flat_content(self, tmp_path):
        """Per-household cross-format correctness gate at the mapping stage:
        the SAME input through both modes yields identical data (the shard
        carries no id columns, same as the flat file)."""
        flat_dir = tmp_path / "flat"
        shard_dir = tmp_path / "shard"
        flat_dir.mkdir()
        shard_dir.mkdir()
        with _output_format("flat"):
            run_standard_pipeline(
                _pipeline_df(), household_id=7, household_id_supplier="H7",
                mapped_folder_path=flat_dir, grid_minutes=GRID,
                cadence=_pipeline_cadence(_pipeline_df()))
        with _output_format("sharded"):
            run_standard_pipeline(
                _pipeline_df(), household_id=7, household_id_supplier="H7",
                mapped_folder_path=shard_dir, grid_minutes=GRID,
                cadence=_pipeline_cadence(_pipeline_df()))
        flat = pd.read_parquet(flat_dir / "household_7_table.parquet",
                               dtype_backend="numpy_nullable")
        shard = pd.read_parquet(
            shard_dir / "sharded" / "HuisIdBSV=7" / "HuisBatchIdBSV=7" / "part.parquet",
            dtype_backend="numpy_nullable",
        )
        pd.testing.assert_frame_equal(flat, shard)

    def test_invalid_output_format_raises(self, tmp_path):
        with _output_format("banana"):
            with pytest.raises(ValueError):
                run_standard_pipeline(
                    _pipeline_df(), household_id=1, household_id_supplier="H1",
                    mapped_folder_path=tmp_path, grid_minutes=GRID,
                    cadence=_pipeline_cadence(_pipeline_df()))


class TestDatasetValidatorsReadBothLayouts:
    """Dataset-level flags must be computed under BOTH mapped layouts.

    update_index used to build the flat filename `household_<id>_table.parquet`
    and guard on os.path.exists. Under the sharded default that guard was always
    False, so NO dataset-level flag was computed for any household -- and the
    omission was silent, which is why it survived. These tests fail against that
    version under "sharded" and pass under "flat".
    """

    HOUSEHOLD_ID = 42

    def _index_df(self):
        return pd.DataFrame({
            "HuisIdBSV": pd.array([self.HOUSEHOLD_ID], dtype="Int64"),
            "HuisIdLeverancier": pd.array(["Huis42"], dtype="string"),
            "ProjectIdLeverancier": pd.array(["P1"], dtype="string"),
            "Dataleverancier": pd.array(["etdmap"], dtype="string"),
        })

    @pytest.fixture
    def _declared_batch(self, tmp_path, monkeypatch):
        """The batch this household is recorded in. The dataset validators read
        the declared grid from it to know how many rows a year should hold."""
        path = tmp_path / "huisbatch.csv"
        pd.DataFrame({
            "HuisIdBSV": pd.array([self.HOUSEHOLD_ID], dtype="Int64"),
            "HuisBatchIdBSV": pd.array([self.HOUSEHOLD_ID], dtype="Int64"),
            "Gegevensfrequentie": pd.array([f"{GRID}min"], dtype="string"),
        }).to_csv(path, index=False)
        monkeypatch.setattr(
            etdmap.options, "household_batch_csv_path", str(path)
        )
        return path

    def _update_index_for(self, tmp_path, monkeypatch, fmt):
        from etdmap.index_helpers import update_index
        from etdmap.mapping_helpers import save_mapped_household

        monkeypatch.setattr(etdmap.options, "mapped_folder_path", str(tmp_path))
        with _output_format(fmt):
            save_mapped_household(_mapped_df(), self.HOUSEHOLD_ID, tmp_path)

        entry = {
            "HuisIdLeverancier": "Huis42",
            "ProjectIdLeverancier": "P1",
            "HuisIdBSV": self.HOUSEHOLD_ID,
        }
        return update_index(
            self._index_df(), entry, data_provider="etdmap", save=False
        )

    @pytest.mark.parametrize("fmt", ["flat", "sharded"])
    def test_dataset_flags_are_evaluated(
        self, tmp_path, monkeypatch, fmt, _declared_batch
    ):
        from etdmap.dataset_validators import dataset_flag_conditions

        out = self._update_index_for(tmp_path, monkeypatch, fmt)

        present = [f for f in dataset_flag_conditions if f in out.columns]
        assert present, (
            f"no dataset flag column was created under the {fmt} layout -- "
            f"the validators did not run"
        )

        evaluated = [f for f in present if out[f].notna().any()]
        assert evaluated, (
            f"dataset flag columns exist but none holds a verdict under the "
            f"{fmt} layout"
        )

    def test_missing_household_raises(self, tmp_path, monkeypatch):
        """A household with no mapped data is a hard failure, not a silent skip.

        update_index cannot do half its job: without the data there are no
        dataset-level flags, and an index row that merely lacks them is
        indistinguishable from one whose validators all returned NA.
        """
        from etdmap.index_helpers import update_index
        from etdmap.mapping_helpers import save_mapped_household

        monkeypatch.setattr(etdmap.options, "mapped_folder_path", str(tmp_path))
        # Another household's shard exists, so the layout is detectable but
        # household 42 has no data of its own.
        with _output_format("sharded"):
            save_mapped_household(_mapped_df(), 99, tmp_path)

        entry = {
            "HuisIdLeverancier": "Huis42",
            "ProjectIdLeverancier": "P1",
            "HuisIdBSV": self.HOUSEHOLD_ID,
        }
        with pytest.raises(FileNotFoundError, match=str(self.HOUSEHOLD_ID)):
            update_index(
                self._index_df(), entry, data_provider="etdmap", save=False
            )


class TestGetMappedDataReadsBothLayouts:
    """get_mapped_data resolved the flat filename and raised when it was
    absent, so under the sharded layout it raised for EVERY household -- which
    took the per-household statistics collector down with it."""

    @pytest.mark.parametrize("fmt", ["flat", "sharded"])
    def test_reads_either_layout(self, tmp_path, monkeypatch, fmt):
        from etdmap.index_helpers import get_mapped_data
        from etdmap.mapping_helpers import save_mapped_household

        monkeypatch.setattr(etdmap.options, "mapped_folder_path", str(tmp_path))
        written = _mapped_df()
        with _output_format(fmt):
            save_mapped_household(written, 42, tmp_path)

        got = get_mapped_data(42)
        pd.testing.assert_frame_equal(got, written)

    @pytest.mark.parametrize("fmt", ["flat", "sharded"])
    def test_does_not_leak_identifier_columns(self, tmp_path, monkeypatch, fmt):
        """Callers iterate the returned columns to compute per-column stats, so
        an injected identifier would be counted as a measured variable."""
        from etdmap.index_helpers import get_mapped_data
        from etdmap.mapping_helpers import save_mapped_household

        monkeypatch.setattr(etdmap.options, "mapped_folder_path", str(tmp_path))
        with _output_format(fmt):
            save_mapped_household(_mapped_df(), 42, tmp_path)

        cols = get_mapped_data(42).columns
        assert "HuisIdBSV" not in cols
        assert "HuisBatchIdBSV" not in cols

    def test_missing_household_raises(self, tmp_path, monkeypatch):
        from etdmap.index_helpers import get_mapped_data
        from etdmap.mapping_helpers import save_mapped_household

        monkeypatch.setattr(etdmap.options, "mapped_folder_path", str(tmp_path))
        with _output_format("sharded"):
            save_mapped_household(_mapped_df(), 99, tmp_path)

        with pytest.raises(FileNotFoundError, match="42"):
            get_mapped_data(42)
