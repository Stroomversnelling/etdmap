"""
TDD (red-first) contract for the sharded-mapping WRITE primitive.

Sharding originates at mapping (etdmap), not in a downstream etdtransform "default"
step (see etdworkflow/docs/refactoring/NATIVE_RESOLUTION_DESIGN.md, Draft ADR F).
`save_household_shard` writes ONE household's mapped data as a hive-partitioned
shard -- the atomic, per-household, idempotent unit the parallel standardized
mapping pipeline builds on. It does NOT exist yet -> red at import until it lands.

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


def _pipeline_df(n=12, freq="5min"):
    """Tiny valid input for run_standard_pipeline (mirrors _minimal_pipeline_df)."""
    from etdmap.data_model import cumulative_columns
    col = cumulative_columns[0]
    dates = pd.date_range("2023-01-01", periods=n, freq=freq)
    return pd.DataFrame({"ReadingDate": dates, col: range(n)})


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
    """run_standard_pipeline honours etdmap.options.mapped_output_format -- the
    same existing API, no new caller-facing parameter (ADR D). Sharded is the
    DEFAULT from the refactor onward; "flat" is the flag (tests / legacy /
    pinned production until the sharded read side lands). Sharded output goes to
    <mapped_folder_path>/sharded/ (the sharded-subfolder convention); the 1:1 era
    batch id equals HuisIdBSV."""

    def test_default_output_format_is_sharded(self):
        # New Options default; refactor decision: one-way to sharded.
        assert etdmap.options.mapped_output_format == "sharded"

    def test_sharded_mode_writes_shard_not_flat(self, tmp_path):
        with _output_format("sharded"):
            run_standard_pipeline(
                _pipeline_df(), huis_code=42, huis_id="HuisX",
                mapped_folder_path=tmp_path,
            )
        shard = tmp_path / "sharded" / "HuisIdBSV=42" / "HuisBatchIdBSV=42" / "part.parquet"
        assert shard.exists()
        assert not (tmp_path / "household_42_table.parquet").exists()

    def test_flat_mode_unchanged_legacy(self, tmp_path):
        with _output_format("flat"):
            run_standard_pipeline(
                _pipeline_df(), huis_code=42, huis_id="HuisX",
                mapped_folder_path=tmp_path,
            )
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
                _pipeline_df(), huis_code=7, huis_id="H7",
                mapped_folder_path=flat_dir,
            )
        with _output_format("sharded"):
            run_standard_pipeline(
                _pipeline_df(), huis_code=7, huis_id="H7",
                mapped_folder_path=shard_dir,
            )
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
                    _pipeline_df(), huis_code=1, huis_id="H1",
                    mapped_folder_path=tmp_path,
                )
