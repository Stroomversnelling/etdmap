"""
Storage-layout helpers: one home for "is this a single parquet file or a
partitioned folder of shards, and how do I read it".

Pipeline data is stored in one of two layouts:

- flat (legacy): one parquet file per household,
  ``household_<n>_table.parquet``.
- sharded: a folder tree whose directory names carry the ids
  (``HuisIdBSV=<n>/HuisBatchIdBSV=<p>/*.parquet`` -- the hive-partitioning
  convention). The ids live in the folder names, not inside the files, and
  are never guessed: a folder name that does not parse raises an error.

Every helper here detects the layout itself, so callers never pass a format.
This module uses only pandas and pyarrow; functions that build lazy query
tables (DuckDB/ibis) live in the transformation package on top of these
helpers, keeping the query engine out of this base package.
"""

import glob as _glob
import logging
import os
import re as _re

import pandas as pd

import etdmap

_SHARD_PART_RE = _re.compile(r"HuisIdBSV=(\d+)[\\/]HuisBatchIdBSV=(\d+)")


def detect_mapped_format(mapped_folder_path=None) -> str:
    """
    Detect the storage layout of a mapped dataset folder.

    Returns "sharded" when hive shards exist under ``<root>/sharded/`` (the
    format going forward -- preferred when both coexist during the
    transition), else "flat" when legacy ``household_<n>_table.parquet`` files
    exist, else raises FileNotFoundError.
    """
    if mapped_folder_path is None:
        mapped_folder_path = etdmap.options.mapped_folder_path
    root = str(mapped_folder_path)
    if _glob.glob(os.path.join(root, "sharded", "HuisIdBSV=*", "HuisBatchIdBSV=*", "*.parquet")):
        return "sharded"
    if _glob.glob(os.path.join(root, "household_*_table.parquet")):
        return "flat"
    raise FileNotFoundError(
        f"No mapped household data found under {root}: neither "
        f"sharded/HuisIdBSV=*/ shards nor household_*_table.parquet files."
    )


def mapped_household_files(huis_id, mapped_folder_path=None) -> list:
    """
    Resolve the on-disk file(s) holding ONE household's mapped data.

    The shared read-layer resolver: no function builds mapped file paths
    inline. Format-detected:

    - sharded: the household's shard file(s), one per HuisBatch, sorted by
      HuisBatchIdBSV. More than one entry means a multi-batch household --
      the resolver returns them all (loaders return what exists); callers
      with a one-file-per-household assumption must guard at their own
      entry point.
    - flat: the single ``household_<n>_table.parquet``; the flat layout
      stores one file per household, so the batch id equals the household id.

    Returns a list of (path, huis_batch_id) tuples.
    Raises FileNotFoundError when the household has no data, and ValueError
    on a shard path that does not parse (never guess a batch id).
    """
    if mapped_folder_path is None:
        mapped_folder_path = etdmap.options.mapped_folder_path
    root = str(mapped_folder_path)
    fmt = detect_mapped_format(root)
    hid = int(huis_id)

    if fmt == "sharded":
        paths = sorted(_glob.glob(os.path.join(
            root, "sharded", f"HuisIdBSV={hid}", "HuisBatchIdBSV=*", "*.parquet"
        )))
        out = []
        for p in paths:
            m = _SHARD_PART_RE.search(p)
            if m is None:
                # NEVER silently default in sharded mode: a quiet 1:1
                # fallback masks multi-batch data and blinds the batch
                # guards.
                raise ValueError(
                    f"Cannot parse HuisIdBSV/HuisBatchIdBSV from shard "
                    f"path {p!r}; refusing to guess the batch id."
                )
            out.append((p, int(m.group(2))))
        out.sort(key=lambda t: t[1])
    else:
        p = os.path.join(root, f"household_{hid}_table.parquet")
        out = [(p, hid)] if os.path.exists(p) else []

    if not out:
        raise FileNotFoundError(
            f"No mapped data for HuisIdBSV {hid} under {root} ({fmt} layout)."
        )
    return out


def read_mapped_households(huis_ids, columns=None, mapped_folder_path=None) -> pd.DataFrame:
    """
    Materialise a batch of households from the mapped dataset to ONE pandas
    DataFrame (nullable dtypes; HuisIdBSV/HuisBatchIdBSV injected as Int64).

    This is the household-batch read path (the chunked-imputation pattern):
    the households' files are resolved DIRECTLY (hive path or flat filename)
    and read individually -- measured 3x faster than pushing a WHERE through a
    glob scan, and RAM is bounded by the batch size.

    Works on both layouts via detect_mapped_format. In the flat layout
    HuisBatchIdBSV equals HuisIdBSV (one file per household).

    Raises FileNotFoundError listing any requested household with no data.
    """
    if mapped_folder_path is None:
        mapped_folder_path = etdmap.options.mapped_folder_path
    root = str(mapped_folder_path)
    fmt = detect_mapped_format(root)

    read_cols = list(columns) if columns is not None else None
    frames = []
    missing = []
    for hid in huis_ids:
        hid = int(hid)
        try:
            files = mapped_household_files(hid, mapped_folder_path=root)
        except FileNotFoundError:
            missing.append(hid)
            continue
        for p, hbid in files:
            df = pd.read_parquet(p, columns=read_cols, dtype_backend="numpy_nullable")
            df.insert(0, "HuisIdBSV", pd.array([hid] * len(df), dtype="Int64"))
            df.insert(1, "HuisBatchIdBSV", pd.array([hbid] * len(df), dtype="Int64"))
            frames.append(df)

    if missing:
        raise FileNotFoundError(
            f"No mapped data for HuisIdBSV {missing} under {root} ({fmt} layout)."
        )
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Generic source helpers: a "source" is a single parquet FILE or a
# hive-sharded DIRECTORY (any pipeline stage artifact).
# ---------------------------------------------------------------------------

def source_is_sharded(path) -> bool:
    """True when path is a directory (read as hive shards)."""
    return os.path.isdir(str(path))


def shard_glob(path) -> str:
    """Recursive parquet glob under a shard directory."""
    return os.path.join(str(path), "**", "*.parquet")


def resolve_source_path(folder, artifact_name: str):
    """
    Resolve a stage artifact inside a folder: the hive shard DIRECTORY named
    ``<artifact_name>`` when present, else the legacy single file
    ``<artifact_name>.parquet``. Returns the path (existence of the fallback
    file is NOT checked -- callers keep their own not-found handling).
    """
    dir_candidate = os.path.join(str(folder), artifact_name)
    if os.path.isdir(dir_candidate):
        return dir_candidate
    return os.path.join(str(folder), f"{artifact_name}.parquet")


def source_schema_types(path) -> dict:
    """
    Column name -> pyarrow type for a source.

    File: the parquet footer. Directory: the union across shard footers
    (first-wins per name -- per-supplier schema differences are unified by
    name, the same union the legacy monolith concat produced). Hive partition
    ids (in the path, not the footers) are NOT included; use
    source_schema_names for the full column-name set.
    """
    import pyarrow.parquet as _pq
    p = str(path)
    if source_is_sharded(p):
        types: dict = {}
        part_files = sorted(_glob.glob(shard_glob(p), recursive=True))
        if not part_files:
            raise FileNotFoundError(f"No parquet shards under {p}.")
        for pf in part_files:
            for field in _pq.read_schema(pf):
                types.setdefault(field.name, field.type)
        return types
    schema = _pq.read_schema(p)
    return {field.name: field.type for field in schema}


def source_schema_names(path) -> set:
    """Column-name set of a source, including hive partition ids for a directory."""
    names = set(source_schema_types(path))
    if source_is_sharded(path):
        names |= {"HuisIdBSV", "HuisBatchIdBSV"}
    return names


def read_source_frame(path, columns=None) -> pd.DataFrame:
    """
    Read a stage artifact -- a single parquet file or a shard directory --
    into one pandas DataFrame with nullable dtypes.

    Directory shards are read in sorted path order (repeat reads return rows
    in the same order). The ids carried in the folder names are added as
    Int64 columns when the files do not store them; a folder name that does
    not parse raises rather than guessing. A requested column that a shard
    does not store reads as missing for that shard's rows (the same union
    behaviour a combined single file would give).
    """
    import pyarrow.parquet as _pq
    p = str(path)
    if not source_is_sharded(p):
        return pd.read_parquet(p, columns=columns, dtype_backend="numpy_nullable")
    part_files = sorted(_glob.glob(shard_glob(p), recursive=True))
    if not part_files:
        raise FileNotFoundError(f"No parquet shards under {p}.")
    frames = []
    for f in part_files:
        m = _SHARD_PART_RE.search(f)
        if m is None:
            raise ValueError(
                f"Cannot parse HuisIdBSV/HuisBatchIdBSV from shard path {f!r}."
            )
        read_cols = columns
        if columns is not None:
            stored = set(_pq.read_schema(f).names)
            read_cols = [c for c in columns if c in stored]
        df = pd.read_parquet(f, columns=read_cols, dtype_backend="numpy_nullable")
        for pos, (name, val) in enumerate(
            (("HuisIdBSV", int(m.group(1))), ("HuisBatchIdBSV", int(m.group(2))))
        ):
            if name not in df.columns and (columns is None or name in columns):
                df.insert(pos, name, pd.array([val] * len(df), dtype="Int64"))
        frames.append(df)
    out = pd.concat(frames, ignore_index=True)
    if columns is not None:
        missing = [c for c in columns if c not in out.columns]
        if missing:
            raise ValueError(f"Column(s) {missing} not found in any shard under {p}.")
        out = out[list(columns)]
    return out


def source_sql(path) -> str:
    """
    DuckDB ``read_parquet(...)`` SQL fragment for a source (a plain string;
    no engine import). Directory sources read the shard glob with
    union_by_name and hive partitioning (ids come from the path).
    """
    def _lit(s) -> str:
        return "'" + str(s).replace("\\", "/").replace("'", "''") + "'"

    p = str(path)
    if source_is_sharded(p):
        return (
            f"read_parquet({_lit(shard_glob(p))}, "
            f"union_by_name=True, hive_partitioning=True)"
        )
    return f"read_parquet({_lit(p)})"
