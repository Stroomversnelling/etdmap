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

    Returns "sharded" when shards exist under ``<root>/sharded/`` (preferred
    when both layouts are present), else "flat" when
    ``household_<n>_table.parquet`` files exist, else raises
    FileNotFoundError.
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


def mapped_household_files(household_id, mapped_folder_path=None) -> list:
    """
    Resolve the on-disk file(s) holding ONE household's mapped data.

    Format-detected:

    - sharded: the household's shard file(s), one per HuisBatch, sorted by
      HuisBatchIdBSV. More than one entry means a multi-batch household --
      the resolver returns them all (loaders return what exists); callers
      with a one-file-per-household assumption must guard at their own
      entry point.
    - flat: the single ``household_<n>_table.parquet``; the flat layout
      stores one file per household, so the batch id equals the household id.

    Returns a list of (path, household_batch_id) tuples.
    Raises FileNotFoundError when the household has no data, and ValueError
    on a shard path that does not parse (never guess a batch id).
    """
    if mapped_folder_path is None:
        mapped_folder_path = etdmap.options.mapped_folder_path
    root = str(mapped_folder_path)
    fmt = detect_mapped_format(root)
    hid = int(household_id)

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


def mapped_household_paths(mapped_folder_path=None, fmt="auto") -> dict:
    """
    Map every household in a mapped folder to its single data file.

    Returns ``{HuisIdBSV: path}``. Tools that must read a SPECIFIC layout --
    notably a comparison of the same dataset stored flat on one side and
    sharded on the other -- pass ``fmt`` explicitly instead of parsing shard
    paths themselves:

    - "flat"    -- ``household_<n>_table.parquet`` at the folder root.
    - "sharded" -- ``HuisIdBSV=<n>/HuisBatchIdBSV=<p>/*.parquet`` shards under
      ``<folder>/sharded/`` or directly under ``<folder>`` (so a ``.../sharded``
      root can be passed as-is).
    - "auto"    -- flat when any flat file exists, else sharded.

    One path per household: a household with more than one batch shard raises
    HouseBatchOverlapError, because a single path cannot represent it. An
    unparseable shard directory name raises rather than being guessed.
    """
    from etdmap.index_helpers import HouseBatchOverlapError

    if mapped_folder_path is None:
        mapped_folder_path = etdmap.options.mapped_folder_path
    root = str(mapped_folder_path)
    if fmt not in ("auto", "flat", "sharded"):
        raise ValueError(f"Unknown fmt {fmt!r}; expected 'auto', 'flat' or 'sharded'.")

    def _flat() -> dict:
        out = {}
        for p in sorted(_glob.glob(os.path.join(root, "household_*_table.parquet"))):
            parts = os.path.basename(p)[: -len(".parquet")].split("_")
            if len(parts) >= 2 and parts[1].isdigit():
                out[int(parts[1])] = p
        return out

    def _sharded() -> dict:
        for base in (os.path.join(root, "sharded"), root):
            hits = sorted(_glob.glob(os.path.join(
                base, "HuisIdBSV=*", "HuisBatchIdBSV=*", "*.parquet"
            )))
            if not hits:
                continue
            out: dict = {}
            for p in hits:
                m = _SHARD_PART_RE.search(p)
                if m is None:
                    raise ValueError(
                        f"Cannot parse HuisIdBSV/HuisBatchIdBSV from shard path "
                        f"{p!r}; refusing to guess the household id."
                    )
                household_id = int(m.group(1))
                if household_id in out:
                    raise HouseBatchOverlapError(
                        f"HuisIdBSV {household_id} has more than one batch shard "
                        f"under {base}; a single path per household cannot "
                        f"represent it. A household in more than one batch is "
                        f"valid in the data model but not yet supported here."
                    )
                out[household_id] = p
            return out
        return {}

    if fmt == "flat":
        return _flat()
    if fmt == "sharded":
        return _sharded()
    return _flat() or _sharded()


def read_mapped_households(household_ids, columns=None, mapped_folder_path=None) -> pd.DataFrame:
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
    for hid in household_ids:
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


def source_read_targets(path, household_ids=None):
    """
    The path(s) to hand a parquet reader for a source.

    Restricting a sharded source to ``household_ids`` returns one glob per
    household, so the reader only opens those shard directories. Filtering
    after the read does not achieve this: the partition column is known only
    once every shard has been opened, which is the expensive part.

    A single-file source has no directories to select from and is returned
    unchanged; callers still apply their own filter for identical results in
    either layout.

    Parameters
    ----------
    path : str or Path
        Artifact: a single parquet file or a shard directory.
    household_ids : iterable of int, optional
        Restrict to these HuisIdBSV values. None reads every shard.

    Returns
    -------
    str or list of str
        A single path/glob, or one glob per requested household.
    """
    p = str(path)
    if not source_is_sharded(p):
        return p
    if household_ids is None:
        return shard_glob(p)
    wanted = sorted({int(h) for h in household_ids})
    if not wanted:
        raise ValueError(
            f"No households requested for {p}; pass None to read them all."
        )
    targets, absent = [], []
    for household_id in wanted:
        shard_dir = os.path.join(p, f"HuisIdBSV={household_id}")
        if os.path.isdir(shard_dir):
            targets.append(os.path.join(shard_dir, "**", "*.parquet"))
        else:
            absent.append(household_id)
    if absent:
        # Normal: an artifact holds only the households that reached this
        # stage, so an index-derived request (e.g. a whole project) can name
        # households the excluded ones among them never produced.
        logging.debug(
            f"[source_read_targets] {len(absent)} requested household(s) have "
            f"no data in {p}: {absent}"
        )
    if not targets:
        raise FileNotFoundError(
            f"None of the requested households have data in {p}: {wanted}"
        )
    return targets


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
        if columns is None:
            df = pd.read_parquet(f, dtype_backend="numpy_nullable")
            n_rows = len(df)
        else:
            stored = set(_pq.read_schema(f).names)
            read_cols = [c for c in columns if c in stored]
            if read_cols:
                df = pd.read_parquet(f, columns=read_cols,
                                     dtype_backend="numpy_nullable")
                n_rows = len(df)
            else:
                # Only partition-path columns were requested (e.g. just
                # HuisIdBSV, which some artifacts store only in the folder
                # name). Reading zero columns yields a zero-length frame, so
                # take the true row count from the footer and build an
                # empty-body frame to inject the ids into.
                n_rows = _pq.read_metadata(f).num_rows
                df = pd.DataFrame(index=range(n_rows))
        for pos, (name, val) in enumerate(
            (("HuisIdBSV", int(m.group(1))), ("HuisBatchIdBSV", int(m.group(2))))
        ):
            if name not in df.columns and (columns is None or name in columns):
                df.insert(min(pos, df.shape[1]), name,
                          pd.array([val] * n_rows, dtype="Int64"))
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
