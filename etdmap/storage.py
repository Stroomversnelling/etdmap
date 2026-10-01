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

# The flat layout stores one file per household and carries the id only here,
# so a reader that ignores the name loses the household identity entirely.
_FLAT_FILE_RE = _re.compile(r"household_(\d+)_table\.parquet$")


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

def shard_root(path):
    """Where a directory's hive partitions actually are, or None if it has none.

    A directory holding ``HuisIdBSV=*`` is its own shard root. A stage folder
    keeps its partitions one level down, under ``sharded/``, beside whatever
    flat files it also carries. Anything else is flat, however many parquet
    files sit in it.

    Resolving this is the difference between reading a folder and guessing at
    it: ``data/mapped`` holds 425 flat ``household_<id>_table.parquet`` files at
    its root and its partitions under ``sharded/``, so a recursive glob from the
    root matches both layouts at once and the reader rejects the mixture.
    """
    p = str(path)
    if not os.path.isdir(p):
        return None
    if _glob.glob(os.path.join(p, "HuisIdBSV=*")):
        return p
    nested = os.path.join(p, "sharded")
    if _glob.glob(os.path.join(nested, "HuisIdBSV=*")):
        return nested
    return None


def source_layout(path) -> str:
    """``"file"``, ``"sharded"`` or ``"flat"``.

    The three read differently, so every caller that branches needs all three
    rather than "is it a directory". A flat directory carries the household
    identity in its file names and nowhere else, so reading it as shards
    returns rows with no ``HuisIdBSV`` at all and raises nothing.
    """
    if not os.path.isdir(str(path)):
        return "file"
    return "sharded" if shard_root(path) is not None else "flat"


def source_is_sharded(path) -> bool:
    """True when path holds hive partitions.

    Not "is a directory": a directory of flat per-household files is a
    directory and is not hive-partitioned, and those are different questions
    with different answers.
    """
    return source_layout(path) == "sharded"


def source_is_directory(path) -> bool:
    """True for either directory layout, i.e. anything that is not one file."""
    return source_layout(path) != "file"


def shard_glob(path) -> str:
    """Recursive parquet glob under the directory's resolved shard root."""
    root = shard_root(path)
    if root is None:
        raise ValueError(
            f"{path} holds no HuisIdBSV=* partitions, at its root or under "
            f"sharded/, so it has no shard glob. Check source_layout first."
        )
    return os.path.join(root, "**", "*.parquet")


def flat_glob(path) -> str:
    """The per-household files directly under a flat directory."""
    return os.path.join(str(path), "household_*_table.parquet")


def resolve_source_path(folder, artifact_name: str):
    """
    Resolve a stage artifact inside a folder: the hive shard DIRECTORY named
    ``<artifact_name>`` -- directly in the folder or under its ``sharded``
    area, where the sharded pipeline writes its stage outputs -- when
    present, else the legacy single file ``<artifact_name>.parquet``.
    Returns the path (existence of the fallback file is NOT checked --
    callers keep their own not-found handling).
    """
    for candidate in (
        os.path.join(str(folder), artifact_name),
        os.path.join(str(folder), "sharded", artifact_name),
    ):
        if os.path.isdir(candidate):
            return candidate
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
    layout = source_layout(p)
    if layout == "file":
        return p
    if household_ids is None:
        return shard_glob(p) if layout == "sharded" else flat_glob(p)
    wanted = sorted({int(h) for h in household_ids})
    if not wanted:
        raise ValueError(
            f"No households requested for {p}; pass None to read them all."
        )
    root = shard_root(p) if layout == "sharded" else None
    targets, absent = [], []
    for household_id in wanted:
        if layout == "flat":
            flat_file = os.path.join(p, f"household_{household_id}_table.parquet")
            if os.path.exists(flat_file):
                targets.append(flat_file)
            else:
                absent.append(household_id)
            continue
        shard_dir = os.path.join(root, f"HuisIdBSV={household_id}")
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
    layout = source_layout(p)
    if layout != "file":
        types: dict = {}
        pattern = shard_glob(p) if layout == "sharded" else flat_glob(p)
        part_files = sorted(_glob.glob(pattern, recursive=True))
        if not part_files:
            raise FileNotFoundError(f"No parquet files under {p} ({layout} layout).")
        for pf in part_files:
            for field in _pq.read_schema(pf):
                types.setdefault(field.name, field.type)
        return types
    schema = _pq.read_schema(p)
    return {field.name: field.type for field in schema}


def source_schema_names(path) -> set:
    """Column-name set of a source, including the ids a directory supplies.

    Either directory layout carries the household ids outside the footers --
    sharded in the partition directories, flat in the file names -- and the
    frame reader injects them, so both report them here.
    """
    names = set(source_schema_types(path))
    if source_is_directory(path):
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
    layout = source_layout(p)
    if layout == "file":
        return pd.read_parquet(p, columns=columns, dtype_backend="numpy_nullable")
    flat = layout == "flat"
    pattern = flat_glob(p) if flat else shard_glob(p)
    part_files = sorted(_glob.glob(pattern, recursive=True))
    if not part_files:
        raise FileNotFoundError(f"No parquet files under {p} ({layout} layout).")
    frames = []
    for f in part_files:
        m = (_FLAT_FILE_RE if flat else _SHARD_PART_RE).search(
            os.path.basename(f) if flat else f
        )
        if m is None:
            raise ValueError(
                f"Cannot parse the household id from {f!r} ({layout} layout)."
            )
        # One file per household in the flat layout, so the batch id is the
        # household id -- the same equivalence mapped_household_files applies.
        ids = (
            (int(m.group(1)), int(m.group(1))) if flat
            else (int(m.group(1)), int(m.group(2)))
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
            (("HuisIdBSV", ids[0]), ("HuisBatchIdBSV", ids[1]))
        ):
            if name not in df.columns and (columns is None or name in columns):
                df.insert(min(pos, df.shape[1]), name,
                          pd.array([val] * n_rows, dtype="Int64"))
        frames.append(df)
    out = pd.concat(frames, ignore_index=True)
    if columns is not None:
        missing = [c for c in columns if c not in out.columns]
        if missing:
            raise ValueError(f"Column(s) {missing} not found in any file under {p}.")
        out = out[list(columns)]
    return out


def source_sql(path) -> str:
    """
    DuckDB ``read_parquet(...)`` SQL fragment for a source (a plain string;
    no engine import). A sharded source reads the shard glob with
    union_by_name and hive partitioning (ids come from the path).

    Raises ``ValueError`` for a flat directory: it carries the household id only
    in its file names, so this fragment would produce rows with no ``HuisIdBSV``
    and no error. Use ``read_source_frame``, which injects the ids.
    """
    def _lit(s) -> str:
        return "'" + str(s).replace("\\", "/").replace("'", "''") + "'"

    p = str(path)
    layout = source_layout(p)
    if layout == "sharded":
        return (
            f"read_parquet({_lit(shard_glob(p))}, "
            f"union_by_name=True, hive_partitioning=True)"
        )
    if layout == "flat":
        raise ValueError(
            f"{p} is a flat directory: the household id lives in the file names, "
            f"so SQL over it would silently drop HuisIdBSV. Read it with "
            f"read_source_frame, or point at the sharded artifact."
        )
    return f"read_parquet({_lit(p)})"
