"""
Storage-layout helpers: one home for "is this a single parquet file or a
partitioned folder of shards, and how do I read it".

A mapped dataset is one household batch: one body of data for a household,
identified by ``ids.HOUSEHOLD_BATCH``. A household (``ids.HOUSEHOLD``) can have
several -- another data structure, or a period after a renovation.

Pipeline data is stored in one of two layouts:

- sharded: a folder tree whose directory names carry both path_ids
  (``HuisIdBSV=<n>/HuisBatchIdBSV=<p>/*.parquet`` -- the hive-partitioning
  convention), so it holds every household batch of every household. The path_ids
  live in the folder names, not inside the files, and are never guessed: a
  folder name that does not parse raises an error.
- flat (legacy): one parquet file per household,
  ``household_<n>_table.parquet``. It has no household-batch level and so
  cannot represent a household with more than one.

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
from etdmap import ids

_SHARD_PART_RE = _re.compile(r"HuisIdBSV=(\d+)[\\/]HuisBatchIdBSV=(\d+)")

# The flat layout carries the household id only in the file name, so a reader
# that ignores the name loses the household identity entirely.
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

    - sharded: one file per household batch of that household, sorted by
      household batch. All of them are returned; a caller that can handle only
      one must guard at its own entry point.
    - flat: the single ``household_<n>_table.parquet``. The layout has no
      household-batch level, so the household id is reported in its place.

    Returns a list of (path, household_batch_id) tuples.
    Raises FileNotFoundError when the household has no data, and ValueError
    on a shard path that does not parse (never guess a household batch id).
    """
    if mapped_folder_path is None:
        mapped_folder_path = etdmap.options.mapped_folder_path
    root = str(mapped_folder_path)
    fmt = detect_mapped_format(root)
    household_id = int(household_id)

    if fmt == "sharded":
        paths = sorted(_glob.glob(os.path.join(
            root, "sharded", f"HuisIdBSV={household_id}", "HuisBatchIdBSV=*",
            "*.parquet"
        )))
        out = []
        for path in paths:
            match = _SHARD_PART_RE.search(path)
            if match is None:
                # NEVER silently default in sharded mode: substituting the
                # household id for the household batch id masks a household
                # delivered in several household batches and blinds the
                # household-batch guards.
                raise ValueError(
                    f"Cannot parse HuisIdBSV/HuisBatchIdBSV from shard "
                    f"path {path!r}; refusing to guess the household batch id."
                )
            out.append((path, int(match.group(2))))
        out.sort(key=lambda found: found[1])
    else:
        path = os.path.join(root, f"household_{household_id}_table.parquet")
        out = [(path, household_id)] if os.path.exists(path) else []

    if not out:
        raise FileNotFoundError(
            f"No mapped data for {ids.HOUSEHOLD} {household_id} under {root} "
            f"({fmt} layout)."
        )
    return out


def mapped_household_paths(mapped_folder_path=None, fmt="auto") -> dict:
    """
    Map every household batch in a mapped folder to one data file.

    Returns ``{ids.HOUSEHOLD_BATCH: path}``, keyed on the household batch, so a household
    delivered as more than one household batch is several entries rather than a
    collision. Use :func:`mapped_household_files` to read every household batch of one
    household.

    Tools that must read a SPECIFIC layout -- notably a comparison of the same
    household batch stored flat on one side and sharded on the other -- pass ``fmt``
    explicitly instead of parsing shard paths themselves:

    - "flat"    -- ``household_<n>_table.parquet`` at the folder root. That
      layout has no household-batch level, so it reports the household id in its
      place, as :func:`read_source_frame` does.
    - "sharded" -- ``HuisIdBSV=<n>/HuisBatchIdBSV=<p>/*.parquet`` shards under
      ``<folder>/sharded/`` or directly under ``<folder>`` (so a ``.../sharded``
      root can be passed as-is).
    - "auto"    -- flat when any flat file exists, else sharded.

    An unparseable shard directory name raises rather than being guessed.
    """
    if mapped_folder_path is None:
        mapped_folder_path = etdmap.options.mapped_folder_path
    root = str(mapped_folder_path)
    if fmt not in ("auto", "flat", "sharded"):
        raise ValueError(f"Unknown fmt {fmt!r}; expected 'auto', 'flat' or 'sharded'.")

    def _flat() -> dict:
        # No household-batch level in this layout, so the household id stands in for it.
        out = {}
        for path in sorted(
            _glob.glob(os.path.join(root, "household_*_table.parquet"))
        ):
            parts = os.path.basename(path)[: -len(".parquet")].split("_")
            if len(parts) >= 2 and parts[1].isdigit():
                out[int(parts[1])] = path
        return out

    def _sharded() -> dict:
        for base in (os.path.join(root, "sharded"), root):
            hits = sorted(_glob.glob(os.path.join(
                base, "HuisIdBSV=*", "HuisBatchIdBSV=*", "*.parquet"
            )))
            if not hits:
                continue
            out: dict = {}
            for path in hits:
                match = _SHARD_PART_RE.search(path)
                if match is None:
                    raise ValueError(
                        f"Cannot parse HuisIdBSV/HuisBatchIdBSV from shard path "
                        f"{path!r}; refusing to guess the dataset id."
                    )
                household_batch_id = int(match.group(2))
                if household_batch_id in out:
                    raise ValueError(
                        f"{ids.HOUSEHOLD_BATCH} {household_batch_id} has more than "
                        f"one shard under {base}. A dataset is one shard; two "
                        f"paths for it means the tree was written twice."
                    )
                out[household_batch_id] = path
            return out
        return {}

    if fmt == "flat":
        return _flat()
    if fmt == "sharded":
        return _sharded()
    return _flat() or _sharded()


def read_mapped_households(
    household_ids, columns=None, mapped_folder_path=None, household_batch_ids=None
) -> pd.DataFrame:
    """
    Materialise several households from the mapped dataset to ONE pandas
    DataFrame (nullable dtypes; ``ids.HOUSEHOLD`` and ``ids.HOUSEHOLD_BATCH``
    injected as Int64).

    This is the household-batch read path (the chunked-imputation pattern):
    the households' files are resolved DIRECTLY (hive path or flat filename)
    and read individually -- measured 3x faster than pushing a WHERE through a
    glob scan, and RAM is bounded by how many households are read at once.

    Works on both layouts via detect_mapped_format. The flat layout has no
    household-batch level, so a read of it reports the household id as the
    household batch.

    Parameters
    ----------
    household_ids : iterable of int
        The households whose directories are read. This is the level the shard
        path has, which is why a read prunes on it.
    columns : list, optional
        The columns to read, or None for all of them.
    mapped_folder_path : optional
        The mapped folder, or None for ``etdmap.options.mapped_folder_path``.
    household_batch_ids : iterable of int, optional
        Narrow the read to these household batch datasets. Pruning the
        directories is the household's job above; this selects exactly, so a
        household delivered as several household batches yields only the ones asked
        for. None reads every household batch of every household given.

    Raises
    ------
    FileNotFoundError
        Listing any requested household with no data, or any requested
        household batch dataset that none of those households holds.
    """
    if mapped_folder_path is None:
        mapped_folder_path = etdmap.options.mapped_folder_path
    root = str(mapped_folder_path)
    fmt = detect_mapped_format(root)
    wanted = (
        None if household_batch_ids is None
        else {int(batch_id) for batch_id in household_batch_ids}
    )

    read_cols = list(columns) if columns is not None else None
    frames = []
    missing = []
    seen = set()
    for household_id in household_ids:
        household_id = int(household_id)
        try:
            files = mapped_household_files(household_id, mapped_folder_path=root)
        except FileNotFoundError:
            missing.append(household_id)
            continue
        for path, household_batch_id in files:
            if wanted is not None and household_batch_id not in wanted:
                continue
            seen.add(household_batch_id)
            df = pd.read_parquet(
                path, columns=read_cols, dtype_backend="numpy_nullable"
            )
            df.insert(
                0, ids.HOUSEHOLD, pd.array([household_id] * len(df), dtype="Int64")
            )
            df.insert(
                1,
                ids.HOUSEHOLD_BATCH,
                pd.array([household_batch_id] * len(df), dtype="Int64"),
            )
            frames.append(df)

    if missing:
        raise FileNotFoundError(
            f"No mapped data for {ids.HOUSEHOLD} {missing} under {root} ({fmt} layout)."
        )
    if wanted is not None and wanted - seen:
        raise FileNotFoundError(
            f"No mapped data for {ids.HOUSEHOLD_BATCH} {sorted(wanted - seen)} "
            f"under {root} ({fmt} layout), among {ids.HOUSEHOLD} "
            f"{sorted(int(h) for h in household_ids)}."
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
    root = str(path)
    if not os.path.isdir(root):
        return None
    if _glob.glob(os.path.join(root, "HuisIdBSV=*")):
        return root
    nested = os.path.join(root, "sharded")
    if _glob.glob(os.path.join(nested, "HuisIdBSV=*")):
        return nested
    return None


def source_layout(path) -> str:
    """``"file"``, ``"sharded"`` or ``"flat"``.

    The three read differently, so every caller that branches needs all three
    rather than "is it a directory". A flat directory carries the household
    identity in its file names and nowhere else, so reading it as shards
    returns rows with no ``ids.HOUSEHOLD`` at all and raises nothing.
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
        Restrict to these household ids. None reads every shard.

    Returns
    -------
    str or list of str
        A single path/glob, or one glob per requested household.
    """
    source = str(path)
    layout = source_layout(source)
    if layout == "file":
        return source
    if household_ids is None:
        return shard_glob(source) if layout == "sharded" else flat_glob(source)
    wanted = sorted({int(household_id) for household_id in household_ids})
    if not wanted:
        raise ValueError(
            f"No households requested for {source}; pass None to read them all."
        )
    base = shard_root(source) if layout == "sharded" else source
    targets, absent = [], []
    for household_id in wanted:
        if layout == "flat":
            flat_file = os.path.join(base, f"household_{household_id}_table.parquet")
            if os.path.exists(flat_file):
                targets.append(flat_file)
            else:
                absent.append(household_id)
            continue
        shard_dir = os.path.join(base, f"HuisIdBSV={household_id}")
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
            f"no data in {source}: {absent}"
        )
    if not targets:
        raise FileNotFoundError(
            f"None of the requested households have data in {source}: {wanted}"
        )
    return targets


def source_schema_types(path) -> dict:
    """
    Column name -> pyarrow type for a source.

    File: the parquet footer. Directory: the union across shard footers
    (first-wins per name -- per-supplier schema differences are unified by
    name, the same union the legacy monolith concat produced). Hive partition
    path_ids (in the path, not the footers) are NOT included; use
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
    """Column-name set of a source, including the path_ids a directory supplies.

    Either directory layout carries the household path_ids outside the footers --
    sharded in the partition directories, flat in the file names -- and the
    frame reader injects them, so both report them here.
    """
    names = set(source_schema_types(path))
    if source_is_directory(path):
        names |= {ids.HOUSEHOLD, ids.HOUSEHOLD_BATCH}
    return names


def read_source_frame(path, columns=None) -> pd.DataFrame:
    """
    Read a stage artifact -- a single parquet file or a shard directory --
    into one pandas DataFrame with nullable dtypes.

    Directory shards are read in sorted path order (repeat reads return rows
    in the same order). The path_ids carried in the folder names are added as
    Int64 columns when the files do not store them; a folder name that does
    not parse raises rather than guessing. A requested column that a shard
    does not store reads as missing for that shard's rows (the same union
    behaviour a combined single file would give).
    """
    import pyarrow.parquet as _pq
    source = str(path)
    layout = source_layout(source)
    if layout == "file":
        return pd.read_parquet(source, columns=columns, dtype_backend="numpy_nullable")
    flat = layout == "flat"
    pattern = flat_glob(source) if flat else shard_glob(source)
    part_files = sorted(_glob.glob(pattern, recursive=True))
    if not part_files:
        raise FileNotFoundError(f"No parquet files under {source} ({layout} layout).")
    frames = []
    for part_file in part_files:
        match = (_FLAT_FILE_RE if flat else _SHARD_PART_RE).search(
            os.path.basename(part_file) if flat else part_file
        )
        if match is None:
            raise ValueError(
                f"Cannot parse the household id from {part_file!r} "
                f"({layout} layout)."
            )
        # The flat layout has no household-batch level, so the household id is
        # reported in its place -- as mapped_household_files does.
        path_ids = (
            (int(match.group(1)), int(match.group(1))) if flat
            else (int(match.group(1)), int(match.group(2)))
        )
        if columns is None:
            df = pd.read_parquet(part_file, dtype_backend="numpy_nullable")
            n_rows = len(df)
        else:
            stored = set(_pq.read_schema(part_file).names)
            read_cols = [c for c in columns if c in stored]
            if read_cols:
                df = pd.read_parquet(part_file, columns=read_cols,
                                     dtype_backend="numpy_nullable")
                n_rows = len(df)
            else:
                # Only partition-path columns were requested (e.g. just the
                # household id, which some artifacts store only in the folder
                # name). Reading zero columns yields a zero-length frame, so
                # take the true row count from the footer and build an
                # empty-body frame to inject the path_ids into.
                n_rows = _pq.read_metadata(part_file).num_rows
                df = pd.DataFrame(index=range(n_rows))
        for pos, (name, val) in enumerate(
            ((ids.HOUSEHOLD, path_ids[0]), (ids.HOUSEHOLD_BATCH, path_ids[1]))
        ):
            if name not in df.columns and (columns is None or name in columns):
                df.insert(min(pos, df.shape[1]), name,
                          pd.array([val] * n_rows, dtype="Int64"))
        frames.append(df)
    out = pd.concat(frames, ignore_index=True)
    if columns is not None:
        missing = [c for c in columns if c not in out.columns]
        if missing:
            raise ValueError(f"Column(s) {missing} not found in any file under {source}.")
        out = out[list(columns)]
    return out


def source_sql(path) -> str:
    """
    DuckDB ``read_parquet(...)`` SQL fragment for a source (a plain string;
    no engine import). A sharded source reads the shard glob with
    union_by_name and hive partitioning (path_ids come from the path).

    Raises ``ValueError`` for a flat directory: it carries the household id only
    in its file names, so this fragment would produce rows with no
    ``ids.HOUSEHOLD`` and no error. Use ``read_source_frame``, which injects the
    path_ids.
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
            f"so SQL over it would silently drop {ids.HOUSEHOLD}. Read it with "
            f"read_source_frame, or point at the sharded artifact."
        )
    return f"read_parquet({_lit(p)})"
