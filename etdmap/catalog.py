"""
catalog.py — Pre-flight derivability check using the committed catalog parquet.

Checks whether all required target columns can be derived from the columns available
in a mapped dataset, using the pre-built catalog in etdmap/data/catalog.parquet.

No SymPy dependency — all checks are pure pandas + set operations on the catalog's
lhs / rhs_vars columns.

Usage:
    from etdmap.catalog import load_catalog, check_derivability, rule_lhs_variables

    catalog_df = load_catalog()
    report = check_derivability(
        available=set(df.columns),
        required_targets={"TerugleveringTotaalNetto", "Zelfgebruik"},
        catalog_df=catalog_df,
    )
    if report["not_derivable"]:
        raise ValueError(f"Missing derivable columns: {report['not_derivable']}")
"""

from importlib.resources import files
from pathlib import Path

import pandas as pd


def load_rules(rules_path=None) -> pd.DataFrame:
    """
    Load the base derivation rules from Rule.csv.

    Parameters
    ----------
    rules_path : str or Path, optional
        Override path. Defaults to the bundled etdmap/data/Rule.csv.

    Returns
    -------
    pd.DataFrame
        Columns: target (str), expression (str), rhs_vars (str), physical_model (str)
    """
    if rules_path is not None:
        path = Path(rules_path)
    else:
        path = Path(str(files("etdmap.data").joinpath("Rule.csv")))

    if not path.exists():
        raise FileNotFoundError(f"[catalog] Rule.csv not found: {path}")

    return pd.read_csv(path, dtype=str)


def _build_rule_variable_sets() -> tuple:
    """Build lhs/rhs variable sets from Rule.csv at import time."""
    try:
        rules_df = load_rules()
    except FileNotFoundError:
        return set(), set(), set()

    lhs = set(rules_df["target"].dropna().str.strip().unique())

    rhs: set = set()
    for cell in rules_df["rhs_vars"].dropna():
        for var in cell.split(","):
            var = var.strip()
            if var:
                rhs.add(var)

    return lhs, rhs, lhs | rhs


rule_lhs_variables: set
rule_rhs_variables: set
rule_all_variables: set
rule_lhs_variables, rule_rhs_variables, rule_all_variables = _build_rule_variable_sets()


def load_catalog(catalog_path=None) -> pd.DataFrame:
    """
    Load the pre-built derivation catalog from etdmap package data.

    Parameters
    ----------
    catalog_path : str or Path, optional
        Override path to the catalog parquet file. Defaults to the bundled
        etdmap/data/catalog.parquet.

    Returns
    -------
    pd.DataFrame
        Columns: lhs (str), rhs_text (str), rhs_vars (list[str]), rhs_var_count (int)

    Raises
    ------
    FileNotFoundError
        If the catalog parquet file does not exist. Run sync_data_model.py to
        build it from the Rule CSV.
    """
    if catalog_path is not None:
        path = Path(catalog_path)
    else:
        path = Path(str(files("etdmap.data").joinpath("catalog.parquet")))

    if not path.exists():
        raise FileNotFoundError(
            f"[catalog] Catalog not found: {path}. "
            f"Run etdworkflow/sync_data_model.py to build catalog.parquet from Rule.csv."
        )

    return pd.read_parquet(path)


def check_derivability(
    available: set,
    required_targets: set,
    catalog_df: pd.DataFrame,
) -> dict:
    """
    Determine which required target columns can be derived from the available columns.

    Uses an iterative fixed-point approach: a newly derivable column is added to
    'available' and may unlock further derivations in subsequent passes.

    Targets that are already in 'available' are considered satisfied (no derivation
    needed — the provider supplied them directly).

    Parameters
    ----------
    available : set[str]
        Column names present in the mapped dataset. Should reflect only columns
        with sufficient non-null data (see DatasetAdapter.effective_raw in etdtransform).
    required_targets : set[str]
        Column names that must be derivable.
    catalog_df : pd.DataFrame
        Loaded from load_catalog(). Must have lhs and rhs_vars columns.

    Returns
    -------
    dict with keys:
      derivable : set[str]   — required targets that can be derived (not already available)
      not_derivable : set[str] — required targets that cannot be derived
    """
    known = set(available)
    remaining = {t for t in required_targets if t not in known}
    derivable = set()

    changed = True
    while changed and remaining:
        changed = False
        still_remaining = set()
        for target in remaining:
            # Check if any catalog entry has lhs==target and rhs_vars ⊆ known
            target_rows = catalog_df[catalog_df["lhs"] == target]
            can_derive = any(
                set(row["rhs_vars"]) <= known
                for _, row in target_rows.iterrows()
            )
            if can_derive:
                derivable.add(target)
                known.add(target)
                changed = True
            else:
                still_remaining.add(target)
        remaining = still_remaining

    return {
        "derivable": derivable,
        "not_derivable": remaining,
    }
