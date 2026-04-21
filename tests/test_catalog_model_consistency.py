"""
Enforces the invariant from etdmap/DECISIONS.md ADR-003:
every LHS target in catalog.parquet must be a member of all_performance_data_columns.

A failure here means a rule was added to Rule.csv for a column that does not exist
in etdmodel.csv. Fix: add the column to etdmodel.csv (with the correct Entiteit) and
run sync_data_model.py to rebuild the catalog.
"""

import pytest

from etdmap.catalog import load_catalog
from etdmap.data_model import all_performance_data_columns


def test_all_catalog_lhs_columns_are_in_model():
    catalog_df = load_catalog()
    catalog_lhs = set(catalog_df["lhs"].dropna().unique())
    model_cols = set(all_performance_data_columns)
    outside_model = catalog_lhs - model_cols
    assert not outside_model, (
        f"Catalog LHS columns not in all_performance_data_columns: {sorted(outside_model)}. "
        f"Add them to etdmodel.csv (Entiteit=PrestatiedataBerekend) and rebuild the catalog."
    )
