from pathlib import Path

import pandas as pd
import pytest

from etdmap.catalog import (
    check_derivability,
    load_catalog,
    load_rules,
    rule_all_variables,
    rule_lhs_variables,
    rule_rhs_variables,
)


def test_load_rules_returns_expected_columns():
    rules_df = load_rules()
    assert isinstance(rules_df, pd.DataFrame)
    for col in ("target", "expression", "rhs_vars", "physical_model"):
        assert col in rules_df.columns, f"Rule.csv missing column: {col}"


def test_load_rules_nonempty():
    rules_df = load_rules()
    assert len(rules_df) > 0


def test_rule_lhs_variables_nonempty():
    assert isinstance(rule_lhs_variables, set)
    assert len(rule_lhs_variables) > 0


def test_rule_rhs_variables_nonempty():
    assert isinstance(rule_rhs_variables, set)
    assert len(rule_rhs_variables) > 0


def test_rule_all_variables_is_union():
    assert rule_all_variables == rule_lhs_variables | rule_rhs_variables


def test_rule_lhs_rhs_are_disjoint_from_each_other_or_overlap_intentionally():
    """LHS targets may appear as RHS inputs (cascading rules) — just verify the sets are sane."""
    assert rule_lhs_variables <= rule_all_variables
    assert rule_rhs_variables <= rule_all_variables


def test_check_derivability_known_derivable():
    """A column derivable via catalog must appear in derivable output."""
    catalog_df = pd.DataFrame([
        {"lhs": "C", "rhs_text": "A + B", "rhs_vars": ["A", "B"], "rhs_var_count": 2},
    ])
    report = check_derivability(
        available={"A", "B"},
        required_targets={"C"},
        catalog_df=catalog_df,
    )
    assert "C" in report["derivable"]
    assert not report["not_derivable"]


def test_check_derivability_cascade():
    """Columns derivable only after a prior derivation must still be found."""
    catalog_df = pd.DataFrame([
        {"lhs": "B", "rhs_text": "A", "rhs_vars": ["A"], "rhs_var_count": 1},
        {"lhs": "C", "rhs_text": "B", "rhs_vars": ["B"], "rhs_var_count": 1},
    ])
    report = check_derivability(
        available={"A"},
        required_targets={"B", "C"},
        catalog_df=catalog_df,
    )
    assert report["derivable"] == {"B", "C"}
    assert not report["not_derivable"]


def test_check_derivability_not_derivable():
    catalog_df = pd.DataFrame([
        {"lhs": "C", "rhs_text": "A + B", "rhs_vars": ["A", "B"], "rhs_var_count": 2},
    ])
    report = check_derivability(
        available={"A"},
        required_targets={"C"},
        catalog_df=catalog_df,
    )
    assert "C" in report["not_derivable"]


def test_load_catalog_succeeds():
    """catalog.parquet must be loadable. If this fails, run sync_data_model.py."""
    catalog_df = load_catalog()
    assert isinstance(catalog_df, pd.DataFrame)
    assert "lhs" in catalog_df.columns
    assert "rhs_vars" in catalog_df.columns
    assert len(catalog_df) > 0
