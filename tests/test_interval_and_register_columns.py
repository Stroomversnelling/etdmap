"""
Mapping-stage handling of cumulative (register) and interval columns: which
columns survive rearrangement, and which side the interval values come from.
"""

import pandas as pd
import pytest

from etdmap.mapping_helpers import add_diff_columns, rearrange_model_columns

CUM = "WatergebruikWarmTapwater"
DIFF = f"{CUM}Diff"


def _interval_only(n: int, per_interval: float) -> pd.DataFrame:
    """A provider delivering the interval column and no register."""
    return pd.DataFrame(
        {
            "ReadingDate": pd.date_range("2026-01-01", periods=n, freq="5min"),
            DIFF: pd.array([per_interval] * n, dtype="Float64"),
        }
    )


def test_supplied_interval_column_survives_rearrange():
    """A provider-supplied interval column is retained through rearrangement."""
    out = rearrange_model_columns(
        household_df=_interval_only(12, 2.0), add_columns=True, context="supplied"
    )

    assert DIFF in out.columns


def test_supplied_interval_values_survive_mapping():
    """
    Provider-supplied interval values remain recoverable after rearrangement and
    interval derivation, under the canonical name or a reserved one.
    """
    n, supplied_total = 12, 24.0
    out = add_diff_columns(
        rearrange_model_columns(
            household_df=_interval_only(n, supplied_total / n),
            add_columns=True,
            context="supplied",
        ),
        context="supplied",
    )

    candidates = [c for c in out.columns if c.startswith(DIFF)]
    recovered = max(
        (float(out[c].sum()) for c in candidates if out[c].notna().any()), default=0.0
    )
    assert recovered == pytest.approx(supplied_total)
