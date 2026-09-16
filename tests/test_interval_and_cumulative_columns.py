"""
Mapping-stage handling of cumulative and interval columns: which
columns survive rearrangement, which side the interval values come from, and
which findings the interval column inherits.
"""

import pandas as pd
import pytest

from etdmap.flags import MappingFlag, flag_column
from etdmap.mapping_helpers import (
    add_diff_columns,
    fill_down_infrequent_devices,
    rearrange_model_columns,
)

CUM = "WatergebruikWarmTapwater"
DIFF = f"{CUM}Diff"


def _interval_only(n: int, per_interval: float) -> pd.DataFrame:
    """A provider delivering the interval column and no cumulative variable."""
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


class TestIntervalsInheritTheirCumulativeFindings:
    """An interval is the difference between two readings, so it is no more
    measured than the readings it comes from.

    Downstream stages filter the interval column, so a finding left only on the
    cumulative column would never reach them.
    """

    @staticmethod
    def _reports_every_third_interval(n=8):
        """A device delivering a reading every third slot, filled down between."""
        values = [10.0 + 3.0 * (i // 3) if i % 3 == 0 else pd.NA for i in range(n)]
        df = pd.DataFrame(
            {
                "ReadingDate": pd.date_range("2026-01-01", periods=n, freq="5min"),
                CUM: pd.array(values, dtype="Float64"),
            }
        )
        fill_down_infrequent_devices(df, [CUM])
        return add_diff_columns(df, context="infrequent")

    def test_a_filled_reading_marks_the_interval_it_produced(self):
        out = self._reports_every_third_interval()
        carried = (out[flag_column(DIFF)] & MappingFlag.FILLED_DOWN.value) != 0

        # Every interval whose own reading was filled.
        assert carried[(out[flag_column(CUM)] & MappingFlag.FILLED_DOWN.value) != 0].all()

    def test_the_interval_after_a_filled_reading_is_marked_too(self):
        """That interval is delivered at one end only: it covers the whole span
        since the last real reading, not the slot it sits on."""
        out = self._reports_every_third_interval()
        carried = (out[flag_column(DIFF)] & MappingFlag.FILLED_DOWN.value) != 0

        # Position 3 carries a delivered reading, but position 2 was filled.
        assert int(out[flag_column(CUM)].iloc[3]) == MappingFlag.NONE.value
        assert bool(carried.iloc[3])

    def test_an_untouched_column_gains_no_findings(self):
        n = 6
        df = pd.DataFrame(
            {
                "ReadingDate": pd.date_range("2026-01-01", periods=n, freq="5min"),
                CUM: pd.array([10.0 + i for i in range(n)], dtype="Float64"),
            }
        )
        fill_down_infrequent_devices(df, [CUM])
        out = add_diff_columns(df, context="clean")

        assert int(out[flag_column(DIFF)].sum()) == MappingFlag.NONE.value
