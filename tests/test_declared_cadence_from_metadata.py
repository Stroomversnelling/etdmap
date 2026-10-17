"""A declared cadence is used in preference to a measured one.

A column's cadence comes from three sources, checked in this order: the
supplier mapping's ``Meetfrequentie``, the spacing measured in the data where
the readings are regular enough to trust, and the grid the household batch
declares.

The declared value going first is deliberate. Corrections are made in the
metadata and the delivered data is static, so a researcher who has looked at a
column and recorded what it does should not be overruled by what a particular
household's record happens to show. Where the two disagree the difference is
logged, and the measured value is kept alongside in ``cadence_detected`` so a
report can surface it.

The loader that reads ``Meetfrequentie`` lives in etdworkflow, which owns the
supplier mapping CSV; etdmap only accepts the dict it produces.
"""

import logging

import pandas as pd
import pytest

from etdmap.index_helpers import frequency_to_minutes
from etdmap.mapping_helpers import detect_column_cadence

GRID = 5
COL = "ElektriciteitsgebruikBoilervat"


def _frame(minutes, periods=288, grid=GRID):
    """A frame on *grid* where COL carries a value every *minutes*."""
    every = minutes // grid
    dates = pd.date_range("2024-01-01", periods=periods, freq=f"{grid}min")
    return pd.DataFrame({
        "ReadingDate": dates,
        COL: pd.array(
            [float(i) if i % every == 0 else pd.NA for i in range(periods)],
            dtype="Float64",
        ),
    })


def _resolved(df, **kwargs):
    kwargs.setdefault("grid_minutes", GRID)
    return detect_column_cadence(df, **kwargs).set_index("column")


class TestMeetfrequentieIsADuration:
    """Values are written as durations, so the same interval may be written in
    whichever unit suits it. Comparing the text would make '1h' and '60min'
    different cadences."""

    @pytest.mark.parametrize(
        "written,minutes",
        [("1h", 60), ("15min", 15), ("60min", 60), ("5min", 5), ("30s", 0.5)],
    )
    def test_the_values_the_supplier_table_carries_parse(self, written, minutes):
        assert frequency_to_minutes(written) == minutes

    def test_an_hour_written_two_ways_is_one_cadence(self):
        assert frequency_to_minutes("1h") == frequency_to_minutes("60min")


class TestTheDeclaredValueIsUsed:
    def test_a_declaration_is_used_where_there_is_one(self):
        out = _resolved(_frame(60), declared={COL: 15})
        assert out.loc[COL, "cadence_minutes"] == 15
        assert out.loc[COL, "cadence_source"] == "declared"

    def test_it_is_used_even_against_a_confident_measurement(self):
        """The data says 60 and says it consistently. The declaration still
        wins: the metadata is where a correction gets made."""
        out = _resolved(_frame(60), declared={COL: 15})
        assert out.loc[COL, "cadence_detected"] == 60
        assert out.loc[COL, "cadence_minutes"] == 15

    def test_the_measurement_is_kept_so_a_report_can_show_the_difference(self):
        out = _resolved(_frame(60), declared={COL: 15})
        assert out.loc[COL, "cadence_detected"] == 60
        assert out.loc[COL, "confidence"] > 0.8

    def test_the_difference_is_logged(self, caplog):
        with caplog.at_level(logging.WARNING):
            _resolved(_frame(60), declared={COL: 15})
        assert "Declared cadence disagrees with the data" in caplog.text
        assert "Using the declared values" in caplog.text

    def test_agreement_is_not_logged(self, caplog):
        with caplog.at_level(logging.WARNING):
            _resolved(_frame(60), declared={COL: 60})
        assert "disagrees" not in caplog.text

    def test_a_column_with_no_declaration_is_measured(self):
        out = _resolved(_frame(60), declared={"SomeOtherColumn": 15})
        assert out.loc[COL, "cadence_minutes"] == 60
        assert out.loc[COL, "cadence_source"] == "detected"

    def test_no_declarations_at_all_is_the_ordinary_case(self):
        out = _resolved(_frame(60), declared={})
        assert out.loc[COL, "cadence_minutes"] == 60
        assert out.loc[COL, "cadence_source"] == "detected"


class TestEteckShapedDeclarations:
    """The rows that actually carry Meetfrequentie today: three variables at
    '1h' and one at '15min', all Eteck. Their shape is a column delivering far
    more coarsely than the grid the batch is recorded on."""

    def test_an_hourly_column_on_a_five_minute_grid(self):
        declared = {COL: int(frequency_to_minutes("1h"))}
        out = _resolved(_frame(60), declared=declared)
        assert out.loc[COL, "cadence_minutes"] == 60

    def test_a_quarter_hourly_column_on_a_five_minute_grid(self):
        declared = {COL: int(frequency_to_minutes("15min"))}
        out = _resolved(_frame(15), declared=declared)
        assert out.loc[COL, "cadence_minutes"] == 15

    def test_a_declaration_survives_an_unmeasurable_column(self):
        """Two readings are too few to measure anything. A declaration is the
        only thing that can speak for such a column, and it does."""
        df = _frame(60, periods=2)
        out = _resolved(df, declared={COL: 60})
        assert pd.isna(out.loc[COL, "cadence_detected"])
        assert out.loc[COL, "cadence_minutes"] == 60
        assert out.loc[COL, "cadence_source"] == "declared"


class TestSeveralCadencesInOneHousehold:
    """Allowed, and the reason cadence is per column rather than per household."""

    def test_each_column_keeps_its_own(self):
        dates = pd.date_range("2024-01-01", periods=288, freq=f"{GRID}min")
        df = pd.DataFrame({"ReadingDate": dates})
        for name, minutes in [("fast", 5), ("medium", 15), ("slow", 60)]:
            every = minutes // GRID
            df[name] = pd.array(
                [float(i) if i % every == 0 else pd.NA for i in range(288)],
                dtype="Float64",
            )

        out = _resolved(df, declared={"medium": 30})
        assert out.loc["fast", "cadence_minutes"] == 5
        assert out.loc["medium", "cadence_minutes"] == 30
        assert out.loc["slow", "cadence_minutes"] == 60

    def test_a_column_coarser_than_the_grid_keeps_its_cadence(self):
        """The grid is where a column falls back to, not a ceiling on it."""
        out = _resolved(_frame(60))
        assert out.loc["ReadingDate", "cadence_minutes"] == GRID
        assert out.loc[COL, "cadence_minutes"] == 60
