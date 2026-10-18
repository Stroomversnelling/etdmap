"""What the supplier delivered, recorded per cell before anything changes it.

Three signals come out of one pass: RAW_DELIVERED_VALUE on a value that arrived,
and on each pd.NA either MISSING_VALUE, where the column was due to report, or
OFF_CADENCE_NA, where it was not. Telling those apart is what makes delivery
completeness measurable, and it needs the cadence the column reports at.

Synthetic frames only -- no pipeline, no fixtures.
"""

import pandas as pd
import pytest

from etdmap.flags import MappingFlag, flag_column
from etdmap.mapping_helpers import mark_delivery_and_cadence

GRID = 5
CUM = "ElektriciteitNetgebruik"
DIFF = "ElektriciteitNetgebruikDiff"


def _cadence(entries):
    """The table detect_column_cadence produces for one household."""
    return pd.DataFrame({
        "column": pd.array(list(entries), dtype="string"),
        "cadence_minutes": pd.array(list(entries.values()), dtype="Int64"),
        "cadence_source": pd.array(["declared"] * len(entries), dtype="string"),
        "cadence_detected": pd.array([pd.NA] * len(entries), dtype="Int64"),
        "confidence": pd.array([pd.NA] * len(entries), dtype="Float64"),
        "n_readings": pd.array([pd.NA] * len(entries), dtype="Int64"),
    })


def _frame(values, column=CUM, n=None):
    n = len(values) if n is None else n
    return pd.DataFrame({
        "HuisIdBSV": pd.array([1] * n, dtype="Int64"),
        "ReadingDate": pd.date_range("2026-01-01", periods=n, freq="5min"),
        column: pd.array(values, dtype="Float64"),
    })


def _flags(df, column):
    name = flag_column(column)
    if name not in df.columns:
        return pd.Series(0, index=df.index, dtype="Int64")
    return df[name].fillna(0).astype("Int64")


def _has(df, column, flag):
    return ((_flags(df, column) & flag.value) != 0).tolist()


def _every_third(n, first=2, missing=()):
    """A coarse column: a reading every third row from *first*."""
    values = [pd.NA] * n
    for row in range(first, n, 3):
        values[row] = 0.6
    for row in missing:
        values[row] = pd.NA
    return values


class TestADeliveredValueIsRecorded:
    def test_every_delivered_cell_is_marked(self):
        df = _frame([1.0, 2.0, pd.NA, 4.0])
        mark_delivery_and_cadence(df, cadence=_cadence({CUM: GRID}), grid_minutes=GRID)
        assert _has(df, CUM, MappingFlag.RAW_DELIVERED_VALUE) == [
            True, True, False, True
        ]

    def test_an_absent_cell_is_not_marked_as_delivered(self):
        df = _frame([pd.NA, 2.0])
        mark_delivery_and_cadence(df, cadence=_cadence({CUM: GRID}), grid_minutes=GRID)
        assert _has(df, CUM, MappingFlag.RAW_DELIVERED_VALUE) == [False, True]

    def test_a_column_that_delivered_nothing_gets_no_findings(self):
        """A model column materialised for a device the household does not have
        reports nothing, so there is no delivery to record and no lattice to
        anchor -- marking its every row MISSING_VALUE would invent a claim."""
        df = _frame([pd.NA, pd.NA, pd.NA])
        mark_delivery_and_cadence(df, cadence=_cadence({CUM: GRID}), grid_minutes=GRID)
        assert flag_column(CUM) not in df.columns


class TestTheTwoKindsOfAbsence:
    def test_a_grid_cadence_column_has_no_off_cadence_rows(self):
        """Reporting every row means every row was due, so each pd.NA is a
        reading that did not arrive."""
        df = _frame([1.0, pd.NA, 3.0, pd.NA])
        mark_delivery_and_cadence(df, cadence=_cadence({CUM: GRID}), grid_minutes=GRID)
        assert _has(df, CUM, MappingFlag.MISSING_VALUE) == [False, True, False, True]
        assert not any(_has(df, CUM, MappingFlag.OFF_CADENCE_NA))

    def test_a_coarse_column_marks_the_rows_between_ticks(self):
        df = _frame(_every_third(9))
        mark_delivery_and_cadence(
            df, cadence=_cadence({CUM: GRID * 3}), grid_minutes=GRID
        )
        assert _has(df, CUM, MappingFlag.OFF_CADENCE_NA) == [
            True, True, False, True, True, False, True, True, False
        ]
        assert not any(_has(df, CUM, MappingFlag.MISSING_VALUE))

    def test_a_coarse_column_marks_a_tick_that_did_not_arrive(self):
        """The one case the two signals exist to separate: row 5 was due a
        reading and row 4 was not, and both are pd.NA."""
        df = _frame(_every_third(9, missing=(5,)))
        mark_delivery_and_cadence(
            df, cadence=_cadence({CUM: GRID * 3}), grid_minutes=GRID
        )
        assert _has(df, CUM, MappingFlag.MISSING_VALUE) == [
            False, False, False, False, False, True, False, False, False
        ]
        assert _has(df, CUM, MappingFlag.OFF_CADENCE_NA)[4] is True

    def test_the_lattice_is_anchored_on_the_first_delivered_reading(self):
        """A column whose first reading lands on row 1 reports on 1, 4, 7 --
        anchoring on row 0 instead would call every tick a missed reading."""
        df = _frame(_every_third(9, first=1))
        mark_delivery_and_cadence(
            df, cadence=_cadence({CUM: GRID * 3}), grid_minutes=GRID
        )
        assert not any(_has(df, CUM, MappingFlag.MISSING_VALUE))

    def test_no_cell_carries_both_kinds_of_absence(self):
        df = _frame(_every_third(12, missing=(5,)))
        mark_delivery_and_cadence(
            df, cadence=_cadence({CUM: GRID * 3}), grid_minutes=GRID
        )
        both = [
            missing and off
            for missing, off in zip(
                _has(df, CUM, MappingFlag.MISSING_VALUE),
                _has(df, CUM, MappingFlag.OFF_CADENCE_NA),
            )
        ]
        assert not any(both)


class TestWithoutALattice:
    """A cadence that cannot be stated in whole grid rows leaves the gaps
    unclassified. Asserting either signal would be a guess."""

    def test_an_unrecorded_cadence_classifies_no_gap(self):
        df = _frame([1.0, pd.NA, 3.0])
        mark_delivery_and_cadence(df, cadence=_cadence({}), grid_minutes=GRID)
        assert _has(df, CUM, MappingFlag.RAW_DELIVERED_VALUE) == [True, False, True]
        assert not any(_has(df, CUM, MappingFlag.MISSING_VALUE))
        assert not any(_has(df, CUM, MappingFlag.OFF_CADENCE_NA))

    def test_a_cadence_that_does_not_divide_the_grid_classifies_no_gap(self):
        df = _frame([1.0, pd.NA, 3.0])
        mark_delivery_and_cadence(df, cadence=_cadence({CUM: 7}), grid_minutes=GRID)
        assert _has(df, CUM, MappingFlag.RAW_DELIVERED_VALUE) == [True, False, True]
        assert not any(_has(df, CUM, MappingFlag.MISSING_VALUE))
        assert not any(_has(df, CUM, MappingFlag.OFF_CADENCE_NA))


class TestBothFormsAreMarked:
    """A delivered interval column is marked in its own right, and a coarse
    register can miss a tick as easily as an interval can."""

    def test_a_delivered_interval_column_is_marked(self):
        df = _frame([pd.NA] * 9)
        df[DIFF] = pd.array(_every_third(9), dtype="Float64")
        mark_delivery_and_cadence(
            df, cadence=_cadence({DIFF: GRID * 3}), grid_minutes=GRID
        )
        assert _has(df, DIFF, MappingFlag.RAW_DELIVERED_VALUE)[2] is True
        assert _has(df, DIFF, MappingFlag.OFF_CADENCE_NA)[1] is True

    def test_each_form_reads_its_own_cadence(self):
        """The register reports hourly and the interval column at the grid, so
        the same row is off-cadence for one and a missed reading for the other."""
        df = _frame(_every_third(9))
        df[DIFF] = pd.array([1.0, pd.NA] + [1.0] * 7, dtype="Float64")
        mark_delivery_and_cadence(
            df,
            cadence=_cadence({CUM: GRID * 3, DIFF: GRID}),
            grid_minutes=GRID,
        )
        assert _has(df, CUM, MappingFlag.OFF_CADENCE_NA)[1] is True
        assert _has(df, DIFF, MappingFlag.MISSING_VALUE)[1] is True


class TestFindingsAccumulate:
    def test_a_delivered_value_a_later_step_blanks_keeps_both_findings(self):
        """The reason this runs before the cleaning steps: a blanked cell has to
        read as delivered and then removed, not as never delivered."""
        from etdmap.flags import set_flags

        df = _frame([1.0, 2.0, 3.0])
        mark_delivery_and_cadence(df, cadence=_cadence({CUM: GRID}), grid_minutes=GRID)

        blanked = pd.Series([False, True, False], index=df.index)
        set_flags(df, CUM, blanked, MappingFlag.NULLED)
        df.loc[blanked, CUM] = pd.NA

        assert _has(df, CUM, MappingFlag.RAW_DELIVERED_VALUE)[1] is True
        assert _has(df, CUM, MappingFlag.NULLED)[1] is True

    def test_nothing_in_the_frame_is_changed(self):
        df = _frame(_every_third(9, missing=(5,)))
        before = df[CUM].astype("Float64").copy()
        mark_delivery_and_cadence(
            df, cadence=_cadence({CUM: GRID * 3}), grid_minutes=GRID
        )
        pd.testing.assert_series_equal(df[CUM], before)


class TestAValueThePipelineWroteIsNotDelivered:
    """fill_down_infrequent_devices runs in the mapper, ahead of this pipeline,
    and leaves no row missing. Without excluding what it wrote, every cell it
    filled would read as delivered."""

    def _filled_down(self, values, filled):
        from etdmap.flags import set_flags

        df = _frame(values)
        mask = pd.Series(filled, index=df.index)
        set_flags(df, CUM, mask, MappingFlag.FILLED_DOWN)
        return df

    def test_a_filled_down_cell_is_not_marked_as_delivered(self):
        df = self._filled_down([1.0, 1.0, 1.0], [False, True, True])
        mark_delivery_and_cadence(df, cadence=_cadence({CUM: GRID}), grid_minutes=GRID)
        assert _has(df, CUM, MappingFlag.RAW_DELIVERED_VALUE) == [True, False, False]

    def test_a_filled_down_cell_is_not_owed_a_reading_either(self):
        """It holds a value, whoever wrote it, so neither absence signal
        applies."""
        df = self._filled_down([1.0, 1.0, 1.0], [False, True, True])
        mark_delivery_and_cadence(df, cadence=_cadence({CUM: GRID}), grid_minutes=GRID)
        assert not any(_has(df, CUM, MappingFlag.MISSING_VALUE))
        assert not any(_has(df, CUM, MappingFlag.OFF_CADENCE_NA))

    def test_a_column_of_nothing_but_written_values_records_no_delivery(self):
        """Zeros written for a device the household does not have report nothing
        the meter said."""
        df = self._filled_down([0.0, 0.0, 0.0], [True, True, True])
        mark_delivery_and_cadence(df, cadence=_cadence({CUM: GRID}), grid_minutes=GRID)
        assert not any(_has(df, CUM, MappingFlag.RAW_DELIVERED_VALUE))

    def test_the_lattice_is_anchored_on_a_delivered_reading(self):
        """Row 0 was written, so the first reading is row 2 and the column is due
        on 2, 5, 8 -- anchoring on the written row would shift the whole
        lattice."""
        values = [0.0] + _every_third(9, first=2)[1:]
        df = self._filled_down(values, [True] + [False] * 8)
        mark_delivery_and_cadence(
            df, cadence=_cadence({CUM: GRID * 3}), grid_minutes=GRID
        )
        assert not any(_has(df, CUM, MappingFlag.MISSING_VALUE))


class TestOnlyModelDataColumns:
    def test_an_identifier_column_is_not_marked(self):
        df = _frame([1.0, 2.0])
        mark_delivery_and_cadence(df, cadence=_cadence({CUM: GRID}), grid_minutes=GRID)
        assert flag_column("HuisIdBSV") not in df.columns
        assert flag_column("ReadingDate") not in df.columns
