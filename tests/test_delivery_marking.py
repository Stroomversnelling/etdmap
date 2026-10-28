"""What the supplier delivered, recorded per cell before anything changes it.

Three signals come out of one pass: RAW_DELIVERED_VALUE on a value that arrived,
and on each pd.NA either ON_CADENCE_NA, where the column was due to report, or
OFF_CADENCE_NA, where it was not. Telling those apart is what makes delivery
completeness measurable, and it needs the cadence the column reports at.

Synthetic frames only -- no pipeline, no fixtures.
"""

import pandas as pd

from etdmap import ids
from etdmap.flags import MappingFlag, flag_column
from etdmap.mapping_helpers import mark_cadence, mark_delivered
from etdmap.testing import cadence_sidecar, datasets_frame, household_batch_of

GRID = 5
CUM = "ElektriciteitNetgebruik"
DIFF = "ElektriciteitNetgebruikDiff"


def _cadence(entries):
    """The table detect_column_cadence produces for one household dataset: a
    row per column of *entries* at its cadence in minutes, the grid on the
    timestamp row, and no dataset id."""
    return cadence_sidecar(entries, grid_minutes=GRID)


def _frame(values, column=CUM, **extra):
    """One dataset's rows: *values* for *column*, plus any further column named.

    Naive timestamps, and no cumulative counterpart is completed: the column
    under test stands alone here, and a completed counterpart would carry a
    delivery record of its own.
    """
    return datasets_frame(
        {household_batch_of(1): {column: list(values), **extra}},
        start="2026-01-01",
        tz=None,
        pairs=False,
    )


def _mark(df, **kwargs):
    """Delivery first, then cadence, which is the order the pipeline runs them
    in and the order the cadence pass depends on: it anchors its lattice on the
    delivery record rather than re-deriving it from values a later step may have
    written."""
    mark_delivered(df)
    mark_cadence(df, **kwargs)


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
        _mark(df, cadence=_cadence({CUM: GRID}), grid_minutes=GRID)
        assert _has(df, CUM, MappingFlag.RAW_DELIVERED_VALUE) == [
            True, True, False, True
        ]

    def test_an_absent_cell_is_not_marked_as_delivered(self):
        df = _frame([pd.NA, 2.0])
        _mark(df, cadence=_cadence({CUM: GRID}), grid_minutes=GRID)
        assert _has(df, CUM, MappingFlag.RAW_DELIVERED_VALUE) == [False, True]

    def test_a_column_that_delivered_nothing_gets_no_cadence_findings(self):
        """It has no first reading, so there is no lattice to anchor and
        nothing to say about which of its gaps were due -- marking its every row
        ON_CADENCE_NA would invent a claim.

        It is not silent, though: ``mark_delivered`` answers every cell
        DELIVERED_NA, which is what distinguishes a column the supplier never
        populated from a column no step reached."""
        df = _frame([pd.NA, pd.NA, pd.NA])
        _mark(df, cadence=_cadence({CUM: GRID}), grid_minutes=GRID)
        assert not any(_has(df, CUM, MappingFlag.ON_CADENCE_NA))
        assert not any(_has(df, CUM, MappingFlag.OFF_CADENCE_NA))
        assert all(_has(df, CUM, MappingFlag.DELIVERED_NA))


class TestTheTwoKindsOfAbsence:
    def test_a_grid_cadence_column_has_no_off_cadence_rows(self):
        """Reporting every row means every row was due, so each pd.NA is a
        reading that did not arrive."""
        df = _frame([1.0, pd.NA, 3.0, pd.NA])
        _mark(df, cadence=_cadence({CUM: GRID}), grid_minutes=GRID)
        assert _has(df, CUM, MappingFlag.ON_CADENCE_NA) == [False, True, False, True]
        assert not any(_has(df, CUM, MappingFlag.OFF_CADENCE_NA))

    def test_a_coarse_column_marks_the_rows_between_ticks(self):
        df = _frame(_every_third(9))
        _mark(
            df, cadence=_cadence({CUM: GRID * 3}), grid_minutes=GRID
        )
        assert _has(df, CUM, MappingFlag.OFF_CADENCE_NA) == [
            True, True, False, True, True, False, True, True, False
        ]
        assert not any(_has(df, CUM, MappingFlag.ON_CADENCE_NA))

    def test_a_coarse_column_marks_a_tick_that_did_not_arrive(self):
        """The one case the two signals exist to separate: row 5 was due a
        reading and row 4 was not, and both are pd.NA."""
        df = _frame(_every_third(9, missing=(5,)))
        _mark(
            df, cadence=_cadence({CUM: GRID * 3}), grid_minutes=GRID
        )
        assert _has(df, CUM, MappingFlag.ON_CADENCE_NA) == [
            False, False, False, False, False, True, False, False, False
        ]
        assert _has(df, CUM, MappingFlag.OFF_CADENCE_NA)[4] is True

    def test_the_lattice_is_anchored_on_the_first_delivered_reading(self):
        """A column whose first reading lands on row 1 reports on 1, 4, 7 --
        anchoring on row 0 instead would call every tick a missed reading."""
        df = _frame(_every_third(9, first=1))
        _mark(
            df, cadence=_cadence({CUM: GRID * 3}), grid_minutes=GRID
        )
        assert not any(_has(df, CUM, MappingFlag.ON_CADENCE_NA))

    def test_no_cell_carries_both_kinds_of_absence(self):
        df = _frame(_every_third(12, missing=(5,)))
        _mark(
            df, cadence=_cadence({CUM: GRID * 3}), grid_minutes=GRID
        )
        both = [
            missing and off
            for missing, off in zip(
                _has(df, CUM, MappingFlag.ON_CADENCE_NA),
                _has(df, CUM, MappingFlag.OFF_CADENCE_NA),
            )
        ]
        assert not any(both)


class TestWithoutALattice:
    """A cadence that cannot be stated in whole grid rows leaves the gaps
    unclassified. Asserting either signal would be a guess."""

    def test_an_unrecorded_cadence_classifies_no_gap(self):
        df = _frame([1.0, pd.NA, 3.0])
        _mark(df, cadence=_cadence({}), grid_minutes=GRID)
        assert _has(df, CUM, MappingFlag.RAW_DELIVERED_VALUE) == [True, False, True]
        assert not any(_has(df, CUM, MappingFlag.ON_CADENCE_NA))
        assert not any(_has(df, CUM, MappingFlag.OFF_CADENCE_NA))

    def test_a_cadence_that_does_not_divide_the_grid_classifies_no_gap(self):
        df = _frame([1.0, pd.NA, 3.0])
        _mark(df, cadence=_cadence({CUM: 7}), grid_minutes=GRID)
        assert _has(df, CUM, MappingFlag.RAW_DELIVERED_VALUE) == [True, False, True]
        assert not any(_has(df, CUM, MappingFlag.ON_CADENCE_NA))
        assert not any(_has(df, CUM, MappingFlag.OFF_CADENCE_NA))


class TestBothFormsAreMarked:
    """A delivered interval column is marked in its own right, and a coarse
    register can miss a tick as easily as an interval can."""

    def test_a_delivered_interval_column_is_marked(self):
        df = _frame([pd.NA] * 9, **{DIFF: _every_third(9)})
        _mark(
            df, cadence=_cadence({DIFF: GRID * 3}), grid_minutes=GRID
        )
        assert _has(df, DIFF, MappingFlag.RAW_DELIVERED_VALUE)[2] is True
        assert _has(df, DIFF, MappingFlag.OFF_CADENCE_NA)[1] is True

    def test_each_form_reads_its_own_cadence(self):
        """The register reports hourly and the interval column at the grid, so
        the same row is off-cadence for one and a missed reading for the other."""
        df = _frame(_every_third(9), **{DIFF: [1.0, pd.NA] + [1.0] * 7})
        _mark(
            df,
            cadence=_cadence({CUM: GRID * 3, DIFF: GRID}),
            grid_minutes=GRID,
        )
        assert _has(df, CUM, MappingFlag.OFF_CADENCE_NA)[1] is True
        assert _has(df, DIFF, MappingFlag.ON_CADENCE_NA)[1] is True


class TestFindingsAccumulate:
    def test_a_delivered_value_a_later_step_blanks_keeps_both_findings(self):
        """The reason this runs before the cleaning steps: a blanked cell has to
        read as delivered and then removed, not as never delivered."""
        from etdmap.flags import set_flags

        df = _frame([1.0, 2.0, 3.0])
        _mark(df, cadence=_cadence({CUM: GRID}), grid_minutes=GRID)

        blanked = pd.Series([False, True, False], index=df.index)
        set_flags(df, CUM, blanked, MappingFlag.NULLED)
        df.loc[blanked, CUM] = pd.NA

        assert _has(df, CUM, MappingFlag.RAW_DELIVERED_VALUE)[1] is True
        assert _has(df, CUM, MappingFlag.NULLED)[1] is True

    def test_nothing_in_the_frame_is_changed(self):
        df = _frame(_every_third(9, missing=(5,)))
        before = df[CUM].astype("Float64").copy()
        _mark(
            df, cadence=_cadence({CUM: GRID * 3}), grid_minutes=GRID
        )
        pd.testing.assert_series_equal(df[CUM], before)


class TestAFillCannotBeMistakenForADelivery:
    """Delivery is recorded on the frame as delivered, before anything fills it,
    so a filled cell cannot read as delivered.

    These tests pin that ordering: they fail if the mark moves behind
    ``fill_down_infrequent_devices``, which is the point at which a fill would
    become indistinguishable from a delivery and have to be subtracted back
    out.
    """

    def _fill(self, df, filled, value):
        """What ``fill_down_infrequent_devices`` does, after delivery was
        recorded: writes a value and records that it wrote one."""
        from etdmap.flags import set_flags

        mask = pd.Series(filled, index=df.index)
        set_flags(df, CUM, mask, MappingFlag.FILLED_DOWN)
        df.loc[mask, CUM] = value
        return df

    def test_a_cell_filled_after_the_mark_reads_as_not_delivered(self):
        df = _frame([1.0, pd.NA, pd.NA])
        mark_delivered(df)
        self._fill(df, [False, True, True], 1.0)
        mark_cadence(df, cadence=_cadence({CUM: GRID}), grid_minutes=GRID)

        assert _has(df, CUM, MappingFlag.RAW_DELIVERED_VALUE) == [
            True, False, False
        ]
        assert _has(df, CUM, MappingFlag.DELIVERED_NA) == [False, True, True]
        # Both facts on the cell: the supplier sent nothing, and we filled it.
        assert _has(df, CUM, MappingFlag.FILLED_DOWN) == [False, True, True]

    def test_a_filled_cell_is_not_owed_a_reading_either(self):
        """It holds a value by cadence time, whoever wrote it, so neither
        absence signal applies."""
        df = _frame([1.0, pd.NA, pd.NA])
        mark_delivered(df)
        self._fill(df, [False, True, True], 1.0)
        mark_cadence(df, cadence=_cadence({CUM: GRID}), grid_minutes=GRID)
        assert not any(_has(df, CUM, MappingFlag.ON_CADENCE_NA))
        assert not any(_has(df, CUM, MappingFlag.OFF_CADENCE_NA))

    def test_a_column_of_nothing_but_filled_values_records_no_delivery(self):
        """A column the supplier never populated reports nothing the meter
        said, however many values the pipeline later writes into it."""
        df = _frame([pd.NA, pd.NA, pd.NA])
        mark_delivered(df)
        self._fill(df, [True, True, True], 0.0)
        mark_cadence(df, cadence=_cadence({CUM: GRID}), grid_minutes=GRID)
        assert not any(_has(df, CUM, MappingFlag.RAW_DELIVERED_VALUE))
        assert all(_has(df, CUM, MappingFlag.DELIVERED_NA))

    def test_the_lattice_is_anchored_on_the_delivery_record(self):
        """Row 0 is empty when delivery is recorded and filled afterwards, so
        the first reading is row 2 and the column is due on 2, 5, 8. Taking the
        anchor from whatever holds a value at cadence time would put it on row 0
        and shift the whole lattice to 0, 3, 6."""
        df = _frame(_every_third(9, first=2))
        mark_delivered(df)
        self._fill(df, [True] + [False] * 8, 0.0)
        mark_cadence(df, cadence=_cadence({CUM: GRID * 3}), grid_minutes=GRID)
        assert not any(_has(df, CUM, MappingFlag.ON_CADENCE_NA))


class TestOnlyModelDataColumns:
    def test_an_identifier_column_is_not_marked(self):
        df = _frame([1.0, 2.0])
        _mark(df, cadence=_cadence({CUM: GRID}), grid_minutes=GRID)
        assert flag_column(ids.HOUSEHOLD) not in df.columns
        assert flag_column("ReadingDate") not in df.columns
