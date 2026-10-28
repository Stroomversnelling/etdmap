"""One synthetic household through the pipeline, carrying the column shapes
that occur together in real data, asserting the flag combinations each produces.

The per-step tests elsewhere each build one frame and exercise one function.
That leaves the interactions untested, and the interactions are where the
defects have been: a flag written before the grid was completed, a delivery
mark taken after the fill had already run, a cadence lattice anchored on a value
the pipeline wrote. None of those are visible to a test that calls one step.

So this file runs ``run_standard_pipeline`` once over four columns chosen to
cover the shapes a supplier actually delivers:

=============================== ==================================================
``BOTH``                        cumulative and interval both delivered
``CUMULATIVE_ONLY``             cumulative delivered, interval derived from it
``INTERVAL_ONLY``               interval delivered, no cumulative to difference
``COARSE``                      cumulative reporting every 15 minutes on a 5-minute grid
=============================== ==================================================

and asserts, per column, what each cell ends up holding. A combination that
changes shows up here as a named expectation rather than as a number moving in a
comparison report.

Synthetic throughout: the frame is built in the test, with no fixture and no
generated dataset, because the point is to control exactly which cells are
delivered.
"""

import pandas as pd
import pytest

import etdmap
from etdmap.flags import MappingFlag, flag_column
from etdmap.mapping_helpers import mark_delivered, run_standard_pipeline
from etdmap.testing import cadence_sidecar, household_batch_of

GRID = 5
N = 12

BOTH = "ElektriciteitsgebruikRadiator"
CUMULATIVE_ONLY = "ElektriciteitsgebruikBoilervat"
INTERVAL_ONLY = "WarmteproductieWarmtepomp"
COARSE = "ElektriciteitsgebruikWTW"

#: Every k-th row, where k is the column's cadence in grid rows.
COARSE_STEP = 3


def _diff(column):
    return f"{column}Diff"


def _floats(values):
    return pd.array(values, dtype="Float64")


#: Each column's cadence in minutes: the interval its readings cover. Also what
#: a detection would have measured, because these timestamps sit exactly on the
#: cadence each column is declared at.
COLUMN_MINUTES = {
    BOTH: GRID,
    _diff(BOTH): GRID,
    CUMULATIVE_ONLY: GRID,
    _diff(INTERVAL_ONLY): GRID,
    COARSE: GRID * COARSE_STEP,
}

#: Readings each column delivers over the span. Every column reports on every
#: row except COARSE, which reports on every COARSE_STEP-th one: a cadence row
#: claiming N readings there would state a delivery the frame does not hold.
COLUMN_READINGS = {
    BOTH: N,
    _diff(BOTH): N,
    CUMULATIVE_ONLY: N,
    _diff(INTERVAL_ONLY): N,
    COARSE: N // COARSE_STEP,
}


#: Per-interval magnitudes, which must stay under the declared 5-minute bounds
#: so the cleaning steps have nothing to act on: the subject here is which
#: findings each column SHAPE produces, not what thresholds do to outliers. The
#: electrical Max is 0.32 kWh per five minutes; WarmteproductieWarmtepompDiff's
#: is 0.0576 GJ. A step over its bound reads as a catch-up, and the frozen-span
#: treatment then blanks a delivered register reading.
STEP = 0.1
HEAT_STEP = 0.01

#: Delivered on purpose as something the register does not imply, so the
#: rewrite is visible in the value and not only in the flags.
DELIVERED_INTERVAL = 0.2


def _frame():
    """The four shapes, on one five-minute grid with no missing rows.

    Local: the column set is the subject -- one cumulative is absent on purpose
    -- and the pipeline takes the frame as the supplier delivered it, with naive
    timestamps and no id columns.
    """
    rows = pd.date_range("2024-01-01 00:00", periods=N, freq=f"{GRID}min")
    return pd.DataFrame({
        "ReadingDate": rows,
        BOTH: _floats([100 + STEP * row for row in range(N)]),
        _diff(BOTH): _floats([DELIVERED_INTERVAL] * N),
        CUMULATIVE_ONLY: _floats([200 + STEP * row for row in range(N)]),
        # The interval alone: its cumulative is never delivered, so there is no
        # register to difference and nothing to derive from.
        _diff(INTERVAL_ONLY): _floats([HEAT_STEP] * N),
        COARSE: _floats([
            10 + STEP * (row // COARSE_STEP) if row % COARSE_STEP == 0
            else pd.NA
            for row in range(N)
        ]),
    })


@pytest.fixture(scope="module")
def mapped(tmp_path_factory):
    """One pipeline run, read back from the artifact it wrote.

    Module-scoped: the run is the expensive part and every test below reads the
    same output, which is also what makes the combinations comparable.
    """
    folder = tmp_path_factory.mktemp("flag_combinations")
    frame = _frame()
    household_id = 9001

    # Delivery is recorded on the frame as delivered, which is what the mappers
    # do before padding and before any fill. Without it no column carries a
    # delivery answer and the cadence pass has no anchor.
    mark_delivered(frame)

    previous = etdmap.options.mapped_output_format
    etdmap.options.mapped_output_format = "flat"
    try:
        run_standard_pipeline(
            frame,
            household_id=household_id,
            household_batch_id=household_batch_of(household_id),
            household_id_supplier=f"H{household_id}",
            mapped_folder_path=folder,
            grid_minutes=GRID,
            # The cadence a detection would have reported, declared instead:
            # each column at the interval its readings cover, and the grid on
            # the timestamp row. Measured throughout -- a detected value equal
            # to the cadence, full confidence, and the readings each column
            # actually delivers -- so a step reading any of the three gets a
            # number rather than pd.NA. No dataset id, which is the shape
            # detect_column_cadence returns and the pipeline expects.
            cadence=cadence_sidecar(
                COLUMN_MINUTES,
                grid_minutes=GRID,
                detected={"ReadingDate": GRID, **COLUMN_MINUTES},
                confidence=1.0,
                n_readings={"ReadingDate": N, **COLUMN_READINGS},
            ),
        )
    finally:
        etdmap.options.mapped_output_format = previous

    return pd.read_parquet(
        folder / f"household_{household_id}_table.parquet",
        dtype_backend="numpy_nullable",
    )


def _held(mapped, column, position):
    return MappingFlag(int(mapped[flag_column(column)].iloc[position]))


def _rows_with(mapped, column, flag):
    bits = mapped[flag_column(column)].fillna(0).astype("Int64")
    return [i for i in range(len(mapped)) if int(bits.iloc[i]) & flag.value]


class TestADeliveredCumulativeWithADeliveredInterval:
    """The supplier sent both halves. The interval is rewritten from the
    register's difference, so the cell holds two facts: a value was delivered
    here, and it was replaced."""

    def test_the_cumulative_reads_as_delivered_and_unchanged(self, mapped):
        held = _held(mapped, BOTH, 5)
        assert MappingFlag.RAW_DELIVERED_VALUE in held
        assert MappingFlag.NOT_CHANGED in held

    def test_the_delivered_interval_records_both_the_delivery_and_the_rewrite(
        self, mapped
    ):
        held = _held(mapped, _diff(BOTH), 5)
        assert MappingFlag.RAW_DELIVERED_VALUE in held
        assert MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE in held

    def test_the_rewritten_interval_is_the_register_difference(self, mapped):
        """The supplier sent 0.2 and the register rises by 0.1, so the cell now
        holds 0.1: the derivation replaced what was delivered, and the value
        says so as well as the flags."""
        assert float(mapped[_diff(BOTH)].iloc[5]) == pytest.approx(STEP)


class TestADeliveredCumulativeAlone:
    """No interval was sent, so the pipeline derives one. Its cells were never
    delivered, and the first has no predecessor to difference against."""

    def test_the_derived_interval_is_not_marked_as_delivered(self, mapped):
        held = _held(mapped, _diff(CUMULATIVE_ONLY), 5)
        assert MappingFlag.RAW_DELIVERED_VALUE not in held
        assert MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE in held

    def test_the_first_interval_is_seeded_rather_than_measured(self, mapped):
        held = _held(mapped, _diff(CUMULATIVE_ONLY), 0)
        assert MappingFlag.FIRST_ZERO in held
        assert float(mapped[_diff(CUMULATIVE_ONLY)].iloc[0]) == 0.0

    def test_the_seeded_cell_is_excluded_from_measurement(self, mapped):
        """FIRST_ZERO is in INVENTED, so a project average does not take it."""
        held = _held(mapped, _diff(CUMULATIVE_ONLY), 0)
        assert held & MappingFlag.NOT_A_MEASUREMENT


class TestADeliveredIntervalAlone:
    """The supplier sent interval amounts and no register. There is nothing to
    difference, so the cumulative column stays empty and must not acquire
    findings that claim otherwise."""

    def test_the_delivered_interval_reads_as_delivered(self, mapped):
        held = _held(mapped, _diff(INTERVAL_ONLY), 5)
        assert MappingFlag.RAW_DELIVERED_VALUE in held

    def test_the_delivered_interval_survives(self, mapped):
        """Nothing overwrites it, because there is no register to difference."""
        assert float(mapped[_diff(INTERVAL_ONLY)].iloc[5]) == pytest.approx(
            HEAT_STEP
        )

    def test_the_absent_cumulative_stays_empty(self, mapped):
        assert mapped[INTERVAL_ONLY].isna().all()

    def test_the_absent_cumulative_is_not_claimed_as_derived(self, mapped):
        """It was never delivered and nothing was derived into it, so the
        derivation flag must not appear -- the guard in add_diff_columns that
        declines to derive from an empty register."""
        assert not _rows_with(
            mapped,
            _diff(INTERVAL_ONLY),
            MappingFlag.VALUE_DERIVED_FROM_CUMULATIVE,
        )


class TestACoarseColumnOnAFinerGrid:
    """Fifteen-minute readings on a five-minute grid: two rows in three were
    never due a reading, and saying so is what stops them reading as missing
    data."""

    def test_the_ticks_are_delivered(self, mapped):
        for position in range(0, N, COARSE_STEP):
            assert MappingFlag.RAW_DELIVERED_VALUE in _held(
                mapped, COARSE, position
            )

    def test_the_rows_between_ticks_were_never_due(self, mapped):
        between = [i for i in range(N) if i % COARSE_STEP != 0]
        assert _rows_with(mapped, COARSE, MappingFlag.OFF_CADENCE_NA) == between

    def test_no_row_between_ticks_reads_as_owed_a_reading(self, mapped):
        assert not _rows_with(mapped, COARSE, MappingFlag.ON_CADENCE_NA)

    def test_the_two_absence_reasons_do_not_overlap(self, mapped):
        """A cell is either owed a reading or not; it cannot be both, and the
        pair is what a delivery-completeness measure reads."""
        owed = set(_rows_with(mapped, COARSE, MappingFlag.ON_CADENCE_NA))
        never = set(_rows_with(mapped, COARSE, MappingFlag.OFF_CADENCE_NA))
        assert not (owed & never)


class TestTheWholeFrameHoldsItsInvariants:
    def test_no_delivered_column_lost_its_delivery_record(self, mapped):
        """Every column the frame was built with delivered on every row, so
        every one of them carries the delivery answer after ten pipeline steps
        -- the record survives padding, derivation and cleaning."""
        for column in (BOTH, CUMULATIVE_ONLY, _diff(INTERVAL_ONLY)):
            assert len(
                _rows_with(mapped, column, MappingFlag.RAW_DELIVERED_VALUE)
            ) == N

    def test_nothing_reads_as_both_delivered_and_not_delivered(self, mapped):
        """The delivery partition, asserted over the artifact rather than over
        one function's output."""
        for column in (BOTH, CUMULATIVE_ONLY, _diff(INTERVAL_ONLY), COARSE):
            delivered = set(
                _rows_with(mapped, column, MappingFlag.RAW_DELIVERED_VALUE)
            )
            absent = set(
                _rows_with(mapped, column, MappingFlag.DELIVERED_NA)
            )
            not_delivered = set(
                _rows_with(mapped, column, MappingFlag.SLOT_NOT_DELIVERED)
            )
            assert not (delivered & absent)
            assert not (delivered & not_delivered)
            assert not (absent & not_delivered)

    def test_no_slot_was_created_because_none_was_missing(self, mapped):
        """The frame arrives complete, so padding adds nothing and no cell may
        claim a row failed to arrive."""
        assert len(mapped) == N
        for column in (BOTH, CUMULATIVE_ONLY, _diff(INTERVAL_ONLY), COARSE):
            assert not _rows_with(
                mapped, column, MappingFlag.SLOT_NOT_DELIVERED
            )
