"""Hoog/Laag tariff registers combine into the root column at mapping.

The root is written wherever EITHER split is delivered, stays NA only where
both are, and a delivered root cell is never overwritten. Every cell the
combination writes carries MappingFlag.TARIFF_COMBINED (an action placed
like CUMULATIVE_CONSTRUCTED: the value carries what delivered registers
reported, so it is neither INVENTED nor NOT_A_MEASUREMENT). An INTERVAL in
which both registers advanced carries MappingFlag.TARIFF_SIMULTANEOUS on the
INTERVAL root -- the alternation guard: tariff registers alternate, so
consumption on both within one interval is a suspicious delivered pattern.
Signal only; nothing is removed. The cumulative root never carries it,
because standing totals are both nonzero at essentially every reading (see
TestTariffSimultaneous for the measured 100%-vs-0.37% evidence).

Reference semantics: the stats-side synthesis in
etdmap.data_stats._synthesise_tariff_roots (hoog.fillna(0) + laag.fillna(0),
NA only where both are NA) -- per cell here, where the stats helper worked
per column.
"""

import pandas as pd
import pytest

from etdmap.flags import MappingFlag, flag_column
from etdmap.mapping_helpers import (
    add_diff_columns,
    add_tariff_root_cadence,
    combine_tariff_registers,
)

ROOT = "ElektriciteitNetgebruik"
HOOG = "ElektriciteitNetgebruikHoog"
LAAG = "ElektriciteitNetgebruikLaag"
ROOT_DIFF = "ElektriciteitNetgebruikDiff"
HOOG_DIFF = "ElektriciteitNetgebruikHoogDiff"
LAAG_DIFF = "ElektriciteitNetgebruikLaagDiff"


def _floats(values):
    return pd.array(values, dtype="Float64")


def _frame(**columns):
    df = pd.DataFrame({name: _floats(vals) for name, vals in columns.items()})
    return df


def _bits(df, column):
    return df[flag_column(column)].fillna(0).astype("Int64")


def _bits_or_zero(df, column):
    """Bits for *column*, or all zeros when no flag column exists.

    set_flags creates the column on first use, so "nothing was flagged" and
    "no flag column" are the same fact. Tests asserting an ABSENCE must
    accept both; tests asserting a bit IS set keep using _bits, where a
    missing column is a failure.
    """
    name = flag_column(column)
    if name not in df.columns:
        return pd.Series(0, index=df.index, dtype="Int64")
    return _bits(df, column)


class TestCombineMatchesReferenceSemantics:
    def test_na_patterns(self):
        df = _frame(
            **{
                HOOG: [1.0, pd.NA, 3.0, pd.NA],
                LAAG: [2.0, 5.0, pd.NA, pd.NA],
            }
        )
        combine_tariff_registers(df)
        assert df[ROOT].dtype == pd.Float64Dtype()
        # both delivered -> sum; one delivered -> that value; both NA -> NA.
        assert df[ROOT][0] == 3.0
        assert df[ROOT][1] == 5.0
        assert df[ROOT][2] == 3.0
        assert pd.isna(df[ROOT][3])

    def test_combined_cells_carry_the_action_flag(self):
        df = _frame(**{HOOG: [1.0, pd.NA], LAAG: [2.0, pd.NA]})
        combine_tariff_registers(df)
        bits = _bits(df, ROOT)
        assert bits[0] & MappingFlag.TARIFF_COMBINED.value
        # The both-NA cell was not written, so it carries no action.
        assert not (bits[1] & MappingFlag.TARIFF_COMBINED.value)

    def test_without_any_split_column_nothing_happens(self):
        df = _frame(**{"ZonopwekBruto": [1.0]})
        before = df.copy()
        combine_tariff_registers(df)
        pd.testing.assert_frame_equal(df, before)

    def test_single_split_column_is_enough(self):
        df = _frame(**{HOOG: [1.5, pd.NA]})
        combine_tariff_registers(df)
        assert df[ROOT][0] == 1.5
        assert pd.isna(df[ROOT][1])


class TestDeliveredRootWins:
    def test_delivered_cells_are_untouched_and_unflagged(self):
        df = _frame(
            **{
                ROOT: [10.0, 20.0, pd.NA],
                HOOG: [1.0, 1.0, 1.0],
                LAAG: [2.0, 2.0, 2.0],
            }
        )
        combine_tariff_registers(df)
        assert df[ROOT][0] == 10.0
        assert df[ROOT][1] == 20.0
        assert df[ROOT][2] == 3.0
        bits = _bits(df, ROOT)
        assert not (bits[0] & MappingFlag.TARIFF_COMBINED.value)
        assert not (bits[1] & MappingFlag.TARIFF_COMBINED.value)
        assert bits[2] & MappingFlag.TARIFF_COMBINED.value


class TestTariffSimultaneous:
    """Alternation is an INTERVAL property, so the test and the flag both
    belong to the interval form.

    These tests were rewritten on 2026-10-02 after the estate remap
    falsified the original contract. The first version tested "both
    registers nonzero" on the CUMULATIVE pair and flagged the cumulative
    root. A cumulative reading is a running total, so both tariff counters
    hold nonzero totals essentially always: the flag fired on 18.5M Watch-E
    and 11.3M Factory Zero cells -- on one household, 103,001 of 103,001
    delivered readings (100%) -- while the interval test was true for 277 of
    ~74,400 consuming intervals (0.37%). The old assertions passed happily
    because the fixtures were single rows, where a cumulative value and an
    interval are indistinguishable. Every case below therefore uses at least
    two readings, so a cumulative total can be told apart from a step.
    """

    def test_fires_where_both_registers_advance_in_one_interval(self):
        # Both totals rise between reading 0 and reading 1.
        df = _frame(**{HOOG: [1.0, 2.0], LAAG: [5.0, 7.0]})
        combine_tariff_registers(df)
        assert _bits(df, ROOT_DIFF)[1] & MappingFlag.TARIFF_SIMULTANEOUS.value

    def test_silent_on_alternation(self):
        # Reading 1 advances hoog only, reading 2 laag only: the normal,
        # alternating pattern.
        df = _frame(**{HOOG: [1.0, 2.0, 2.0], LAAG: [5.0, 5.0, 6.0]})
        combine_tariff_registers(df)
        bits = _bits_or_zero(df, ROOT_DIFF)
        assert not bool((bits & MappingFlag.TARIFF_SIMULTANEOUS.value).any())

    def test_silent_on_na_pairs(self):
        df = _frame(**{HOOG: [pd.NA, 1.0], LAAG: [2.0, pd.NA]})
        combine_tariff_registers(df)
        bits = _bits_or_zero(df, ROOT_DIFF)
        assert not bool((bits & MappingFlag.TARIFF_SIMULTANEOUS.value).any())

    def test_the_cumulative_root_never_carries_the_signal(self):
        """THE REGRESSION. Standing cumulative totals are both nonzero at
        every reading while the intervals alternate perfectly -- the exact
        shape that made the original test fire on 100% of readings. The
        cumulative root must stay clean; only the interval root may speak."""
        df = _frame(
            **{
                HOOG: [100.0, 101.0, 101.0, 102.0],
                LAAG: [200.0, 200.0, 201.0, 201.0],
            }
        )
        combine_tariff_registers(df)
        cumulative_bits = _bits_or_zero(df, ROOT)
        assert not bool(
            (cumulative_bits & MappingFlag.TARIFF_SIMULTANEOUS.value).any()
        )
        interval_bits = _bits_or_zero(df, ROOT_DIFF)
        assert not bool(
            (interval_bits & MappingFlag.TARIFF_SIMULTANEOUS.value).any()
        )

    def test_fires_even_where_the_root_was_delivered(self):
        """The signal marks the suspicious delivered pattern on the splits;
        whether the root cell itself was delivered does not change what was
        observed."""
        df = _frame(
            **{ROOT: [9.0, 9.5], HOOG: [1.0, 2.0], LAAG: [5.0, 7.0]}
        )
        combine_tariff_registers(df)
        assert _bits(df, ROOT_DIFF)[1] & MappingFlag.TARIFF_SIMULTANEOUS.value
        assert not bool(
            (_bits_or_zero(df, ROOT) & MappingFlag.TARIFF_COMBINED.value).any()
        )
        assert df[ROOT].tolist() == [9.0, 9.5]

    def test_a_reset_is_not_simultaneity(self):
        """A negative step is a meter reset or a correction case, already
        recorded by the CUMULATIVE_* findings. Flagging it here would
        conflate two different findings and hide both."""
        df = _frame(**{HOOG: [100.0, 0.0], LAAG: [200.0, 201.0]})
        combine_tariff_registers(df)
        bits = _bits_or_zero(df, ROOT_DIFF)
        assert not bool((bits & MappingFlag.TARIFF_SIMULTANEOUS.value).any())

    def test_interval_splits_are_tested_directly(self):
        """When the supplier delivers the interval splits themselves, the
        delivered values ARE the per-interval consumption -- no differencing,
        and the flag lands on the interval root it already is."""
        df = _frame(**{HOOG_DIFF: [1.0, 0.0], LAAG_DIFF: [2.0, 3.0]})
        combine_tariff_registers(df)
        bits = _bits(df, ROOT_DIFF)
        assert bits[0] & MappingFlag.TARIFF_SIMULTANEOUS.value
        assert not (bits[1] & MappingFlag.TARIFF_SIMULTANEOUS.value)

    def test_the_signal_survives_add_diff_columns(self):
        """The flag is written on the interval root before add_diff_columns
        creates that column's values; flags are history and accumulate, so
        the bit must still be there afterwards."""
        df = _frame(
            **{HOOG: [1.0, 2.0], LAAG: [5.0, 7.0], "ReadingDate": [0.0, 300.0]}
        )
        df["ReadingDate"] = pd.to_datetime(df["ReadingDate"], unit="s")
        combine_tariff_registers(df)
        assert _bits(df, ROOT_DIFF)[1] & MappingFlag.TARIFF_SIMULTANEOUS.value
        out = add_diff_columns(df, context="simultaneous-survives")
        assert _bits(out, ROOT_DIFF)[1] & MappingFlag.TARIFF_SIMULTANEOUS.value


class TestDiffAndCumulativeForms:
    def test_interval_splits_combine_into_the_diff_root(self):
        df = _frame(
            **{
                HOOG_DIFF: [0.5, pd.NA, pd.NA],
                LAAG_DIFF: [pd.NA, 0.25, pd.NA],
            }
        )
        combine_tariff_registers(df)
        assert df[ROOT_DIFF][0] == 0.5
        assert df[ROOT_DIFF][1] == 0.25
        assert pd.isna(df[ROOT_DIFF][2])
        bits = _bits(df, ROOT_DIFF)
        assert bits[0] & MappingFlag.TARIFF_COMBINED.value

    def test_combined_cumulative_feeds_add_diff_columns(self):
        """Called before add_diff_columns, the combined cumulative root
        produces its Diff naturally."""
        df = pd.DataFrame(
            {
                "ReadingDate": pd.to_datetime(
                    ["2024-01-01 00:00", "2024-01-01 00:05", "2024-01-01 00:10"]
                ),
                HOOG: _floats([100.0, 100.5, 101.0]),
                LAAG: _floats([50.0, 50.0, 50.5]),
            }
        )
        combine_tariff_registers(df)
        assert list(df[ROOT]) == [150.0, 150.5, 151.5]
        out = add_diff_columns(df, context="test")
        assert ROOT_DIFF in out.columns
        diffs = out[ROOT_DIFF]
        assert pd.isna(diffs[0]) or diffs[0] == 0.0
        assert diffs[1] == 0.5
        assert diffs[2] == 1.0


class TestFlagsAreHistory:
    def test_existing_bits_survive_the_combination(self):
        df = _frame(**{HOOG: [1.0], LAAG: [2.0]})
        df[flag_column(ROOT)] = pd.array(
            [MappingFlag.OUT_OF_BOUNDS_HIGH.value], dtype="Int64"
        )
        combine_tariff_registers(df)
        bits = _bits(df, ROOT)
        assert bits[0] & MappingFlag.OUT_OF_BOUNDS_HIGH.value
        assert bits[0] & MappingFlag.TARIFF_COMBINED.value


class TestTaxonomyPlacement:
    """Behavioural placement facts the taxonomy file also pins structurally:
    stated here against the member names so the intent is readable beside
    the producer tests."""

    def test_combined_is_an_action_but_not_invented(self):
        assert MappingFlag.TARIFF_COMBINED & MappingFlag.ACTIONS
        assert MappingFlag.TARIFF_COMBINED & MappingFlag.ACTIONS_TAKEN
        assert not (MappingFlag.TARIFF_COMBINED & MappingFlag.INVENTED)
        assert not (MappingFlag.TARIFF_COMBINED & MappingFlag.NOT_A_MEASUREMENT)

    def test_simultaneous_is_a_signal_and_still_a_measurement(self):
        assert MappingFlag.TARIFF_SIMULTANEOUS & MappingFlag.SIGNALS
        assert not (MappingFlag.TARIFF_SIMULTANEOUS & MappingFlag.ACTIONS)
        assert not (
            MappingFlag.TARIFF_SIMULTANEOUS & MappingFlag.NOT_A_MEASUREMENT
        )

    def test_a_combined_cell_escapes_not_changed(self):
        """TARIFF_COMBINED is in ACTIONS_TAKEN, so mark_unchanged_values must
        not stamp NOT_CHANGED over a combined cell."""
        from etdmap.mapping_helpers import mark_unchanged_values

        df = _frame(**{HOOG: [1.0, pd.NA], LAAG: [2.0, pd.NA]})
        combine_tariff_registers(df)
        mark_unchanged_values(df)
        bits = _bits(df, ROOT)
        assert not (bits[0] & MappingFlag.NOT_CHANGED.value)
        assert bits[1] & MappingFlag.NOT_CHANGED.value


GRID = 5


def _cadence(rows):
    """A measured cadence table: (column, cadence_minutes, source, detected,
    confidence, n_readings) per row."""
    return pd.DataFrame({
        "column": pd.array([r[0] for r in rows], dtype="string"),
        "cadence_minutes": pd.array([r[1] for r in rows], dtype="Int64"),
        "cadence_source": pd.array([r[2] for r in rows], dtype="string"),
        "cadence_detected": pd.array([r[3] for r in rows], dtype="Int64"),
        "confidence": pd.array([r[4] for r in rows], dtype="Float64"),
        "n_readings": pd.array([r[5] for r in rows], dtype="Int64"),
    })


def _row(table, column):
    rows = table[table["column"] == column]
    assert len(rows) == 1, f"expected one row for {column}, got {len(rows)}"
    return rows.iloc[0]


class TestTheCombinedRootGetsItsOwnCadence:
    """Cadence is measured on the columns as delivered, so it runs before the
    combination and the root it builds has no row. The root is the column the
    averaging and the fill work on -- the splits are excluded from both -- so a
    sidecar without that row cannot answer what form the register arrived in or
    what interval its readings cover.

    The root's cadence is its splits': it holds a value exactly where one of
    them did.
    """

    def _combined(self, hoog, laag, cadence):
        df = _frame(**{HOOG: hoog, LAAG: laag})
        combine_tariff_registers(df)
        return add_tariff_root_cadence(df, cadence)

    def test_the_root_takes_the_cadence_of_its_splits(self):
        table = self._combined(
            [1.0, 2.0], [10.0, 20.0],
            _cadence([
                ("ReadingDate", GRID, "detected", GRID, 1.0, 2),
                (HOOG, 60, "declared", 60, 0.99, 2),
                (LAAG, 60, "declared", 60, 0.99, 2),
            ]),
        )
        assert int(_row(table, ROOT)["cadence_minutes"]) == 60

    def test_a_derived_row_says_it_was_combined(self):
        """So a reader can tell a root the supplier sent from one mapping built."""
        table = self._combined(
            [1.0], [10.0],
            _cadence([
                (HOOG, GRID, "detected", GRID, 1.0, 1),
                (LAAG, GRID, "detected", GRID, 1.0, 1),
            ]),
        )
        assert _row(table, ROOT)["cadence_source"] == "combined"

    def test_readings_are_counted_on_the_combined_column(self):
        """Not taken from either split: the root reports wherever either did, so
        counting one half would understate a register that alternates."""
        table = self._combined(
            [1.0, pd.NA, pd.NA], [pd.NA, 20.0, pd.NA],
            _cadence([
                (HOOG, GRID, "detected", GRID, 1.0, 1),
                (LAAG, GRID, "detected", GRID, 1.0, 1),
            ]),
        )
        assert int(_row(table, ROOT)["n_readings"]) == 2

    def test_one_tariff_alone_still_gives_the_root_a_cadence(self):
        table = self._combined(
            [1.0, 2.0], [pd.NA, pd.NA],
            _cadence([
                (HOOG, 15, "detected", 15, 0.95, 2),
            ]),
        )
        assert int(_row(table, ROOT)["cadence_minutes"]) == 15

    def test_the_weaker_measurement_governs_the_confidence(self):
        table = self._combined(
            [1.0], [10.0],
            _cadence([
                (HOOG, GRID, "detected", GRID, 0.9, 1),
                (LAAG, GRID, "detected", GRID, 0.6, 1),
            ]),
        )
        assert float(_row(table, ROOT)["confidence"]) == pytest.approx(0.6)

    def test_disagreeing_measurements_leave_detected_unstated(self):
        """A declared cadence can govern both splits while their measurements
        differ. Averaging those would report a number neither register showed."""
        table = self._combined(
            [1.0], [10.0],
            _cadence([
                (HOOG, 60, "declared", 55, 0.9, 1),
                (LAAG, 60, "declared", 65, 0.9, 1),
            ]),
        )
        row = _row(table, ROOT)
        assert int(row["cadence_minutes"]) == 60
        assert pd.isna(row["cadence_detected"])

    def test_a_delivered_root_keeps_its_own_measured_row(self):
        """It was measured on the data as delivered, which beats anything
        derived from the splits beside it."""
        table = self._combined(
            [1.0], [10.0],
            _cadence([
                (ROOT, 15, "detected", 15, 1.0, 1),
                (HOOG, 60, "detected", 60, 1.0, 1),
                (LAAG, 60, "detected", 60, 1.0, 1),
            ]),
        )
        row = _row(table, ROOT)
        assert int(row["cadence_minutes"]) == 15
        assert row["cadence_source"] == "detected"

    def test_nothing_is_derived_when_the_splits_have_no_rows(self):
        """There is no evidence to derive from, and inventing a cadence would be
        worse than leaving the question open."""
        df = _frame(**{HOOG: [1.0], LAAG: [10.0]})
        combine_tariff_registers(df)
        table = add_tariff_root_cadence(
            df, _cadence([("ReadingDate", GRID, "detected", GRID, 1.0, 1)])
        )
        assert ROOT not in set(table["column"])

    def test_the_measured_rows_are_left_alone(self):
        before = _cadence([
            (HOOG, 60, "declared", 60, 0.99, 2),
            (LAAG, 60, "declared", 60, 0.99, 2),
        ])
        after = self._combined([1.0, 2.0], [10.0, 20.0], before)
        for column in (HOOG, LAAG):
            assert _row(after, column).to_dict() == _row(before, column).to_dict()


class TestSplitsAtDifferentCadencesRaise:
    """The root has no single cadence then. Which one governs is a question
    about how that supplier delivers the two registers, so it belongs in the
    supplier mapping rather than being settled by picking one here.
    """

    def _attempt(self, hoog_minutes, laag_minutes):
        df = _frame(**{HOOG: [1.0, 2.0], LAAG: [10.0, 20.0]})
        combine_tariff_registers(df)
        return add_tariff_root_cadence(
            df,
            _cadence([
                (HOOG, hoog_minutes, "declared", hoog_minutes, 1.0, 2),
                (LAAG, laag_minutes, "declared", laag_minutes, 1.0, 2),
            ]),
        )

    def test_it_raises(self):
        with pytest.raises(ValueError, match="different"):
            self._attempt(15, 60)

    def test_the_message_names_both_cadences_and_the_root(self):
        with pytest.raises(ValueError) as caught:
            self._attempt(15, 60)
        message = str(caught.value)
        assert "15min" in message and "1h" in message
        assert ROOT in message

    def test_agreeing_cadences_do_not_raise(self):
        table = self._attempt(60, 60)
        assert int(_row(table, ROOT)["cadence_minutes"]) == 60


class TestThePipelineHandsTheDerivedRowBack:
    """The sidecar is written from what ``run_standard_pipeline`` returns, not
    from the table the mapper measured. A row derived inside the pipeline is
    lost otherwise -- which is how the root came to have no cadence and no
    recorded delivery despite holding a full year of readings.
    """

    def _run(self, tmp_path, cadence, household_id=901):
        import etdmap
        from etdmap.mapping_helpers import run_standard_pipeline

        df = pd.DataFrame({
            "ReadingDate": pd.date_range("2024-01-01", periods=12, freq="5min"),
            HOOG: _floats([float(i) for i in range(12)]),
            LAAG: _floats([float(i) * 2 for i in range(12)]),
        })
        combine_tariff_registers(df)
        previous = etdmap.options.mapped_output_format
        etdmap.options.mapped_output_format = "flat"
        try:
            return run_standard_pipeline(
                df,
                household_id=household_id,
                household_id_supplier=f"H{household_id}",
                mapped_folder_path=tmp_path,
                grid_minutes=GRID,
                cadence=cadence,
            )
        finally:
            etdmap.options.mapped_output_format = previous

    def _measured(self):
        return _cadence([
            ("ReadingDate", GRID, "detected", GRID, 1.0, 12),
            (HOOG, GRID, "detected", GRID, 1.0, 12),
            (LAAG, GRID, "detected", GRID, 1.0, 12),
        ])

    def test_the_returned_cadence_carries_the_root(self, tmp_path):
        entry = self._run(tmp_path, self._measured())
        returned = entry["_cadence"]
        assert ROOT in set(returned["column"])
        assert _row(returned, ROOT)["cadence_source"] == "combined"

    def test_the_table_passed_in_is_not_relied_on_to_carry_it(self, tmp_path):
        """The caller's own table is what used to be written to the sidecar, and
        it cannot hold the derived row."""
        measured = self._measured()
        self._run(tmp_path, measured)
        assert ROOT not in set(measured["column"])

    def test_the_root_is_marked_as_delivered(self, tmp_path):
        """With a cadence row the root now has, the delivery marking reaches it
        -- the point of deriving the row before step 6b rather than after."""
        self._run(tmp_path, self._measured())
        out = pd.read_parquet(
            tmp_path / "household_901_table.parquet",
            dtype_backend="numpy_nullable",
        )
        bits = _bits(out, ROOT)
        assert (bits & MappingFlag.RAW_DELIVERED_VALUE.value).any()
