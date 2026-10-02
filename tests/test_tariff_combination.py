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
from etdmap.mapping_helpers import add_diff_columns, combine_tariff_registers

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
