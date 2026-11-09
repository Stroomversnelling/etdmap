"""The cumulative-fault findings must reach the frame, not just the classifier.

``_detect_negative_diff_cases`` classifies every decreasing reading in a
cumulative column as one of ``CUMULATIVE_JUMP_BACK``,
``CUMULATIVE_REPEAT_NEGATIVE``, ``CUMULATIVE_METER_RESET`` or
``CUMULATIVE_NO_RECOVERY``, and marks a total that returned to zero with
``CUMULATIVE_ZERO_TOTAL``. ``_apply_negative_diff_corrections`` used that
classification only to choose which readings to blank, and recorded nothing but
``NULLED``. The pipeline therefore said THAT a reading was removed and never WHY,
so all five bits were dead in the output.

These tests pin the finding onto the reading it describes, for each of the five
faults, and pin the correction itself so that recording a finding cannot quietly
change which values are removed: the removal assertions state the expected
blanked positions and the expected surviving values outright.
"""

import logging

import pandas as pd
import pytest

from etdmap.data_model import interval_column, monotonic_columns
from etdmap.flags import MappingFlag, flag_column
from etdmap.mapping_helpers import _detect_negative_diff_cases, add_diff_columns

COL = "ElektriciteitNetgebruik"
OTHER = "Gasgebruik"
DIFF = interval_column(COL)
FLAGS = flag_column(COL)

# The five findings this module is about. The first four are mutually exclusive
# per decreasing reading; the zero total is independent of them.
FAULTS = (
    MappingFlag.CUMULATIVE_JUMP_BACK,
    MappingFlag.CUMULATIVE_REPEAT_NEGATIVE,
    MappingFlag.CUMULATIVE_METER_RESET,
    MappingFlag.CUMULATIVE_NO_RECOVERY,
    MappingFlag.CUMULATIVE_ZERO_TOTAL,
)

# One crafted series per fault, plus the shapes that make the edges explicit.
SHAPES = {
    # A dip that returns to at least the pre-dip value: the device kept count,
    # so the readings across the decrease are the faulty ones.
    "jump_back": [10.0, 11.0, 9.0, 12.0, 13.0],
    # Two decreases in succession, then a recovery.
    "consecutive_negatives": [10.0, 11.0, 9.0, 7.0, 12.0, 13.0],
    # Counts up again from the lower value and never reaches 102: a reset.
    "meter_reset": [100.0, 101.0, 102.0, 5.0, 6.0, 7.0, 8.0],
    # A decrease with no later increase at all.
    "no_recovery": [10.0, 11.0, 12.0, 5.0, 5.0, 5.0],
    # A total that went back to zero after accumulating.
    "zero_total": [10.0, 11.0, 0.0, 12.0, 13.0],
    "zero_run": [10.0, 11.0, 0.0, 0.0, 0.0, 12.0, 13.0],
    # A reset whose own interval is NA because a reading is missing in front of
    # it: classified, but no value is removed.
    "na_before_dip": [100.0, 101.0, pd.NA, 5.0, 6.0, 7.0],
    # A meter that had not started counting. Not a zero total.
    "leading_zeros": [0.0, 0.0, 93.255304, 89.184582, 93.656304],
}


@pytest.fixture(autouse=True)
def _quiet_logging():
    """The correction routine logs per anomaly; these tests read data, not logs."""
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


def _frame(values, column=COL):
    """One-cumulative-column frame on a regular 5 minute grid."""
    return pd.DataFrame(
        {
            "ReadingDate": pd.date_range(
                "2024-01-01", periods=len(values), freq="5min"
            ),
            column: pd.array(values, dtype="Float64"),
        }
    )


def _findings(df, column):
    """The findings recorded about *column*, zero where none were recorded."""
    name = flag_column(column)
    if name not in df.columns:
        return pd.Series(
            MappingFlag.NONE.value, index=df.index, dtype="Int64"
        )
    return df[name].fillna(MappingFlag.NONE.value)


def _on(df, flag, column=COL):
    """Row by row, whether *flag* is recorded about *column*."""
    return ((_findings(df, column) & flag.value) != 0).tolist()


def _run(name):
    return add_diff_columns(_frame(SHAPES[name]).copy(), context="fault flags")


def test_the_column_under_test_is_a_monotonic_cumulative_variable():
    """The correction only runs for columns the data model declares monotonic.
    If that declaration moves, these tests would pass vacuously."""
    assert COL in monotonic_columns
    assert OTHER in monotonic_columns
    assert DIFF is not None


class TestEachFaultReachesTheFrame:
    """One test per finding, on the reading the finding describes."""

    def test_jump_back(self):
        out = _run("jump_back")
        assert _on(out, MappingFlag.CUMULATIVE_JUMP_BACK) == [
            False, False, True, False, False
        ]

    def test_repeat_negative(self):
        out = _run("consecutive_negatives")
        # The first decrease is followed by another, so it is the unreliable
        # stretch; the second recovers past its own dip, so it is a jump back.
        assert _on(out, MappingFlag.CUMULATIVE_REPEAT_NEGATIVE) == [
            False, False, True, False, False, False
        ]
        assert _on(out, MappingFlag.CUMULATIVE_JUMP_BACK) == [
            False, False, False, True, False, False
        ]

    def test_meter_reset(self):
        out = _run("meter_reset")
        assert _on(out, MappingFlag.CUMULATIVE_METER_RESET) == [
            False, False, False, True, False, False, False
        ]

    def test_no_recovery(self):
        out = _run("no_recovery")
        # The finding sits on the decrease itself, not on every reading the
        # correction goes on to blank.
        assert _on(out, MappingFlag.CUMULATIVE_NO_RECOVERY) == [
            False, False, False, True, False, False
        ]

    def test_zero_total(self):
        out = _run("zero_total")
        assert _on(out, MappingFlag.CUMULATIVE_ZERO_TOTAL) == [
            False, False, True, False, False
        ]
        # Independent of the four: the same reading is also a jump back.
        assert _on(out, MappingFlag.CUMULATIVE_JUMP_BACK) == [
            False, False, True, False, False
        ]

    def test_zero_total_covers_every_zero_after_a_total(self):
        out = _run("zero_run")
        assert _on(out, MappingFlag.CUMULATIVE_ZERO_TOTAL) == [
            False, False, True, True, True, False, False
        ]

    def test_leading_zeros_are_not_a_zero_total(self):
        """A meter that has not started counting is not faulty."""
        out = _run("leading_zeros")
        assert not any(_on(out, MappingFlag.CUMULATIVE_ZERO_TOTAL))
        assert _on(out, MappingFlag.CUMULATIVE_JUMP_BACK) == [
            False, False, False, True, False
        ]

    @pytest.mark.parametrize("name", sorted(SHAPES))
    def test_the_frame_carries_exactly_what_the_classifier_found(self, name):
        """The write is a transcription: every bit the classifier set, on the
        same reading, and no bit it did not set."""
        df = _frame(SHAPES[name])
        cases = _detect_negative_diff_cases(df, COL)
        assert cases is not None, f"{name}: no fault to transcribe"
        expected = cases.flags.reindex(df.index, fill_value=MappingFlag.NONE.value)

        out = add_diff_columns(df.copy(), context="fault flags")
        for flag in FAULTS:
            assert _on(out, flag) == ((expected & flag.value) != 0).tolist(), (
                f"{name}: {flag.name} not transcribed onto the frame"
            )

    def test_a_clean_column_gets_no_fault_finding(self):
        df = _frame([10.0, 11.0, 0.0, 12.0, 13.0])
        df[OTHER] = pd.array([1.0, 2.0, 3.0, 4.0, 5.0], dtype="Float64")
        out = add_diff_columns(df.copy(), context="fault flags")
        for flag in FAULTS:
            assert not any(_on(out, flag, column=OTHER)), (
                f"{flag.name} recorded on a column that never decreased"
            )


class TestTheFindingIsNotTheRemoval:
    """A finding says what was observed; ``NULLED`` says a value was removed.
    The two are recorded separately and do not imply each other."""

    def test_a_fault_is_recorded_even_when_no_value_is_removed(self):
        """A missing reading in front of the dip leaves the interval NA, so the
        reset cannot be blamed on any one value and nothing is blanked. The
        observation still stands."""
        out = _run("na_before_dip")
        assert _on(out, MappingFlag.CUMULATIVE_METER_RESET) == [
            False, False, False, True, False, False
        ]
        assert not any(_on(out, MappingFlag.NULLED))

    def test_nulled_marks_every_reading_the_correction_removed(self):
        out = _run("no_recovery")
        assert _on(out, MappingFlag.NULLED) == [
            False, False, False, True, True, True
        ]

    def test_every_fault_finding_is_a_signal_not_an_action(self):
        for flag in FAULTS:
            assert flag & MappingFlag.SIGNALS
            assert not flag & MappingFlag.ACTIONS


class TestTheTranscribedSetIsExactlyWhatTheClassifierProduces:
    """The transcription loop walks ``CUMULATIVE_UNUSABLE``. A classification the
    classifier can set but that set does not name would be written nowhere, so
    the two are pinned against each other from both sides."""

    def test_the_set_names_the_five_findings(self):
        assert set(MappingFlag.CUMULATIVE_UNUSABLE) == set(FAULTS)

    def test_the_classifier_sets_nothing_outside_the_set(self):
        known = MappingFlag.CUMULATIVE_UNUSABLE.value
        for name, values in SHAPES.items():
            cases = _detect_negative_diff_cases(_frame(values), COL)
            # Masking to the known bits leaves the value unchanged only when no
            # other bit was set.
            assert ((cases.flags & known) == cases.flags).all(), (
                f"{name}: classifier set a bit outside CUMULATIVE_UNUSABLE"
            )

    def test_the_shapes_between_them_produce_every_finding_in_the_set(self):
        """Guards the test module against becoming vacuous."""
        seen = MappingFlag.NONE
        for values in SHAPES.values():
            cases = _detect_negative_diff_cases(_frame(values), COL)
            for flag in FAULTS:
                if ((cases.flags & flag.value) != 0).any():
                    seen |= flag
        assert seen == MappingFlag.CUMULATIVE_UNUSABLE


class TestSignalsStayOnTheCumulativeColumn:
    """Only findings naming a value the pipeline wrote are carried onto the
    interval column. A decrease describes the delivered reading."""

    @pytest.mark.parametrize("name", sorted(SHAPES))
    def test_the_interval_column_carries_no_fault_finding(self, name):
        out = add_diff_columns(_frame(SHAPES[name]).copy(), context="fault flags")
        for flag in FAULTS:
            assert not any(_on(out, flag, column=DIFF)), (
                f"{name}: {flag.name} leaked onto '{DIFF}'"
            )


# ---------------------------------------------------------------------------
# The correction itself must not move. Recording a finding is a write to the
# findings column only: the same readings are blanked and the same values
# survive. Expectations are stated outright rather than compared against a
# second run, so a change in what is removed fails here.
# ---------------------------------------------------------------------------

REMOVALS = {
    "jump_back": {
        "blanked": [False, False, True, False, False],
        "values": [10.0, 11.0, None, 12.0, 13.0],
        "intervals": [0.0, 1.0, None, None, 1.0],
    },
    "consecutive_negatives": {
        "blanked": [False, False, True, True, False, False],
        "values": [10.0, 11.0, None, None, 12.0, 13.0],
        "intervals": [0.0, 1.0, None, None, None, 1.0],
    },
    "meter_reset": {
        "blanked": [False, False, False, True, False, False, False],
        "values": [100.0, 101.0, 102.0, None, 6.0, 7.0, 8.0],
        "intervals": [0.0, 1.0, 1.0, None, None, 1.0, 1.0],
    },
    "no_recovery": {
        "blanked": [False, False, False, True, True, True],
        "values": [10.0, 11.0, 12.0, None, None, None],
        "intervals": [0.0, 1.0, 1.0, None, None, None],
    },
    "zero_total": {
        "blanked": [False, False, True, False, False],
        "values": [10.0, 11.0, None, 12.0, 13.0],
        "intervals": [0.0, 1.0, None, None, 1.0],
    },
    "zero_run": {
        "blanked": [False, False, True, True, True, False, False],
        "values": [10.0, 11.0, None, None, None, 12.0, 13.0],
        "intervals": [0.0, 1.0, None, None, None, None, 1.0],
    },
    "na_before_dip": {
        # The reading at position 2 was never delivered; the correction removed
        # nothing, so the negative interval at position 3 is absent only because
        # its predecessor is.
        "blanked": [False, False, True, False, False, False],
        "values": [100.0, 101.0, None, 5.0, 6.0, 7.0],
        "intervals": [0.0, 1.0, None, None, 1.0, 1.0],
    },
    "leading_zeros": {
        "blanked": [False, False, False, True, False],
        "values": [0.0, 0.0, 93.255304, None, 93.656304],
        "intervals": [0.0, 0.0, 93.255304, None, None],
    },
}


def _as_expected(values):
    """pd.NA for the positions stated as None, so the list reads cleanly."""
    return [pd.NA if value is None else value for value in values]


class TestTheCorrectionDoesNotMove:
    @pytest.mark.parametrize("name", sorted(REMOVALS))
    def test_the_same_readings_are_blanked(self, name):
        out = _run(name)
        assert out[COL].isna().tolist() == REMOVALS[name]["blanked"]

    @pytest.mark.parametrize("name", sorted(REMOVALS))
    def test_the_surviving_readings_are_unchanged(self, name):
        out = _run(name)
        pd.testing.assert_series_equal(
            out[COL].reset_index(drop=True),
            pd.Series(
                pd.array(_as_expected(REMOVALS[name]["values"]), dtype="Float64"),
                name=COL,
            ),
            obj=f"{name}: {COL}",
        )

    @pytest.mark.parametrize("name", sorted(REMOVALS))
    def test_the_intervals_are_unchanged(self, name):
        out = _run(name)
        pd.testing.assert_series_equal(
            out[DIFF].reset_index(drop=True),
            pd.Series(
                pd.array(_as_expected(REMOVALS[name]["intervals"]), dtype="Float64"),
                name=DIFF,
            ),
            obj=f"{name}: {DIFF}",
        )

    def test_a_second_cumulative_column_is_untouched(self):
        df = _frame([10.0, 11.0, 0.0, 12.0, 13.0])
        df[OTHER] = pd.array([1.0, 2.0, 3.0, 4.0, 5.0], dtype="Float64")
        out = add_diff_columns(df.copy(), context="fault flags")
        assert out[OTHER].tolist() == [1.0, 2.0, 3.0, 4.0, 5.0]
