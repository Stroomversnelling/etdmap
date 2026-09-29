"""MappingFlag members divide into signals and actions, and the composites
follow that division.

A signal names something observed about a value. An action names something the
pipeline did to it. The two are independent: a value can carry a signal with no
action, an action with no signal, or both. Composites that mix the categories
cannot be filtered on cleanly, which is what these tests pin.
"""

import pytest

from etdmap.flags import MappingFlag

SIGNALS = {
    MappingFlag.OUT_OF_BOUNDS_LOW,
    MappingFlag.OUT_OF_BOUNDS_HIGH,
    MappingFlag.CUMULATIVE_JUMP_BACK,
    MappingFlag.CUMULATIVE_REPEAT_NEGATIVE,
    MappingFlag.CUMULATIVE_METER_RESET,
    MappingFlag.CUMULATIVE_NO_RECOVERY,
    MappingFlag.CUMULATIVE_ZERO_TOTAL,
    MappingFlag.FROZEN_SPAN,
    MappingFlag.CATCHUP,
}
ACTIONS = {
    MappingFlag.FILLED_DOWN,
    MappingFlag.FILLED_ZERO,
    MappingFlag.FIRST_ZERO,
    MappingFlag.NULLED,
    MappingFlag.CUMULATIVE_CONSTRUCTED,
    MappingFlag.NOT_CHANGED,
}


def _single_bits():
    return {m for m in MappingFlag if m.name and bin(m.value).count("1") == 1}


class TestTheTaxonomyIsComplete:
    def test_every_member_is_a_signal_or_an_action(self):
        missing = sorted(m.name for m in _single_bits() - SIGNALS - ACTIONS)
        assert not missing, (
            f"MappingFlag members classified as neither signal nor action: "
            f"{missing}. Add each to SIGNALS or ACTIONS in this file."
        )

    def test_no_member_is_both(self):
        both = sorted(m.name for m in SIGNALS & ACTIONS)
        assert not both, f"members in both categories: {both}"


class TestInventedHoldsActionsOnly:
    """INVENTED selects values the pipeline wrote, so a reader can exclude them.
    A signal in it would exclude values that were merely observed."""

    def test_invented_contains_no_signal(self):
        leaked = sorted(m.name for m in SIGNALS if m & MappingFlag.INVENTED)
        assert not leaked, f"signals inside INVENTED: {leaked}"

    def test_invented_holds_the_writing_actions(self):
        assert MappingFlag.INVENTED == (
            MappingFlag.FILLED_DOWN
            | MappingFlag.FILLED_ZERO
            | MappingFlag.NULLED
            | MappingFlag.FIRST_ZERO
        )

    def test_a_constructed_cumulative_is_not_invented(self):
        """The column was built, but its values carry the delivered intervals'
        information. Excluding them from averaging would discard real data."""
        assert not (MappingFlag.CUMULATIVE_CONSTRUCTED & MappingFlag.INVENTED)


class TestNotAMeasurement:
    """The one test a project average filters on."""

    @pytest.mark.parametrize("flag", [
        MappingFlag.FROZEN_SPAN,
        MappingFlag.CATCHUP,
        MappingFlag.OUT_OF_BOUNDS_LOW,
        MappingFlag.OUT_OF_BOUNDS_HIGH,
        MappingFlag.FILLED_DOWN,
        MappingFlag.FILLED_ZERO,
        MappingFlag.FIRST_ZERO,
    ])
    def test_excluded_from_the_average(self, flag):
        assert flag & MappingFlag.NOT_A_MEASUREMENT, (
            f"{flag.name} does not reach NOT_A_MEASUREMENT, so the project "
            f"average would use it"
        )

    def test_frozen_span_reaches_it_directly_not_through_invented(self):
        """FROZEN_SPAN is a signal, so it leaves INVENTED. It must be named in
        NOT_A_MEASUREMENT in its own right or the average silently regains it."""
        assert not (MappingFlag.FROZEN_SPAN & MappingFlag.INVENTED)
        assert MappingFlag.FROZEN_SPAN & MappingFlag.NOT_A_MEASUREMENT

    def test_a_constructed_cumulative_still_counts_as_a_measurement(self):
        assert not (
            MappingFlag.CUMULATIVE_CONSTRUCTED & MappingFlag.NOT_A_MEASUREMENT
        )


class TestValuesAreStable:
    """Bits are written into every mapped dataset. Members are appended, so an
    existing member keeps its value and the enum is built with auto()."""

    @pytest.mark.parametrize("name,value", [
        ("OUT_OF_BOUNDS_LOW", 1),
        ("OUT_OF_BOUNDS_HIGH", 2),
        ("CUMULATIVE_JUMP_BACK", 4),
        ("CUMULATIVE_REPEAT_NEGATIVE", 8),
        ("CUMULATIVE_METER_RESET", 16),
        ("CUMULATIVE_NO_RECOVERY", 32),
        ("CUMULATIVE_ZERO_TOTAL", 64),
        ("FROZEN_SPAN", 128),
        ("CATCHUP", 256),
        ("FILLED_DOWN", 512),
        ("FILLED_ZERO", 1024),
    ])
    def test_existing_members_keep_their_value(self, name, value):
        assert MappingFlag[name].value == value

    def test_new_members_were_appended(self):
        for name in ("FIRST_ZERO", "NULLED", "CUMULATIVE_CONSTRUCTED",
                     "NOT_CHANGED"):
            assert MappingFlag[name].value > MappingFlag.FILLED_ZERO.value, (
                f"{name} was inserted rather than appended, which renumbers "
                f"every member after it"
            )


class TestNotChangedIsDerivable:
    """NOT_CHANGED is the absence of every other action. Defining the action
    set once keeps the two from drifting apart as flags are added."""

    def test_the_action_set_is_named_once(self):
        assert MappingFlag.ACTIONS_TAKEN == (
            MappingFlag.INVENTED | MappingFlag.CUMULATIVE_CONSTRUCTED
        )

    def test_it_holds_every_action_except_not_changed(self):
        expected = {m for m in ACTIONS if m is not MappingFlag.NOT_CHANGED}
        held = {m for m in ACTIONS if m & MappingFlag.ACTIONS_TAKEN}
        assert held == expected

    def test_not_changed_is_not_one_of_them(self):
        assert not (MappingFlag.NOT_CHANGED & MappingFlag.ACTIONS_TAKEN)

    def test_it_holds_no_signal(self):
        leaked = sorted(m.name for m in SIGNALS if m & MappingFlag.ACTIONS_TAKEN)
        assert not leaked, f"signals inside ACTIONS_TAKEN: {leaked}"


class TestTheCategoriesAreDeclaredNotDescribed:
    """SIGNALS and ACTIONS are composites on the class, so a report can label any
    flag without keeping its own copy of the split. A copy is what drifts."""

    def test_every_single_bit_is_in_exactly_one_category(self):
        for member in _single_bits():
            in_signals = bool(member & MappingFlag.SIGNALS)
            in_actions = bool(member & MappingFlag.ACTIONS)
            assert in_signals != in_actions, (
                f"{member.name} is in "
                f"{'both' if in_signals else 'neither'} category"
            )

    def test_the_declared_categories_match_this_file(self):
        """The lists at the top of this file are the human statement; the
        composites are what code reads. They must agree."""
        assert {m.name for m in _single_bits() if m & MappingFlag.SIGNALS} == {
            m.name for m in SIGNALS
        }
        assert {m.name for m in _single_bits() if m & MappingFlag.ACTIONS} == {
            m.name for m in ACTIONS
        }

    def test_the_two_categories_do_not_overlap(self):
        assert not (MappingFlag.SIGNALS & MappingFlag.ACTIONS)

    def test_together_they_cover_every_bit_in_use(self):
        covered = MappingFlag.SIGNALS | MappingFlag.ACTIONS
        for member in _single_bits():
            assert member & covered, f"{member.name} is in neither category"
