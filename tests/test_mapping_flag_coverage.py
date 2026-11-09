"""Every MappingFlag member has a test somewhere in this suite.

The producer tests live in more than one file -- cumulative faults, frozen
spans, bounds, constructed cumulatives each have their own -- so coverage is
collected by scanning every test file, not by recording within one.
"""

import ast
from pathlib import Path

from etdmap.flags import MappingFlag

ALL_FLAGS = {m for m in MappingFlag if m.name and bin(m.value).count("1") == 1}

# Files whose subject is the vocabulary itself: they name members to check
# the class, not the behaviour, so a mention there is not a test of the flag.
STRUCTURAL_TEST_FILES = {"test_mapping_flag_taxonomy.py"}


def _members_mentioned_in_tests(class_name):
    """Member names referenced as `<class_name>.<MEMBER>` in any test file.

    A flag a producer test asserts on is necessarily named there, so a member
    no test file even mentions has no test. This is an inventory check only --
    that the assertion behind a mention is a real one is each producer test's
    own job.
    """
    mentioned = set()
    for path in sorted(Path(__file__).parent.glob("test_*.py")):
        if path.name in STRUCTURAL_TEST_FILES:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id == class_name
            ):
                mentioned.add(node.attr)
    return mentioned


# Vocabulary declared ahead of its producer: the meaning is fixed in
# DESCRIPTIONS, but nothing writes the flag yet. A member leaves this set the
# moment a producer test names it -- the honesty test below fails and says so.
# Each one's DESCRIPTIONS entry opens with RESERVED and says what will set it,
# so the reason for the exemption is recorded once, beside the meaning.
RESERVED: set[str] = set()

# Vocabulary whose producer has been removed. Nothing writes it and nothing will;
# the member stays declared because its bit value is in data already written, and
# removing it would renumber every later member. Its DESCRIPTIONS entry opens
# with DEPRECATED.
#
# Kept apart from RESERVED so the two cannot be read as the same thing: one is
# not implemented yet, the other is no longer implemented.
RETIRED = {
    "CUMULATIVE_CONSTRUCTED",
    "FILLED_ZERO",
}

# Members no test needs to name, whichever direction they are absent in.
UNPRODUCED = RESERVED | RETIRED


class TestEveryMemberIsCovered:
    """Contract guard: a flag added to MappingFlag with no test anywhere fails.

    Vocabulary with no producer sits in RESERVED or RETIRED instead of staying
    red: all four tests here are green in the steady state, and each direction
    of drift fails exactly one of them.
    """

    def test_every_member_with_a_producer_is_tested(self):
        mentioned = _members_mentioned_in_tests("MappingFlag")
        missing = sorted(
            m.name for m in ALL_FLAGS
            if m.name not in mentioned and m.name not in UNPRODUCED
        )
        assert not missing, f"MappingFlag members with no test: {missing}"

    def test_the_reserved_list_is_honest(self):
        """A member both declared reserved and tested is one whose producer has
        arrived: remove it from RESERVED rather than let the exception outlive
        its reason."""
        mentioned = _members_mentioned_in_tests("MappingFlag")
        arrived = sorted(RESERVED & mentioned)
        assert not arrived, (
            f"now produced and tested -- remove from RESERVED: {arrived}"
        )

    def test_the_two_lists_do_not_overlap(self):
        """A member cannot be both awaiting a producer and past one."""
        both = sorted(RESERVED & RETIRED)
        assert not both, f"in both RESERVED and RETIRED: {both}"

    def test_the_lists_name_real_members(self):
        unknown = sorted(UNPRODUCED - {m.name for m in ALL_FLAGS})
        assert not unknown, (
            f"RESERVED/RETIRED name members that do not exist: {unknown}"
        )
