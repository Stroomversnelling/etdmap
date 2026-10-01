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


class TestEveryMemberIsCovered:
    """Contract guard: a flag added to MappingFlag with no test anywhere fails."""

    def test_every_member_was_asserted_on(self):
        mentioned = _members_mentioned_in_tests("MappingFlag")
        missing = sorted(m.name for m in ALL_FLAGS if m.name not in mentioned)
        assert not missing, f"MappingFlag members with no test: {missing}"
