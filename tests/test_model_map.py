"""The data model file and ``ids.py`` say the same thing.

``etdmap/data/model.yaml`` states the model once: the entities, their
relations, which key to write, and how to name each in prose. Everything that
reads the model reads that file, so the one way it can mislead is by naming an
id ``ids.py`` does not have, or by leaving one of ids' names unmapped. Both are
mechanical, so both are tested here rather than noticed later.

The file is also what an agent brief carries, verbatim, via
``conformance.model_brief``. A brief cannot then say something the model does
not -- which is the failure this file exists to prevent.
"""

import re

import pytest

from etdmap import conformance, ids

#: Any word in the file that looks like an id name: upper case, three or more
#: characters. Scanned over the text rather than the parsed structure, so a
#: name invented inside a comment or a prose paragraph is caught too.
NAME_IN_TEXT = re.compile(r"\b[A-Z][A-Z_]{2,}\b")

#: Upper-case words in the file that are not id names: the id scheme, the
#: sidecar registry a comment points at, and two words the `enforces` section
#: uses for how a check works rather than for anything in the model.
NOT_IDS = frozenset({"BSV", "SIDECARS", "API", "AST"})

#: The sections the file must have, because something reads each one: the
#: briefs read `formal_names` and `prose`, the keying checks read `keys`,
#: `entities` and `relationships`, and the gate reads `enforces`.
REQUIRED_SECTIONS = (
    "unit_of_analysis",
    "formal_names",
    "entities",
    "relationships",
    "keys",
    "ids",
    "prose",
    "enforces",
)


@pytest.fixture(scope="module")
def model():
    return conformance.model()


@pytest.fixture(scope="module")
def text():
    return conformance.model_text()


def ids_names():
    """Every public id name ``ids.py`` defines."""
    return {
        name for name in dir(ids)
        if name.isupper() and not name.startswith("_")
    }


def test_the_file_names_no_id_that_does_not_exist(text):
    """A name in the file resolves on ``ids``, or it is a typo nobody sees."""
    used = set(NAME_IN_TEXT.findall(text)) - NOT_IDS
    unknown = sorted(used - ids_names())

    assert not unknown, (
        f"model.yaml names {unknown}, which etdmap.ids does not define. "
        f"Either the name is wrong, or it is a word rather than an id and "
        f"belongs in NOT_IDS here with what it is."
    )


def test_every_id_is_in_the_file(text):
    """An id added to ``ids.py`` is placed in the model, not left implicit."""
    used = set(NAME_IN_TEXT.findall(text))
    unmapped = sorted(ids_names() - used)

    assert not unmapped, (
        f"etdmap.ids defines {unmapped}, which model.yaml does not mention. "
        f"Say what each one identifies, or -- for a name that is not an "
        f"entity id -- describe it under the `ids:` section as the others are."
    )


def test_the_sections_something_reads_are_present(model):
    """A missing section makes a brief or a check silently cover less."""
    missing = [name for name in REQUIRED_SECTIONS if name not in model]
    assert not missing, f"model.yaml has no {missing} section"


def test_the_unit_of_analysis_is_the_household_batch(model):
    """The one claim every check and every brief rests on."""
    assert model["unit_of_analysis"] == "HOUSEHOLD_BATCH"
    assert model["entities"]["HOUSEHOLD"]["unit_of"] == "nothing"


def test_the_key_to_write_is_one_column(model):
    """The surrogate stands in for the natural key; it does not join it."""
    keyed = model["keys"]["HOUSEHOLD_BATCH"]

    assert keyed["write"] == ["HOUSEHOLD_BATCH"]
    assert sorted(keyed["surrogate_for"]) == ["BATCH", "HOUSEHOLD", "PROJECT"]
    # Every forbidden key is a LIST of names, because the check that reads this
    # compares it against the key a groupby or a merge was given.
    assert all(isinstance(key, list) for key in keyed["never_write"])
    assert ["HOUSEHOLD"] in keyed["never_write"]


def test_the_household_is_a_key_only_for_the_guard_and_the_layout(model):
    """Every allowed site is storage or the 1:1 guard -- none is analysis.

    Asserted as the exact list rather than a property, so adding a site is a
    deliberate edit here and not a quiet widening. The keying check reads this
    list, so a site added without reason stops the check reporting it.
    """
    allowed = model["entities"]["HOUSEHOLD"]["key_use_allowed_in"]

    assert allowed == [
        "etdmap/etdmap/index_helpers.py",        # guard, and the index itself
        "etdmap/etdmap/storage.py",              # outer shard path level
        "etdtransform/etdtransform/impute.py",   # the guard again, pre-impute
    ]
    assert model["entities"]["HOUSEHOLD"]["unit_of"] == "nothing"


def test_a_brief_is_the_file_verbatim(text):
    """A brief quotes the model; it never restates it in other words."""
    brief = conformance.model_brief("formal_names", "keys")

    assert "formal_names:" in brief
    assert "keys:" in brief
    for line in brief.splitlines():
        assert line in text, f"model_brief invented a line: {line!r}"


def test_a_brief_for_a_section_that_does_not_exist_raises():
    """Rather than handing back a brief that quietly lacks it."""
    with pytest.raises(KeyError, match="no such section"):
        conformance.model_brief("formal_names", "cardinalities")
