"""Static checks on test source, and the data model the checks hold to.

One definition of each check, so the conformance tests in both suites and the
consolidation metric in etdworkflow cannot drift apart. The checks import only
`ast` and the sidecar registry, so they are cheap to call on a whole tree.

Every check returns a list of ``Finding``. A check reports a SITE, never a
verdict about a file: a test whose subject is the malformed shape a guard must
reject is exempted by name, with its reason, where the check is called.

:func:`model` reads ``data/model.yaml``, which states the entities, their
relations, which key to write and how to name each one in prose. It is the one
statement of the data model: read it rather than restating it, and
:func:`model_brief` hands a caller the sections it needs verbatim.

What the checks are for is in ``docs/WRITING_TESTS.md``. The short version:
a test that assembles ids, a schema or a dtype by hand encodes a model of the
data rather than exercising the one the library has.
"""

import ast
import re
from typing import NamedTuple

import etdmap.ids as ids

#: The packaged data model, relative to this package.
MODEL_FILE = "model.yaml"

#: The calls that take a KEY, and the keywords each takes it under. The first
#: positional argument is the key for all but ``merge``, whose first positional
#: is the frame being joined.
#:
#: Both engines are listed. pandas spells it ``groupby`` and ibis spells it
#: ``group_by``; leaving the lazy spelling out made every ibis keying site in
#: etdtransform invisible, which is most of them.
_KEY_CALLS = {
    "groupby": ("by",),
    "group_by": ("by",),
    "merge": ("on", "left_on", "right_on"),
    "sort_values": ("by",),
    "order_by": ("by",),
    "drop_duplicates": ("subset",),
    "distinct": ("on",),
    "set_index": ("keys",),
}
_KEY_IN_FIRST_POSITION = frozenset({
    "groupby", "group_by", "sort_values", "order_by", "drop_duplicates",
    "distinct", "set_index",
})

#: The parsed model, once per process.
_MODEL = None


class Finding(NamedTuple):
    """One site a check objects to."""

    path: str
    line: int
    detail: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line} {self.detail}"


#: The id columns, under every spelling a test might use: the raw name, and the
#: attribute name on the ``ids`` module.
_ID_NAMES = frozenset({ids.HOUSEHOLD, ids.HOUSEHOLD_BATCH})
_ID_ATTRS = frozenset({"HOUSEHOLD", "HOUSEHOLD_BATCH"})

#: Numpy dtypes, which no data column may carry (ADR-005).
_NUMPY_DTYPES = frozenset({"float64", "int64", "bool", "float", "int"})

#: Calls that produce a column of values rather than one value.
_COLUMN_CALLS = frozenset({"array", "Series", "arange", "range", "full"})


def model_text() -> str:
    """The data model file as written.

    From the installed package rather than a path the caller supplies, so a
    downstream repo and this one cannot be reading different copies.
    """
    import importlib.resources as resources

    source = resources.files("etdmap").joinpath("data", MODEL_FILE)
    return source.read_text(encoding="utf-8")


def model() -> dict:
    """The data model, parsed. Read once per process."""
    global _MODEL
    if _MODEL is None:
        import yaml

        _MODEL = yaml.safe_load(model_text())
    return _MODEL


def model_brief(*sections: str) -> str:
    """The named top-level sections of the model, verbatim.

    For handing an agent or a reader the part of the model its work touches.
    Verbatim because a summary of the model is the thing that drifts: this
    returns the file's own lines, so a brief carrying it cannot say something
    the model does not.

    Raises
    ------
    KeyError
        When a section is not in the file, rather than returning a brief that
        silently lacks it.
    """
    lines = model_text().splitlines()
    starts = {
        line.split(":", 1)[0]: number
        for number, line in enumerate(lines)
        if line and not line[0].isspace() and not line.startswith("#")
        and ":" in line
    }
    missing = [name for name in sections if name not in starts]
    if missing:
        raise KeyError(
            f"no such section(s) in {MODEL_FILE}: {missing}; "
            f"available: {sorted(starts)}"
        )
    ordered = sorted(starts.values())
    out = []
    for name in sections:
        begin = starts[name]
        after = [number for number in ordered if number > begin]
        end = after[0] if after else len(lines)
        out.append("\n".join(lines[begin:end]).rstrip())
    return "\n\n".join(out)


def sidecar_distinctive_columns() -> frozenset:
    """The columns that belong to one mapping sidecar and nothing else.

    Derived from the sidecar registry so it tracks the data model. Every
    sidecar carries the household batch id and ``column``, so those two say nothing
    about which sidecar a dict is restating and are excluded.
    """
    from etdmap.mapping_helpers import SIDECARS

    per_sidecar = [frozenset(sidecar.dtypes) for sidecar in SIDECARS.values()]
    shared = frozenset.intersection(*per_sidecar)
    return frozenset.union(*per_sidecar) - shared


def _key_name(key):
    """The column a dict key names, however it is written."""
    if isinstance(key, ast.Constant) and isinstance(key.value, str):
        return key.value
    if isinstance(key, ast.Attribute):
        return key.attr
    return None


def _dict_key_names(node):
    return {name for name in (_key_name(key) for key in node.keys) if name}


def _is_column(value) -> bool:
    """True where *value* is a column of values rather than a single value."""
    if isinstance(value, (ast.List, ast.Tuple, ast.ListComp, ast.GeneratorExp)):
        return True
    if isinstance(value, ast.Call):
        func = value.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(
            func, "id", ""
        )
        return name in _COLUMN_CALLS
    return False


def _astype_dicts(node):
    """The dict literals passed to an ``astype`` call."""
    if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
        return
    if node.func.attr != "astype":
        return
    for argument in node.args:
        if isinstance(argument, ast.Dict):
            yield argument


def _takes_numpy_dtype(node) -> bool:
    """True where a numpy dtype is the correct argument.

    ADR-005 allows an explicit conversion for a third-party function that needs
    a real array, so ``to_numpy(dtype=...)`` and the numpy constructors are not
    violations. Without this a check reports correct code.
    """
    if not isinstance(node.func, ast.Attribute):
        return False
    if node.func.attr == "to_numpy":
        return True
    value = node.func.value
    return isinstance(value, ast.Name) and value.id in ("np", "numpy")


def id_columns_in_construction(tree, path) -> list:
    """An id column assembled by hand, as a LIST of values, in test data.

    EITHER id: the household batch or the household, under either spelling --
    the ``ids`` attribute or the column literal. Which one is written is not
    the point; writing the column at all is.

    A SCALAR id is correct and is how ``sidecar_frame`` is called: one value
    says which entity every row of that frame belongs to. A LIST assigns the
    ids, which is the builders' job -- they keep the two ids apart, so a site
    reaching for the wrong one finds no rows instead of quietly matching.
    """
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        for key, value in zip(node.keys, node.values):
            name = _key_name(key)
            if name in _ID_NAMES or name in _ID_ATTRS:
                if _is_column(value):
                    found.append(Finding(
                        path, key.lineno,
                        f"{name} built as a column of ids; the builders assign "
                        f"ids (etdmap.testing.household_batches_frame, index_frame)",
                    ))
    return found


def sidecar_schema_restated(tree, path) -> list:
    """A sidecar's schema written out in an ``astype`` map.

    The dtypes belong to the library's own map, which ``sidecar_frame`` reads.
    A restated one drifts from it unseen, and an omitted column reads as
    "nothing recorded" rather than raising.
    """
    distinctive = sidecar_distinctive_columns()
    found = []
    for node in ast.walk(tree):
        for mapping in _astype_dicts(node):
            named = _dict_key_names(mapping) & distinctive
            if named:
                found.append(Finding(
                    path, mapping.lineno,
                    f"restates sidecar column(s) {sorted(named)}; take the "
                    f"shape from etdmap.testing.sidecar_frame",
                ))
    return found


def numpy_dtypes_in_construction(tree, path) -> list:
    """A numpy dtype on a data column.

    `pd.NA` cannot be held by one, so a test about missing data built on a
    numpy dtype asserts numpy's semantics against a library built on nullable
    ones.
    """
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for mapping in _astype_dicts(node):
            for key, value in zip(mapping.keys, mapping.values):
                if isinstance(value, ast.Constant) and value.value in _NUMPY_DTYPES:
                    found.append(Finding(
                        path, mapping.lineno,
                        f"{_key_name(key) or '?'} cast to {value.value!r}; "
                        f"use the nullable dtype (Float64/Int64/boolean)",
                    ))
        if _takes_numpy_dtype(node):
            continue
        for keyword in node.keywords:
            if (keyword.arg == "dtype"
                    and isinstance(keyword.value, ast.Constant)
                    and keyword.value.value in _NUMPY_DTYPES):
                found.append(Finding(
                    path, node.lineno,
                    f"dtype={keyword.value.value!r}; use the nullable dtype "
                    f"(Float64/Int64/boolean)",
                ))
    return found


#: ``{column literal: the ids attribute naming it}``, for reading a key
#: whichever spelling a site used.
_BY_LITERAL = {
    getattr(ids, name): name
    for name in dir(ids)
    if name.isupper() and isinstance(getattr(ids, name), str)
}


def _key_names(node) -> frozenset:
    """The id names a key expression holds, under either spelling."""
    if isinstance(node, (ast.List, ast.Tuple)):
        return frozenset().union(*(_key_names(e) for e in node.elts)) \
            if node.elts else frozenset()
    if isinstance(node, ast.Attribute):
        return frozenset({node.attr}) if node.attr in _BY_LITERAL.values() \
            else frozenset()
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        name = _BY_LITERAL.get(node.value)
        return frozenset({name}) if name else frozenset()
    return frozenset()


def _keys_given_to(node) -> list:
    """Every key expression a call was given, as sets of id names."""
    name = node.func.attr if isinstance(node.func, ast.Attribute) else getattr(
        node.func, "id", ""
    )
    if name not in _KEY_CALLS:
        return []
    given = []
    if name in _KEY_IN_FIRST_POSITION and node.args:
        given.append(_key_names(node.args[0]))
    for keyword in node.keywords:
        if keyword.arg in _KEY_CALLS[name]:
            given.append(_key_names(keyword.value))
    return [key for key in given if key]


def _path_allows(path, allowed) -> bool:
    """True where *path* is one of the *allowed* module paths.

    Matched on the tail, because a caller may report a path relative to a repo,
    to a tree, or as a bare file name, and the model states repo-relative ones.
    """
    normalised = str(path).replace("\\", "/")
    return any(
        normalised.endswith(entry) or normalised == entry.rsplit("/", 1)[-1]
        for entry in allowed
    )


def household_keyed_grouping(tree, path) -> list:
    """A key that names the household where the household batch is the unit.

    Three readings of the model, which ``data/model.yaml`` states:

    - the household together with its project or its delivery batch is
      ``keys.never_write`` -- the surrogate spelled the long way. Wrong
      everywhere, including the modules below, because the invariant making it
      work is one it does not state.
    - the household ALONE is a key only where
      ``entities.HOUSEHOLD.key_use_allowed_in`` says: the guard that finds a
      household with more than one household batch dataset, and the outer
      shard path level.
    - carrying the household as an attribute -- copied, joined on by something
      else, reported -- is not a key use and not reported here.

    A key that is exactly ``ids.SHARD_PATH_KEYS`` is the on-disk layout rather
    than a key, and groups identically to the household batch dataset alone
    because the household batch determines its household.
    """
    model_ = model()
    allowed = model_["entities"]["HOUSEHOLD"]["key_use_allowed_in"]
    household = "HOUSEHOLD"
    never = [frozenset(key) for key in model_["keys"]["HOUSEHOLD_BATCH"]["never_write"]]
    composite = [key for key in never if key != frozenset({household})]
    layout = frozenset(_BY_LITERAL[name] for name in ids.SHARD_PATH_KEYS)
    here = _path_allows(path, allowed)

    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for key in _keys_given_to(node):
            if key == layout:
                continue
            if key in composite:
                found.append(Finding(
                    path, node.lineno,
                    f"keyed on {sorted(key)}, which is the household batch "
                    f"dataset's own key spelled the long way; key on "
                    f"{ids.HOUSEHOLD_BATCH}",
                ))
            elif household in key and not here:
                found.append(Finding(
                    path, node.lineno,
                    f"keyed on {sorted(key)}; the household is the unit of "
                    f"nothing -- key on {ids.HOUSEHOLD_BATCH}",
                ))
    return found


def duplicate_dict_keys(tree, path) -> list:
    """A dict literal naming one key twice.

    Python keeps the last, so the first is dropped without a word -- a column
    a test believes it declared, silently absent or holding another value.
    """
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        seen = set()
        for key in node.keys:
            if not isinstance(key, ast.Constant):
                continue
            if key.value in seen:
                found.append(Finding(
                    path, key.lineno, f"duplicate key {key.value!r}",
                ))
            seen.add(key.value)
    return found


# ---------------------------------------------------------------------------
# Prose. The model fixes one name per thing; these find the places that use
# another. They read docstrings, comments and MESSAGES -- a raise or a log
# line is read by a person as often as a docstring is. An identifier is code,
# and a rename is a different job from a rewording.
# ---------------------------------------------------------------------------

#: A word standing alone, not part of an identifier: ``batch`` matches in
#: "per household batch" but not in ``household_batch_table_frame`` or "HuisBatch", because the
#: character beside it there is a word character. Hyphenated forms count as
#: qualified -- "household-batch flag" says which one as plainly as the
#: spaced form does.
_BARE_BATCH = re.compile(
    r"(?<!household )(?<!household-)(?<!delivery )(?<!delivery-)"
    r"\bbatch(es)?\b",
    re.IGNORECASE,
)
_BARE_DATASET = re.compile(r"\bdatasets?\b", re.IGNORECASE)

#: The marks that make a word a citation rather than a use.
_QUOTED = ("'", '"', "`")


def _prose_lines(tree, source) -> dict:
    """``{line number: text}`` for every docstring and comment line."""
    lines = source.split("\n")
    claimed = {}
    for node in ast.walk(tree):
        if (isinstance(node, ast.Expr)
                and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)):
            for number in range(node.lineno, (node.end_lineno or node.lineno) + 1):
                if number <= len(lines):
                    claimed[number] = lines[number - 1]
    for number, line in enumerate(lines, start=1):
        if line.strip().startswith("#"):
            claimed[number] = line
    return claimed


def _word_before_dataset() -> frozenset:
    """The words the model allows directly before "dataset".

    Derived from ``prose.dataset_alone_allowed_for`` and from the formal name
    itself, so the allowance tracks the model rather than a list kept here.
    """
    prose = model()["prose"]
    phrases = list(prose["dataset_alone_allowed_for"])
    phrases.append(prose["HOUSEHOLD_BATCH"]["say"])
    allowed = set()
    for phrase in phrases:
        for match in re.finditer(r"([\w/-]+)\s+datasets?\b", phrase):
            allowed.update(match.group(1).lower().split("/"))
    return frozenset(allowed)


def _is_named_token(line, start, end) -> bool:
    """True where the word is being NAMED rather than used in a sentence.

    Three forms: a reference to the constant (``ids.BATCH``); the word itself
    in quotes or backticks; and the word inside a quoted PHRASE, which is how
    prose about the naming rule has to cite the wording it rejects -- as in
    a bare "the dataset". None of them leaves a reader wondering which one is
    meant.

    The phrase case is read off an odd number of quote marks to the left,
    which is what "inside a quoted span" means on a single line.
    """
    before = line[:start]
    if re.search(r"\bids\.$", before):
        return True
    if any(before.count(quote) % 2 == 1 for quote in _QUOTED):
        return True
    opens = before[-1:] in _QUOTED
    closes = line[end:end + 1] in _QUOTED
    return opens and closes


def _message_texts(tree, source) -> list:
    """``(line, text)`` for every string constant that is NOT a docstring.

    A ``raise``, a ``logging`` call and a warning are read by a person as
    often as a docstring is, so the naming rules hold there too -- and nothing
    was looking at them, which is how 49 messages kept saying "dataset".

    f-strings arrive as their literal pieces, which is what the rules apply
    to: the interpolated values are data.

    A single-word string is skipped. Prose has more than one word, so a lone
    word is a VALUE or a key -- the ``cadence_source`` value ``'dataset'``
    among them, which the model allows by name.
    """
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef,
                             ast.ClassDef)):
            first = node.body[0] if node.body else None
            if (isinstance(first, ast.Expr)
                    and isinstance(first.value, ast.Constant)
                    and isinstance(first.value.value, str)):
                docstrings.add(id(first.value))
    out = []
    for node in ast.walk(tree):
        if (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and id(node) not in docstrings):
            if len(node.value.split()) < 2:
                continue
            out.append((node.lineno, node.value))
    return out


def _bare_batch_in(text, path, number) -> list:
    """Findings for a bare "batch" in one piece of text."""
    found = []
    for match in _BARE_BATCH.finditer(text):
        if _is_named_token(text, match.start(), match.end()):
            continue
        found.append(Finding(
            path, number,
            f"bare {match.group(0)!r}: say \"delivery batch\" or "
            f"\"household batch\"",
        ))
    return found


def _bare_dataset_in(text, path, number, allowed) -> list:
    """Findings for a bare "dataset" in one piece of text."""
    found = []
    for match in _BARE_DATASET.finditer(text):
        if _is_named_token(text, match.start(), match.end()):
            continue                          # cited, not used in a sentence
        before = text[:match.start()].rstrip()
        previous = re.search(r"([\w/-]+)$", before)
        if previous and previous.group(1).lower() in allowed:
            continue
        found.append(Finding(
            path, number,
            f"bare {match.group(0)!r}: say which one, or "
            f"\"household batch dataset\"",
        ))
    return found


def bare_batch(tree, path, source) -> list:
    """"batch" standing alone where it does not mean the delivery.

    The word carries "household" or "delivery" in front of it, so which of the
    two a sentence means is never left to the reader. ``prose.batch_rule``.
    """
    found = []
    for number, line in sorted(_prose_lines(tree, source).items()):
        found.extend(_bare_batch_in(line, path, number))
    return found


def bare_dataset(tree, path, source) -> list:
    """"dataset" standing for a household batch dataset.

    Raw, mapped, generated and project-level datasets all exist, as does the
    whole estate, so a bare "the dataset" leaves which one unsaid.
    ``prose.dataset_alone_allowed_for`` holds the senses that keep the plain
    word.
    """
    allowed = _word_before_dataset()
    found = []
    for number, line in sorted(_prose_lines(tree, source).items()):
        found.extend(_bare_dataset_in(line, path, number, allowed))
    return found


def bare_batch_in_message(tree, path, source) -> list:
    """The same rule, in a message rather than a docstring."""
    found = []
    for number, text in _message_texts(tree, source):
        found.extend(_bare_batch_in(text, path, number))
    return found


def bare_dataset_in_message(tree, path, source) -> list:
    """The same rule, in a message rather than a docstring."""
    allowed = _word_before_dataset()
    found = []
    for number, text in _message_texts(tree, source):
        found.extend(_bare_dataset_in(text, path, number, allowed))
    return found


def raw_id_in_prose(tree, path, source) -> list:
    """A column literal written out in prose instead of the name for it.

    A literal in a sentence is a second spelling of the model: it goes stale
    silently when the column is renamed. ``prose.literals_allowed_in`` keeps
    the ones a path on disk really contains.
    """
    literals = [name for name in _BY_LITERAL if name.startswith("Huis")]
    found = []
    for number, line in sorted(_prose_lines(tree, source).items()):
        for literal in literals:
            if literal not in line:
                continue
            tail = line[line.index(literal) + len(literal):]
            if tail.startswith("="):
                continue                      # a hive partition level
            if "/" in line or "partition" in line.lower():
                continue                      # a path, glob or partition name
            found.append(Finding(
                path, number,
                f"{literal!r} in prose; name the id it is "
                f"({_BY_LITERAL[literal]})",
            ))
    return found


#: What a TEST must not build by hand, by the name its counter and its failure
#: report use. The conformance test in each repo holds its own tests/ tree to
#: every one of these.
CHECKS = {
    "id_columns_in_construction": id_columns_in_construction,
    "sidecar_schema_restated": sidecar_schema_restated,
    "numpy_dtypes_in_construction": numpy_dtypes_in_construction,
    "duplicate_dict_keys": duplicate_dict_keys,
}

#: Checks on a KEY, for any tree. Separate from CHECKS because the subject is
#: the data model rather than how a test builds its data, and because a library
#: module legitimately builds an id column that a test may not.
KEY_CHECKS = {
    "household_keyed_grouping": household_keyed_grouping,
}

#: The prose checks, which are given the source text as well as the tree.
PROSE_CHECKS = {
    "bare_batch": bare_batch,
    "bare_dataset": bare_dataset,
    "raw_id_in_prose": raw_id_in_prose,
    "bare_batch_in_message": bare_batch_in_message,
    "bare_dataset_in_message": bare_dataset_in_message,
}

#: Every check. ``loc_metric`` counts all of them, so a check cannot be added
#: here and then go unmeasured while it waits to become a gate.
ALL_CHECKS = {**CHECKS, **KEY_CHECKS, **PROSE_CHECKS}


def scan(paths, checks=None) -> dict:
    """Run *checks* over *paths*, as ``{check name: [Finding, ...]}``.

    *paths* is an iterable of ``(path_for_reporting, source_text)``. The caller
    reads the files, so the same scan serves a test walking its own tree and a
    metric walking several.
    """
    names = list(checks) if checks is not None else list(CHECKS)
    found = {name: [] for name in names}
    for path, source in paths:
        try:
            tree = ast.parse(source)
        except SyntaxError:
            continue
        for name in names:
            if name in PROSE_CHECKS:
                found[name].extend(PROSE_CHECKS[name](tree, str(path), source))
            else:
                found[name].extend(ALL_CHECKS[name](tree, str(path)))
    return found
