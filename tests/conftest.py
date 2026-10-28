import os
from pathlib import Path

import pandas as pd
import pytest
import yaml

from etdmap.testing import (
        generate_mapped_dataset,
        generate_raw_dataset,
)


def load_config(config_path):
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)

test_config_path = Path("config_test.yaml")
if os.path.isfile(test_config_path):
    config = load_config(test_config_path)
else:
        raise FileNotFoundError("no file named 'config_test.yaml'")


def pytest_addoption(parser):
    parser.addoption("--copy-data", action="store_true", help="Copy data to a persistent folder")

@pytest.fixture
def valid_metadata_file(tmp_path):
    """A supplier metadata workbook, as read_metadata is given one.

    Built here rather than from a shared builder because the values are the
    subject: a row whose BSV household id is absent, which is what a workbook
    looks like before the ids are assigned, and an extra column the reader has
    to carry through. Written under tmp_path, which pytest makes unique per
    run.
    """
    metadata_file = tmp_path / "metadata.xlsx"

    data = {
        ids.HOUSEHOLD_SUPPLIER: ["A123", "B456", "C789"],
        ids.HOUSEHOLD: [101, 202, pd.NA],
        "Meenemen": [True, False, True],
        ids.PROJECT_SUPPLIER: ["P001", "P002", "P003"],
        ids.PROJECT: [1, 2, 3],
        "Notities": ["First note", pd.NA, "Final note"],
        ids.SUPPLIER: ["CompanyX", "CompanyY", "CompanyZ"],
        # Every value positive, which is all any reader of it asserts.
        "num_column": [5, 50, 95],
    }

    pd.DataFrame(data).to_excel(metadata_file, sheet_name="Data", index=False)
    return str(metadata_file)


@pytest.fixture
def invalid_metadata_file(tmp_path):
    # Create an invalid Excel file missing the required column
    metadata_file = tmp_path / "invalid_metadata.xlsx"
    data = {
        "SomeColumn": [1, 2, 3],
    }
    df = pd.DataFrame(data)
    df.to_excel(metadata_file, sheet_name="Data", index=False)
    return str(metadata_file)



@pytest.fixture(scope="session")
def raw_data_fixture(tmp_path_factory):
    """Raw supplier-shaped input, generated into a temporary folder."""
    return generate_raw_dataset(tmp_path_factory.mktemp("raw_fixture"))







@pytest.fixture(scope="session")
def mapped_fixtures(tmp_path_factory):
    """The synthetic mapped dataset, generated into this session's own folder.

    Generation lives in `etdmap.testing` so downstream suites can build the
    same dataset for themselves. Tests that read the mapped files request this
    fixture, so they run in any order or in isolation.

    Into a temporary folder rather than the configured one: the dataset is
    written and rewritten by the stages that read it, so a folder shared with
    another suite or another run is state they pass between them.
    """
    config = load_config(Path("config_test.yaml"))["etdmap_configuration"]
    # The supplier metadata and the project mapping are generated for the
    # households this run makes. A supplied workbook covers the households it
    # was written for, so the join leaves the rest unresolved as soon as the
    # shape is the caller's.
    return generate_mapped_dataset(
        tmp_path_factory.mktemp("mapped_fixture") / "mapped",
        bsv_metadata_file=config["bsv_metadata_file"],
        raw_folder=tmp_path_factory.mktemp("raw_fixture"),
    )

# ---------------------------------------------------------------------------
# No skipped tests (ADR-007). A skip hides a missing prerequisite or an
# unmet condition as "not run", which reads as green when it is not. This
# hook turns ANY skip -- pytest.skip(), skip/skipif markers, importorskip --
# into a hard failure at runtime.
#
# THE RULE: if a test is there, you may not write skipping in code. Make the
# prerequisite available (build the fixture/artifact it needs), or let the
# test fail so the gap is visible. Do not add pytest.skip.
# ---------------------------------------------------------------------------
import pytest as _pytest

from etdmap import ids

# Only these classes of skip may exist. A skip is allowed when its reason
# begins with "<TOKEN>: " and TOKEN is a key below.
#
# The allowlist is EMPTY on purpose. No skip in this code base has yet been
# shown to be legitimate -- every one examined turned out to be a missing
# prerequisite that should fail loudly instead. Empty means every skip is a
# hard failure until someone argues a class onto the list IN REVIEW. Adding a
# key is a deliberate, visible decision; deciding in the moment is not.
ALLOWED_SKIP_CLASSES = {
    # "PLATFORM": "behaviour genuinely differs by OS and is covered elsewhere",
    # "OPTIONAL_DEP": "an optional third-party package is not installed",
}


def _skip_reason(report):
    """
    The reason text of a skip.

    report.longrepr for a skip is the tuple (path, lineno, "Skipped: <reason>")
    for both a skipif marker and an in-body pytest.skip(). Verified against
    pytest directly rather than assumed.
    """
    longrepr = report.longrepr
    if isinstance(longrepr, tuple) and len(longrepr) >= 3:
        text = str(longrepr[2])
    else:
        text = str(longrepr)
    prefix = "Skipped: "
    return text[len(prefix):] if text.startswith(prefix) else text


@_pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if not report.skipped:
        return
    reason = _skip_reason(report)
    token = reason.split(":", 1)[0].strip() if ":" in reason else ""
    if token in ALLOWED_SKIP_CLASSES:
        return
    allowed = sorted(ALLOWED_SKIP_CLASSES) or ["(none -- the allowlist is empty)"]
    report.outcome = "failed"
    report.longrepr = (
        "SKIP NOT ALLOWED (ADR-007).\n"
        "  reason given: %r\n"
        "  skip class  : %r\n"
        "  allowed     : %s\n"
        "If a test is there, it must run. Provide the prerequisite it needs, or\n"
        "let it fail so the gap is visible. If a skip is genuinely warranted,\n"
        "add its class to ALLOWED_SKIP_CLASSES in review -- never silently."
        % (reason, token, allowed)
    )


def pytest_report_header(config):
    """Report the installed etd packages this run is testing against."""
    import importlib.metadata as metadata

    found = {}
    for dist in metadata.distributions():
        name = dist.metadata["Name"]
        if name and name.startswith("etd"):
            found[name] = "  %s %s -- %s" % (name, dist.version, dist.locate_file(""))
    if not found:
        return []
    return ["etd packages:"] + [found[name] for name in sorted(found)]
