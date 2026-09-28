import hashlib
import json
import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml
from numpy.random import PCG64, Generator

import etdmap
import etdmap.index_helpers as index_helpers
import etdmap.mapping_helpers as mapping
from etdmap.data_model import cumulative_columns
from etdmap.index_helpers import bsv_metadata_columns, metadata_dtypes, read_metadata
from etdmap.record_validators import record_flag_conditions
from etdmap.testing import (
    SYNTHETIC_DIFF_RANGES,
    generate_mapped_dataset,
    generate_raw_dataset,
)




# set paths
def _clean_mapped_folder(mapped_folder_path):
    """Remove generated files from mapped folder to ensure clean test state."""
    from pathlib import Path
    mapped_folder = Path(mapped_folder_path)
    if mapped_folder.exists():
        for f in mapped_folder.glob("household_*.parquet"):
            f.unlink()
        index_file = mapped_folder / "index.parquet"
        if index_file.exists():
            index_file.unlink()


@pytest.fixture(scope="session", autouse=True)
def clean_mapped_folder_before_tests():
    """
    Clean mapped folder before etdmap tests run.

    This ensures tests start from a clean state without leftover files
    from previous test runs. The mapped files are regenerated as part
    of testing the mapping logic.
    """
    test_config_path = Path("config_test.yaml")
    if os.path.isfile(test_config_path):
        with open(test_config_path, 'r') as file:
            config = yaml.safe_load(file)
        _clean_mapped_folder(config['etdmap_configuration']['mapped_folder_path'])
    yield
def load_config(config_path):
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)

test_config_path = Path("config_test.yaml")
if os.path.isfile(test_config_path):
    config = load_config(test_config_path)
else:
        raise FileNotFoundError("no file named 'config_test.yaml'")

file_names = [
    "index.parquet",
    "household_1_table.parquet",
    "household_2_table.parquet",
    "household_3_table.parquet",
    "household_4_table.parquet",
    "household_5_table.parquet",
    "household_6_table.parquet",
    "household_7_table.parquet",
    "household_8_table.parquet",
    "household_9_table.parquet",
    "household_10_table.parquet",
]

def pytest_addoption(parser):
    parser.addoption("--copy-data", action="store_true", help="Copy data to a persistent folder")

@pytest.fixture
def valid_metadata_file(tmp_path):
    # tmp_path fixture provides a temp directory unique
    # to the test run
    # Create a valid Excel file with the required columns
    metadata_file = tmp_path / "metadata.xlsx"

    data = {}

    data = {
        "HuisIdLeverancier": ["A123", "B456", "C789"],
        "HuisIdBSV": [101, 202, None],  # One missing value for demonstration
        "Meenemen": [True, False, True],
        "ProjectIdLeverancier": ["P001", "P002", "P003"],
        "ProjectIdBSV": [1, 2, 3],
        "Notities": ["First note", None, "Final note"],  # One missing value
        "Dataleverancier": ["CompanyX", "CompanyY", "CompanyZ"],
    }

    data["num_column"] = np.random.randint(1, 100, size=3)
    data["num_column"].sort()
    df = pd.DataFrame(data)

    df.to_excel(metadata_file, sheet_name="Data", index=False)
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



@pytest.fixture
def load_metadata():
    def _load_metadata(filepath):
        with open(filepath, "r") as f:
            return json.load(f)
    # return inner function as ficture
    return _load_metadata




@pytest.fixture(scope="session")
def mapped_fixtures(tmp_path_factory):
    """The synthetic mapped dataset, generated into the configured folder.

    Generation lives in `etdmap.testing` so downstream suites can build the
    same dataset for themselves. Tests that read the mapped files request this
    fixture, so they run in any order or in isolation.
    """
    config = load_config(Path("config_test.yaml"))["etdmap_configuration"]
    return generate_mapped_dataset(
        config["mapped_folder_path"],
        bsv_metadata_file=config["bsv_metadata_file"],
        supplier_metadata_xlsx_file=config["supplier_metadata_xlsx_file"],
        project_mapping_csv_path=config["project_mapping_csv_path"],
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
