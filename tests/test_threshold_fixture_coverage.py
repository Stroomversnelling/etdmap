"""
Contract guard: the synthetic test fixtures must keep pace with the thresholds
data model.

Generation ranges are intentionally frozen and decoupled from thresholds.csv
(see tests/conftest.py::SYNTHETIC_DIFF_RANGES). That keeps the suite hermetic
and non-circular, but it removes the old property where the fixtures auto-tracked
the model. This guard recovers that deliberately: it WARNS (does not fail) when
thresholds.csv grows variables, columns, or unit types the fixtures no longer
cover, so a contributor extends the fixtures / pinned sets on purpose.

Warn-only mirrors the existing "fixture outdated" warnings in
test_index_helpers.py. The companion test_coverage_guard_detects_drift proves
the guard actually fires.
"""
import warnings

from conftest import SYNTHETIC_DIFF_RANGES

from etdmap.data_model import cumulative_columns, load_thresholds

# Pinned 2026-06-19. Update DELIBERATELY (and extend the fixtures) when the
# thresholds data model legitimately grows -- that deliberate edit is the point.
EXPECTED_THRESHOLD_COLUMNS = {
    "Variabele", "ThresholdType", "Eenheid", "Min", "Max", "ThresholdToelichting",
}
EXPECTED_UNIT_TYPES = {
    "1 / uur", "A", "GJ", "V", "W", "graden C", "kW", "kWh", "liter",
    "liter / uur", "m3", "m3 / uur", "ppm", "procenten",
}


def _coverage_drift(th) -> list[str]:
    """Return human-readable messages for each way the fixtures have fallen
    behind the thresholds data model. Empty list == fully covered."""
    msgs = []

    # Schema: new/removed columns in thresholds.csv.
    cols = set(th.columns)
    if cols != EXPECTED_THRESHOLD_COLUMNS:
        msgs.append(
            f"thresholds.csv columns changed (added={sorted(cols - EXPECTED_THRESHOLD_COLUMNS)}, "
            f"removed={sorted(EXPECTED_THRESHOLD_COLUMNS - cols)})"
        )

    # Unit vocabulary: new units may need deliberate generator/validator handling.
    new_units = set(th["Eenheid"].dropna().astype(str)) - EXPECTED_UNIT_TYPES
    if new_units:
        msgs.append(f"new unit type(s) in thresholds.csv: {sorted(new_units)}")

    # Variable coverage: the fixture generates raw cumulative meters from
    # SYNTHETIC_DIFF_RANGES; that spec must stay aligned with cumulative_columns
    # and each column's per-interval (5-minute) Diff threshold.
    diff_5min = set(th.loc[th["ThresholdType"].astype(str) == "5-minute", "Variabele"])
    uncovered = [c for c in cumulative_columns if c not in SYNTHETIC_DIFF_RANGES]
    stale = [c for c in SYNTHETIC_DIFF_RANGES if c not in cumulative_columns]
    no_threshold = [c for c in cumulative_columns if f"{c}Diff" not in diff_5min]
    if uncovered or stale or no_threshold:
        msgs.append(
            f"variable-coverage drift (cumulative columns without a SYNTHETIC_DIFF_RANGES "
            f"entry={uncovered}; stale range entries not in cumulative_columns={stale}; "
            f"cumulative columns without a 5-minute Diff threshold={no_threshold})"
        )

    return msgs


def test_thresholds_fixture_coverage():
    """Warn (not fail) if the fixtures no longer cover the full thresholds data model."""
    for msg in _coverage_drift(load_thresholds()):
        warnings.warn(
            "Fixtures currently do not cover the full thresholds data model: "
            + msg
            + " -- extend tests/conftest.py::SYNTHETIC_DIFF_RANGES and/or the pinned "
            "EXPECTED_* sets in this file.",
            UserWarning,
            stacklevel=2,
        )


def test_coverage_guard_detects_drift():
    """The guard must actually fire on schema and unit drift (test the test)."""
    th = load_thresholds().copy()
    th["NewField"] = ""                                   # schema drift
    th.loc[th.index[0], "Eenheid"] = "FAKE_UNIT"          # unit drift
    joined = " ".join(_coverage_drift(th))
    assert "columns changed" in joined
    assert "FAKE_UNIT" in joined
