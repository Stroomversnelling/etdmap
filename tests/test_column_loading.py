"""A column can be declared after import, and the model returns afterwards.

A behavioural test states the bound it is judged against rather than inheriting
whatever the metadata declares this week. That needs two things of the library:
a column's rows can be merged into the model tables at runtime, and every
structure derived from them follows -- including the ones a column reaches
without naming, which is why the restore is a rebuild rather than an undo.
"""

import pandas as pd
import pytest

import etdmap.data_model as data_model
from etdmap.data_model import (
    data_analysis_columns,
    model_column_order,
    rebuild_model,
    resolve_bounds,
    thresholds_dict,
)
from etdmap.testing import declare_synthetic_model, model_rows, synthetic_model

#: Names nothing in the model uses, so a collision cannot make a test pass.
CUMULATIVE = "TestLoadedVariable"
INTERVAL = "TestLoadedVariableDiff"

CUMULATIVE_ROW = {
    "Variabele": CUMULATIVE,
    "Entiteit": "Prestatiedata",
    "Type variabele": "number",
    "Cumulatief": "ja",
    "Monotoon": "ja",
    "Momentaan": "nee",
    "Diff": "nee",
    "ComplementVariabele": INTERVAL,
}
INTERVAL_ROW = {
    "Variabele": INTERVAL,
    "Entiteit": "Prestatiedata",
    "Type variabele": "number",
    "Cumulatief": "nee",
    "Monotoon": "nee",
    "Momentaan": "nee",
    "Diff": "ja",
    "ComplementVariabele": CUMULATIVE,
}
# A cumulative variable is bounded against a yearly total and its interval
# column against one interval, so the pair needs a row of each kind.
CUMULATIVE_THRESHOLD = {
    "Variabele": CUMULATIVE,
    "ThresholdType": "cumulatief",
    "Min": 0.0,
    "Max": 99999.0,
}
INTERVAL_THRESHOLD = {
    "Variabele": INTERVAL,
    "ThresholdType": "5-minute",
    "Min": 0.0,
    "Max": 1.0,
}

#: Every module-level structure the model is derived into.
STRUCTURE_NAMES = sorted(
    name
    for name in dir(data_model)
    if not name.startswith("_")
    and isinstance(getattr(data_model, name), (dict, list))
)

#: Declared in code rather than derived from the model, so a rebuild leaves
#: them alone and they are not part of the restore.
LITERAL_STRUCTURES = {
    "BOOLEAN_SYNONYMS",
    "peak_offpeak_substrings",
    "test_aggregation_columns",
}


def structures() -> dict:
    """A copy of every model structure, for comparing whole."""
    return {
        name: (
            dict(value)
            if isinstance(value := getattr(data_model, name), dict)
            else list(value)
        )
        for name in STRUCTURE_NAMES
    }


def identities() -> dict:
    """The object identity of every model structure."""
    return {name: id(getattr(data_model, name)) for name in STRUCTURE_NAMES}


@pytest.fixture(autouse=True)
def _model_restored():
    """Leave the model as it was found, whatever a test did to it."""
    yield
    rebuild_model()


class TestLoadingAColumn:
    def test_a_loaded_threshold_is_the_bound_that_resolves(self):
        """The point of the mechanism: the bound under test is the test's."""
        rebuild_model(extra_threshold_rows=[INTERVAL_THRESHOLD])

        assert resolve_bounds(thresholds_dict[INTERVAL], 5, INTERVAL) == (
            0.0,
            1.0,
        )

    def test_a_loaded_threshold_reaches_a_module_that_imported_it_by_name(self):
        """Consumers bind the dict itself, so a rebuild must write into it."""
        from etdmap.record_validators import thresholds_dict as via_validators

        rebuild_model(extra_threshold_rows=[INTERVAL_THRESHOLD])

        assert via_validators[INTERVAL]["Max"] == 1.0

    def test_a_loaded_column_lands_in_the_lists_its_classifiers_select(self):
        rebuild_model(
            extra_model_rows=[CUMULATIVE_ROW, INTERVAL_ROW],
            extra_threshold_rows=[CUMULATIVE_THRESHOLD, INTERVAL_THRESHOLD],
        )

        assert CUMULATIVE in data_model.cumulative_columns
        assert CUMULATIVE in data_model.monotonic_columns
        assert INTERVAL in data_model.diff_columns
        assert CUMULATIVE not in data_model.momentaan_columns

    def test_a_loaded_pair_resolves_through_the_complement_declaration(self):
        rebuild_model(
            extra_model_rows=[CUMULATIVE_ROW, INTERVAL_ROW],
            extra_threshold_rows=[CUMULATIVE_THRESHOLD, INTERVAL_THRESHOLD],
        )

        assert data_model.interval_column(CUMULATIVE) == INTERVAL
        assert data_model.cumulative_column(INTERVAL) == CUMULATIVE
        assert INTERVAL in data_model.monotonic_interval_columns

    def test_a_loaded_column_gets_the_per_column_checks(self):
        """A column with no check is a column the mapping stage cannot judge."""
        from etdmap.dataset_validators import dataset_flag_conditions
        from etdmap.record_validators import record_flag_conditions

        rebuild_model(
            extra_model_rows=[CUMULATIVE_ROW, INTERVAL_ROW],
            extra_threshold_rows=[CUMULATIVE_THRESHOLD, INTERVAL_THRESHOLD],
        )

        assert f"validate_{INTERVAL}" in record_flag_conditions
        assert f"validate_{INTERVAL}_outliers" in record_flag_conditions
        assert f"validate_{CUMULATIVE}" in dataset_flag_conditions

    def test_reloading_a_column_replaces_its_entry(self):
        rebuild_model(extra_threshold_rows=[INTERVAL_THRESHOLD])
        rebuild_model(
            extra_threshold_rows=[{**INTERVAL_THRESHOLD, "Max": 9.0}]
        )

        assert thresholds_dict[INTERVAL]["Max"] == 9.0
        assert [c for c in thresholds_dict if c == INTERVAL] == [INTERVAL]

    def test_a_loaded_row_may_override_a_column_the_model_declares(self):
        """Stating a bound for a real column is the ordinary case for a test."""
        declared = next(iter(data_model.diff_columns))
        rebuild_model(
            extra_threshold_rows=[
                {
                    "Variabele": declared,
                    "ThresholdType": "5-minute",
                    "Min": 0.0,
                    "Max": 0.25,
                }
            ]
        )

        assert thresholds_dict[declared]["Max"] == 0.25

    def test_a_field_the_model_does_not_have_is_refused(self):
        """A typo in a row would otherwise be silently carried as no value."""
        with pytest.raises(ValueError, match="does not have"):
            rebuild_model(extra_model_rows=[{"NotAModelField": "x"}])

    def test_a_cumulative_column_with_no_complement_is_refused(self):
        with pytest.raises(ValueError):
            rebuild_model(
                extra_model_rows=[
                    {**CUMULATIVE_ROW, "ComplementVariabele": pd.NA}
                ],
                extra_threshold_rows=[CUMULATIVE_THRESHOLD],
            )


class TestTheWritesAreInPlace:
    """Consumers hold the structures themselves, so rebinding would leave them
    reading the model as it was when they imported it."""

    def test_every_structure_keeps_its_identity(self):
        before = identities()
        rebuild_model(
            extra_model_rows=[CUMULATIVE_ROW, INTERVAL_ROW],
            extra_threshold_rows=[CUMULATIVE_THRESHOLD, INTERVAL_THRESHOLD],
        )

        assert identities() == before

    def test_data_analysis_columns_still_tracks_the_column_order(self):
        """It is an alias, which only survives a rebuild that writes in place."""
        rebuild_model(extra_model_rows=[CUMULATIVE_ROW, INTERVAL_ROW],
                      extra_threshold_rows=[CUMULATIVE_THRESHOLD,
                                            INTERVAL_THRESHOLD])

        assert data_analysis_columns is model_column_order
        assert CUMULATIVE in data_analysis_columns


class TestTheModelReturns:
    """A rebuild with no rows is the restore, so a declaration cannot leak into
    the next test. Compared whole rather than by the entries a load wrote: a
    column reaches the pairing lookups, the indexed lookups and the per-column
    checks without naming any of them."""

    def test_a_rebuild_with_no_rows_changes_nothing(self):
        before = structures()
        rebuild_model()

        assert structures() == before

    def test_a_new_column_is_gone_afterwards(self):
        before = structures()
        rebuild_model(
            extra_model_rows=[CUMULATIVE_ROW, INTERVAL_ROW],
            extra_threshold_rows=[CUMULATIVE_THRESHOLD, INTERVAL_THRESHOLD],
        )
        rebuild_model()

        assert structures() == before

    def test_an_overridden_threshold_is_restored(self):
        before = structures()
        declared = next(iter(data_model.diff_columns))
        rebuild_model(
            extra_threshold_rows=[
                {
                    "Variabele": declared,
                    "ThresholdType": "5-minute",
                    "Min": 0.0,
                    "Max": 0.25,
                }
            ]
        )
        rebuild_model()

        assert structures() == before

    def test_a_changed_classifier_is_restored(self):
        """The case an undo would get wrong: the column moves between lists.

        A row replaces the declaration it names, so the override carries the
        shipped row with one classifier changed rather than that field alone.
        """
        before = structures()
        declared = next(
            c for c in data_model.cumulative_columns
            if c in data_model.monotonic_columns
        )
        shipped = data_model.load_etdmodel()
        row = shipped[shipped["Variabele"] == declared].iloc[0].to_dict()

        rebuild_model(extra_model_rows=[{**row, "Monotoon": "nee"}])
        assert declared not in data_model.monotonic_columns
        assert declared in data_model.cumulative_columns
        rebuild_model()

        assert structures() == before

    def test_the_declaration_context_restores_on_the_way_out(self):
        before = structures()
        with declare_synthetic_model() as model:
            assert model.device in data_model.cumulative_columns

        assert structures() == before


class TestEveryStructureIsAccountedFor:
    def test_each_structure_is_derived_or_a_declared_literal(self):
        """A structure in neither set would silently survive a restore, so a
        new one has to be added to the rebuild or named as a literal."""
        before = structures()
        rebuild_model(
            extra_model_rows=[CUMULATIVE_ROW, INTERVAL_ROW],
            extra_threshold_rows=[CUMULATIVE_THRESHOLD, INTERVAL_THRESHOLD],
        )
        moved = {
            name for name in STRUCTURE_NAMES
            if structures()[name] != before[name]
        }

        assert moved, "the load reached no structure at all"
        assert not moved & LITERAL_STRUCTURES


class TestTheDeclarationTranslatesToRows:
    def test_a_declared_model_is_derived_by_the_real_pairing_rules(self):
        """The declaration supplies rows; the pairings are not assembled by
        the helper, so a test exercises the derivation the pipeline uses."""
        model = synthetic_model()

        with declare_synthetic_model(model):
            assert data_model.interval_column(model.device) == (
                model.device_complement
            )
            assert data_model.tariff_root_to_splits[model.device] == (
                model.high,
                model.low,
            )

    def test_every_declared_column_becomes_a_row(self):
        model = synthetic_model()
        rows, threshold_rows = model_rows(model)

        assert {row["Variabele"] for row in rows} == set(model.complements)
        assert {row["Variabele"] for row in threshold_rows} == set(
            model.thresholds
        )
