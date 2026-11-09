"""A declaration that cannot be loaded leaves the model as it was.

``rebuild_model`` replaces the contents of every derived container in place, so
there is a window in which some hold the new model and some the old. A module
reading one of them in that window reads a model that was never declared: the
thresholds of one and the column list of another.

``test_column_loading.py`` covers what a successful declaration reaches. This
covers the failures -- one that cannot be derived from, and one that is refused
only once the per-column checks are built, which is after the containers have
been written.

The containers are checked by identity as well as by contents, because the
restore has to keep the object every ``from etdmap.data_model import ...``
binding already holds. Restoring by rebinding would read as correct here and
leave every importer on the failed model.
"""

import pandas as pd
import pytest

from etdmap import data_model, dataset_validators, record_validators
from etdmap.testing import cumulative_pair, model_row

#: Names nothing the shipped model names, so what the assertions below see is
#: this declaration and not a shipped row.
DECLARED = "QAtomicDevice"
DECLARED_INTERVAL = f"{DECLARED}Diff"

#: A complete declaration of that variable: the pair and both threshold rows.
#: Declared without the thresholds it is refused, which is the failure these
#: tests are about.
GOOD_ROWS, GOOD_THRESHOLDS = cumulative_pair(DECLARED, Monotoon="ja")

#: Every container rebuild_model writes, by the name it is read under.
CONTAINERS = (
    "cumulative_columns",
    "momentaan_columns",
    "diff_columns",
    "monotonic_columns",
    "model_column_order",
    "model_column_type",
    "allowed_supplier_metadata_columns",
    "supplier_metadata_dtypes",
    "all_performance_data_columns",
    "required_performance_data_columns",
    "performance_column_dtypes",
    "thresholds_dict",
    "tariff_partner_columns",
    "tariff_root_to_splits",
    "interval_by_cumulative",
    "cumulative_by_interval",
    "monotonic_interval_columns",
    "variables_by_categorie",
    "variables_by_eenheid",
    "physical_model_by_project",
)


def _state():
    """What every container holds, and which object holds it."""
    return {
        name: (id(getattr(data_model, name)), _contents(getattr(data_model, name)))
        for name in CONTAINERS
    }


def _contents(container):
    return list(container) if isinstance(container, list) else dict(container)


@pytest.fixture
def shipped():
    """The shipped model before the test, restored after it.

    Restored whatever the test did, so a failure here cannot leave a declared
    column in the model the rest of the session reads.
    """
    state = _state()
    yield state
    data_model.rebuild_model()


class TestADeclarationThatCannotBeDerivedFrom:
    """Refused before anything is written."""

    def test_an_unknown_field_is_refused(self, shipped):
        with pytest.raises(ValueError, match="fields the data model does not have"):
            data_model.rebuild_model(
                extra_model_rows=[model_row(DECLARED, NotAField="x")]
            )

    def test_the_model_is_untouched_after_it(self, shipped):
        with pytest.raises(ValueError):
            data_model.rebuild_model(
                extra_model_rows=[model_row(DECLARED, NotAField="x")]
            )

        assert _state() == shipped

    def test_an_unreadable_complement_pair_is_refused(self, shipped):
        """Declared as a pair of two interval columns, which the link cannot
        be read from."""
        with pytest.raises(ValueError, match="not one Cumulatief"):
            data_model.rebuild_model(
                extra_model_rows=[
                    model_row(DECLARED, Diff="ja",
                               ComplementVariabele=DECLARED_INTERVAL),
                    model_row(DECLARED_INTERVAL, Diff="ja",
                               ComplementVariabele=DECLARED),
                ]
            )

        assert _state() == shipped


class TestADeclarationRefusedAfterTheContainersAreWritten:
    """The window this is about: the per-column checks are built last, and a
    cumulative variable with no threshold row is refused there."""

    def _declare(self):
        return data_model.rebuild_model(extra_model_rows=GOOD_ROWS)

    def test_it_is_refused(self, shipped):
        with pytest.raises(ValueError, match="no row in the thresholds"):
            self._declare()

    def test_the_declaration_is_not_left_in_the_model(self, shipped):
        with pytest.raises(ValueError):
            self._declare()

        assert DECLARED not in data_model.cumulative_columns
        assert data_model.interval_column(DECLARED) is None

    def test_every_container_is_back_to_the_shipped_model(self, shipped):
        with pytest.raises(ValueError):
            self._declare()

        assert _state() == shipped

    def test_the_per_column_checks_agree_with_the_model_again(self, shipped):
        """The containers and the checks derived from them are restored
        together. A restore of the containers alone would leave the checks
        holding the declaration."""
        with pytest.raises(ValueError):
            self._declare()

        assert f"validate_{DECLARED}" not in dataset_validators.dataset_flag_conditions
        assert f"validate_{DECLARED_INTERVAL}" not in record_validators.record_flag_conditions

    def test_the_declarations_own_error_is_the_one_raised(self, shipped):
        """Not an error from the restore. The caller is told what was wrong
        with what they declared."""
        with pytest.raises(ValueError) as raised:
            self._declare()

        assert "no row in the thresholds" in str(raised.value)
        assert DECLARED in str(raised.value)


class TestTheSameDeclarationWithItsThresholds:
    """The contrast: nothing above is failing because the declaration is
    unusable, only because it is incomplete."""

    def test_it_loads(self, shipped):
        data_model.rebuild_model(
            extra_model_rows=GOOD_ROWS, extra_threshold_rows=GOOD_THRESHOLDS
        )

        assert DECLARED in data_model.cumulative_columns
        assert data_model.interval_column(DECLARED) == DECLARED_INTERVAL
        assert DECLARED_INTERVAL in data_model.monotonic_interval_columns
        assert data_model.thresholds_dict[DECLARED_INTERVAL]["Max"] == 1.0

    def test_the_containers_are_the_same_objects(self, shipped):
        data_model.rebuild_model(
            extra_model_rows=GOOD_ROWS, extra_threshold_rows=GOOD_THRESHOLDS
        )

        now = _state()
        assert [now[name][0] for name in CONTAINERS] == [
            shipped[name][0] for name in CONTAINERS
        ]

    def test_restoring_removes_it_again(self, shipped):
        data_model.rebuild_model(
            extra_model_rows=GOOD_ROWS, extra_threshold_rows=GOOD_THRESHOLDS
        )
        data_model.rebuild_model()

        assert _state() == shipped


class TestTheShippedTablesAreNeverWrittenTo:
    """Which is what makes the restore available at all."""

    def test_a_declaration_does_not_reach_the_held_tables(self, shipped):
        data_model.rebuild_model(
            extra_model_rows=GOOD_ROWS, extra_threshold_rows=GOOD_THRESHOLDS
        )

        assert DECLARED not in set(data_model._model_frame_base["Variabele"])
        assert DECLARED not in set(data_model._thresholds_frame_base["Variabele"])

    def test_two_declarations_do_not_accumulate(self, shipped):
        """Each starts from the shipped tables, so the second does not carry
        the first."""
        other = "QAtomicOther"
        data_model.rebuild_model(
            extra_model_rows=GOOD_ROWS, extra_threshold_rows=GOOD_THRESHOLDS
        )
        data_model.rebuild_model(
            extra_model_rows=[model_row(other, Diff="ja")],
            extra_threshold_rows=[
                {"Variabele": other, "Min": 0.0, "Max": 1.0,
                 "ThresholdType": "5-minute"}
            ],
        )

        assert other in data_model.diff_columns
        assert DECLARED not in data_model.cumulative_columns
        assert pd.isna(data_model.thresholds_dict.get(DECLARED, {}).get("Max", pd.NA))
