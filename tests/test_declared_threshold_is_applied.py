"""A declared threshold reaches the data as a bound, at the column's cadence.

``test_bound_flags.py`` covers what a bound does once resolved -- it passes the
pair in directly. This file covers the step before it: that the bound applied
is the one the model declares, and that it is scaled to the interval the column
reports at rather than used as written.

The model is declared by the test. What matters is the behaviour -- a declared
Max becomes the limit, and the limit moves with the cadence -- not which
variables the metadata happens to carry, so nothing here names a real column or
reads a real threshold.
"""

import pandas as pd
import pytest

from etdmap.data_model import resolve_bounds, scale_bounds
from etdmap.flags import MappingFlag, bound_flags
from etdmap.testing import declare_synthetic_model, synthetic_model

#: The bounds this file declares. Any numbers work; stating them is the point.
MIN = 0.1
MAX = 0.5


@pytest.fixture
def model():
    with declare_synthetic_model(
        synthetic_model(maximum=MAX, minimum=MIN)
    ) as declared:
        yield declared


@pytest.fixture
def column(model):
    return model.device_complement


@pytest.fixture
def declared(model, column):
    """The threshold entry the declared model carries for *column*.

    Read through the module rather than imported at the top of this file: the
    declaration is installed per test, and a module-level import would bind
    the real thresholds before it.
    """
    from etdmap.data_model import thresholds_dict

    return thresholds_dict[column]


class TestTheDeclaredBoundIsTheOneApplied:
    def test_the_bound_is_the_declared_pair_at_the_declared_interval(
        self, declared, column
    ):
        """No scaling at the interval the threshold was written against."""
        assert resolve_bounds(declared, 5, column) == (MIN, MAX)

    def test_a_value_over_the_declared_max_is_out_of_bounds(
        self, declared, column
    ):
        low, high = resolve_bounds(declared, 5, column)
        values = pd.Series(pd.array([MIN, MAX, MAX + 0.01], dtype="Float64"))

        assert bound_flags(values, low, high).tolist() == [
            MappingFlag.NONE,
            MappingFlag.NONE,
            MappingFlag.OUT_OF_BOUNDS_HIGH,
        ]

    def test_a_value_under_the_declared_min_is_out_of_bounds(
        self, declared, column
    ):
        low, high = resolve_bounds(declared, 5, column)
        values = pd.Series(pd.array([MIN - 0.01, MIN], dtype="Float64"))

        assert bound_flags(values, low, high).tolist() == [
            MappingFlag.OUT_OF_BOUNDS_LOW,
            MappingFlag.NONE,
        ]


class TestTheBoundMovesWithTheCadence:
    """A per-interval threshold states a limit for one reporting interval, so a
    column reporting less often is judged against more of it. Using the
    declared number unscaled would reject a coarse column's ordinary readings.
    """

    @pytest.mark.parametrize("cadence", [15, 60])
    def test_a_coarser_cadence_gives_a_higher_limit(
        self, declared, column, cadence
    ):
        _, at_grid = resolve_bounds(declared, 5, column)
        _, at_cadence = resolve_bounds(declared, cadence, column)

        assert at_cadence > at_grid

    def test_the_same_reading_passes_at_its_own_cadence_and_fails_at_the_grid(
        self, declared, column
    ):
        _, at_five = resolve_bounds(declared, 5, column)
        _, at_sixty = resolve_bounds(declared, 60, column)
        reading = pd.Series(
            pd.array([(at_five + at_sixty) / 2], dtype="Float64")
        )

        assert bound_flags(reading, MIN, at_five).tolist() == [
            MappingFlag.OUT_OF_BOUNDS_HIGH
        ]
        assert bound_flags(reading, MIN, at_sixty).tolist() == [
            MappingFlag.NONE
        ]

    def test_an_unresolved_cadence_is_refused_not_assumed(
        self, declared, column
    ):
        """Falling back to the declared pair would silently decide the column
        reports at the interval the bound was written against."""
        with pytest.raises(ValueError):
            resolve_bounds(declared, None, column)


class TestWhatTheDeclarationDoesNotSay:
    def test_a_column_the_model_does_not_declare_has_no_threshold(self, model):
        """The declaration is the whole source of bounds, so a column absent
        from it is unbounded rather than bounded by a default."""
        from etdmap.data_model import thresholds_dict

        assert "NotInTheDeclaredModel" not in thresholds_dict

    def test_an_undeclared_max_is_no_upper_limit(self, declared):
        """A one-sided threshold is normal in the real metadata, and the
        missing side must not collapse to zero."""
        low, high = scale_bounds({**declared, "Max": pd.NA}, 5)

        assert pd.isna(high)
        values = pd.Series(pd.array([10_000.0], dtype="Float64"))
        assert bound_flags(values, low, high).tolist() == [MappingFlag.NONE]
