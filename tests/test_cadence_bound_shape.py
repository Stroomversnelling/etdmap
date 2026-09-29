"""The upper bound a column is judged against, scaled to its cadence.

A bound is declared against one unit of time. A column reporting hourly
delivers twelve of those units in one reading, so the bound it is judged
against has to grow -- but not in proportion, because a peak that is plausible
for five minutes is not plausible sustained for an hour.

    B(n) = n * sustained + (Max - sustained) * n ** exponent

with n the ratio of the column's cadence to the threshold's own unit,
`sustained` the rate that can be held indefinitely, and `exponent` how fast the
remainder grows. At n = 1 this is Max for any exponent, so a column reporting
at the declared unit is unaffected.
"""

import math

import pandas as pd
import pytest

from etdmap.data_model import scale_upper_bound


class TestTheAnchor:
    """At the declared cadence the bound is the declared bound, whatever the
    shape parameters say. Anything else would move every column at once."""

    @pytest.mark.parametrize("sustained,exponent", [
        (0.0, 0.5), (0.0, 1.0), (0.3, 0.5), (1.5, 1.0), (0.0, 0.0),
    ])
    def test_b_of_one_is_max(self, sustained, exponent):
        assert scale_upper_bound(1.5, 1.0, sustained, exponent) == pytest.approx(1.5)


class TestTheShape:
    def test_a_linear_exponent_scales_in_proportion(self):
        """exponent = 1, sustained = 0 reproduces the plain multiplication the
        bound used before a shape was declarable."""
        assert scale_upper_bound(1.5, 12.0, 0.0, 1.0) == pytest.approx(18.0)

    def test_a_square_root_exponent_grows_more_slowly(self):
        assert scale_upper_bound(1.5, 12.0, 0.0, 0.5) == pytest.approx(1.5 * 12 ** 0.5)

    def test_a_sustained_rate_is_carried_in_full(self):
        """What can be held indefinitely scales with n; only the excess is
        damped."""
        got = scale_upper_bound(1.5, 12.0, 0.5, 0.5)
        assert got == pytest.approx(12 * 0.5 + (1.5 - 0.5) * 12 ** 0.5)

    def test_it_is_monotonic_in_n(self):
        previous = 0.0
        for n in (1, 2, 3, 6, 12, 24):
            current = scale_upper_bound(1.5, float(n), 0.0, 0.5)
            assert current > previous
            previous = current


class TestDefaults:
    def test_absent_shape_parameters_use_the_declared_defaults(self):
        import pandas as pd

        assert scale_upper_bound(1.5, 12.0, pd.NA, pd.NA) == pytest.approx(
            scale_upper_bound(1.5, 12.0, 0.0, 0.5)
        )

    def test_an_absent_max_stays_absent(self):
        import pandas as pd

        assert pd.isna(scale_upper_bound(pd.NA, 12.0, 0.0, 0.5))


class TestItRefusesImpossibleParameters:
    def test_a_sustained_rate_above_max_raises(self):
        with pytest.raises(ValueError, match="UpperThresholdSustained"):
            scale_upper_bound(1.5, 12.0, 2.0, 0.5)

    @pytest.mark.parametrize("exponent", [-0.1, 1.1, 2.0])
    def test_an_exponent_outside_the_unit_interval_raises(self, exponent):
        with pytest.raises(ValueError, match="UpperThresholdExponent"):
            scale_upper_bound(1.5, 12.0, 0.0, exponent)

    def test_the_boundaries_are_allowed(self):
        scale_upper_bound(1.5, 12.0, 0.0, 0.0)
        scale_upper_bound(1.5, 12.0, 0.0, 1.0)
        scale_upper_bound(1.5, 12.0, 1.5, 0.5)


class TestTheThresholdEntryCarriesTheShape:
    def test_the_loader_exposes_both_parameters(self):
        from etdmap.data_model import load_thresholds_as_dict

        entry = load_thresholds_as_dict()["ElektriciteitNetgebruikDiff"]
        assert "UpperThresholdSustained" in entry
        assert "UpperThresholdExponent" in entry


class TestItSubsumesTheLinearBound:
    """Stage one of adopting the shape: with exponent 1 and no sustained rate
    the new formula must reproduce the plain multiplication the pipeline used
    before, for every declared threshold at every cadence in use. Only then is
    a change of exponent a change of policy rather than of mechanism."""

    @pytest.mark.parametrize("cadence", [1.0, 5.0, 15.0, 60.0])
    def test_every_declared_threshold_matches_the_old_scaling(self, cadence):
        from etdmap.data_model import load_thresholds_as_dict, scale_bounds
        from etdmap.flags import scale_bound_to_cadence

        mismatches = []
        for column, declared in load_thresholds_as_dict().items():
            linear = dict(declared)
            linear["UpperThresholdSustained"] = 0.0
            linear["UpperThresholdExponent"] = 1.0

            _, got = scale_bounds(linear, cadence)
            expected = scale_bound_to_cadence(
                declared.get("Max"), declared.get("ThresholdType"), cadence
            )
            if pd.isna(expected) and pd.isna(got):
                continue
            if pd.isna(expected) != pd.isna(got) or not math.isclose(
                float(got), float(expected), rel_tol=1e-12
            ):
                mismatches.append(f"{column}: {got} != {expected}")

        assert not mismatches, (
            f"the shaped bound differs from the linear one at exponent 1 for "
            f"{len(mismatches)} column(s): {mismatches[:5]}"
        )
