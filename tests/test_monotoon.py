"""`Monotoon` says whether a cumulative variable only ever increases.

The averaging and the negative-diff corrections apply to monotonic cumulative
variables and to nothing else. They read this rather than inferring it from a
declared Min of zero: a Min is a physical bound on a value, which is a
different fact from whether a series can go backwards.
"""


from etdmap.data_model import (
    cumulative_columns,
    interval_column,
    monotonic_columns,
    monotonic_interval_columns,
)


class TestMonotonicColumns:
    def test_every_monotonic_column_is_cumulative(self):
        strays = sorted(set(monotonic_columns) - set(cumulative_columns))
        assert not strays, (
            f"declared Monotoon=ja but not Cumulatief=ja: {strays}. Only a "
            f"cumulative variable can be monotonic in this sense."
        )

    def test_the_known_meters_are_monotonic(self):
        for column in (
            "ElektriciteitNetgebruik",
            "ElektriciteitTeruglevering",
            "Gasgebruik",
            "WatergebruikWarmTapwater",
            "ZonopwekTotaal",
        ):
            assert column in monotonic_columns, f"{column} is not Monotoon=ja"

    def test_it_never_exceeds_the_cumulative_columns(self):
        """Every cumulative variable is Monotoon=ja today, so the two lists are
        equal. That is a fact about the data, not a rule -- a meter that
        legitimately dips is Cumulatief=ja with Monotoon=nee, and would appear
        in one list and not the other."""
        assert set(monotonic_columns) <= set(cumulative_columns)


class TestMonotonicIntervalColumns:
    """The interval column of a monotonic cumulative variable: what the
    averaging and the fill actually operate on."""

    def test_each_one_is_the_interval_of_a_monotonic_cumulative(self):
        expected = {
            interval_column(c) for c in monotonic_columns if interval_column(c)
        }
        assert set(monotonic_interval_columns) == expected

    def test_a_known_pair_resolves(self):
        assert interval_column("ElektriciteitNetgebruik") in monotonic_interval_columns

    def test_none_is_none(self):
        assert None not in monotonic_interval_columns
