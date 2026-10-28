import pandas as pd
import pytest

from etdmap.data_model import cumulative_columns, interval_column
from etdmap.dataset_validators import (
    dataset_flag_conditions,
    validate_columns,
)
from etdmap.index_helpers import read_metadata


def test_validate_columns(valid_metadata_file):
    """Basic test for validate_columns with a valid condition function."""
    def condition_func(df):
        return df["num_column"] > 0

    test_df = read_metadata(valid_metadata_file)
    result = validate_columns(test_df, ["num_column"], condition_func)
    assert result is True

def test_dataset_flag_conditions():
    """Test keys and values in dataset_flag_conditions.

    Tests:
    1. Whether testsfuncs for all cumulative columns are present
    2. Whether special checks are present:
        - "validate_monitoring_data_counts"
        - "validate_energiegebruik_warmteopwekker"
        - "validate_approximately_one_year_of_records"
        - "validate_columns_exist"
        - "validate_no_readingdate_gap"
    3. Wheter all values are functions.
    """
    assert all("validate_" + col in dataset_flag_conditions for col in cumulative_columns)
    # The interval column is the one the data model links to the cumulative
    # variable, not its name plus "Diff".
    assert all(
        "validate_" + interval_column(col) in dataset_flag_conditions
        for col in cumulative_columns
    )

    special_checks = (
        "validate_monitoring_data_counts",
        "validate_energiegebruik_warmteopwekker",
        "validate_approximately_one_year_of_records",
        "validate_columns_exist",
        "validate_no_readingdate_gap"
    )
    assert all(check in dataset_flag_conditions for check in special_checks)

    # check if each value in dict is a function
    assert all(callable(value) for value in dataset_flag_conditions.values())


class TestRowCountFollowsTheGrid:
    """How many rows a complete year holds depends on the interval the
    household is recorded on: 105,120 at five minutes, 35,040 at fifteen, 8,760
    hourly.

    The check used to be a fixed 100,000..110,000 window, which is a statement
    about the five-minute grid alone. Every household on any other grid failed
    it unconditionally, and the flag went into the index as a verdict rather
    than as "this was never applicable".
    """

    GRIDS = [5, 15, 60]

    def _year(self, grid_minutes):
        """A full year of readings on *grid_minutes*."""
        rows = int((365 * 24 * 60) / grid_minutes)
        return pd.DataFrame({
            "ReadingDate": pd.date_range(
                "2024-01-01", periods=rows, freq=f"{grid_minutes}min"
            ),
        })

    @pytest.mark.parametrize("grid_minutes", GRIDS)
    def test_a_full_year_passes_on_its_own_grid(self, grid_minutes):
        from etdmap.dataset_validators import validate_monitoring_data_counts
        df = self._year(grid_minutes)
        assert validate_monitoring_data_counts(df, grid_minutes) is True

    @pytest.mark.parametrize("grid_minutes", [15, 60])
    def test_a_full_year_failed_the_old_fixed_window(self, grid_minutes):
        """The contrast that makes the test above mean something: these row
        counts sit outside the window that used to be hardcoded."""
        rows = len(self._year(grid_minutes))
        assert not (100000 <= rows <= 110000)

    @pytest.mark.parametrize("grid_minutes", GRIDS)
    def test_a_third_of_a_year_fails_on_its_own_grid(self, grid_minutes):
        from etdmap.dataset_validators import validate_monitoring_data_counts
        df = self._year(grid_minutes).iloc[: len(self._year(grid_minutes)) // 3]
        assert validate_monitoring_data_counts(df, grid_minutes) is False

    def test_an_unknown_grid_reports_nothing_rather_than_a_verdict(self, caplog):
        """Without the grid there is no expected count, so nothing was checked.
        pd.NA says that; True or False would both be claims."""
        import logging

        from etdmap.dataset_validators import validate_monitoring_data_counts
        df = self._year(5)
        with caplog.at_level(logging.WARNING):
            assert validate_monitoring_data_counts(df, None) is pd.NA
        assert "no grid was given" in caplog.text

    def test_an_empty_frame_reports_nothing(self):
        from etdmap.dataset_validators import validate_monitoring_data_counts
        assert validate_monitoring_data_counts(pd.DataFrame(), 5) is pd.NA


class TestValidatorsReturnNaRatherThanRaising:
    """A validator handed a frame without the columns it needs reports that
    nothing was checked. It used to raise KeyError, which update_index caught
    and recorded as pd.NA -- the same result, reached by swallowing an
    exception, which also swallowed real faults."""

    def test_energiegebruik_warmteopwekker_without_its_parts(self):
        from etdmap.dataset_validators import (
            validate_energiegebruik_warmteopwekker,
        )
        df = pd.DataFrame({
            "ReadingDate": pd.date_range("2024-01-01", periods=3, freq="5min"),
        })
        assert validate_energiegebruik_warmteopwekker(df) is pd.NA

    def test_it_does_not_add_a_column_to_the_frame_it_is_given(self):
        from etdmap.dataset_validators import (
            validate_energiegebruik_warmteopwekker,
        )
        df = pd.DataFrame({
            "ReadingDate": pd.date_range("2024-01-01", periods=3, freq="5min"),
            "ElektriciteitsgebruikWarmtepomp": pd.array(
                [1.0, 2.0, 3.0], dtype="Float64"
            ),
            "ElektriciteitsgebruikBooster": pd.array(
                [1.0, 2.0, 3.0], dtype="Float64"
            ),
            "ElektriciteitsgebruikBoilervat": pd.array(
                [1.0, 2.0, 3.0], dtype="Float64"
            ),
        })
        before = list(df.columns)
        validate_energiegebruik_warmteopwekker(df)
        assert list(df.columns) == before

    def test_the_cumulative_outlier_summary_without_its_flag_column(self):
        from etdmap.dataset_validators import (
            create_validate_func_outliers_neg_cum,
        )
        col = next(
            c for c in cumulative_columns if interval_column(c) is not None
        )
        validator = create_validate_func_outliers_neg_cum(col)
        assert validator(pd.DataFrame({"ReadingDate": []})) is pd.NA
