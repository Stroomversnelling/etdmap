"""Nothing judges a reading against a five-minute interval it did not declare.

Two validators used to compare consecutive readings against a fixed 300
seconds. A household recorded on a fifteen-minute grid then failed on every
row: the finding said "gap" where the data was complete, and the dataset-level
flag said the series was broken when it was intact.

The spacing a household is recorded on is the grid its batch declares, which
mapping pads to before these run. So the interval to compare against is the one
in the data, and a gap is a spacing that departs from it.
"""

import pandas as pd
import pytest

from etdmap.dataset_validators import validate_no_readingdate_gap
from etdmap.record_validators import validate_observed_spacing


def _frame(freq, periods=12, drop=None):
    dates = pd.date_range("2026-01-01", periods=periods, freq=freq).tolist()
    if drop is not None:
        dates.pop(drop)
    return pd.DataFrame({
        "ReadingDate": dates,
        "ElektriciteitNetgebruik": pd.array(
            [float(i) for i in range(len(dates))], dtype="Float64"
        ),
    })


class TestTheRecordValidatorFollowsTheGrid:
    @pytest.mark.parametrize("freq", ["5min", "15min", "1h", "30s"])
    def test_a_complete_series_passes_on_any_grid(self, freq):
        result = validate_observed_spacing(_frame(freq))
        # The first row has no predecessor to be spaced from.
        assert result.iloc[1:].all(), (
            f"a complete {freq} series reports rows as wrongly spaced"
        )

    @pytest.mark.parametrize("freq", ["5min", "15min", "1h"])
    def test_a_missing_slot_is_found_on_any_grid(self, freq):
        result = validate_observed_spacing(_frame(freq, drop=5))
        assert not result.all(), f"the missing slot in a {freq} series was not found"

    def test_only_the_row_after_the_gap_is_marked(self):
        result = validate_observed_spacing(_frame("15min", drop=5))
        # Dropping position 5 leaves the row that was position 6 twice as far
        # from its predecessor; every other spacing is unchanged.
        assert int((~result.fillna(True)).sum()) == 1


class TestTheDatasetValidatorFollowsTheGrid:
    @pytest.mark.parametrize("freq", ["5min", "15min", "1h", "30s"])
    def test_a_complete_series_has_no_gap_on_any_grid(self, freq):
        assert validate_no_readingdate_gap(_frame(freq)) is True

    @pytest.mark.parametrize("freq", ["5min", "15min", "1h"])
    def test_a_missing_slot_is_a_gap_on_any_grid(self, freq):
        assert validate_no_readingdate_gap(_frame(freq, drop=5)) is False

    def test_a_single_reading_has_no_gap(self):
        """One reading has no spacing, so there is nothing it departs from."""
        assert validate_no_readingdate_gap(_frame("5min", periods=1)) is True
