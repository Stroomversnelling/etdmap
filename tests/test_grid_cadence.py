"""The grid is per household batch, not a constant.

Each batch declares the interval its data sits on as ``Gegevensfrequentie``.
Everything that pads, fills or judges a value against "one slot" has to use that
batch's grid: on any other grid a five-minute assumption pads the wrong slots and
judges a plausible reading impossible.
"""

import logging

import pandas as pd
import pytest

import etdmap
from etdmap.index_helpers import (
    HouseBatchOverlapError,
    frequency_to_minutes,
    grid_minutes_for_household,
    grid_minutes_for_household_batch,
)
from etdmap.mapping_helpers import (
    cadence_label,
    ensure_intervals,
    fill_down_infrequent_devices,
)


@pytest.fixture(autouse=True)
def _quiet_logs():
    logging.disable(logging.ERROR)
    yield
    logging.disable(logging.NOTSET)


class TestFrequencyToMinutes:
    """A frequency is a duration, so it is parsed as one. Comparing the text
    instead would make '1h' and '60min' different grids."""

    @pytest.mark.parametrize(
        "declared, minutes",
        [("5min", 5.0), ("15min", 15.0), ("1h", 60.0), ("24h", 1440.0), ("30s", 0.5)],
    )
    def test_it_reads_any_unit(self, declared, minutes):
        assert frequency_to_minutes(declared) == minutes

    @pytest.mark.parametrize("declared", [None, pd.NA, "", "every so often", "0min"])
    def test_an_unusable_value_is_not_a_guess(self, declared):
        assert pd.isna(frequency_to_minutes(declared))

    def test_it_round_trips_with_the_label(self):
        for minutes in (0.5, 5, 15, 60, 1440):
            assert frequency_to_minutes(cadence_label(minutes)) == float(minutes)


class TestGridMinutesForHousehold:
    @pytest.fixture
    def batch_table(self, tmp_path):
        path = tmp_path / "huisbatch.csv"
        pd.DataFrame({
            "HuisIdBSV": pd.array([1, 2, 3], dtype="Int64"),
            "HuisBatchIdBSV": pd.array([1, 2, 3], dtype="Int64"),
            "Gegevensfrequentie": pd.array(["5min", "15min", None], dtype="string"),
        }).to_csv(path, index=False)

        previous = etdmap.options.household_batch_csv_path
        etdmap.options.household_batch_csv_path = str(path)
        yield path
        etdmap.options.household_batch_csv_path = previous

    def test_it_reads_the_declared_grid(self, batch_table):
        assert grid_minutes_for_household(1) == 5.0
        assert grid_minutes_for_household(2) == 15.0

    def test_a_household_with_no_declared_grid_raises(self, batch_table):
        with pytest.raises(ValueError, match="Gegevensfrequentie"):
            grid_minutes_for_household(3)

    def test_a_household_not_in_the_table_raises(self, batch_table):
        """Mapping a household whose batch row is not yet synced would pad it to
        a grid nobody declared, and the index would then say otherwise."""
        with pytest.raises(ValueError, match="not in the household batch table"):
            grid_minutes_for_household(99)

    def test_no_batch_table_at_all_raises(self):
        previous = etdmap.options.household_batch_csv_path
        etdmap.options.household_batch_csv_path = None
        try:
            with pytest.raises(ValueError, match="household_batch_csv_path"):
                grid_minutes_for_household(1)
        finally:
            etdmap.options.household_batch_csv_path = previous

    def test_the_message_names_the_household(self, batch_table):
        with pytest.raises(ValueError, match="Household 99"):
            grid_minutes_for_household(99)

    def test_it_points_at_the_data_model(self, batch_table):
        """The grid is declared metadata, so the fix is a sync, not a code
        change."""
        with pytest.raises(ValueError, match="data model"):
            grid_minutes_for_household(99)


class TestTheGridIsKeyedOnTheBatch:
    """``Gegevensfrequentie`` is declared per ``HuisBatchIdBSV``, so the lookup
    is keyed on the batch. One ``HuisIdBSV`` may have several datasets, each
    declaring its own grid, and filtering on the household cannot tell them
    apart.
    """

    @pytest.fixture
    def two_batches(self, tmp_path):
        """Household 7 has two datasets at different grids; household 8 has one."""
        path = tmp_path / "huisbatch_multi.csv"
        pd.DataFrame({
            "HuisIdBSV": pd.array([7, 7, 8], dtype="Int64"),
            "HuisBatchIdBSV": pd.array([7, 70, 8], dtype="Int64"),
            "Gegevensfrequentie": pd.array(
                ["5min", "15min", "1h"], dtype="string"
            ),
        }).to_csv(path, index=False)

        previous = etdmap.options.household_batch_csv_path
        etdmap.options.household_batch_csv_path = str(path)
        yield path
        etdmap.options.household_batch_csv_path = previous

    def test_each_batch_resolves_its_own_grid(self, two_batches):
        assert grid_minutes_for_household_batch(7) == 5.0
        assert grid_minutes_for_household_batch(70) == 15.0
        assert grid_minutes_for_household_batch(8) == 60.0

    def test_a_household_with_two_batches_refuses_to_answer(self, two_batches):
        """The grid is a property of the dataset. Returning the first listed
        batch's frequency pads the other dataset to a grid nobody declared for
        it."""
        with pytest.raises(HouseBatchOverlapError) as excinfo:
            grid_minutes_for_household(7)

        message = str(excinfo.value)
        assert "7" in message
        assert "70" in message

    def test_a_household_with_one_batch_still_resolves(self, two_batches):
        assert grid_minutes_for_household(8) == 60.0

    def test_a_batch_not_in_the_table_raises(self, two_batches):
        with pytest.raises(ValueError, match="999"):
            grid_minutes_for_household_batch(999)

    def test_the_message_names_the_batch_and_the_data_model(self, two_batches):
        with pytest.raises(ValueError) as excinfo:
            grid_minutes_for_household_batch(999)

        message = str(excinfo.value)
        assert "HuisBatchIdBSV 999" in message
        assert "data model" in message

    def test_a_batch_with_no_declared_grid_raises(self, tmp_path):
        path = tmp_path / "huisbatch_blank.csv"
        pd.DataFrame({
            "HuisIdBSV": pd.array([5], dtype="Int64"),
            "HuisBatchIdBSV": pd.array([50], dtype="Int64"),
            "Gegevensfrequentie": pd.array([None], dtype="string"),
        }).to_csv(path, index=False)

        previous = etdmap.options.household_batch_csv_path
        etdmap.options.household_batch_csv_path = str(path)
        try:
            with pytest.raises(ValueError, match="Gegevensfrequentie"):
                grid_minutes_for_household_batch(50)
        finally:
            etdmap.options.household_batch_csv_path = previous

    def test_a_table_with_no_batch_column_raises(self, tmp_path):
        """An older synced table has no HuisBatchIdBSV. Resolving the grid off
        HuisIdBSV instead would answer for a key the data model does not use."""
        path = tmp_path / "huisbatch_legacy.csv"
        pd.DataFrame({
            "HuisIdBSV": pd.array([1], dtype="Int64"),
            "Gegevensfrequentie": pd.array(["5min"], dtype="string"),
        }).to_csv(path, index=False)

        previous = etdmap.options.household_batch_csv_path
        etdmap.options.household_batch_csv_path = str(path)
        try:
            with pytest.raises(ValueError, match="HuisBatchIdBSV"):
                grid_minutes_for_household_batch(1)
            with pytest.raises(ValueError, match="HuisBatchIdBSV"):
                grid_minutes_for_household(1)
        finally:
            etdmap.options.household_batch_csv_path = previous

    def test_a_duplicated_batch_row_raises(self, tmp_path):
        """A batch id names one dataset, so two rows for it is a metadata
        fault rather than a choice to make."""
        path = tmp_path / "huisbatch_dup.csv"
        pd.DataFrame({
            "HuisIdBSV": pd.array([3, 3], dtype="Int64"),
            "HuisBatchIdBSV": pd.array([30, 30], dtype="Int64"),
            "Gegevensfrequentie": pd.array(["5min", "15min"], dtype="string"),
        }).to_csv(path, index=False)

        previous = etdmap.options.household_batch_csv_path
        etdmap.options.household_batch_csv_path = str(path)
        try:
            with pytest.raises(ValueError, match="2 rows"):
                grid_minutes_for_household_batch(30)
        finally:
            etdmap.options.household_batch_csv_path = previous


class TestEnsureIntervalsUsesTheGridItIsGiven:
    """The count of expected records is what decides whether padding happens at
    all, so it has to be counted in the grid being padded to."""

    @staticmethod
    def _readings(freq, periods, drop):
        dates = pd.date_range("2026-01-01", periods=periods, freq=freq)
        df = pd.DataFrame({
            "ReadingDate": dates,
            "WatergebruikWarmTapwater": pd.array(
                [float(i) for i in range(periods)], dtype="Float64"
            ),
        })
        return df.drop(index=drop).reset_index(drop=True)

    def test_a_gap_on_a_15_minute_grid_is_padded(self):
        out = ensure_intervals(self._readings("15min", 8, drop=[3, 4]), freq="15min")

        assert len(out) == 8
        gaps = out["ReadingDate"].diff().dropna().unique()
        assert list(gaps) == [pd.Timedelta(minutes=15)]

    def test_the_padded_rows_are_the_missing_ones(self):
        out = ensure_intervals(self._readings("15min", 8, drop=[3, 4]), freq="15min")
        missing = out.loc[out["WatergebruikWarmTapwater"].isna(), "ReadingDate"]

        assert list(missing) == [
            pd.Timestamp("2026-01-01 00:45"),
            pd.Timestamp("2026-01-01 01:00"),
        ]

    def test_an_hourly_grid_works_too(self):
        out = ensure_intervals(self._readings("1h", 6, drop=[2]), freq="1h")

        assert len(out) == 6
        assert list(out["ReadingDate"].diff().dropna().unique()) == [
            pd.Timedelta(hours=1)
        ]

    def test_a_complete_series_is_left_alone(self):
        df = self._readings("15min", 8, drop=[])
        out = ensure_intervals(df.copy(), freq="15min")

        pd.testing.assert_frame_equal(out, df)


class TestFillingStepsInTheGridItIsGiven:
    """A filled value sits on the slots the device reports on, which are counted
    in grid rows. The count is taken in whole seconds, so a sub-minute grid is
    neither truncated to zero nor decided by a float comparison.
    """

    CUM = "ElektriciteitsgebruikBoilervat"

    def _filled(self, n, reports_every, grid_minutes, column_minutes):
        values = [
            10.0 + i if i % reports_every == 0 else pd.NA for i in range(n)
        ]
        df = pd.DataFrame({
            "ReadingDate": pd.date_range(
                "2026-01-01", periods=n, freq=f"{grid_minutes}min"
            ),
            self.CUM: pd.array(values, dtype="Float64"),
        })
        cadence = pd.DataFrame({
            "HuisIdBSV": pd.array([1], dtype="Int64"),
            "column": pd.array([self.CUM], dtype="string"),
            "cadence_minutes": pd.array([column_minutes], dtype="Int64"),
        })
        out = fill_down_infrequent_devices(
            df, [self.CUM], cadence=cadence, grid_minutes=grid_minutes
        )
        return int(out[self.CUM].notna().sum())

    @pytest.mark.parametrize(
        "grid_minutes, column_minutes, reports_every, expected",
        [
            (5, 15, 3, 4),
            (15, 60, 4, 3),
            (60, 1440, 24, 1),
        ],
        ids=["15min column on a 5min grid", "hourly on 15min", "daily on hourly"],
    )
    def test_it_fills_one_slot_per_reporting_interval(
        self, grid_minutes, column_minutes, reports_every, expected
    ):
        assert self._filled(12, reports_every, grid_minutes, column_minutes) == expected

    def test_a_cadence_that_does_not_land_on_the_grid_fills_every_row(self):
        """Rounding to the nearest whole step would fill at a spacing that is
        not the cadence, and not say so."""
        assert self._filled(12, 3, 5, 7) == 12
