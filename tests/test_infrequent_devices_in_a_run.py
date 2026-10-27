"""An infrequently reporting device, through a real mapping run.

``test_mapping_helpers.py::TestFillDownInfrequentDevices``,
``test_grid_cadence.py::TestFillingStepsInTheGridItIsGiven`` and
``test_delivery_marking.py::TestAFillCannotBeMistakenForADelivery`` cover
``fill_down_infrequent_devices`` as a function, on frames they build. This
covers the shape the generated dataset delivers to it: a column carrying a
value on every row leaves the fill nothing to do, so a run exercises it only if
the devices arrive sparse.

The raw and the mapped dataset come from separate folders generated with the
same defaults, so a household's number is the same data in both.
"""

import pandas as pd
import pytest

from etdmap.flags import MappingFlag, flag_column
from etdmap.mapping_helpers import CADENCE_FILENAME
from etdmap.storage import mapped_household_files
from etdmap.testing import INFREQUENT_DEVICE_COLUMNS

#: Read from both datasets. Household 1 also carries the declared gap, which is
#: in another column.
HOUSEHOLD = 1

#: Reports on every row, so it stands against the devices: a column that lost
#: its readings would otherwise look like a device that reports rarely.
DENSE_COLUMN = "WatergebruikWarmTapwater"


@pytest.fixture(scope="module")
def raw_household(raw_data_fixture):
    return pd.read_parquet(
        f"{raw_data_fixture}/household_{HOUSEHOLD}_table.parquet",
        dtype_backend="numpy_nullable",
    )


@pytest.fixture(scope="module")
def mapped_household(mapped_fixtures):
    """The household's mapped data, read through its household batch.

    A mapped dataset is keyed on HuisBatchIdBSV; the generator writes one
    household batch per household.
    """
    files = mapped_household_files(HOUSEHOLD, mapped_folder_path=mapped_fixtures)
    assert len(files) == 1, (
        f"household {HOUSEHOLD} has {len(files)} household batches, so there "
        f"is no single one to read here"
    )
    path, _household_batch_id = files[0]
    return pd.read_parquet(path, dtype_backend="numpy_nullable")


@pytest.fixture(scope="module")
def cadence(mapped_fixtures):
    """The dataset's rows of the sidecar the mapping measured and wrote.

    Keyed on the household batch, which the generator numbers with the
    household id.
    """
    frame = pd.read_parquet(
        mapped_fixtures / CADENCE_FILENAME, dtype_backend="numpy_nullable"
    )
    rows = frame[frame["HuisBatchIdBSV"] == HOUSEHOLD]
    return rows.set_index("column")


class TestTheDeviceArrivesSparse:
    @pytest.mark.parametrize("column", INFREQUENT_DEVICE_COLUMNS)
    def test_it_reports_on_a_small_share_of_the_rows(self, raw_household, column):
        readings = int(raw_household[column].notna().sum())

        assert readings, f"'{column}' delivered nothing at all"
        assert readings < len(raw_household) / 10, (
            f"'{column}' carries {readings} of {len(raw_household)} rows. A "
            f"device that reports on most rows is a dense column, and the fill "
            f"then has nothing to hold"
        )

    @pytest.mark.parametrize("column", INFREQUENT_DEVICE_COLUMNS)
    def test_its_readings_are_unevenly_spaced(self, raw_household, column):
        """Which is why no cadence can be measured off it: these devices report
        when they run, not on a schedule."""
        positions = pd.Series(
            raw_household.index[raw_household[column].notna()], dtype="Int64"
        )
        gaps = positions.diff().dropna()

        assert len(gaps) > 1, f"'{column}' has too few readings to space"
        assert gaps.nunique() > 1, (
            f"every gap between '{column}' readings is {gaps.iloc[0]} rows, "
            f"which is a cadence rather than a device reporting when it runs"
        )

    def test_a_dense_column_reports_on_every_row(self, raw_household):
        assert int(raw_household[DENSE_COLUMN].notna().sum()) == len(raw_household)


class TestTheMappingHoldsTheLastReading:
    @pytest.mark.parametrize("column", INFREQUENT_DEVICE_COLUMNS)
    def test_no_measured_cadence_is_trusted(self, cadence, column):
        assert cadence.loc[column, "cadence_source"] == "dataset", (
            f"'{column}' reports irregularly, so the gap measured off it is a "
            f"weak claim and the column must fall back to the grid. A trusted "
            f"cadence here means the fixture made it regular"
        )

    def test_a_dense_column_does_have_a_measured_cadence(self, cadence):
        """Against which the devices' fallback means something: a sidecar that
        trusted nothing would satisfy the test above for the wrong reason."""
        assert cadence.loc[DENSE_COLUMN, "cadence_source"] == "detected"

    @pytest.mark.parametrize("column", INFREQUENT_DEVICE_COLUMNS)
    def test_the_column_is_complete_afterwards(self, mapped_household, column):
        assert int(mapped_household[column].notna().sum()) == len(mapped_household), (
            f"'{column}' is a cumulative variable: while the device is idle it "
            f"holds its last reading, so every row carries a value once the "
            f"fill has run"
        )

    @pytest.mark.parametrize("column", INFREQUENT_DEVICE_COLUMNS)
    def test_every_row_the_device_did_not_report_on_is_marked(
        self, raw_household, mapped_household, column
    ):
        delivered = int(raw_household[column].notna().sum())
        filled = int(
            (
                (mapped_household[flag_column(column)] & MappingFlag.FILLED_DOWN.value)
                != 0
            ).sum()
        )

        assert filled == len(mapped_household) - delivered, (
            f"'{column}' arrived with {delivered} readings over "
            f"{len(mapped_household)} rows, so the rest hold a value the fill "
            f"wrote and each of those cells records it"
        )

    @pytest.mark.parametrize("column", INFREQUENT_DEVICE_COLUMNS)
    def test_no_fill_is_removed_again(self, mapped_household, column):
        """A fill is placed on a cadence slot and removed from the rows between
        only where a cadence was trusted. These columns have none, so every
        filled value stays."""
        nulled = (
            mapped_household[flag_column(column)] & MappingFlag.NULLED.value
        ) != 0

        assert not nulled.any()
