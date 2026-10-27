"""The dataset validators' verdicts reach index.parquet from a real run.

``test_dataset_validators.py`` covers each validator as a function, both ways,
on frames it builds. ``test_index_helpers.py`` covers that the columns are
written and hold ``bool`` or NA. Neither covers the step between: that the value
written is the validator's verdict. A write of all-True, or all-NA, would
satisfy both and be caught here.

It also states what the fixture's shape implies. The dataset is shaped for
integration -- enough households per project to meet the averaging floor, over a
span of days -- so the checks written against a production year of data fail on
it, and correctly. A change of span moves those verdicts, and these say which
ones.
"""

import pandas as pd
import pytest

from etdmap.testing import DEFAULT_DAYS

#: True whatever the span. The grid is unbroken and every model column is
#: present, which is what these two ask.
SHAPE_INDEPENDENT_TRUE = (
    "validate_columns_exist",
    "validate_no_readingdate_gap",
)

#: Written against a production year. The fixture spans DEFAULT_DAYS, so these
#: refuse it -- the dataset is not a year of monitoring data and does not claim
#: to be.
REFUSED_AT_THIS_SPAN = (
    "validate_approximately_one_year_of_records",
    "validate_monitoring_data_counts",
)


@pytest.fixture(scope="module")
def index_verdicts(mapped_fixtures):
    """The index's validate columns, from the generated dataset."""
    frame = pd.read_parquet(
        mapped_fixtures / "index.parquet", dtype_backend="numpy_nullable"
    )
    return frame.filter(like="validate")


class TestTheVerdictsAreWrittenNotDefaulted:
    def test_the_index_carries_verdicts(self, index_verdicts):
        """Guards the rest: a frame with no validate column would satisfy every
        assertion below by having nothing to check."""
        assert not index_verdicts.empty
        assert len(index_verdicts.columns) > 1

    def test_not_every_verdict_is_the_same(self, index_verdicts):
        """A pass that wrote one value everywhere would read as working. The
        fixture meets some checks and fails others, so both appear."""
        settled = [
            bool(column.dropna().all())
            for _, column in index_verdicts.items()
            if column.notna().any()
        ]

        assert any(settled), "no verdict is True"
        assert not all(settled), "no verdict is False"


class TestWhatTheShapeImplies:
    @pytest.mark.parametrize("check", SHAPE_INDEPENDENT_TRUE)
    def test_the_shape_independent_checks_pass(self, index_verdicts, check):
        assert check in index_verdicts.columns
        assert index_verdicts[check].dropna().all(), (
            f"{check} does not depend on the span, and the fixture is a "
            f"complete grid with every model column, so it must pass"
        )

    @pytest.mark.parametrize("check", REFUSED_AT_THIS_SPAN)
    def test_the_annual_checks_refuse_this_span(self, index_verdicts, check):
        verdicts = index_verdicts[check].dropna()

        assert len(verdicts), f"{check} reached no verdict at all"
        assert not verdicts.any(), (
            f"{check} is written against a year of data and the fixture spans "
            f"{DEFAULT_DAYS} days, so it must refuse it. Passing means either "
            f"the span grew or the check stopped reading it"
        )

    def test_a_blanked_reading_is_not_a_timestamp_gap(self, index_verdicts):
        """The one declared gap blanks values, not rows, so the reading-date
        grid stays unbroken. A gap that removed rows would be a different
        fixture and would flip this."""
        assert index_verdicts["validate_no_readingdate_gap"].dropna().all()
