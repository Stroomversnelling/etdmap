"""The device-absence declarations are explicit metadata claims, and the
data can falsify them. Two pieces enforce that contract in etdmap:

- ``parse_device_declaration`` turns a household's declaration cell into a
  set and raises on names the data model does not know (inconsistent
  metadata is always a hard failure, same contract as the sync's check).
- ``assert_no_delivered_in_declared_absent`` raises when a declared-absent
  variable -- or any other column of the same device -- holds delivered
  readings. Migration was contradiction-free by construction, so the guard
  exists for future drift: a device installed later that starts reporting
  raises until the metadata is corrected.
"""

import pandas as pd
import pytest

import etdmap
from etdmap import ids
from etdmap.data_model import parse_device_declaration
from etdmap.flags import MappingFlag, set_flags
from etdmap.mapping_helpers import assert_no_delivered_in_declared_absent
from etdmap.testing import (
    dataset_frame,
    declare_synthetic_model,
    household_of_batch,
    index_frame,
)

RADIATOR = "ElektriciteitsgebruikRadiator"
BOILERVAT = "ElektriciteitsgebruikBoilervat"


@pytest.fixture
def model():
    """The model the guard reads, declared by the test.

    The guard's behaviour is about the model's *relations* -- which columns
    belong to one device -- not about any particular variable name. Naming
    real variables would pin these tests to whatever the Grist-derived
    metadata happens to say today, which is exactly the drift the synthetic
    declaration avoids.
    """
    with declare_synthetic_model() as declared:
        yield declared


def _frame(columns, n_rows=None):
    """A household frame holding exactly *columns*, as a supplier delivered
    it: tz-naive, no id columns.

    No counterpart is completed. The guard reads the whole family of a
    declared device, so a completed counterpart is another delivered column
    of that family rather than one the test put on the frame.
    """
    if n_rows is None:
        n_rows = max(len(values) for values in columns.values())
    return dataset_frame(
        n_rows=n_rows,
        start="2026-01-01",
        tz=None,
        project=None,
        pairs=False,
        **columns,
    )


class TestParseDeviceDeclaration:
    def test_comma_separated_names_parse_to_a_set(self):
        assert parse_device_declaration(f"{RADIATOR}, {BOILERVAT}") == {
            RADIATOR,
            BOILERVAT,
        }

    def test_na_empty_and_whitespace_are_empty_declarations(self):
        assert parse_device_declaration(pd.NA) == set()
        assert parse_device_declaration(None) == set()
        assert parse_device_declaration("") == set()
        assert parse_device_declaration("   ") == set()

    def test_stray_commas_and_spacing_are_tolerated(self):
        assert parse_device_declaration(f" {RADIATOR} ,, ") == {RADIATOR}

    def test_unknown_name_is_a_hard_failure(self):
        with pytest.raises(ValueError, match="ElektriciteitsgebruikRadiatorTypo"):
            parse_device_declaration("ElektriciteitsgebruikRadiatorTypo")


class TestAssertNoDeliveredInDeclaredAbsent:
    def test_all_na_column_passes(self):
        df = _frame({RADIATOR: [pd.NA, pd.NA, pd.NA]})
        assert_no_delivered_in_declared_absent(df, {RADIATOR}, context="hh 1")

    def test_absent_column_passes(self):
        df = _frame({}, n_rows=2)
        assert_no_delivered_in_declared_absent(df, {RADIATOR})

    def test_delivered_reading_raises_with_count_and_dates(self):
        df = _frame({RADIATOR: [pd.NA, 1.5, 2.0, pd.NA]})
        with pytest.raises(ValueError) as err:
            assert_no_delivered_in_declared_absent(df, {RADIATOR}, context="hh 7")
        message = str(err.value)
        assert "hh 7" in message
        assert RADIATOR in message
        assert "2 delivered reading(s)" in message
        assert "2026-01-01 00:05" in message  # first
        assert "2026-01-01 00:10" in message  # last
        assert "Correct the metadata" in message

    def test_invented_values_do_not_falsify(self):
        """Zeros the pipeline itself wrote (INVENTED bits) are not the meter
        reporting -- the legacy zero-prefill must not read as a device."""
        df = _frame({RADIATOR: [0.0, 0.0, 0.0]})
        set_flags(
            df, RADIATOR, pd.Series(True, index=df.index), MappingFlag.FILLED_ZERO
        )
        assert_no_delivered_in_declared_absent(df, {RADIATOR})

    def test_a_mix_raises_on_the_delivered_cells_only(self):
        df = _frame({RADIATOR: [0.0, 7.5, pd.NA]})
        set_flags(
            df,
            RADIATOR,
            pd.Series([True, False, False], index=df.index),
            MappingFlag.FILLED_ZERO,
        )
        with pytest.raises(ValueError, match="1 delivered reading"):
            assert_no_delivered_in_declared_absent(df, {RADIATOR})

    def test_the_interval_pair_also_falsifies(self):
        """A declaration names the cumulative form; the device reporting on
        the interval form is the same device reporting."""
        from etdmap.data_model import interval_column

        pair = interval_column(RADIATOR)
        assert pair is not None, "fixture relies on the model's declared pair"
        with pytest.raises(ValueError, match=pair):
            assert_no_delivered_in_declared_absent(
                _frame({pair: [pd.NA, 3.0]}), {RADIATOR}
            )

    def test_every_violation_is_named_in_one_raise(self):
        df = _frame({RADIATOR: [1.0, 2.0], BOILERVAT: [5.0, pd.NA]})
        with pytest.raises(ValueError) as err:
            assert_no_delivered_in_declared_absent(df, {RADIATOR, BOILERVAT})
        message = str(err.value)
        assert RADIATOR in message and BOILERVAT in message

    def test_empty_declaration_is_a_no_op(self):
        df = _frame({RADIATOR: [1.0, 2.0]})
        assert_no_delivered_in_declared_absent(df, set())


class TestEveryColumnOfTheDeviceFalsifiesTheDeclaration:
    """A declaration names a physical device, and the device reports through
    any of its columns: the variable, its declared complement, or -- for a
    tariff quantity -- either register and their complements.

    Mapping sums the registers into the combined variable, and the guard runs
    before that sum, so on a tariff device the registers are the only place
    the readings are. A register is a family member exactly as a complement
    is.
    """

    @pytest.mark.parametrize(
        "register", ["high", "low", "high_complement", "low_complement"]
    )
    def test_a_delivered_register_raises(self, model, register):
        column = getattr(model, register)
        df = _frame({column: [pd.NA, 4.0]})
        with pytest.raises(ValueError, match=column):
            assert_no_delivered_in_declared_absent(df, {model.device})

    @pytest.mark.parametrize("member", ["device", "device_complement"])
    def test_the_variable_and_its_complement_still_raise(self, model, member):
        column = getattr(model, member)
        df = _frame({column: [pd.NA, 9.0]})
        with pytest.raises(ValueError, match=column):
            assert_no_delivered_in_declared_absent(df, {model.device})

    def test_a_family_with_nothing_delivered_passes(self, model):
        family = (
            model.device,
            model.high,
            model.low,
            model.high_complement,
            model.low_complement,
        )
        df = _frame({column: [pd.NA, pd.NA] for column in family})
        assert_no_delivered_in_declared_absent(df, {model.device})

    def test_an_invented_register_value_does_not_falsify(self, model):
        """The existing rule -- a cell the pipeline wrote is not a delivered
        reading -- has to survive the widening, or a filled register starts
        reading as a device."""
        df = _frame({model.high: [0.0, 0.0]})
        set_flags(
            df,
            model.high,
            pd.Series(True, index=df.index),
            MappingFlag.FILLED_DOWN,
        )
        assert_no_delivered_in_declared_absent(df, {model.device})

    def test_every_delivering_register_is_named_in_one_raise(self, model):
        df = _frame({model.high: [1.0, 2.0], model.low: [3.0, pd.NA]})
        with pytest.raises(ValueError) as err:
            assert_no_delivered_in_declared_absent(df, {model.device})
        message = str(err.value)
        assert model.high in message and model.low in message

    def test_the_family_is_the_declared_device_not_every_column(self, model):
        """The widening comes from the declared relations. A delivered column
        belonging to another device is not a falsification."""
        df = _frame({model.plain: [1.0, 2.0]})
        assert_no_delivered_in_declared_absent(df, {model.device})

    def test_a_device_with_no_registers_is_still_fully_checked(self, model):
        """The resolver must not assume every device has registers."""
        df = _frame({model.plain_complement: [pd.NA, 6.0]})
        with pytest.raises(ValueError, match=model.plain_complement):
            assert_no_delivered_in_declared_absent(df, {model.plain})

    def test_the_family_is_the_same_whichever_member_is_declared(self, model):
        """Satisfaction is a property of the device, so the closure over the
        declared relations has to be symmetric: declaring a register reaches
        the whole device, exactly as declaring the combined variable does."""
        df = _frame({model.low: [pd.NA, 4.0]})
        with pytest.raises(ValueError, match=model.low):
            assert_no_delivered_in_declared_absent(df, {model.high})


#: The dataset the declaration belongs to, a second dataset, and the household
#: the first describes.
DATASET = 1001
SECOND_DATASET = 2001
HOUSEHOLD = household_of_batch(DATASET)

#: The supplier and project the raise has to name beside the dataset, so the
#: declaration can be found and corrected in Grist, and the delivery batch it
#: must not be confused with. Each differs in value from the household and
#: from the dataset, so a message naming the wrong one cannot read as right.
SUPPLIER = "Watch-E"
PROJECT = 7
DELIVERY_BATCH = 2


class TestTheRaiseNamesWhoseDeclarationItIs:
    """The fix is a Grist edit, found by supplier and project, and the
    declaration belongs to one household's dataset -- HuisBatchIdBSV, not the
    delivery batch. A message carrying only the mapper's own label cannot be
    acted on.
    """

    @staticmethod
    def _write_index(folder, household_batch_ids, household_of=None):
        """The index the raise reads the declaration's owner from."""
        index_frame(
            household_batch_ids,
            household_of=household_of,
            project=PROJECT,
            batch=DELIVERY_BATCH,
            supplier=SUPPLIER,
            Meenemen=True,
        ).to_parquet(folder / "index.parquet", engine="pyarrow")

    def test_supplier_project_and_household_batch_are_named(
        self, model, tmp_path, monkeypatch
    ):
        self._write_index(tmp_path, [DATASET])
        monkeypatch.setattr(etdmap.options, "mapped_folder_path", tmp_path)
        df = _frame({model.high: [pd.NA, 4.0]})

        with pytest.raises(ValueError) as err:
            assert_no_delivered_in_declared_absent(
                df,
                {model.device},
                context=f"hh {HOUSEHOLD}",
                household_id=HOUSEHOLD,
            )

        message = str(err.value)
        assert SUPPLIER in message
        assert f"{ids.PROJECT} {PROJECT}" in message
        assert f"{ids.HOUSEHOLD_BATCH} {DATASET}" in message
        # The violation detail is unchanged by the decoration.
        assert model.high in message
        assert "1 delivered reading(s)" in message

    def test_a_household_with_two_datasets_names_both_batches(
        self, model, tmp_path, monkeypatch
    ):
        """One household can have several datasets, and a declaration belongs
        to one of them. Naming every candidate beats naming one at random."""
        self._write_index(
            tmp_path,
            [DATASET, SECOND_DATASET],
            household_of={SECOND_DATASET: HOUSEHOLD},
        )
        monkeypatch.setattr(etdmap.options, "mapped_folder_path", tmp_path)
        df = _frame({model.high: [pd.NA, 4.0]})

        with pytest.raises(ValueError) as err:
            assert_no_delivered_in_declared_absent(
                df, {model.device}, household_id=HOUSEHOLD
            )

        message = str(err.value)
        assert str(DATASET) in message and str(SECOND_DATASET) in message

    def test_an_absent_index_still_raises_on_the_violation(
        self, model, tmp_path, monkeypatch
    ):
        """The identifiers decorate the message. Failing to find them must
        never swallow the contradiction the guard exists to report."""
        monkeypatch.setattr(etdmap.options, "mapped_folder_path", tmp_path)
        df = _frame({model.high: [pd.NA, 4.0]})

        with pytest.raises(ValueError) as err:
            assert_no_delivered_in_declared_absent(
                df,
                {model.device},
                context=f"hh {HOUSEHOLD}",
                household_id=HOUSEHOLD,
            )

        message = str(err.value)
        assert model.high in message
        assert f"hh {HOUSEHOLD}" in message

    def test_a_household_absent_from_the_index_still_raises(
        self, model, tmp_path, monkeypatch
    ):
        self._write_index(tmp_path, [DATASET])
        monkeypatch.setattr(etdmap.options, "mapped_folder_path", tmp_path)
        df = _frame({model.high: [pd.NA, 4.0]})

        with pytest.raises(ValueError, match=model.high):
            assert_no_delivered_in_declared_absent(
                df,
                {model.device},
                # The household the second dataset belongs to, which the index
                # written above does not cover.
                household_id=household_of_batch(SECOND_DATASET),
            )

    def test_without_a_household_id_the_message_is_unchanged(self, model):
        """Every existing caller omits it, so the decoration is additive."""
        df = _frame({model.high: [pd.NA, 4.0]})

        with pytest.raises(ValueError) as err:
            assert_no_delivered_in_declared_absent(df, {model.device})

        message = str(err.value)
        assert ids.PROJECT not in message
        assert model.high in message
