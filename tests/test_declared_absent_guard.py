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
from etdmap.data_model import parse_device_declaration
from etdmap.flags import MappingFlag, set_flags
from etdmap.mapping_helpers import assert_no_delivered_in_declared_absent
from etdmap.testing import declare_synthetic_model

RADIATOR = "ElektriciteitsgebruikRadiator"
BOILERVAT = "ElektriciteitsgebruikBoilervat"


@pytest.fixture
def model(monkeypatch):
    """The model the guard reads, declared by the test.

    The guard's behaviour is about the model's *relations* -- which columns
    belong to one device -- not about any particular variable name. Naming
    real variables would pin these tests to whatever the Grist-derived
    metadata happens to say today, which is exactly the drift the synthetic
    declaration avoids.
    """
    return declare_synthetic_model(monkeypatch)


def _frame(values, column=RADIATOR, n=None):
    n = n if n is not None else len(values)
    return pd.DataFrame(
        {
            "ReadingDate": pd.date_range("2026-01-01", periods=n, freq="5min"),
            column: pd.array(values, dtype="Float64"),
        }
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
        df = _frame([pd.NA, pd.NA, pd.NA])
        assert_no_delivered_in_declared_absent(df, {RADIATOR}, context="hh 1")

    def test_absent_column_passes(self):
        df = pd.DataFrame(
            {"ReadingDate": pd.date_range("2026-01-01", periods=2, freq="5min")}
        )
        assert_no_delivered_in_declared_absent(df, {RADIATOR})

    def test_delivered_reading_raises_with_count_and_dates(self):
        df = _frame([pd.NA, 1.5, 2.0, pd.NA])
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
        df = _frame([0.0, 0.0, 0.0])
        set_flags(
            df, RADIATOR, pd.Series(True, index=df.index), MappingFlag.FILLED_ZERO
        )
        assert_no_delivered_in_declared_absent(df, {RADIATOR})

    def test_a_mix_raises_on_the_delivered_cells_only(self):
        df = _frame([0.0, 7.5, pd.NA])
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
        df = _frame([pd.NA, pd.NA], column=pair)
        with pytest.raises(ValueError, match=pair):
            assert_no_delivered_in_declared_absent(
                df.assign(**{pair: pd.array([pd.NA, 3.0], dtype="Float64")}),
                {RADIATOR},
            )

    def test_every_violation_is_named_in_one_raise(self):
        df = _frame([1.0, 2.0])
        df[BOILERVAT] = pd.array([5.0, pd.NA], dtype="Float64")
        with pytest.raises(ValueError) as err:
            assert_no_delivered_in_declared_absent(df, {RADIATOR, BOILERVAT})
        message = str(err.value)
        assert RADIATOR in message and BOILERVAT in message

    def test_empty_declaration_is_a_no_op(self):
        df = _frame([1.0, 2.0])
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
        df = _frame([pd.NA, 4.0], column=column)
        with pytest.raises(ValueError, match=column):
            assert_no_delivered_in_declared_absent(df, {model.device})

    @pytest.mark.parametrize("member", ["device", "device_complement"])
    def test_the_variable_and_its_complement_still_raise(self, model, member):
        column = getattr(model, member)
        df = _frame([pd.NA, 9.0], column=column)
        with pytest.raises(ValueError, match=column):
            assert_no_delivered_in_declared_absent(df, {model.device})

    def test_a_family_with_nothing_delivered_passes(self, model):
        df = _frame([pd.NA, pd.NA], column=model.device)
        for column in (
            model.high,
            model.low,
            model.high_complement,
            model.low_complement,
        ):
            df[column] = pd.array([pd.NA, pd.NA], dtype="Float64")
        assert_no_delivered_in_declared_absent(df, {model.device})

    def test_an_invented_register_value_does_not_falsify(self, model):
        """The existing rule -- a cell the pipeline wrote is not a delivered
        reading -- has to survive the widening, or a filled register starts
        reading as a device."""
        df = _frame([0.0, 0.0], column=model.high)
        set_flags(
            df,
            model.high,
            pd.Series(True, index=df.index),
            MappingFlag.FILLED_DOWN,
        )
        assert_no_delivered_in_declared_absent(df, {model.device})

    def test_every_delivering_register_is_named_in_one_raise(self, model):
        df = _frame([1.0, 2.0], column=model.high)
        df[model.low] = pd.array([3.0, pd.NA], dtype="Float64")
        with pytest.raises(ValueError) as err:
            assert_no_delivered_in_declared_absent(df, {model.device})
        message = str(err.value)
        assert model.high in message and model.low in message

    def test_the_family_is_the_declared_device_not_every_column(self, model):
        """The widening comes from the declared relations. A delivered column
        belonging to another device is not a falsification."""
        df = _frame([1.0, 2.0], column=model.plain)
        assert_no_delivered_in_declared_absent(df, {model.device})

    def test_a_device_with_no_registers_is_still_fully_checked(self, model):
        """The resolver must not assume every device has registers."""
        df = _frame([pd.NA, 6.0], column=model.plain_complement)
        with pytest.raises(ValueError, match=model.plain_complement):
            assert_no_delivered_in_declared_absent(df, {model.plain})

    def test_the_family_is_the_same_whichever_member_is_declared(self, model):
        """Satisfaction is a property of the device, so the closure over the
        declared relations has to be symmetric: declaring a register reaches
        the whole device, exactly as declaring the combined variable does."""
        df = _frame([pd.NA, 4.0], column=model.low)
        with pytest.raises(ValueError, match=model.low):
            assert_no_delivered_in_declared_absent(df, {model.high})


class TestTheRaiseNamesWhoseDeclarationItIs:
    """The fix is a Grist edit, found by supplier and project, and the
    declaration belongs to one household's dataset -- HuisBatchIdBSV, not the
    delivery batch. A message carrying only the mapper's own label cannot be
    acted on.
    """

    @staticmethod
    def _write_index(folder, rows):
        index_df = pd.DataFrame(
            {
                "HuisIdLeverancier": pd.array(
                    [r["HuisIdLeverancier"] for r in rows], dtype="string"
                ),
                "HuisIdBSV": pd.array(
                    [r["HuisIdBSV"] for r in rows], dtype="Int64"
                ),
                "ProjectIdLeverancier": pd.array(
                    [r["ProjectIdLeverancier"] for r in rows], dtype="string"
                ),
                "ProjectIdBSV": pd.array(
                    [r["ProjectIdBSV"] for r in rows], dtype="Int64"
                ),
                "Dataleverancier": pd.array(
                    [r["Dataleverancier"] for r in rows], dtype="string"
                ),
                "Meenemen": pd.array(
                    [True for _ in rows], dtype="boolean"
                ),
                # read_index reads the index strictly, so every declared
                # metadata column has to be present even when it is empty.
                "Notities": pd.array(
                    [pd.NA for _ in rows], dtype="string"
                ),
                "HuisBatchIdBSV": pd.array(
                    [r["HuisBatchIdBSV"] for r in rows], dtype="Int64"
                ),
                "BatchIdBSV": pd.array(
                    [r["BatchIdBSV"] for r in rows], dtype="Int64"
                ),
                "Gegevensfrequentie": pd.array(
                    ["5min" for _ in rows], dtype="string"
                ),
            }
        )
        index_df.to_parquet(folder / "index.parquet", engine="pyarrow")

    @classmethod
    def _one_household(cls, folder):
        cls._write_index(
            folder,
            [
                {
                    "HuisIdLeverancier": "A1",
                    "HuisIdBSV": 250,
                    "ProjectIdLeverancier": "7",
                    "ProjectIdBSV": 7,
                    "Dataleverancier": "Watch-E",
                    "HuisBatchIdBSV": 251,
                    "BatchIdBSV": 2,
                }
            ],
        )

    def test_supplier_project_and_household_batch_are_named(
        self, model, tmp_path, monkeypatch
    ):
        self._one_household(tmp_path)
        monkeypatch.setattr(etdmap.options, "mapped_folder_path", tmp_path)
        df = _frame([pd.NA, 4.0], column=model.high)

        with pytest.raises(ValueError) as err:
            assert_no_delivered_in_declared_absent(
                df, {model.device}, context="hh 250", household_id=250
            )

        message = str(err.value)
        assert "Watch-E" in message
        assert "ProjectIdBSV 7" in message
        assert "HuisBatchIdBSV 251" in message
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
            [
                {
                    "HuisIdLeverancier": "A1",
                    "HuisIdBSV": 250,
                    "ProjectIdLeverancier": "7",
                    "ProjectIdBSV": 7,
                    "Dataleverancier": "Watch-E",
                    "HuisBatchIdBSV": 251,
                    "BatchIdBSV": 2,
                },
                {
                    "HuisIdLeverancier": "A1",
                    "HuisIdBSV": 250,
                    "ProjectIdLeverancier": "7",
                    "ProjectIdBSV": 7,
                    "Dataleverancier": "Watch-E",
                    "HuisBatchIdBSV": 252,
                    "BatchIdBSV": 3,
                },
            ],
        )
        monkeypatch.setattr(etdmap.options, "mapped_folder_path", tmp_path)
        df = _frame([pd.NA, 4.0], column=model.high)

        with pytest.raises(ValueError) as err:
            assert_no_delivered_in_declared_absent(
                df, {model.device}, household_id=250
            )

        message = str(err.value)
        assert "251" in message and "252" in message

    def test_an_absent_index_still_raises_on_the_violation(
        self, model, tmp_path, monkeypatch
    ):
        """The identifiers decorate the message. Failing to find them must
        never swallow the contradiction the guard exists to report."""
        monkeypatch.setattr(etdmap.options, "mapped_folder_path", tmp_path)
        df = _frame([pd.NA, 4.0], column=model.high)

        with pytest.raises(ValueError) as err:
            assert_no_delivered_in_declared_absent(
                df, {model.device}, context="hh 250", household_id=250
            )

        message = str(err.value)
        assert model.high in message
        assert "hh 250" in message

    def test_a_household_absent_from_the_index_still_raises(
        self, model, tmp_path, monkeypatch
    ):
        self._one_household(tmp_path)
        monkeypatch.setattr(etdmap.options, "mapped_folder_path", tmp_path)
        df = _frame([pd.NA, 4.0], column=model.high)

        with pytest.raises(ValueError, match=model.high):
            assert_no_delivered_in_declared_absent(
                df, {model.device}, household_id=999
            )

    def test_without_a_household_id_the_message_is_unchanged(self, model):
        """Every existing caller omits it, so the decoration is additive."""
        df = _frame([pd.NA, 4.0], column=model.high)

        with pytest.raises(ValueError) as err:
            assert_no_delivered_in_declared_absent(df, {model.device})

        message = str(err.value)
        assert "ProjectIdBSV" not in message
        assert model.high in message
