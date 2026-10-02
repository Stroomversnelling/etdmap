"""The device-absence declarations are explicit metadata claims, and the
data can falsify them. Two pieces enforce that contract in etdmap:

- ``parse_device_declaration`` turns a household's declaration cell into a
  set and raises on names the data model does not know (inconsistent
  metadata is always a hard failure, same contract as the sync's check).
- ``assert_no_delivered_in_declared_absent`` raises when a declared-absent
  variable -- or its interval/cumulative pair -- holds delivered readings.
  Migration was contradiction-free by construction, so the guard exists
  for future drift: a device installed later that starts reporting raises
  until the metadata is corrected.
"""

import pandas as pd
import pytest

from etdmap.data_model import parse_device_declaration
from etdmap.flags import MappingFlag, flag_column, set_flags
from etdmap.mapping_helpers import assert_no_delivered_in_declared_absent

RADIATOR = "ElektriciteitsgebruikRadiator"
BOILERVAT = "ElektriciteitsgebruikBoilervat"


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
