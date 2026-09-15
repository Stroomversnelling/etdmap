"""Cadence detection: how often a column actually reports, read from its own
timestamps. Synthetic frames only -- no pipeline, no fixtures."""

import pandas as pd
import pytest

from etdmap.mapping_helpers import (
    cadence_label,
    cadence_to_rows,
    detect_column_cadence,
    fill_down_infrequent_devices,
    write_column_cadence,
)

START = '2024-01-01 00:00'


def _frame(specs, periods=288):
    """One 5-minute grid, with each column carrying a value every Nth slot.

    specs maps column name -> the column's own cadence in minutes.
    """
    grid = pd.date_range(START, periods=periods, freq='5min')
    df = pd.DataFrame({'ReadingDate': grid})
    for col, minutes in specs.items():
        every = minutes // 5
        values = [
            float(i) if i % every == 0 else pd.NA for i in range(periods)
        ]
        df[col] = pd.array(values, dtype='Float64')
    return df


def _cadence(df, **kwargs):
    out = detect_column_cadence(df, **kwargs)
    return dict(zip(out['column'], out['cadence_minutes']))


def _row(out, column):
    """One column's row. Never index by position: ReadingDate is in there too."""
    return out.set_index('column').loc[column]


class TestDetectColumnCadence:
    def test_reads_each_column_at_its_own_cadence(self):
        df = _frame({'fast': 5, 'medium': 15, 'slow': 60})
        assert _cadence(df) == {
            'ReadingDate': 5, 'fast': 5, 'medium': 15, 'slow': 60
        }

    def test_a_partial_opening_day_does_not_mislead(self):
        # 24 readings on a 5-minute register: the same count a complete hourly
        # register produces. Counting readings would confuse the two; gaps do not.
        five = _frame({'five': 5}, periods=24)
        sixty = _frame({'sixty': 60}, periods=24 * 12)
        assert len(five) == 24
        assert int(sixty['sixty'].notna().sum()) == 24
        assert _cadence(five)['five'] == 5
        assert _cadence(sixty)['sixty'] == 60

    def test_an_outage_does_not_change_the_cadence(self):
        df = _frame({'x': 15})
        df.loc[40:80, 'x'] = pd.NA
        assert _cadence(df)['x'] == 15

    def test_confidence_reports_how_regular_the_column_is(self):
        regular = detect_column_cadence(_frame({'x': 15}))
        assert _row(regular, 'x')['confidence'] == 1.0

        df = _frame({'x': 15})
        df.loc[40:80, 'x'] = pd.NA
        gappy = detect_column_cadence(df)
        assert 0.0 < _row(gappy, 'x')['confidence'] < 1.0

    def test_too_few_readings_yields_na_not_a_guess(self):
        df = _frame({'x': 5}, periods=2)
        out = detect_column_cadence(df)
        assert pd.isna(_row(out, 'x')['cadence_detected'])
        assert _row(out, 'x')['n_readings'] == 2

    def test_all_na_column_yields_na(self):
        df = _frame({'x': 5})
        df['x'] = pd.array([pd.NA] * len(df), dtype='Float64')
        out = detect_column_cadence(df)
        assert pd.isna(_row(out, 'x')['cadence_detected'])
        assert _row(out, 'x')['n_readings'] == 0

    def test_nullable_dtypes(self):
        out = detect_column_cadence(_frame({'x': 15}))
        assert str(out['cadence_minutes'].dtype) == 'Int64'
        assert str(out['confidence'].dtype) == 'Float64'
        assert str(out['n_readings'].dtype) == 'Int64'
        assert str(out['column'].dtype) == 'string'

    def test_timestamp_jitter_does_not_split_one_cadence(self):
        # Readings arrive a few seconds off the slot, varying per reading.
        df = _frame({'x': 15})
        offsets = pd.to_timedelta(
            [(i % 7) - 3 for i in range(len(df))], unit='s'
        )
        df['ReadingDate'] = df['ReadingDate'] + offsets
        assert _cadence(df)['x'] == 15

    def test_columns_argument_selects_and_missing_columns_are_skipped(self):
        df = _frame({'a': 5, 'b': 60})
        assert _cadence(df, columns=['b', 'absent']) == {'b': 60}

    def test_the_dataset_cadence_is_measured_as_its_own_row(self):
        # The grid the data sits on, detected in the same pass, so nothing
        # downstream has to measure it again.
        df = _frame({'x': 60})
        assert _cadence(df)['ReadingDate'] == 5


class TestCadenceMustRunBeforeFilling:
    def test_filling_destroys_the_evidence(self):
        """fill_down_infrequent_devices makes every row non-missing, so the
        cadence has to be measured ahead of it -- which is why detection sits in
        the mapper rather than in run_standard_pipeline."""
        df = _frame({'x': 60})
        assert _cadence(df)['x'] == 60

        filled = fill_down_infrequent_devices(df.copy(), columns=['x'])
        assert _cadence(filled)['x'] == 5


class TestCadenceSidecar:
    def test_round_trips_with_nullable_dtypes(self, tmp_path):
        df = _frame({'fast': 5, 'slow': 60})
        rows = cadence_to_rows(7, detect_column_cadence(df))
        rows += cadence_to_rows(9, detect_column_cadence(_frame({'fast': 15})))

        path = write_column_cadence(rows, str(tmp_path))
        out = pd.read_parquet(path, dtype_backend='numpy_nullable')

        assert list(out.columns) == [
            'HuisIdBSV', 'column', 'cadence_minutes', 'cadence_source',
            'cadence_detected', 'confidence', 'n_readings'
        ]
        assert str(out['HuisIdBSV'].dtype) == 'Int64'
        assert str(out['cadence_minutes'].dtype) == 'Int64'
        # The same column reports at different cadences in different households.
        by_house = out.set_index(['HuisIdBSV', 'column'])['cadence_minutes']
        assert by_house[(7, 'fast')] == 5
        assert by_house[(9, 'fast')] == 15

    def test_empty_writes_a_typed_empty_frame(self, tmp_path):
        path = write_column_cadence([], str(tmp_path))
        out = pd.read_parquet(path, dtype_backend='numpy_nullable')
        assert len(out) == 0
        assert str(out['cadence_minutes'].dtype) == 'Int64'

    def test_unmeasurable_column_survives_as_na(self, tmp_path):
        rows = cadence_to_rows(1, detect_column_cadence(_frame({'x': 5}, periods=2)))
        path = write_column_cadence(rows, str(tmp_path))
        out = pd.read_parquet(path, dtype_backend='numpy_nullable')
        assert pd.isna(out.set_index('column').loc['x', 'cadence_detected'])

    def test_no_cadence_yields_no_rows(self):
        assert cadence_to_rows(1, None) == []


class TestCadenceResolution:
    """Declared beats detected beats the dataset's own cadence."""

    def _resolved(self, df, declared=None, **kwargs):
        return detect_column_cadence(df, declared=declared, **kwargs).set_index('column')

    def test_a_confident_detection_is_used(self):
        r = self._resolved(_frame({'x': 60}))
        assert r.loc['x', 'cadence_minutes'] == 60
        assert r.loc['x', 'cadence_source'] == 'detected'

    def test_an_overridden_detection_is_kept_for_the_report(self):
        r = self._resolved(_frame({'x': 60}), declared={'x': 15})
        assert r.loc['x', 'cadence_minutes'] == 15
        assert r.loc['x', 'cadence_detected'] == 60

    def test_a_declaration_overrides_the_data(self):
        r = self._resolved(_frame({'x': 60}), declared={'x': 15})
        assert r.loc['x', 'cadence_minutes'] == 15
        assert r.loc['x', 'cadence_source'] == 'declared'

    def test_a_declaration_that_contradicts_the_data_is_reported(self, caplog):
        with caplog.at_level('WARNING'):
            self._resolved(_frame({'x': 60}), declared={'x': 15})
        assert '15min declared' in caplog.text
        assert 'reports every 1h' in caplog.text

    def test_an_agreeing_declaration_is_not_reported(self, caplog):
        with caplog.at_level('WARNING'):
            self._resolved(_frame({'x': 60}), declared={'x': 60})
        assert caplog.text == ''

    def test_an_irregular_column_falls_back_to_the_dataset(self):
        # Readings at scattered intervals: the mode means little.
        df = _frame({'x': 5})
        keep = {0, 1, 7, 8, 30, 61, 95, 96, 150}
        df['x'] = pd.array(
            [float(i) if i in keep else pd.NA for i in range(len(df))],
            dtype='Float64',
        )
        r = self._resolved(df)
        assert r.loc['x', 'confidence'] < 0.8
        assert r.loc['x', 'cadence_minutes'] == 5
        assert r.loc['x', 'cadence_source'] == 'dataset'

    def test_an_unmeasurable_column_falls_back_to_the_dataset(self):
        df = _frame({'x': 5})
        df['x'] = pd.array([pd.NA] * len(df), dtype='Float64')
        r = self._resolved(df)
        assert r.loc['x', 'cadence_minutes'] == 5
        assert r.loc['x', 'cadence_source'] == 'dataset'

    def test_no_declaration_is_the_normal_case(self):
        # Meetfrequentie is unpopulated by design.
        r = self._resolved(_frame({'x': 60}), declared=None)
        assert r.loc['x', 'cadence_source'] == 'detected'

    def test_the_declared_value_reaches_the_fill(self):
        df = _frame({'x': 60})
        resolved = detect_column_cadence(df, declared={'x': 15})
        out = fill_down_infrequent_devices(df.copy(), columns=['x'], cadence=resolved)
        # Declared 15min on a 5-minute grid: every 3rd row, not every 12th.
        assert int(out['x'].notna().sum()) == len(df) // 3


class TestFillAtDeviceCadence:
    def test_without_cadence_fills_every_row_as_before(self):
        df = _frame({'x': 60})
        out = fill_down_infrequent_devices(df.copy(), columns=['x'])
        assert out['x'].notna().all()

    def test_fills_only_the_slots_the_device_reports_on(self):
        df = _frame({'x': 60})
        cadence = detect_column_cadence(df)
        out = fill_down_infrequent_devices(df.copy(), columns=['x'], cadence=cadence)

        # One value an hour on a 5-minute grid, not one every five minutes.
        assert int(out['x'].notna().sum()) == len(df) // 12
        on_the_hour = out['ReadingDate'].dt.minute == 0
        assert out.loc[on_the_hour, 'x'].notna().all()
        assert out.loc[~on_the_hour, 'x'].isna().all()

    def test_conserves_every_delivered_reading(self):
        df = _frame({'x': 60})
        delivered = df.loc[df['x'].notna(), ['ReadingDate', 'x']]
        out = fill_down_infrequent_devices(
            df.copy(), columns=['x'], cadence=detect_column_cadence(df)
        )
        kept = out.loc[out['ReadingDate'].isin(delivered['ReadingDate']), 'x']
        assert kept.tolist() == delivered['x'].tolist()

    def test_an_off_slot_reading_is_kept_where_it_is(self):
        df = _frame({'x': 60})
        # A real reading at 00:35, which is not on any hourly slot.
        off_slot = df['ReadingDate'] == pd.Timestamp('2024-01-01 00:35')
        df.loc[off_slot, 'x'] = 999.0

        out = fill_down_infrequent_devices(
            df.copy(), columns=['x'], cadence=detect_column_cadence(df)
        )
        assert out.loc[off_slot, 'x'].iloc[0] == 999.0

    def test_undetectable_cadence_falls_back_to_every_row(self):
        df = _frame({'x': 5}, periods=2)
        cadence = detect_column_cadence(df)
        assert pd.isna(cadence.loc[0, 'cadence_minutes'])

        out = fill_down_infrequent_devices(df.copy(), columns=['x'], cadence=cadence)
        assert out['x'].notna().all()

    def test_a_coarser_grid_is_measured_not_assumed(self):
        """A 15-minute dataset has no 5-minute rows. The fallback is the grid's
        own cadence, so a device at the grid cadence still fills every row."""
        grid = pd.date_range(START, periods=96, freq='15min')
        df = pd.DataFrame({'ReadingDate': grid})
        df['x'] = pd.array(
            [float(i) if i % 4 == 0 else pd.NA for i in range(96)], dtype='Float64'
        )
        by_col = _cadence(df)
        assert by_col['ReadingDate'] == 15
        assert by_col['x'] == 60
        cadence = detect_column_cadence(df)

        out = fill_down_infrequent_devices(
            df.copy(), columns=['x'], cadence=cadence, grid_minutes=15
        )
        # One value an hour on a 15-minute grid: every 4th row, not every row.
        assert int(out['x'].notna().sum()) == 24

    def test_a_cadence_that_does_not_divide_the_hour(self):
        """Slots step in rows, so a 7-minute grid with a 21-minute device works.
        A wall-clock floor would have no aligned slots to land on."""
        grid = pd.date_range(START, periods=60, freq='7min')
        df = pd.DataFrame({'ReadingDate': grid})
        df['x'] = pd.array(
            [float(i) if i % 3 == 0 else pd.NA for i in range(60)], dtype='Float64'
        )
        by_col = _cadence(df)
        assert by_col['ReadingDate'] == 7
        assert by_col['x'] == 21
        cadence = detect_column_cadence(df)

        out = fill_down_infrequent_devices(
            df.copy(), columns=['x'], cadence=cadence, grid_minutes=7
        )
        assert int(out['x'].notna().sum()) == 20

    def test_a_five_minute_device_is_unchanged_by_cadence(self):
        df = _frame({'x': 5})
        with_cadence = fill_down_infrequent_devices(
            df.copy(), columns=['x'], cadence=detect_column_cadence(df)
        )
        without = fill_down_infrequent_devices(df.copy(), columns=['x'])
        pd.testing.assert_series_equal(with_cadence['x'], without['x'])

    def test_never_present_device_still_becomes_zero(self):
        df = _frame({'x': 5})
        df['x'] = pd.array([pd.NA] * len(df), dtype='Float64')
        out = fill_down_infrequent_devices(
            df.copy(), columns=['x'], cadence=detect_column_cadence(df)
        )
        assert (out['x'] == 0.0).all()

    def test_each_column_uses_its_own_cadence(self):
        df = _frame({'fast': 5, 'slow': 60})
        out = fill_down_infrequent_devices(
            df.copy(), columns=['fast', 'slow'], cadence=detect_column_cadence(df)
        )
        assert out['fast'].notna().all()
        assert int(out['slow'].notna().sum()) == len(df) // 12


class TestFillIsLosslessWhateverTheCadence:
    """A detected cadence is a measurement, not a fact, and a declared one can be
    stale. The fill must not depend on either being right.

    The invariant: filling at a cadence only removes *restatements*. Forward-fill
    the result back onto the dense grid and it must reproduce the every-row fill
    exactly -- for any cadence, however wrong.
    """

    def _check(self, values, declared=None, periods=48):
        grid = pd.date_range(START, periods=periods, freq='5min')
        df = pd.DataFrame({'ReadingDate': grid})
        df['x'] = pd.array(values, dtype='Float64')

        dense = fill_down_infrequent_devices(df.copy(), columns=['x'])['x']
        cadence = detect_column_cadence(df, declared=declared)
        sparse = fill_down_infrequent_devices(
            df.copy(), columns=['x'], cadence=cadence
        )['x']

        delivered = df['x'].notna()
        assert sparse[delivered].equals(df['x'][delivered]), 'a delivered reading was lost'
        assert sparse.ffill().bfill().equals(dense), 'sparsening changed the data'
        return cadence.set_index('column').loc['x']

    def test_an_off_cadence_reading_in_between_survives(self):
        values = [float(i) if i % 12 == 0 else pd.NA for i in range(48)]
        values[7] = 99.0
        self._check(values)

    def test_scattered_in_between_readings_lower_confidence_to_the_grid(self):
        # Irregular readings make the modal gap a weak claim, so resolution
        # falls back to the dataset cadence and nothing is sparsened at all.
        values = [float(i) if i % 12 == 0 else pd.NA for i in range(48)]
        for i in (5, 19, 33):
            values[i] = 500.0 + i
        row = self._check(values)
        assert row['confidence'] < 0.8
        assert row['cadence_source'] == 'dataset'

    def test_a_wrong_declaration_is_still_lossless(self):
        # Declared four-hourly on a device that reports every fifteen minutes.
        values = [float(i) if i % 3 == 0 else pd.NA for i in range(48)]
        row = self._check(values, declared={'x': 240})
        assert row['cadence_minutes'] == 240
        assert row['cadence_detected'] == 15

    def test_leading_and_trailing_gaps(self):
        values = [pd.NA] * 6 + [float(i) if i % 12 == 0 else pd.NA for i in range(42)]
        values[-4:] = [pd.NA] * 4
        self._check(values)

    def test_a_single_reading(self):
        values = [pd.NA] * 48
        values[20] = 7.0
        self._check(values)

    def test_a_never_present_device(self):
        self._check([pd.NA] * 48)

    def test_a_cadence_off_the_grid_fills_every_row(self, caplog):
        # 8 minutes on a 5-minute grid has no slots. Rounding to the nearest
        # whole step would fill every 10 minutes and call it 8.
        grid = pd.date_range(START, periods=48, freq='5min')
        df = pd.DataFrame({'ReadingDate': grid})
        df['x'] = pd.array(
            [float(i) if i % 4 == 0 else pd.NA for i in range(48)], dtype='Float64'
        )
        cadence = detect_column_cadence(df, declared={'x': 8})
        with caplog.at_level('INFO'):
            out = fill_down_infrequent_devices(
                df.copy(), columns=['x'], cadence=cadence
            )
        assert out['x'].notna().all()
        assert 'does not land on the 5min grid' in caplog.text


class TestCadenceLabel:
    @pytest.mark.parametrize(
        'minutes,expected',
        [(0.5, '30s'), (5, '5min'), (15, '15min'), (60, '1h'), (1440, '24h')],
    )
    def test_written_in_its_natural_unit(self, minutes, expected):
        assert cadence_label(minutes) == expected

    def test_unknown_stays_unknown(self):
        assert pd.isna(cadence_label(pd.NA))
