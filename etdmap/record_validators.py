import pandas as pd
from pandas import DataFrame, Series

from etdmap.data_model import cumulative_columns


def load_thresholds():
    thresholds_file = r'.\data\thresholds.csv'
    df = pd.read_csv(thresholds_file)
    return df


thresholds_df = load_thresholds()


def load_thresholds_as_dict() -> dict:
    thresholds_dict = {}
    for _, row in thresholds_df.iterrows():
        col = row['Variabele']
        thresholds_dict[col] = {'Min': row['Min'], 'Max': row['Max']}
    return thresholds_dict


thresholds_dict = load_thresholds_as_dict()


def validate_thresholds(df: pd.DataFrame) -> pd.Series:
    columns = [col for col in df.columns if col in thresholds_dict]

    ge_masks = pd.DataFrame(
        {col: df[col] >= thresholds_dict[col]['Min'] for col in columns},
    )
    le_masks = pd.DataFrame(
        {col: df[col] <= thresholds_dict[col]['Max'] for col in columns},
    )

    valid_masks = ge_masks & le_masks
    valid_combined = valid_masks.any(axis=1)

    # All relevant columns have NA values (set to pd.NA)
    all_na_rows = df[columns].isna().all(axis=1)
    valid_combined[all_na_rows] = pd.NA

    # Replace any NaN with pd.NA explicitly
    valid_combined = valid_combined.where(pd.notna(valid_combined), pd.NA)

    return valid_combined


def get_columns_threshold_validator(cols):
    return lambda df: validate_thresholds(df[cols])


def validate_columns(df: DataFrame, columns: list, condition_func) -> Series:
    """
    Helper function to validate columns with a given condition function.
    """
    if all(col in df.columns for col in columns):
        valid_mask = df[columns].notna().all(axis=1)
        condition = pd.Series(pd.NA, dtype='boolean', index=df.index)
        condition[valid_mask] = condition_func(df[valid_mask])
        return condition
    else:
        return pd.Series(pd.NA, dtype='boolean', index=df.index)


def validate_reading_date_reading_date_uniek(df: DataFrame) -> Series:
    # df['ReadingDate'] should only have unique values
    return ~df.duplicated(subset=['ReadingDate'])


def validate_300sec(df: DataFrame) -> Series:
    df = df.sort_values('ReadingDate')
    df['ReadingDateDiff'] = df['ReadingDate'].diff().dt.total_seconds().abs()
    columns = ['ReadingDateDiff']

    def condition_func(df):
        return df['ReadingDateDiff'] == 300

    result = validate_columns(df, columns, condition_func)
    df.drop(columns=['ReadingDateDiff'], inplace=True)
    return result


def validate_elektriciteit_vermogen(df: DataFrame) -> Series:
    columns = ['ElektriciteitVermogen']

    def condition_func(df: pd.DataFrame) -> bool:
        return (df['ElektriciteitVermogen'] >= -20000) & (
            df['ElektriciteitVermogen'] <= 20000
        )

    return validate_columns(df, columns, condition_func)


def validate_temperatuur_warm_tapwater(df: DataFrame) -> Series:
    columns = ['TemperatuurWarmTapwater']

    def condition_func(df: pd.DataFrame) -> bool:
        return (df['TemperatuurWarmTapwater'] >= 30) & (
            df['TemperatuurWarmTapwater'] <= 90
        )

    return validate_columns(df, columns, condition_func)


def validate_temperatuur_woonkamer(df: DataFrame) -> Series:
    columns = ['TemperatuurWoonkamer']

    def condition_func(df: pd.DataFrame) -> bool:
        return (df['TemperatuurWoonkamer'] >= 5) & (df['TemperatuurWoonkamer'] <= 35)

    return validate_columns(df, columns, condition_func)


def validate_temperatuur_setpoint_woonkamer(df: DataFrame) -> Series:
    columns = ['TemperatuurSetpointWoonkamer']

    def condition_func(df: pd.DataFrame) -> bool:
        return (df['TemperatuurSetpointWoonkamer'] >= 5) & (
            df['TemperatuurSetpointWoonkamer'] <= 35
        )

    return validate_columns(df, columns, condition_func)


def validate_zon_opwek_momentaan(df: DataFrame) -> Series:
    columns = ['Zon-opwekMomentaan']

    def condition_func(df: pd.DataFrame) -> bool:
        return (df['Zon-opwekMomentaan'] >= 0) & (df['Zon-opwekMomentaan'] <= 20000)

    return validate_columns(df, columns, condition_func)


def validate_zon_opwek_totaal_diff(df: DataFrame) -> Series:
    columns = ['Zon-opwekTotaalDiff']

    def condition_func(df: pd.DataFrame) -> bool:
        return (df['Zon-opwekTotaalDiff'] * 12 >= 0) & (
            df['Zon-opwekTotaalDiff'] * 12 <= 20000
        )

    return validate_columns(df, columns, condition_func)


def validate_co2(df: DataFrame) -> Series:
    columns = ['CO2']

    def condition_func(df: pd.DataFrame) -> bool:
        return (df['CO2'] >= 250) & (df['CO2'] <= 2500)

    return validate_columns(df, columns, condition_func)


def validate_luchtvochtigheid(df: DataFrame) -> Series:
    columns = ['Luchtvochtigheid']

    def condition_func(df: pd.DataFrame) -> bool:
        return (df['Luchtvochtigheid'] >= 20) & (df['Luchtvochtigheid'] <= 100)

    return validate_columns(df, columns, condition_func)


def validate_ventilatiedebiet(df: DataFrame) -> Series:
    columns = ['Ventilatiedebiet']

    def condition_func(df: pd.DataFrame) -> bool:
        return (df['Ventilatiedebiet'] >= 0) & (df['Ventilatiedebiet'] <= 500)

    return validate_columns(df, columns, condition_func)


def validate_elektriciteitgebruik(df: DataFrame) -> Series:
    columns = [
        'ElektriciteitsgebruikHuishoudelijk',
        'Zon-opwekTotaal',
        'ElektriciteitNetgebruikHoog',
        'ElektriciteitNetgebruikLaag',
    ]

    def condition_func(df: pd.DataFrame) -> bool:
        return (
            df['ElektriciteitsgebruikHuishoudelijk']
            <= df['Zon-opwekTotaal']
            + df['ElektriciteitNetgebruikHoog']
            + df['ElektriciteitNetgebruikLaag']
        )

    return validate_columns(df, columns, condition_func)


def validate_warmteproductie(df: DataFrame) -> Series:
    columns = ['WarmteproductieWarmtepomp', 'WarmteproductieWarmTapwater']

    def condition_func(df: pd.DataFrame) -> bool:
        return df['WarmteproductieWarmtepomp'] >= df['WarmteproductieWarmTapwater']

    return validate_columns(df, columns, condition_func)


# Combine all validators into the dictionary
record_flag_conditions = {
    'validate_reading_date_uniek': validate_reading_date_reading_date_uniek,
    'validate_300sec': validate_300sec,
    'validate_elektriciteit_vermogen': validate_elektriciteit_vermogen,
    'validate_temperatuur_warm_tapwater': validate_temperatuur_warm_tapwater,
    'validate_temperatuur_woonkamer': validate_temperatuur_woonkamer,
    'validate_temperatuur_setpoint_woonkamer': validate_temperatuur_setpoint_woonkamer,  # noqa E501
    'validate_zon_opwek_momentaan': validate_zon_opwek_momentaan,
    'validate_zon_opwek_totaal_diff': validate_zon_opwek_totaal_diff,
    #'validate_zonopwek_totaal_tegen_gebruik': validate_zonopwek_totaal_tegen_gebruik,  # noqa E501
    'validate_co2': validate_co2,
    'validate_luchtvochtigheid': validate_luchtvochtigheid,
    'validate_ventilatiedebiet': validate_ventilatiedebiet,
    'validate_elektriciteitgebruik': validate_elektriciteitgebruik,
    'validate_warmteproductie': validate_warmteproductie,
    'validate_thresholds': validate_thresholds,
}


def validate_not_outliers(x: Series):
    x = x[x > 0]
    q1 = x.quantile(0.25)
    q3 = x.quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    return ~((x < lower_bound) | (x > upper_bound))


# loop to produce cumulative column validators per cumulative column
for col in cumulative_columns:

    def validate_cumulative(df: DataFrame, cum_col=col) -> Series:
        df['temp_diff_valid'] = validate_not_outliers(df[cum_col + 'Diff'])
        columns = [cum_col + 'Diff', 'temp_diff_valid']

        def condition_func(df: pd.DataFrame) -> bool:
            return (df[cum_col + 'Diff'] >= 0) & (df['temp_diff_valid'])

        result = validate_columns(df, columns, condition_func)
        df.drop(columns=['temp_diff_valid'], inplace=True)
        return result

    record_flag_conditions['validate_' + col + 'Diff'] = validate_cumulative
