import pandas as pd
import os
from pandas import DataFrame, Series
from etl.mapping.mapping_helpers import cumulative_columns


def load_thresholds():
    thresholds_file = os.getenv("THRESHOLDS_FILE")
    df = pd.read_excel(thresholds_file)
    return df


thresholds_df = load_thresholds()


def load_thresholds_as_dict() -> dict:
    thresholds_dict = {}
    for _, row in thresholds_df.iterrows():
        col = row["Variabele"]
        thresholds_dict[col] = {"Min": row["Min"], "Max": row["Max"]}
    return thresholds_dict


thresholds_dict = load_thresholds_as_dict()


def validate_thresholds(df: pd.DataFrame) -> pd.Series:
    columns = [col for col in df.columns if col in thresholds_dict]

    ge_masks = pd.DataFrame(
        {col: df[col] >= thresholds_dict[col]["Min"] for col in columns}
    )
    le_masks = pd.DataFrame(
        {col: df[col] <= thresholds_dict[col]["Max"] for col in columns}
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
        condition = pd.Series(pd.NA, dtype="boolean", index=df.index)
        condition[valid_mask] = condition_func(df[valid_mask])
        return condition
    else:
        return pd.Series(pd.NA, dtype="boolean", index=df.index)


def validate_reading_date_reading_date_uniek(df: DataFrame) -> Series:
    # df['ReadingDate'] should only have unique values
    return ~df.duplicated(subset=["ReadingDate"])


def validate_300sec(df: DataFrame) -> Series:
    df = df.sort_values("ReadingDate")
    df["ReadingDateDiff"] = df["ReadingDate"].diff().dt.total_seconds().abs()
    columns = ["ReadingDateDiff"]
    condition_func = lambda df: (df["ReadingDateDiff"] == 300)
    result = validate_columns(df, columns, condition_func)
    df.drop(columns=["ReadingDateDiff"], inplace=True)
    return result


def validate_elektriciteit_vermogen(df: DataFrame) -> Series:
    columns = ["ElektriciteitVermogen"]
    condition_func = lambda df: (df["ElektriciteitVermogen"] >= -20000) & (
        df["ElektriciteitVermogen"] <= 20000
    )
    return validate_columns(df, columns, condition_func)


def validate_temperatuur_warm_tapwater(df: DataFrame) -> Series:
    columns = ["TemperatuurWarmTapwater"]
    condition_func = lambda df: (df["TemperatuurWarmTapwater"] >= 30) & (
        df["TemperatuurWarmTapwater"] <= 90
    )
    return validate_columns(df, columns, condition_func)


def validate_temperatuur_woonkamer(df: DataFrame) -> Series:
    columns = ["TemperatuurWoonkamer"]
    condition_func = lambda df: (df["TemperatuurWoonkamer"] >= 5) & (
        df["TemperatuurWoonkamer"] <= 35
    )
    return validate_columns(df, columns, condition_func)


def validate_temperatuur_setpoint_woonkamer(df: DataFrame) -> Series:
    columns = ["TemperatuurSetpointWoonkamer"]
    condition_func = lambda df: (df["TemperatuurSetpointWoonkamer"] >= 5) & (
        df["TemperatuurSetpointWoonkamer"] <= 35
    )
    return validate_columns(df, columns, condition_func)


def validate_zon_opwek_momentaan(df: DataFrame) -> Series:
    columns = ["Zon-opwekMomentaan"]
    condition_func = lambda df: (df["Zon-opwekMomentaan"] >= 0) & (
        df["Zon-opwekMomentaan"] <= 20000
    )
    return validate_columns(df, columns, condition_func)


def validate_zon_opwek_totaal_diff(df: DataFrame) -> Series:
    columns = ["Zon-opwekTotaalDiff"]

    condition_func = lambda df: (df["Zon-opwekTotaalDiff"] * 12 >= 0) & (
        df["Zon-opwekTotaalDiff"] * 12 <= 20000
    )
    return validate_columns(df, columns, condition_func)


# def validate_zonopwek_totaal_tegen_gebruik(df: DataFrame) -> Series:
#     df2 = df.copy()
#     df2['Zon-opwekTotaalDiff2'] = df2['Zon-opwekTotaal'].diff()
#     df2['ElektriciteitTerugleveringHoogDiff2'] = df2['ElektriciteitTerugleveringHoog'].diff()
#     df2['ElektriciteitTerugleveringLaagDiff2'] = df2['ElektriciteitTerugleveringLaag'].diff()
#     df2['ElektriciteitsgebruikWarmtepompDiff2'] = df2['ElektriciteitsgebruikWarmtepomp'].diff()
#     df2['ElektriciteitsgebruikBoilervatDiff2'] = df2['ElektriciteitsgebruikBoilervat'].diff()

#     columns = ['Zon-opwekTotaalDiff2','ElektriciteitTerugleveringHoogDiff2','ElektriciteitTerugleveringLaagDiff2', 'ElektriciteitsgebruikWTWDiff2', 'ElektriciteitsgebruikWarmtepompDiff2', 'ElektriciteitsgebruikBoilervatDiff2']

#     condition_func = lambda x: x['Zon-opwekTotaalDiff2'] >= x['ElektriciteitTerugleveringHoogDiff2'] + x['ElektriciteitTerugleveringLaagDiff2'] + x['ElektriciteitsgebruikWTWDiff2'] + x['ElektriciteitsgebruikWarmtepompDiff2'] + x['ElektriciteitsgebruikBoilervatDiff2']

#     return validate_columns(df2, columns, condition_func)


def validate_co2(df: DataFrame) -> Series:
    columns = ["CO2"]
    condition_func = lambda df: (df["CO2"] >= 250) & (df["CO2"] <= 2500)
    return validate_columns(df, columns, condition_func)


def validate_luchtvochtigheid(df: DataFrame) -> Series:
    columns = ["Luchtvochtigheid"]
    condition_func = lambda df: (df["Luchtvochtigheid"] >= 20) & (
        df["Luchtvochtigheid"] <= 100
    )
    return validate_columns(df, columns, condition_func)


def validate_ventilatiedebiet(df: DataFrame) -> Series:
    columns = ["Ventilatiedebiet"]
    condition_func = lambda df: (df["Ventilatiedebiet"] >= 0) & (
        df["Ventilatiedebiet"] <= 500
    )
    return validate_columns(df, columns, condition_func)


def validate_elektriciteitgebruik(df: DataFrame) -> Series:
    columns = [
        "ElektriciteitsgebruikHuishoudelijk",
        "Zon-opwekTotaal",
        "ElektriciteitNetgebruikHoog",
        "ElektriciteitNetgebruikLaag",
    ]
    condition_func = (
        lambda df: df["ElektriciteitsgebruikHuishoudelijk"]
        <= df["Zon-opwekTotaal"]
        + df["ElektriciteitNetgebruikHoog"]
        + df["ElektriciteitNetgebruikLaag"]
    )
    return validate_columns(df, columns, condition_func)


def validate_warmteproductie(df: DataFrame) -> Series:
    columns = ["WarmteproductieWarmtepomp", "WarmteproductieWarmTapwater"]
    condition_func = (
        lambda df: df["WarmteproductieWarmtepomp"] >= df["WarmteproductieWarmTapwater"]
    )
    return validate_columns(df, columns, condition_func)


# Combine all validators into the dictionary
record_flag_conditions = {
    "validate_reading_date_uniek": validate_reading_date_reading_date_uniek,
    "validate_300sec": validate_300sec,
    "validate_elektriciteit_vermogen": validate_elektriciteit_vermogen,
    "validate_temperatuur_warm_tapwater": validate_temperatuur_warm_tapwater,
    "validate_temperatuur_woonkamer": validate_temperatuur_woonkamer,
    "validate_temperatuur_setpoint_woonkamer": validate_temperatuur_setpoint_woonkamer,
    "validate_zon_opwek_momentaan": validate_zon_opwek_momentaan,
    "validate_zon_opwek_totaal_diff": validate_zon_opwek_totaal_diff,
    #'validate_zonopwek_totaal_tegen_gebruik': validate_zonopwek_totaal_tegen_gebruik,
    "validate_co2": validate_co2,
    "validate_luchtvochtigheid": validate_luchtvochtigheid,
    "validate_ventilatiedebiet": validate_ventilatiedebiet,
    "validate_elektriciteitgebruik": validate_elektriciteitgebruik,
    "validate_warmteproductie": validate_warmteproductie,
    "validate_thresholds": validate_thresholds,
}


def validate_not_outliers(x: Series):
    x = x[x > 0]
    q1 = x.quantile(0.25)
    q3 = x.quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    return ~((x < lower_bound) | (x > upper_bound))


def validate_not_outliers(x: pd.Series) -> pd.Series:
    non_zero_x = x[x > 0]

    if non_zero_x.empty:
        return True

    # q1 = non_zero_x.quantile(0.25)
    # q3 = non_zero_x.quantile(0.75)
    # iqr = q3 - q1
    # upper_bound = q3 + 1.5 * iqr

    # not using a lower bound since we are ignoring 0 values and it may be hard to define that accurately...
    # lower_bound = q1 - 1.5 * iqr

    upper_bound = non_zero_x.quantile(0.98) * 2

    outliers = ~((x > 0) & (x > upper_bound))

    return outliers


# loop to produce cumulative column validators per cumulative column
for col in cumulative_columns:

    def validate_cumulative(df: DataFrame, cum_col=col) -> Series:
        df["temp_diff_valid"] = validate_not_outliers(df[cum_col + "Diff"])
        columns = [cum_col + "Diff", "temp_diff_valid"]
        condition_func = lambda df: (df[cum_col + "Diff"] >= 0) & (
            df["temp_diff_valid"]
        )
        result = validate_columns(df, columns, condition_func)
        df.drop(columns=["temp_diff_valid"], inplace=True)
        return result

    record_flag_conditions["validate_" + col + "Diff"] = validate_cumulative
