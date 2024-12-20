import pandas as pd
from pandas import DataFrame

from etdmap.data_model import cumulative_columns

year_allowed_jitter = 18  # approx 5% of the year


def validate_columns(df: DataFrame, columns: list, condition_func) -> bool:
    """
    Validate a dataset based on specified columns in a DataFrame based on a given condition function.

    Parameters:
    df (DataFrame): The input DataFrame to be validated.
    columns (list): A list of column names to be checked for validity.
    condition_func (function): A function that takes a DataFrame as an argument and returns a boolean Series indicating which rows meet the validation criteria.

    Returns:
    bool: True if all valid rows meet the specified condition, pd.NA if no valid rows are found or if any of the specified columns do not exist in the DataFrame.
    """
    if all(col in df.columns for col in columns):
        valid_mask = df[columns].notna().all(axis=1)
        if valid_mask.any():
            condition = pd.Series(pd.NA, dtype="boolean", index=df.index)
            condition[valid_mask] = condition_func(df[valid_mask])
            return condition.all(skipna=True)
        else:
            return pd.NA
    else:
        return pd.NA


def validate_gasgebruik(df: DataFrame) -> bool:
    columns = ["Gasgebruik"]

    def condition_func(df: pd.DataFrame) -> bool:
        return (df["Gasgebruik"].diff().dropna() >= 0) & (
            df["Gasgebruik"].diff().dropna() <= 5000
        )

    return validate_columns(df, columns, condition_func)


def validate_monitoring_data_counts(df: DataFrame) -> bool:
    min_count, max_count = 100000, 110000
    return pd.NA if df.empty else min_count <= len(df) <= max_count


def validate_cumulative_variable(df: DataFrame, column: str) -> bool:
    columns = [column]

    def condition_func(df):
        return df[column].diff().dropna() >= 0

    return validate_columns(df, columns, condition_func)


def validate_range(
    df: DataFrame,
    column: str,
    min_value: float,
    max_value: float,
) -> bool:
    if column in df.columns:
        enough_days = 365 - year_allowed_jitter
        df_sorted = df.sort_values("ReadingDate")
        df_sorted = df_sorted[df_sorted[column].notna()]
        if df_sorted.empty:
            return pd.NA
        date_diff = (
            df_sorted["ReadingDate"].max() - df_sorted["ReadingDate"].min()
        ).days
        yearly_diff = df_sorted[column].iloc[-1] - df_sorted[column].iloc[0]
        return (
            pd.NA
            if pd.isna(date_diff) or pd.isna(yearly_diff)
            else (date_diff >= enough_days) and (min_value <= yearly_diff <= max_value)
        )
    else:
        return pd.NA


def validate_approximately_one_year_of_records(df: DataFrame) -> bool:
    if "ReadingDate" in df.columns:
        date_diff = (df["ReadingDate"].max() - df["ReadingDate"].min()).days
        return (365 - year_allowed_jitter) <= date_diff <= (365 + year_allowed_jitter)
    else:
        return pd.NA


def validate_column_exists(df: DataFrame, column_name: str) -> bool:
    return column_name in df.columns


def validate_columns_exist(df: DataFrame) -> bool:
    columns_to_check = [
        "ReadingDate",
        "Ventilatiedebiet",
        "CO2",
        "ElektriciteitNetgebruikHoog",
        "ElektriciteitNetgebruikLaag",
        "ElektriciteitTerugleveringHoog",
        "ElektriciteitTerugleveringLaag",
        "ElektriciteitVermogen",
        "ElektriciteitsgebruikWTW",
        "ElektriciteitsgebruikWarmtepomp",
        "ElektriciteitsgebruikBooster",
        "ElektriciteitsgebruikBoilervat",
        "ElektriciteitsgebruikHuishoudelijk",
        "TemperatuurWarmTapwater",
        "TemperatuurWoonkamer",
        "WarmteproductieWarmtepomp",
        "TemperatuurSetpointWoonkamer",
        "Zon-opwekMomentaan",
        "Zon-opwekTotaal",
        "Luchtvochtigheid",
    ]
    return all(validate_column_exists(df, col) for col in columns_to_check)


def validate_elektriciteit_netgebruik_hoog(df: DataFrame) -> bool:
    return validate_cumulative_variable(
        df,
        "ElektriciteitNetgebruikHoog",
    ) & validate_range(df, "ElektriciteitNetgebruikHoog", 100, 20000)


def validate_elektriciteit_netgebruik_laag(df: DataFrame) -> bool:
    return validate_cumulative_variable(
        df,
        "ElektriciteitNetgebruikLaag",
    ) & validate_range(df, "ElektriciteitNetgebruikLaag", 100, 20000)


def validate_elektriciteit_teruglevering_hoog(df: DataFrame) -> bool:
    return validate_cumulative_variable(
        df,
        "ElektriciteitTerugleveringHoog",
    ) & validate_range(df, "ElektriciteitTerugleveringHoog", 100, 20000)


def validate_elektriciteit_teruglevering_laag(df: DataFrame) -> bool:
    return validate_cumulative_variable(
        df,
        "ElektriciteitTerugleveringLaag",
    ) & validate_range(df, "ElektriciteitTerugleveringLaag", 100, 20000)


def validate_elektriciteitsgebruik_warmtepomp(df: DataFrame) -> bool:
    return validate_cumulative_variable(
        df,
        "ElektriciteitsgebruikWarmtepomp",
    ) & validate_range(df, "ElektriciteitsgebruikWarmtepomp", 100, 20000)


def validate_zon_opwek_totaal(df: DataFrame) -> bool:
    return validate_cumulative_variable(
        df,
        "Zon-opwekTotaal",
    ) & validate_range(
        df,
        "Zon-opwekTotaal",
        0,
        20000,
    )


def validate_energiegebruik_warmteopwekker(df: DataFrame) -> bool:
    df["EnergiegebruikWarmteopwekker"] = (
        df["ElektriciteitsgebruikWarmtepomp"]
        + df["ElektriciteitsgebruikBooster"]
        + df["ElektriciteitsgebruikBoilervat"]
    )
    return validate_range(df, "EnergiegebruikWarmteopwekker", 100, 20000)


def validate_elektriciteitsgebruik_wtw(df: DataFrame) -> bool:
    return validate_cumulative_variable(
        df,
        "ElektriciteitsgebruikWTW",
    ) & validate_range(df, "ElektriciteitsgebruikWTW", 0, 1000)


def validate_elektriciteitsgebruik_booster(df: DataFrame) -> bool:
    return validate_cumulative_variable(
        df,
        "ElektriciteitsgebruikBooster",
    ) & validate_range(df, "ElektriciteitsgebruikBooster", 0, 20000)


def validate_elektriciteitsgebruik_boilervat(df: DataFrame) -> bool:
    return validate_cumulative_variable(
        df,
        "ElektriciteitsgebruikBoilervat",
    ) & validate_range(df, "ElektriciteitsgebruikBoilervat", 0, 20000)


def validate_elektriciteitsgebruik_huishoudelijk(df: DataFrame) -> bool:
    return validate_cumulative_variable(
        df,
        "ElektriciteitsgebruikHuishoudelijk",
    ) & validate_range(df, "ElektriciteitsgebruikHuishoudelijk", 0, 20000)


def validate_warmteproductie_warmtepomp(df: DataFrame) -> bool:
    return validate_cumulative_variable(
        df,
        "WarmteproductieWarmtepomp",
    ) & validate_range(df, "WarmteproductieWarmtepomp", 0, 250)


def validate_watergebruik_warm_tapwater(df: DataFrame) -> bool:
    return validate_cumulative_variable(
        df,
        "WatergebruikWarmTapwater",
    ) & validate_range(df, "WatergebruikWarmTapwater", 0, 200000)


def validate_no_readingdate_gap(df: DataFrame) -> bool:
    time_diffs = df["ReadingDate"].diff().dt.total_seconds()
    valid = all(time_diffs[1:] == 300)
    return valid


dataset_flag_conditions = {
    "validate_gasgebruik": validate_gasgebruik,
    "validate_monitoring_data_counts": validate_monitoring_data_counts,
    "validate_elektriciteit_netgebruik_hoog": validate_elektriciteit_netgebruik_hoog,  # E501
    "validate_elektriciteit_netgebruik_laag": validate_elektriciteit_netgebruik_laag,  # E501
    "validate_elektriciteit_teruglevering_hoog": validate_elektriciteit_teruglevering_hoog,  # E501
    "validate_elektriciteit_teruglevering_laag": validate_elektriciteit_teruglevering_laag,  # E501
    "validate_elektriciteitsgebruik_warmtepomp": validate_elektriciteitsgebruik_warmtepomp,  # E501
    "validate_zon_opwek_totaal": validate_zon_opwek_totaal,
    "validate_elektriciteitsgebruik_wtw": validate_elektriciteitsgebruik_wtw,
    "validate_elektriciteitsgebruik_booster": validate_elektriciteitsgebruik_booster,  # E501
    "validate_elektriciteitsgebruik_boilervat": validate_elektriciteitsgebruik_boilervat,  # E501
    "validate_elektriciteitsgebruik_huishoudelijk": validate_elektriciteitsgebruik_huishoudelijk,  # E501
    "validate_energiegebruik_warmteopwekker": validate_energiegebruik_warmteopwekker,  # E501
    "validate_warmteproductie_warmtepomp": validate_warmteproductie_warmtepomp,
    "validate_watergebruik_warm_tapwater": validate_watergebruik_warm_tapwater,
    "validate_approximately_one_year_of_records": validate_approximately_one_year_of_records,  # E501
    "validate_columns_exist": validate_columns_exist,
    "validate_no_readingdate_gap": validate_no_readingdate_gap,
}

for col in cumulative_columns:

    def validate_no_outliers_negative_cumulative_diff(
        df: DataFrame,
        cum_col=col,
    ) -> bool:
        return all(
            df["validate_" + cum_col + "Diff"].isna()
            | df["validate_" + cum_col + "Diff"],
        )

    dataset_flag_conditions["validate_" + col + "Diff"] = (
        validate_no_outliers_negative_cumulative_diff
    )
