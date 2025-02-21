# etdmap
__"Energietransitie Dataset" mapping package__

This package provides the required data model and helpers to map raw data to the `Energietransitie dataset` (ETD). The ETD is a model defining important variables for energy in the built environment, which are used to inform policy and planning decisions in the Netherlands. 

## Citation

If you use this package or any code in this package or refer to output, please use the following citations for attribution:

_Witkamp, Dickinson, Izeboud (2024). etdmap: A Python package for mapping raw data to the Energietransitie Dataset (ETD) model._


## The data model

The dataset includes two primary types of variables:  

1. **Metadata Variables**: Descriptive information about the project or house, such as identifiers, physical attributes, or systems.  
2. **Performance Variables**: Time-series data describing the energy performance or environmental conditions.

### Examples of Variables  

| **Category**     | **Variable Name**             | **Type**   | **Description**                                              |
| ---------------- | ----------------------------- | ---------- | ------------------------------------------------------------ |
| **Metadata**     | `ProjectIdLeverancier`        | String     | Unique identifier for the project assigned by the data provider. |
|                  | `Bouwjaar`                    | Integer    | Construction year of the house.                              |
|                  | `WarmteopwekkerCategorie`     | String     | Type of heating system (e.g., hybrid heat pump).             |
|                  | `Oppervlakte`                 | Integer    | Surface area in square meters.                               |
| **Performance**  | `ReadingDate`                 | Date       | Timestamp of the reading (Format: YYYY-MM-DD HH:MM:SS).      |
|                  | `ElektriciteitNetgebruikHoog` | Number     | Electricity consumption at high tariff (kWh, cumulative).    |
|                  | `TemperatuurWoonkamer`        | Number     | Current living room temperature in degrees Celsius.          |
|                  | `Zon-opwekTotaal`             | Number     | Total solar energy generated (kWh, cumulative).              |

This categorization ensures the dataset is comprehensive for energy-related policy and planning while maintaining clarity for data contributors and users.

See the section on data processing and mapping to learn more about how to access this metadata in a python script.

## Installation

_Note: If only using ETD datasets for analysis, one would use the `etdanalyze` and `etdtranform` packages and install their requirements, which will automatically install `etdmap`._

To use this package on its own and map raw data in a workflow, you can install it with pip:

```bash
git clone https://github.com/Stroomversnelling/etdmap.git
cd etdmap
pip install .
```

### Developing and contributing

If you would like to contribute to the package code, we would create an environment and install it in editable mode:

```bash	
git clone https://github.com/Stroomversnelling/etdmap.git
cd etdmap
python -m venv .venv
source .venv/bin/activate  # On Windows use `.venv\Scripts\activate`
pip install -e .
```

## Data processing and mapping

`etdmap` is a package that provides data mapping functionalities for energy-related datasets. It includes functions to map and transform data according to specific schemas, ensuring consistency across different sources. It also includes some utility variables like `cumulative_columns`, which can be used.

The process of mapping raw data files to the ETD model involves several key functions. Here's an overview of the main functions used in this process.

### Data model and column definitions and thresholds

_Note: This API is relatively stable but subject to change as it is still under development. Please follow our repository or, if necessary, use a tagged releases to ensure stability._

From `data_model.py`:

1. `load_thresholds()`: 
   - Purpose: Loads predefined thresholds for data validation. These include thresholds for cumulative (annual) values as well as those for 5 minute intervals and instantaneous meassurements.
   - Description: Reads a CSV file into a Pandas dataframe containing threshold values for various variables used in data validation.

2. `load_etdmodel()`: 
   - Purpose: Loads the ETD model specification.
   - Description: Reads a CSV file into a Pandas dataframe that defines the structure and requirements of the ETD model.

```python
from etdmap.record_validators import load_thresholds
thresholds_df = load_thresholds()
```

Additionally, there are some other dictionaries and lists that are currently used such as a list of `cumulative_columns` that can be used in other packages. Other useful definitions include `model_column_order`, which defines the order of columns in a model, and `model_column_type`, which provides the Pandas series types for each column.

These are defined in `data_model.py`: 

```python
from etdmap.data_model import cumulative_columns, model_column_order, model_column_type
```

### Managing the dataset index

`index_helpers.py` contains the functions used to manage metadata and add files to the index. It also has functions for managing household, project metadata, and BSV metdata.

From `index_helpers.py`:

1. `read_index()`: 
   - **Purpose**: Reads the current index file.
   - **Description**: Loads the existing index from a parquet file or creates a new one if it doesn't exist.

2. `get_household_id_pairs()`: 
   - **Purpose**: Generates pairs of HuisIdBSV and filenames in order to maintain unique ids in the dataset.
   - **Description**: Creates a list of tuples containing HuisIdBSV and corresponding filenames for new and existing entries.

3. `update_index()`: 
   - **Purpose**: Updates the index with new entries.
   - **Description**: Adds new entries to the index and recalculates or adds flag columns for dataset validators.

4. `add_supplier_metadata_to_index()`: 
   - **Purpose**: Adds metadata columns to the index.
   - **Description**: Updates the index with additional metadata from the supplier, matching on the HuisIdLeverancier column.

### Mapping raw data to the model specification

From `mapping_helpers.py`:

1. `rearrange_model_columns()`: 
   - **Purpose**: Ensures all required columns are present and in the correct order.
   - **Description**: Adds missing columns, fills them with NA values, and arranges columns according to the model specification.

2. `add_diff_columns()`: 
   - **Purpose**: Calculates difference columns for cumulative variables.
   - **Description**: Computes the difference between consecutive readings for cumulative variables and adds these as new columns.

3. `ensure_intervals()`: 
   - **Purpose**: Ensures consistent time intervals in the data. Defaults are `date_column = 'ReadingDate', freq='5min'`
   - **Description**: Adds missing timestamps to ensure a consistent time series, typically with 5-minute or 15-minute intervals.

4. `fill_down_infrequent_devices()`: 
   - **Purpose**: Fills gaps in infrequently updated device data. At the moment, this defaults to: 'ElektriciteitsgebruikBoilervat', 'ElektriciteitsgebruikRadiator', 'ElektriciteitsgebruikBooster'. This can be changed based on data source requirements.
   - **Description**: For certain columns that update infrequently, this function fills down the last known value.

5. `validate_cumulative_variables()`:
   - **Purpose**: Validates cumulative variables in a DataFrame for various quality checks.
   - **Description**:

   - *Time gap check*: Ensures there are no gaps in readings greater than a specified time delta (default is 1 hour).
   - *Negative difference check*: Verifies that cumulative values are not decreasing over time.
   - *Unexpected zero check*: Identifies and flags unexpected zero values in cumulative readings.
   - *Data availability check*: Ensures that at least a specified percentage of values (default 90%) are not NA.

### Alignment of clocks from different devices and merging data (ALPHA - untested code)

There are functions in `mapping_clock_helpers.py` that are setup to help align clocks from multiple devices and address situations where readings are spaced out in different intervals during the mapping process.

### Validators

There are various validators available. There are two major categories:

1. **Dataset validators**: These are used to validate the entire dataset, including checks on cumulative columns and differences between consecutive records.
2. **Record validators**: These are used to validate individual records, including checks on instantaneous measurements and 5-minute intervals.

## Guidance for suppliers of data and for data preparation

### Prepping raw data

Use the `Vereist` (`required`) column in the data model in order to check if your metadata and your raw data from device readings are complete and correct. These are the currently required columns (as of 20 Feb 2025):

| Entiteit      | Variabele                       | Key         | Definitie                                                                                                                          | Type variabele | Vereist     | Resolutie    | Wie vult?       | Bron                                  | AVG gevoelig |
| ------------- | ------------------------------- | ----------- | ---------------------------------------------------------------------------------------------------------------------------------- | -------------- | ----------- | ------------ | --------------- | ------------------------------------- | ------------ |
| Metadata      | ProjectIdLeverancier            | nee         | code toegekend door dataleverancier                                                                                                | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | ja           |
| Metadata      | HuisIdLeverancier               | ja          | code toegekend door dataleverancier                                                                                                | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | ja           |
| Metadata      | Weerstation                     | nee         | dichtstbijzijnde KNMI weerstation                                                                                                  | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | nee          |
| Metadata      | Oppervlakte                     | nee         | Ag, oftewel m2 gebruiksoppervlak                                                                                                   | integer        | ja          | vaste waarde | Dataleverancier | BAG / EP-online / Energielabel / Vabi | nee          |
| Metadata      | Compactheid                     | nee         | Als/Ag, oftewel m2 verliesoppervlak / m2 gebruiksoppervlak                                                                         | number         | ja          | vaste waarde | Dataleverancier | EP-online / Energielabel              | nee          |
| Metadata      | Warmtebehoefte                  | nee         | kWh_th/m2/jr, volgens NTA8800                                                                                                      | number         | ja          | vaste waarde | Dataleverancier | EP-online / Energielabel              | nee          |
| Metadata      | PrimairFossielGebruik           | nee         | kWh/m2/jr, volgens NTA8800                                                                                                         | number         | ja          | vaste waarde | Dataleverancier | EP-online / Energielabel              | nee          |
| Metadata      | Bouwjaar                        | nee         | Oorspronkelijk bouwjaar                                                                                                            | integer        | ja          | vaste waarde | Dataleverancier | BAG/Vabi                              | nee          |
| Metadata      | Renovatiejaar                   | nee         | Jaar van renovatie                                                                                                                 | integer        | ja          | vaste waarde | Dataleverancier | Vabi                                  | nee          |
| Metadata      | WoningType                      | nee         | Wat voor woningbouw is het? Kiezen uit: EGW, MGW                                                                                   | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | nee          |
| Metadata      | WarmteopwekkerCategorie         | nee         | Hoe wordt verwarmd? Kiezen uit: Hybride warmtepomp, Lucht-water warmtepomp, Water-water warmtepomp, Lucht-lucht warmtepomp, Anders | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | nee          |
| Metadata      | Warmteopwekker                  | nee         | Hoe wordt verwarmd? Kiezen uit: Collectief, Individueel                                                                            | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | nee          |
| Metadata      | Ventilatiesysteem               | nee         | Hoe wordt geventileerd? Kiezen uit: Natuurlijke ventilatie, Type C, Type D, Type E                                                 | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | nee          |
| Metadata      | Kookinstallatie                 | nee         | Hoe wordt gekookt? Kiezen uit: Gas, Inductie, Anders                                                                               | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | nee          |
| Metadata      | EPV                             | nee         | Wordt EPV geïnd? Kiezen uit: EPV 1.0, EPV 2.0 Basis, EPV 2.0 Hoogwaardig, Nee, Niet bekend                                         | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | nee          |
| Prestatiedata | HuisIdLeverancier               | ja          | code toegekend door dataleverancier                                                                                                | string         | ja          | vaste waarde | Dataleverancier | Dataleverancier                       | ja           |
| Prestatiedata | ReadingDate                     | nee         | Format: YYYY-MM-DD HH:MM:SS                                                                                                        | date           | ja          | 5 minuten    | Dataleverancier | Gateway / Portal                      | ja           |
| Prestatiedata | ElektriciteitNetgebruikHoog     | nee         | kWh (cumulatief)                                                                                                                   | number         | ja          | 5 minuten    | Dataleverancier | Slimme meter                          | ja           |
| Prestatiedata | ElektriciteitNetgebruikLaag     | nee         | kWh (cumulatief)                                                                                                                   | number         | ja          | 5 minuten    | Dataleverancier | Slimme meter                          | ja           |
| Prestatiedata | ElektriciteitTerugleveringHoog  | nee         | kWh (cumulatief)                                                                                                                   | number         | ja          | 5 minuten    | Dataleverancier | Slimme meter                          | ja           |
| Prestatiedata | ElektriciteitTerugleveringLaag  | nee         | kWh (cumulatief)                                                                                                                   | number         | ja          | 5 minuten    | Dataleverancier | Slimme meter                          | ja           |
| Prestatiedata | ElektriciteitsgebruikWarmtepomp | nee         | kWh (cumulatief)                                                                                                                   | number         | ja          | 5 minuten    | Dataleverancier | Gateway / Portal                      | ja           |
| Prestatiedata | Zon-opwekTotaal                 | nee         | kWh (cumulatief)                                                                                                                   | number         | ja          | 5 minuten    | Dataleverancier | Gateway / Portal                      | ja           |

#### Validation of columns

One should check the stats for each raw data column using the `collect_column_stats()` function to ensure that the data meets expected statistical properties. The statistics include:

   - 'Identifier': The identifier for the dataset.
   - 'column': The name of the column.
   - 'type': The data type of the column.
   - 'count': The number of non-null values in the column.
   - 'missing': The number of missing values in the column.
   - 'min': The minimum value in the column, if applicable.
   - 'max': The maximum value in the column, if applicable.
   - 'mean': The mean value in the column, if applicable.
   - 'median': The median value in the column, if applicable.
   - 'iqr': The interquartile range (IQR) of the column, if applicable.
   - 'quantile_25': The 25th percentile value in the column, if applicable.
   - 'quantile_75': The 75th percentile value in the column, if applicable.
   - 'top5': A dictionary with the top 5 most frequent values and their counts, if applicable.

This helps to validate whether or not a particular household/unit has sufficient data of sufficient quality to include in the mapped datasets.

#### Validation of cumulative columns

The `etdmap` package provides helpers in `mapping_helpers.py` to enable the processing and validation of columns. 
Cumulative columns are processsed and also validated using the `add_diff_columns()` function. It produces a `diff`, a variable with the incremental increase in the cumulative variable and expects cumulative variables to monotonically increase. In other words, they should and never decrease. This assumption might be incorrect in cases where meter readings naturally decrease and in those cases, custom logic may need to be written.

By default, `add_diff_columns()` relies on the function `validate_cumulative_variables()` to ensure that cumulative variables meet specific conditions:
- 90% available data
- Max gap of 1 hour of consecutive missing data
- No "unexpected" zero values in cumulative data
- No decreasing cumulative values

The `validate_cumulative_variables` function checks for logical consistency in cumulative variables but may not cover all edge cases or handle complex data scenarios effectively. To customize this validation behaviour, one may write a custom wrapper function or new function with different checks and pass it to `add_diff_columns()` using the `validate_func` parameter. If this validation is not correctly implemented, problems may not be reported. If `drop_unvalidated` is `True` then function may drop valid data or keep invalid data. By default, `drop_unvalidated` is `False`: data which does not pass these tests is kept but problems are reported in the log. 

_It is important to manually check the logs when adding new datasets. In addition, one should check the stats for each mapped column using the `get_mapped_data_stats()` function to ensure that the data meets the expected statistical properties._

#### Filling data values for devices which report infrequently

Sometimes devices are simply not reporting because they are inactive.  In order to ensure the column is not seen as mostly 'missing' data and rather as an unchanging value, the `fill_down_infrequent_devices()` function uses forward fill followed by backward fill to impute missing values for specified columns. 

_This approach may not be suitable if devices if there are underlying issues with the device data, which would be amplified or ignored, especially if the data source is misbehaving. We recommend visually inspecting these columns / data before applying the function._

#### Time Series Consistency

At the moment, custom logic has been used for each data source to ensure the data provided is available with a fixed interval (typically 5 minutes). _Example logic has been added to the file `mapping_clock_helpers.py`. It is alpha quality/pseudo code and should not be used at the moment. This should be tested and available to use in a subsequent release._

After the data has fixed (5 min) interval, the `ensure_intervals()` function attempts to ensure a consistent time series by adding missing intervals and removing excess records for the same moment. This process can lead to incorrect data if the raw data does not naturally fit into the expected frequency.

### Specific function notes

#### `add_diff_columns()`

1. **Negative differences**:
   - Assumes that negative differences in cumulative columns are erroneous and attempts to correct them by setting values to `pd.NA`. This assumption might not hold true if the meter readings naturally decrease or if there are legitimate zero jumps.
   
2. **Error handling**:
   - Logs errors when it encounters negative differences after corrections but does not address underlying issues in the data collection process.

3. **Handling of large gaps without rates of change**:
   - Does not consider the time elapsed between readings, which can lead to incorrect assumptions about the nature of the data.
   - When there are larger gaps in the `diff` data (missing intermediate readings), the function may incorrectly assume that the next available reading is a continuation of the meter reading when perhaps a meter reset has taken place and no cumulative values have been collected for some time (bringing the reading back to the original reading level). If there is a suspicion of these errors, custom logic will need to be added and some assumption about the rate of change in the cumulative variable.

4. **Known edge cases**:
   - Edge cases where multiple negative dips occur consecutively or where there is a significant pause in data collection are not fully addressed.

#### `fill_down_infrequent_devices()`

1. **Imputation Strategy**:
   - Uses forward fill followed by backward fill to impute missing values, which can lead to inaccuracies if the device reports irregularly or if there are underlying issues with data collection.
   
2. **Assumption of continuity**:
   - Assumes that missing values should be filled based on the last available reading, which might not be appropriate for devices that report infrequently or have non-continuous usage patterns.

3. **Final filling with zeroes**:
   - Fills column with zeros if all values are NA. This assumption may not be correct if the device simply was not reporting.

4. **Column specificity**:
   - Processes specified variables by default: `ElektriciteitsgebruikBoilervat`, `ElektriciteitsgebruikRadiator`, `ElektriciteitsgebruikBooster`. Can handle other columns if passed as an argument. Some datasets may require different processing.

#### `ensure_intervals()`

1. **Frequency assumption**:
   - Assumes a fixed frequency for the time series data (e.g., 5-minute intervals) and the only check it does is for the number of expected records. If the raw data does not naturally fit into this frequency the errors may occur as additional pre-processing is required.
   
2. **Data source issues**:
   - Does not handle cases where the data source is misbehaving, such as reporting data at irregular intervals or with incorrect timestamps.

3. **Adding vs. removing records**:
   - Adds missing records to ensure consistent intervals but does not verify the correctness of these retained records where removed. It will report inconsistency with respect to the number of records expected.
   
4. **Excess records**:
   - Attempts to reduce excess records by performing a left merge, which might lead to data loss if the excess records contain valid information.

#### `rearrange_model_columns()`

1. **Column order**:
   - Ensures that the DataFrame columns are in a specific order with additional columns left at the end/right of the dataset.





