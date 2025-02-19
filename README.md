# etdmap
__"Energietransitie Dataset" mapping package__

This package provides the required data model and helpers to map raw data to the `Energietransitie dataset` (ETD). The ETD is a model defining important variables for energy in the built environment, which are used to inform policy and planning decisions in the Netherlands. 

## The data model

The dataset includes two primary types of variables:  

1. **Metadata Variables**: Descriptive information about the project or house, such as identifiers, physical attributes, or systems.  
2. **Performance Variables**: Time-series data describing the energy performance or environmental conditions.

### Examples of Variables  

| **Category**     | **Variable Name**             | **Type**   | **Description**                                              |  
|-------------------|-------------------------------|------------|--------------------------------------------------------------|  
| **Metadata**      | `ProjectIdLeverancier`       | String     | Unique identifier for the project assigned by the data provider. |  
|                   | `Bouwjaar`                   | Integer    | Construction year of the house.                              |  
|                   | `WarmteopwekkerCategorie`    | String     | Type of heating system (e.g., hybrid heat pump).             |  
|                   | `Oppervlakte`                | Integer    | Surface area in square meters.                              |  
| **Performance**   | `ReadingDate`                | Date       | Timestamp of the reading (Format: YYYY-MM-DD HH:MM:SS).     |  
|                   | `ElektriciteitNetgebruikHoog`| Number     | Electricity consumption at high tariff (kWh, cumulative).   |  
|                   | `TemperatuurWoonkamer`       | Number     | Current living room temperature in degrees Celsius.         |  
|                   | `Zon-opwekTotaal`            | Number     | Total solar energy generated (kWh, cumulative).             |  

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
   - Purpose: Reads the current index file.
   - Description: Loads the existing index from a parquet file or creates a new one if it doesn't exist.

2. `get_household_id_pairs()`: 
   - Purpose: Generates pairs of HuisIdBSV and filenames in order to maintain unique ids in the dataset.
   - Description: Creates a list of tuples containing HuisIdBSV and corresponding filenames for new and existing entries.

3. `update_index()`: 
   - Purpose: Updates the index with new entries.
   - Description: Adds new entries to the index and recalculates or adds flag columns for dataset validators.

4. `add_supplier_metadata_to_index()`: 
   - Purpose: Adds metadata columns to the index.
   - Description: Updates the index with additional metadata from the supplier, matching on the HuisIdLeverancier column.

### Mapping raw data to the model specification

From `mapping_helpers.py`:

1. `rearrange_model_columns()`: 
   - Purpose: Ensures all required columns are present and in the correct order.
   - Description: Adds missing columns, fills them with NA values, and arranges columns according to the model specification.

2. `add_diff_columns()`: 
   - Purpose: Calculates difference columns for cumulative variables.
   - Description: Computes the difference between consecutive readings for cumulative variables and adds these as new columns.

3. `ensure_intervals()`: 
   - Purpose: Ensures consistent time intervals in the data. Defaults are `date_column = 'ReadingDate', freq='5min'`
   - Description: Adds missing timestamps to ensure a consistent time series, typically with 5-minute or 15-minute intervals.

4. `fill_down_infrequent_devices()`: 
   - Purpose: Fills gaps in infrequently updated device data. At the moment, this defaults to: 'ElektriciteitsgebruikBoilervat', 'ElektriciteitsgebruikRadiator', 'ElektriciteitsgebruikBooster'. This can be changed based on data source requirements.
   - Description: For certain columns that update infrequently, this function fills down the last known value.

5. `validate_cumulative_variables()`:
   - Purpose: Validates cumulative variables in a DataFrame for various quality checks.
   - Description:
       - Time gap check: Ensures there are no gaps in readings greater than a specified time delta (default is 1 hour).
       - Negative difference check: Verifies that cumulative values are not decreasing over time.
       - Unexpected zero check: Identifies and flags unexpected zero values in cumulative readings.
       - Data availability check: Ensures that at least a specified percentage of values (default 90%) are not NA.

### Alignment of clocks from different devices and merging data (ALPHA - untested code)

There are functions in `mapping_clock_helpers.py` that are setup to help align clocks from multiple devices and address situations where readings are spaced out in different intervals during the mapping process.

### Validators

There are various validators available. There are two major categories:

1. **Dataset validators**: These are used to validate the entire dataset, including checks on cumulative columns and differences between consecutive records.
2. **Record validators**: These are used to validate individual records, including checks on instantaneous measurements and 5-minute intervals.









