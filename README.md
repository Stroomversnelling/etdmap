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

## Installation

Normally, one would use the `etdanalyze` and `etdtranform` packages and install their requirements, which will automatically install `etdmap`. 

However, if you want to use this package on its own, you can install it with pip:

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

## Use

`etdmap` is a package that provides data mapping functionalities for energy-related datasets. It includes functions to map and transform data according to specific schemas, ensuring consistency across different sources. It also includes some utility variables like `cumulative_columns`, which can be used in other packages.

### Column definitions and thresholds

```python
from etdmap.data_model import cumulative_columns, model_column_order, model_column_type

# Use cumulative_columns in your code
print(cumulative_columns)
```

Other useful definitions include `model_column_order`, which defines the order of columns in a model, and `model_column_type`, which provides the Pandas series types for each column. 

The function `load_thresholds()` loads predefined thresholds used in record validation. These include thresholds for cumulative (annual) values as well as those for 5 minute intervals and instantaneous meassurements.

```python
from etdmap.record_validators import load_thresholds

thresholds_df = load_thresholds()
```

### Managing the dataset index

`index_helpers.py` contains the functions used to manage metadata and add files to the index. It also has functions for managing household, project metadata, and BSV metdata.

### Validators

There are various validators available. There are two major categories:

1. **Dataset validators**: These are used to validate the entire dataset, including checks on cumulative columns and differences between consecutive records.
2. **Record validators**: These are used to validate individual records, including checks on instantaneous measurements and 5-minute intervals.









