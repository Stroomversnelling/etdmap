# etdmap
__"Energietransitie Dataset" mapping package__

This package provides the required data model and helpers to map raw data to the `Energietransitie dataset` (ETD). The ETD is a model defining important variables for energy in teh built environment, which are used to inform policy and planning decisions in the Netherlands. 

## The data model

The dataset includes two primary types of variables:  

1. **Metadata Variables**: Descriptive information about the project or house, such as identifiers, physical attributes, or systems.  
2. **Performance Variables**: Time-series data describing the energy performance or environmental conditions.

### Examples of Variables  

| **Category**     | **Variable Name**             | **Type**   | **Description**                                              |  
|-------------------|-------------------------------|------------|--------------------------------------------------------------|  
| **Metadata**      | `ProjectId`                  | String     | Unique identifier for the project assigned by the data provider. |  
|                   | `Bouwjaar`                   | Integer    | Construction year of the house.                              |  
|                   | `WarmteopwekkerCategorie`    | String     | Type of heating system (e.g., hybrid heat pump).             |  
|                   | `Oppervlakte`                | Integer    | Surface area in square meters.                              |  

| **Category**     | **Variable Name**             | **Type**   | **Description**                                              |  
|-------------------|-------------------------------|------------|--------------------------------------------------------------|  
| **Performance**   | `ReadingDate`                | Date       | Timestamp of the reading (Format: YYYY-MM-DD HH:MM:SS).     |  
|                   | `ElektriciteitNetgebruikHoog`| Number     | Electricity consumption at high tariff (kWh, cumulative).   |  
|                   | `TemperatuurWoonkamer`       | Number     | Current living room temperature in degrees Celsius.         |  
|                   | `Zon-opwekTotaal`            | Number     | Total solar energy generated (kWh, cumulative).             |  

This categorization ensures the dataset is comprehensive for energy-related policy and planning while maintaining clarity for data contributors and users.

## Installation


## Use







