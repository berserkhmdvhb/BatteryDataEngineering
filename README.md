![Data Engineering](https://img.shields.io/badge/Data%20Engineering-grey_ETL-blue) 
![ETL](https://img.shields.io/badge/ETL-blue) 
![Dashboard](https://img.shields.io/badge/Dashboard-blue) 
![Data Pipeline](https://img.shields.io/badge/Data%20Pipeline-blue) 
![Analytics](https://img.shields.io/badge/Analytics-blue)

# BatteryDataEngineering <img src="logo2.png" align="right" style="width: 15%;"/>
This repo is dedicated to processing and engineering li-ion battery datasets. The metatdata for these sources can be found in [here](https://github.com/berserkhmdvhb/BatteryDataEngineering/tree/main/Metadata)
The datasets are stored in [Github repo](https://github.com/berserkhmdvhb/BatteryDatasets).
Uinsg Databricks, a pipeline is created to ingest data from the Github repo to cloud, then data is cleaned and wrangled, also additional features were added. Finally, data were aggregated to each cycle

# Pipeline

## Steps

1. **Ingest Data to Cloud:**
   For NASA, CSV files of all batteries were stored in 3 dataframes associated with 3 profiles, which are charge, discharge, and impedance. The metadata CSV was usefor automatic detection profile of each file. For Stanford, data were converted from XLSX to CSV using Spark, and then stored in 3 dataframes associated with 3 manufacturers. Also 3 different temperatures used for each file are stored as metadata. All data are stored as delta tables. Using Medallion Architecture, both data and code are organized. Raw tables are stored as BRONZE tables.

3. **Data Wrangling:** Numeric and string columns are cleaned. Missing and `NULL` values are handled. Some transformations and corrections were made to temperatures. The impedance dataframe contain complex numbers, which are handled by extracting imaginary and real parts, and also to enrich data with maginute and phase for all the complex numbers. All cleaned tables are stored as SILVER tables.

4. **Data Aggregation:** Using each battery data (temperature, current, voltage, ...), the statistical summary of each battery cycle (charge, discharge) are extracted and stored as GOLD tables.

Finally, SQL queries (defined [here](https://github.com/berserkhmdvhb/BatteryDataEngineering/blob/main/Source/SQL_queries.md)) are defined to provide dashboards in Databricks.


## Visualizations

### Diagram

![mermaid-diagram-2023-08-28-154209](https://github.com/berserkhmdvhb/BatteryDataEngineering/assets/48640037/432b38d3-958b-4e34-a041-2993dc372d82)

### DAG

<img width="1443" alt="Screenshot 2023-08-28 174348" src="https://github.com/berserkhmdvhb/BatteryDataEngineering/assets/48640037/fadc4d60-f7f1-4968-abce-90ca7cabfccc">


## Dashboard

<img width="1806" alt="Screenshot 2023-08-28 172600" src="https://github.com/berserkhmdvhb/BatteryDataEngineering/assets/48640037/671fcdfd-347a-44de-8889-8fb5bce1f086">


## Data Samples

### NASA

- Discharge

| VoltageMeasured |	CurrentMeasured |	TemperatureMeasured	| CurrentLoad	| VoltageLoad	| Duration |	
| --------------- | --------------- | ------------------- | ----------- | ----------- | -------- |
| 4.246764125510136 |	-0.0014110647362388163 | 6.234018870000787 |	0.0002 | 4.262 | 9.36	|

- Charge

| VoltageMeasured	| CurrentMeasured	| TemperatureMeasured	| CurrentCharge	| VoltageCharge	| Duration |	
| --------------- | --------------- | ------------------- | ------------- | ------------- | -------- |
| 3.746592135437914 |	1.4890569689379058 | 5.9925280590812635	| 1.4995 | 4.618 | 2.594000000000001 |

- Impedance

| SenseCurrent | BatteryCurrent | CurrentRatio | BatteryImpedance | RectifiedImpedance |
| ------------ | -------------- | ------------ | ---------------- | ------------------ | 
| ( (929.0427856445312-49.546653747558594j) | (227.34068298339844-68.11133575439453j) | (3.8098847365794053+0.9235024808405093j) | (0.22133620478684887+0.04668101354100503j) |	(0.17505020080728648-0.019801974566721604j)	) |

Links
[1](https://calce.umd.edu/battery-data)
