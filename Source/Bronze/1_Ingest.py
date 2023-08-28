# Databricks notebook source
# MAGIC %md
# MAGIC # Setup and Imports

# COMMAND ----------

#!pip install openpyxl

# COMMAND ----------

import requests
import pandas as pd
from pyspark import SparkFiles

# COMMAND ----------

from pyspark.sql.functions import col, lit, dense_rank, when
from pyspark.sql.types import ArrayType, StringType, BooleanType, IntegerType, FloatType, LongType, StructField, StructType
from pyspark.sql.window import Window
from pyspark.sql import DataFrame, Row
import sys

# COMMAND ----------

schema = "LION"

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE SCHEMA LION

# COMMAND ----------

# MAGIC %md
# MAGIC # Import from Github

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. NASA

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Metadata

# COMMAND ----------

url = "https://raw.githubusercontent.com/berserkhmdvhb/BatteryDatasets/main/Datasets/NASA/metadata.csv"
sc.addFile(url)
path  = SparkFiles.get("metadata.csv")
metadata_df = spark.read.csv("file://" + path, header = True, inferSchema = True)
profiles = [row.type for row in metadata_df.select('type').collect()]
ambient_temperatures = [row.ambient_temperature for row in metadata_df.select('ambient_temperature').collect()] 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Batteries Files

# COMMAND ----------

base_url = "https://github.com/berserkhmdvhb/BatteryDatasets/blob/main/Datasets/NASA/data/"
repo = "https://raw.githubusercontent.com/berserkhmdvhb/BatteryDatasets/main/"
response = requests.get(base_url)
assert response.status_code == 200
directories = response.json()['payload']['tree']['items']
csv_dirs = [repo + directories[i]['path'] for i in range(len(directories))]
file_names = [directories[i]['name'] for i in range(len(directories))]

# initialize discharge dataframe
url = csv_dirs[0]
sc.addFile(url)
path = SparkFiles.get(file_names[0])
batteries_discharge = (spark
                        .read
                        .csv("file://" + path, header = True, inferSchema = True)
                        .withColumn("Profile", lit(profiles[0]))
                        .withColumn("Idx", lit(0))
                        .withColumn("file", lit(file_names[0]))
                        .withColumn("ambient_temperature", lit(ambient_temperatures[0]))
                        
)
# initialize impedance dataframe
url = csv_dirs[1]
sc.addFile(url)
path = SparkFiles.get(file_names[1])
batteries_impedance = (spark
                        .read
                        .csv("file://" + path, header = True, inferSchema = True)
                        .withColumn("Profile", lit(profiles[1]))
                        .withColumn("Idx", lit(0))
                        .withColumn("file", lit(file_names[1]))
                        .withColumn("ambient_temperature", lit(ambient_temperatures[1]))
                        
)
# initialize charge dataframe
url = csv_dirs[2]
sc.addFile(url)
path = SparkFiles.get(file_names[2])
batteries_charge = (spark
                        .read
                        .csv("file://" + path, header = True, inferSchema = True)
                        .withColumn("Profile", lit(profiles[2]))
                        .withColumn("Idx", lit(0))
                        .withColumn("file", lit(file_names[2]))
                        .withColumn("ambient_temperature", lit(ambient_temperatures[2]))
                        
) 

# COMMAND ----------

 
# integrate all data on each profile dataframe
for i in range(3, len(directories)):
    url = csv_dirs[i]
    if url[-4:] == ".csv":
        sc.addFile(url)
        path  = SparkFiles.get(file_names[i])
        df_temp = (spark
                    .read
                    .csv("file://" + path, header = True, inferSchema = True)
                    .withColumn("Profile", lit(profiles[i]))
                    .withColumn("Idx", lit(i))
                    .withColumn("file", lit(file_names[i]))
                    .withColumn("ambient_temperature", lit(ambient_temperatures[i]))
        )
        if profiles[i] == "discharge":
            batteries_discharge = batteries_discharge.union(df_temp)
        elif profiles[i] == "charge":
            batteries_charge = batteries_charge.union(df_temp)
        elif profiles[i] == "impedance":
            batteries_impedance = batteries_impedance.union(df_temp)
          

# COMMAND ----------

# MAGIC %md
# MAGIC ### Store as Delta Tables

# COMMAND ----------

batteries_impedance = batteries_impedance.select(col("Sense_current").alias("SenseCurrent"), 
                           col("Battery_current").alias("BatteryCurrent"), 
                           col("Current_ratio").alias("CurrentRatio"), 
                           col("Battery_impedance").alias("BatteryImpedance"), 
                           col("Rectified_Impedance").alias("RectifiedImpedance"),
                           "Profile", 
                           "Idx", 
                           col("file").alias("FileName"), 
                           col("ambient_temperature").alias("AmbientTemperature")
                           )

batteries_charge = batteries_charge.select(col("Voltage_measured").alias("VoltageMeasured"), 
                           col("Current_measured").alias("CurrentMeasured"), 
                           col("Temperature_measured").alias("TemperatureMeasured"), 
                           col("Current_charge").alias("CurrentCharge"), 
                           col("Voltage_charge").alias("VoltageCharge"),
                           col("Time").alias("Duration"), 
                           "Profile", 
                           "Idx", 
                           col("file").alias("FileName"), 
                           col("ambient_temperature").alias("AmbientTemperature")
                           )

batteries_discharge = batteries_discharge.select(col("Voltage_measured").alias("VoltageMeasured"), 
                           col("Current_measured").alias("CurrentMeasured"), 
                           col("Temperature_measured").alias("TemperatureMeasured"), 
                           col("Current_load").alias("CurrentLoad"), 
                           col("Voltage_load").alias("VoltageLoad"),
                           col("Time").alias("Duration"), 
                           "Profile", 
                           "Idx", 
                           col("file").alias("FileName"), 
                           col("ambient_temperature").alias("AmbientTemperature")
                           )


# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC distinct_values = batteries_charge.select("Idx").distinct().orderBy(col("Idx"))
# MAGIC distinct_values = distinct_values.withColumn("MappedIdx", dense_rank().over(windowSpec))
# MAGIC OldIdx = [row.Idx for row in distinct_values.select('Idx').collect()]
# MAGIC NewIdx = [row.MappedIdx for row in distinct_values.select('MappedIdx').collect()]
# MAGIC MapDict = {}
# MAGIC for key, value in zip(OldIdx, NewIdx):
# MAGIC     MapDict[key] = value
# MAGIC mapping_df = spark.createDataFrame([(k, v) for k, v in MapDict.items()], ["OldIdx", "NewIdx"])
# MAGIC result_df = batteries_charge.join(mapping_df, batteries_charge["Idx"] == mapping_df["OldIdx"], "left_outer")
# MAGIC test = result_df.withColumn("Idx", when(mapping_df["NewIdx"].isNotNull(), mapping_df["NewIdx"]).otherwise(col("Idx")))
# MAGIC ```

# COMMAND ----------

batteries_impedance.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Bronze_NASA_impedance')
batteries_charge.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Bronze_NASA_charge')
batteries_discharge.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Bronze_NASA_discharge')

# COMMAND ----------

del batteries_impedance, batteries_charge, batteries_discharge

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Stanford

# COMMAND ----------

base_url = "https://github.com/berserkhmdvhb/BatteryDatasets/blob/main/Datasets/Stanford/galvanostatic_discharge_test/"
repo = "https://raw.githubusercontent.com/berserkhmdvhb/BatteryDatasets/main/"
response = requests.get(base_url)

directories = response.json()['payload']['tree']['items']
dirs = [repo + directories[i]['path'] for i in range(len(directories) - 1)]
manufacturers = [directories[i]['name'] for i in range(len(directories) -1)]

# COMMAND ----------

#LFP
LFP_url = base_url + manufacturers[0] + "/"
response = requests.get(LFP_url)
directories = response.json()['payload']['tree']['items']
dirs = [repo + directories[i]['path'] for i in range(len(directories))]
temperatures = [directories[i]['name'] for i in range(len(directories))]

## all temperatures
LFP_temp_url = LFP_url + temperatures[0] + "/"
response = requests.get(LFP_temp_url)
directories = response.json()['payload']['tree']['items']
xlsx_dirs = [repo + directories[i]['path'] for i in range(len(directories))]
file_names = [directories[i]['name'] for i in range(len(directories))]
url = xlsx_dirs[0]
sc.addFile(url)
path = SparkFiles.get(file_names[0])
LFP_df = (spark.createDataFrame(pd.read_excel(path))
            .withColumn("Manufacturer", lit(manufacturers[0]))
            .withColumn("Temperature", lit(temperatures[0]))
            .withColumn("Idx", lit(0))
            .withColumn("file", lit(file_names[0]))
)
emptyRDD = spark.sparkContext.emptyRDD()
LFP_df = spark.createDataFrame(emptyRDD, LFP_df.schema)

for i in range(0, len(temperatures)):
    print(temperatures[i])
    LFP_temp_url = LFP_url + temperatures[i] + "/"
    response = requests.get(LFP_temp_url)
    directories = response.json()['payload']['tree']['items']
    xlsx_dirs = [repo + directories[i]['path'] for i in range(len(directories))]
    file_names = [directories[i]['name'] for i in range(len(directories))]
    for j in range(0, len(directories)):
        url = xlsx_dirs[j]
        if url[-5:] == ".xlsx":
            sc.addFile(url)
            path = SparkFiles.get(file_names[j])
            df_temp = (spark.createDataFrame(pd.read_excel(path))
                    .withColumn("Manufacturer", lit(manufacturers[0]))
                    .withColumn("Temperature", lit(temperatures[i]))
                    .withColumn("Idx", lit(int(f"{i}" + f"{j}")))
                    .withColumn("file", lit(file_names[j]))
                    )
            LFP_df = LFP_df.union(df_temp)

# COMMAND ----------

LFP_df = LFP_df.select(col("Date_Time").alias("Time"), 
              col("Test_Time(s)").alias("TestTime"), 
              col("Step_Time(s)").alias("StepTime"),
              col("Step_Index").alias("StepIndex"),
              col("Voltage(V)").alias("Voltage"), 
              col("Current(A)").alias("Current"),
              col("Surface_Temp(degC)").alias("SurfaceTemperature"),
              "Temperature", 
              "Idx", 
              col("file").alias("FileName")             
              )

# COMMAND ----------

LFP_df.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Bronze_Stf_LFP_discharge')

# COMMAND ----------

del LFP_df

# COMMAND ----------

base_url = "https://github.com/berserkhmdvhb/BatteryDatasets/blob/main/Datasets/Stanford/galvanostatic_discharge_test/"
repo = "https://raw.githubusercontent.com/berserkhmdvhb/BatteryDatasets/main/"
response = requests.get(base_url)

directories = response.json()['payload']['tree']['items']
dirs = [repo + directories[i]['path'] for i in range(len(directories) - 1)]
manufacturers = [directories[i]['name'] for i in range(len(directories) -1)]

# COMMAND ----------

#NCA
NCA_url = base_url + manufacturers[1] + "/"
response = requests.get(NCA_url)
directories = response.json()['payload']['tree']['items']
dirs = [repo + directories[i]['path'] for i in range(len(directories))]
temperatures = [directories[i]['name'] for i in range(len(directories))]

## all temperatures
NCA_temp_url = NCA_url + temperatures[0] + "/"
response = requests.get(NCA_temp_url)
directories = response.json()['payload']['tree']['items']
xlsx_dirs = [repo + directories[i]['path'] for i in range(len(directories))]
file_names = [directories[i]['name'] for i in range(len(directories))]
url = xlsx_dirs[0]
sc.addFile(url)
path = SparkFiles.get(file_names[0])
NCA_df = (spark.createDataFrame(pd.read_excel(path))
            .withColumn("Manufacturer", lit(manufacturers[1]))
            .withColumn("Temperature", lit(temperatures[0]))
            .withColumn("Idx", lit(0))
            .withColumn("file", lit(file_names[0]))
)
emptyRDD = spark.sparkContext.emptyRDD()
NCA_df = spark.createDataFrame(emptyRDD, NCA_df.schema)

for i in range(0, len(temperatures)):
    print(temperatures[i])
    NCA_temp_url = NCA_url + temperatures[i] + "/"
    response = requests.get(NCA_temp_url)
    directories = response.json()['payload']['tree']['items']
    xlsx_dirs = [repo + directories[i]['path'] for i in range(len(directories))]
    file_names = [directories[i]['name'] for i in range(len(directories))]
    for j in range(0, len(directories)):
        url = xlsx_dirs[j]
        if url[-5:] == ".xlsx":
            sc.addFile(url)
            path = SparkFiles.get(file_names[j])
            df_temp = (spark.createDataFrame(pd.read_excel(path))
                    .withColumn("Manufacturer", lit(manufacturers[1]))
                    .withColumn("Temperature", lit(temperatures[i]))
                    .withColumn("Idx", lit(int(f"{i}" + f"{j}")))
                    .withColumn("file", lit(file_names[j]))
                    )
            NCA_df = NCA_df.union(df_temp)

# COMMAND ----------

NCA_df = NCA_df.select(col("Date_Time").alias("Time"), 
              col("Test_Time(s)").alias("TestTime"), 
              col("Step_Time(s)").alias("StepTime"),
              col("Step_Index").alias("StepIndex"),
              col("Voltage(V)").alias("Voltage"), 
              col("Current(A)").alias("Current"),
              col("Surface_Temp(degC)").alias("SurfaceTemperature"),
              "Temperature", 
              "Idx", 
              col("file").alias("FileName")             
              )

# COMMAND ----------

NCA_df.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Bronze_Stf_NCA_discharge')

# COMMAND ----------

del NCA_df

# COMMAND ----------

base_url = "https://github.com/berserkhmdvhb/BatteryDatasets/blob/main/Datasets/Stanford/galvanostatic_discharge_test/"
repo = "https://raw.githubusercontent.com/berserkhmdvhb/BatteryDatasets/main/"
response = requests.get(base_url)

directories = response.json()['payload']['tree']['items']
dirs = [repo + directories[i]['path'] for i in range(len(directories) - 1)]
manufacturers = [directories[i]['name'] for i in range(len(directories) -1)]

# COMMAND ----------

#NMC
NMC_url = base_url + manufacturers[2] + "/"
response = requests.get(NMC_url)
directories = response.json()['payload']['tree']['items']
dirs = [repo + directories[i]['path'] for i in range(len(directories))]
temperatures = [directories[i]['name'] for i in range(len(directories))]

## all temperatures
NMC_temp_url = NMC_url + temperatures[0] + "/"
response = requests.get(NMC_temp_url)
directories = response.json()['payload']['tree']['items']
xlsx_dirs = [repo + directories[i]['path'] for i in range(len(directories))]
file_names = [directories[i]['name'] for i in range(len(directories))]
url = xlsx_dirs[0]
sc.addFile(url)
path = SparkFiles.get(file_names[0])
NMC_df = (spark.createDataFrame(pd.read_excel(path))
            .withColumn("Manufacturer", lit(manufacturers[2]))
            .withColumn("Temperature", lit(temperatures[0]))
            .withColumn("Idx", lit(0))
            .withColumn("file", lit(file_names[0]))
)
emptyRDD = spark.sparkContext.emptyRDD()
NMC_df = spark.createDataFrame(emptyRDD, NMC_df.schema)

for i in range(0, len(temperatures)):
    print(temperatures[i])
    NMC_temp_url = NMC_url + temperatures[i] + "/"
    response = requests.get(NMC_temp_url)
    directories = response.json()['payload']['tree']['items']
    xlsx_dirs = [repo + directories[i]['path'] for i in range(len(directories))]
    file_names = [directories[i]['name'] for i in range(len(directories))]
    for j in range(0, len(directories)):
        url = xlsx_dirs[j]
        if url[-5:] == ".xlsx":
            sc.addFile(url)
            path = SparkFiles.get(file_names[j])
            df_temp = (spark.createDataFrame(pd.read_excel(path))
                    .withColumn("Manufacturer", lit(manufacturers[2]))
                    .withColumn("Temperature", lit(temperatures[i]))
                    .withColumn("Idx", lit(int(f"{i}" + f"{j}")))
                    .withColumn("file", lit(file_names[j]))
                    )
            NMC_df = NMC_df.union(df_temp)
    

# COMMAND ----------

NMC_df = NMC_df.select(col("Date_Time").alias("Time"), 
              col("Test_Time(s)").alias("TestTime"), 
              col("Step_Time(s)").alias("StepTime"),
              col("Step_Index").alias("StepIndex"),
              col("Voltage(V)").alias("Voltage"), 
              col("Current(A)").alias("Current"),
              col("Surface_Temp(degC)").alias("SurfaceTemperature"),
              "Temperature", 
              "Idx", 
              col("file").alias("FileName")             
              )

# COMMAND ----------

NMC_df.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Bronze_Stf_NMC_discharge')

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE LION.Bronze_NASA_impedance

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE LION.Bronze_NASA_charge

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE LION.Bronze_NASA_discharge

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE LION.Bronze_Stf_LFP_discharge

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE LION.Bronze_Stf_NCA_discharge

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE LION.Bronze_Stf_NMC_discharge
