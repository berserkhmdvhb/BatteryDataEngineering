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

from pyspark.sql.functions import col, lit
from pyspark.sql.types import ArrayType, StringType, BooleanType, IntegerType, FloatType, LongType, StructField, StructType
from pyspark.sql import DataFrame, Row
import sys

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

# MAGIC %sql
# MAGIC --CREATE SCHEMA LION

# COMMAND ----------

schema = "LION"

# COMMAND ----------

batteries_impedance.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.NASA_batteries_impedance')
batteries_charge.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.NASA_batteries_charge')
batteries_discharge.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.NASA_batteries_discharge')

# COMMAND ----------

del batteries_impedance, batteries_charge, batteries_discharge

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stanford

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
                    .withColumn("Temperature", lit(temperatures[j]))
                    .withColumn("Idx", lit(int(f"{i}" + f"{j}")))
                    .withColumn("file", lit(file_names[j]))
                    )
            NCA_df = NCA_df.union(df_temp)
    

# COMMAND ----------

NCA_df.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Stanford_NCA_batteries_discharge')

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

NMC_df.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Stanford_NMC_batteries_discharge')

# COMMAND ----------


