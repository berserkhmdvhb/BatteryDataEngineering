# Databricks notebook source
# MAGIC %md
# MAGIC # Setup and Imports

# COMMAND ----------

import requests
from pyspark import SparkFiles

# COMMAND ----------

from pyspark.sql.functions import col, lit
from pyspark.sql.types import ArrayType, StringType, BooleanType, IntegerType, FloatType, LongType
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
if response.status_code == 200:
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
    for i in range(3, len(directories)):
        url = csv_dirs[i]
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
