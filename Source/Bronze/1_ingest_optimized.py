# Databricks notebook source
# MAGIC %md
# MAGIC # Optimized Battery Dataset Ingestion

# COMMAND ----------
import requests
from urllib.parse import urljoin
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import SparkFiles
from functools import reduce
import pandas as pd

# COMMAND ----------
schema = "LION"

# COMMAND ----------
# Utility Functions

def fetch_github_tree(url: str) -> list:
    response = requests.get(url)
    response.raise_for_status()
    return response.json()['payload']['tree']['items']

def fetch_and_load_csv_to_df(url: str, filename: str, extra_columns: dict):
    sc.addFile(url)
    path = SparkFiles.get(filename)
    df = spark.read.csv(f"file://{path}", header=True, inferSchema=True)
    for col_name, col_val in extra_columns.items():
        df = df.withColumn(col_name, F.lit(col_val))
    return df

def fetch_and_load_xlsx_to_df(url: str, filename: str, extra_columns: dict):
    sc.addFile(url)
    path = SparkFiles.get(filename)
    df = spark.createDataFrame(pd.read_excel(path))
    for col_name, col_val in extra_columns.items():
        df = df.withColumn(col_name, F.lit(col_val))
    return df

def collect_column_values(df, column: str) -> list:
    return [row[column] for row in df.select(column).collect()]

# COMMAND ----------
# NASA Metadata
url = "https://raw.githubusercontent.com/berserkhmdvhb/BatteryDatasets/main/Datasets/NASA/metadata.csv"
sc.addFile(url)
path = SparkFiles.get("metadata.csv")
metadata_df = spark.read.csv(f"file://{path}", header=True, inferSchema=True)
profiles = collect_column_values(metadata_df, 'type')
ambient_temperatures = collect_column_values(metadata_df, 'ambient_temperature')

# COMMAND ----------
# NASA Files
base_url = "https://github.com/berserkhmdvhb/BatteryDatasets/blob/main/Datasets/NASA/data/"
repo = "https://raw.githubusercontent.com/berserkhmdvhb/BatteryDatasets/main/"
directories = fetch_github_tree(base_url)
csv_urls = [urljoin(repo, d["path"]) for d in directories if d["path"].endswith(".csv")]
file_names = [d["name"] for d in directories if d["path"].endswith(".csv")]

dfs_by_profile = {"discharge": [], "charge": [], "impedance": []}

for i, (url, name) in enumerate(zip(csv_urls, file_names)):
    profile = profiles[i]
    ambient_temp = ambient_temperatures[i]
    df = fetch_and_load_csv_to_df(url, name, {
        "Profile": profile,
        "Idx": i,
        "file": name,
        "ambient_temperature": ambient_temp
    })
    dfs_by_profile[profile].append(df)

# COMMAND ----------
# Select and Rename Columns
select_and_rename = {
    "impedance": {
        "Sense_current": "SenseCurrent",
        "Battery_current": "BatteryCurrent",
        "Current_ratio": "CurrentRatio",
        "Battery_impedance": "BatteryImpedance",
        "Rectified_Impedance": "RectifiedImpedance"
    },
    "charge": {
        "Voltage_measured": "VoltageMeasured",
        "Current_measured": "CurrentMeasured",
        "Temperature_measured": "TemperatureMeasured",
        "Current_charge": "CurrentCharge",
        "Voltage_charge": "VoltageCharge",
        "Time": "Duration"
    },
    "discharge": {
        "Voltage_measured": "VoltageMeasured",
        "Current_measured": "CurrentMeasured",
        "Temperature_measured": "TemperatureMeasured",
        "Current_load": "CurrentLoad",
        "Voltage_load": "VoltageLoad",
        "Time": "Duration"
    }
}

for profile, dfs in dfs_by_profile.items():
    final_df = reduce(lambda a, b: a.unionByName(b), dfs)
    rename_map = select_and_rename[profile]
    select_cols = [F.col(old).alias(new) for old, new in rename_map.items()] + [
        "Profile", "Idx", F.col("file").alias("FileName"), F.col("ambient_temperature").alias("AmbientTemperature")
    ]
    final_df = final_df.select(*select_cols)
    final_df.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.Bronze_NASA_{profile}")

# COMMAND ----------
# Stanford Manufacturer Loader

def load_stanford_manufacturer(manufacturer: str):
    base_url = f"https://github.com/berserkhmdvhb/BatteryDatasets/blob/main/Datasets/Stanford/galvanostatic_discharge_test/{manufacturer}/"
    repo = "https://raw.githubusercontent.com/berserkhmdvhb/BatteryDatasets/main/"
    temperatures = [d["name"] for d in fetch_github_tree(base_url)]

    all_dfs = []
    for i, temp in enumerate(temperatures):
        temp_url = base_url + temp + "/"
        files = fetch_github_tree(temp_url)
        for j, file in enumerate(files):
            if file["name"].endswith(".xlsx"):
                url = urljoin(repo, file["path"])
                df = fetch_and_load_xlsx_to_df(url, file["name"], {
                    "Manufacturer": manufacturer,
                    "Temperature": temp,
                    "Idx": int(f"{i}{j}"),
                    "file": file["name"]
                })
                all_dfs.append(df)

    if not all_dfs:
        return

    final_df = reduce(lambda a, b: a.unionByName(b), all_dfs)
    final_df = final_df.select(
        F.col("Date_Time").alias("Time"),
        F.col("Test_Time(s)").alias("TestTime"),
        F.col("Step_Time(s)").alias("StepTime"),
        F.col("Step_Index").alias("StepIndex"),
        F.col("Voltage(V)").alias("Voltage"),
        F.col("Current(A)").alias("Current"),
        F.col("Surface_Temp(degC)").alias("SurfaceTemperature"),
        "Temperature", "Idx", F.col("file").alias("FileName")
    )
    table_name = f"{schema}.Bronze_Stf_{manufacturer.upper()}_discharge"
    final_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

# COMMAND ----------
load_stanford_manufacturer("LFP")
load_stanford_manufacturer("NCA")
load_stanford_manufacturer("NMC")

# COMMAND ----------
# Optimize All Tables
for table in [
    "NASA_impedance", "NASA_charge", "NASA_discharge",
    "Stf_LFP_discharge", "Stf_NCA_discharge", "Stf_NMC_discharge"
]:
    spark.sql(f"OPTIMIZE {schema}.Bronze_{table}")
