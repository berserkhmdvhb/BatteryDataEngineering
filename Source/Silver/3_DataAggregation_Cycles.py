# Databricks notebook source
# MAGIC %md
# MAGIC # Setup and Imports

# COMMAND ----------

from pyspark.sql.functions import col, 

from pyspark.sql.types import ArrayType, FloatType, DoubleType, StringType, BooleanType, IntegerType, LongType, TimestampType, DateType
from pyspark.sql import DataFrame, Row
import sys
import re
import math
from typing import List



# for checking data types in functions
## for specifying list data type
from typing import List
## for specifying spark dataframe
from pyspark.sql import DataFrame


# For showing large rows if needed (with truncate=False)
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Cycle Dataframes

# COMMAND ----------

nasa_charge = spark.table(f"{schema}.Bronze_NASA_charge")
nasa_discharge = spark.table(f"{schema}.Bronze_NASA_discharge")
nasa_impedance = spark.table(f"{schema}.Bronze_NASA_impedance")

stanford_discharge_LFP = spark.table(f"{schema}.Bronze_Stf_LFP_discharge")
stanford_discharge_NCA = spark.table(f"{schema}.Bronze_Stf_NCA_discharge")
stanford_discharge_NMC = spark.table(f"{schema}.Bronze_Stf_NMC_discharge")

# COMMAND ----------

nasa_discharge_cycle= (nasa_discharge
                         .groupBy(col("Idx").alias("CycleNumber"))
                         .agg(median("CurrentMeasured").alias("CycleMeasuredCurrent_Median"), 
                              median("VoltageMeasured").alias("CycleMeasuredVoltage_Median"), 
                              median("CurrentLoad").alias("CycleLoadCurrent_Median"), 
                              median("VoltageLoad").alias("CycleLoadVoltage_Median"), 
                              sum("TemperatureMeasured").alias("CycleTemperatue_Total"), 
                              sum("Duration").alias("CycleDuration_Total"),
                              first("Profile"),
                              first(col("AmbientTemperature").alias("CycleAmbientTemperature"))
                            )
                        )
nasa_charge_cycle= (nasa_charge
                         .groupBy(col("Idx").alias("CycleNumber"))
                         .agg(median("CurrentMeasured").alias("CycleMeasuredCurrent_Median"), 
                              median("VoltageMeasured").alias("CycleMeasuredVoltage_Median"), 
                              median("CurrentCharge").alias("CycleChargeCurrent_Median"), 
                              median("VoltageCharge").alias("CycleChargeVoltage_Median"), 
                              sum("TemperatureMeasured").alias("CycleTemperatue_Total"), 
                              sum("Duration").alias("CycleDuration_Total"),
                              first("Profile"),
                              first(col("AmbientTemperature").alias("CycleAmbientTemperature"))
                            )
                        )

# COMMAND ----------


