# Databricks notebook source
from pyspark.sql.functions import col, lit, sum
from pyspark.sql.types import ArrayType, StringType, BooleanType, IntegerType, FloatType, LongType
from pyspark.sql import DataFrame, Row
import sys

# COMMAND ----------

schema = "LION"

# COMMAND ----------

nasa_charge = spark.table(f"{schema}.nasa_batteries_charge")
nasa_discharge = spark.table(f"{schema}.nasa_batteries_discharge")
nasa_impedance = spark.table(f"{schema}.nasa_batteries_impedance")

stanford_discharge_LFP = spark.table(f"{schema}.Stanford_LFP_batteries_discharge")
stanford_discharge_NCA = spark.table(f"{schema}.Stanford_NCA_batteries_discharge")
stanford_discharge_NMC = spark.table(f"{schema}.Stanford_NMC_batteries_discharge")

# COMMAND ----------

batteris_charge_cycle = 

# COMMAND ----------

batteries_charge_aggregated= (batteries_charge
                              .groupBy("Idx")
                              .agg(sum("Current_measured").alias("Total_Current"), 
                                   sum("Voltage_measured").alias("Total_Voltage"),
                                   sum("Temperature_measured").alias("Total_Temperatue")
                                   )
)
batteries_discharge_aggregated= (batteries_discharge
                              .groupBy("Idx")
                              .agg(sum("Current_measured").alias("Total_Current"), 
                                   sum("Voltage_measured").alias("Total_Voltage"),
                                   sum("Temperature_measured").alias("Total_Temperatue")
                                   )
)

# COMMAND ----------

batteries_charge_aggregated.display()

# COMMAND ----------

batteries_discharge_aggregated.display()
