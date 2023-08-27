# Databricks notebook source
from pyspark.sql.functions import col, lit, sum
from pyspark.sql.types import ArrayType, StringType, BooleanType, IntegerType, FloatType, LongType
from pyspark.sql import DataFrame, Row
import sys

# COMMAND ----------

schema = "LION"

# COMMAND ----------

batteries_charge = spark.table(f"{schema}.nasa_batteries_charge")
batteries_discharge = spark.table(f"{schema}.nasa_batteries_discharge")
batteries_impedance = spark.table(f"{schema}.nasa_batteries_impedance")

# COMMAND ----------

batteries_charge.display()

# COMMAND ----------

batteries_impedance.display()

# COMMAND ----------

batteries_discharge.printSchema()

# COMMAND ----------

batteries_charge.printSchema()

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
