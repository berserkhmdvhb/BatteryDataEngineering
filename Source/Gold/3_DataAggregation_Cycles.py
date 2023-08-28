# Databricks notebook source
# MAGIC %md
# MAGIC # Setup and Imports

# COMMAND ----------

from pyspark.sql.functions import col, median, sum, first, max, min, unix_timestamp, lit

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

schema = "LION"

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Cycle Dataframes

# COMMAND ----------

nasa_charge = spark.table(f"{schema}.Silver_NASA_charge")
nasa_discharge = spark.table(f"{schema}.Silver_NASA_discharge")
nasa_impedance = spark.table(f"{schema}.Silver_NASA_impedance")

stanford_discharge_LFP = spark.table(f"{schema}.Silver_Stf_LFP_discharge")
stanford_discharge_NCA = spark.table(f"{schema}.Silver_Stf_NCA_discharge")
stanford_discharge_NMC = spark.table(f"{schema}.Silver_Stf_NMC_discharge")

# COMMAND ----------

stanford_discharge_LFP = stanford_discharge_LFP.withColumn("Manufacture", lit("LFP"))
stanford_discharge_NCA = stanford_discharge_NCA.withColumn("Manufacture", lit("NCA"))
stanford_discharge_NMC = stanford_discharge_NMC.withColumn("Manufacture", lit("NMC"))

# COMMAND ----------

nasa_discharge = (nasa_discharge
                         .groupBy(col("Idx").alias("CycleNumber"))
                         .agg(median("CurrentMeasured").alias("CycleCurrent_Median"), 
                              sum("CurrentMeasured").alias("CycleCurrent_Total"), 
                              median("VoltageMeasured").alias("CycleVoltage_Median"), 
                              median("CurrentLoad").alias("CycleLoadCurrent_Median"), 
                              median("VoltageLoad").alias("CycleLoadVoltage_Median"), 
                              max("TemperatureMeasured").alias("CycleTemperatue_Max"), 
                              min("TemperatureMeasured").alias("CycleTemperatue_Min"),
                              median("TemperatureMeasured").alias("CycleTemperatue_Median"),  
                              (max("Duration")/3600).cast(FloatType()).alias("CycleDuration"),
                              first(col("AmbientTemperature")).alias("CycleAmbientTemperature"),
                              first(col("Profile")).alias("Profile")
                            )
                        )
nasa_charge = (nasa_charge
                         .groupBy(col("Idx").alias("CycleNumber"))
                         .agg(median("CurrentMeasured").alias("CycleCurrent_Median"), 
                              sum("CurrentMeasured").alias("CycleCurrent_Total"), 
                              median("VoltageMeasured").alias("CycleVoltage_Median"), 
                              median("CurrentCharge").alias("CycleChargeCurrent_Median"), 
                              median("VoltageCharge").alias("CycleChargeVoltage_Median"), 
                              max("TemperatureMeasured").alias("CycleTemperatue_Max"), 
                              min("TemperatureMeasured").alias("CycleTemperatue_Min"), 
                              median("TemperatureMeasured").alias("CycleTemperatue_Median"), 
                              (max("Duration")/3600).cast(FloatType()).alias("CycleDuration"),
                              first(col("AmbientTemperature")).alias("CycleAmbientTemperature"),
                              first(col("Profile")).alias("Profile")
                            )
                    )

stanford_discharge_LFP = (stanford_discharge_LFP
                                .groupBy(col("Idx").alias("CycleNumber"))
                                .agg(median("Current").alias("CycleCurrent_Median"), 
                                     median("Voltage").alias("CycleVoltage_Median"), 
                                     max("SurfaceTemperature").alias("CycleTemperatue_Max"), 
                                     min("SurfaceTemperature").alias("CycleTemperatue_Min"), 
                                     median("SurfaceTemperature").alias("CycleTemperatue_Median"), 
                                     ((max(unix_timestamp(col("Time"))) - min(unix_timestamp(col("Time"))))/3600).cast(FloatType()).alias("CycleDuration"),
                                     first(col("Temperature")).alias("CycleStaticTemperatue"),
                                     first(col("Manufacture")).alias("Manufacture")
                                    )
                                )    

stanford_discharge_NCA = (stanford_discharge_NCA
                                .groupBy(col("Idx").alias("CycleNumber"))
                                .agg(median("Current").alias("CycleCurrent_Median"), 
                                     median("Voltage").alias("CycleVoltage_Median"), 
                                     max("SurfaceTemperature").alias("CycleTemperatue_Max"), 
                                     min("SurfaceTemperature").alias("CycleTemperatue_Min"), 
                                     median("SurfaceTemperature").alias("CycleTemperatue_Median"), 
                                     ((max(unix_timestamp(col("Time"))) - min(unix_timestamp(col("Time"))))/3600).cast(FloatType()).alias("CycleDuration"),
                                     first(col("Temperature")).alias("CycleStaticTemperatue"),
                                     first(col("Manufacture")).alias("Manufacture")
                                    )
                                )    
stanford_discharge_NMC = (stanford_discharge_NMC
                                .groupBy(col("Idx").alias("CycleNumber"))
                                .agg(median("Current").alias("CycleCurrent_Median"), 
                                     median("Voltage").alias("CycleVoltage_Median"), 
                                     max("SurfaceTemperature").alias("CycleTemperatue_Max"), 
                                     min("SurfaceTemperature").alias("CycleTemperatue_Min"), 
                                     median("SurfaceTemperature").alias("CycleTemperatue_Median"), 
                                     ((max(unix_timestamp(col("Time"))) - min(unix_timestamp(col("Time"))))/3600).cast(FloatType()).alias("CycleDuration"),
                                     first(col("Temperature")).alias("CycleStaticTemperatue"),
                                     first(col("Manufacture")).alias("Manufacture")
                                    )
                                )                                            

# COMMAND ----------

nasa_charge.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Gold_NASA_charge_cycles')
nasa_discharge.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Gold_NASA_discharge_cycles')
#nasa_impedance.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Gold_NASA_impedance_cycles')


# COMMAND ----------

stanford_discharge_LFP.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Gold_Stf_LFP_discharge_cycles')
stanford_discharge_NCA.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Gold_Stf_NCA_discharge_cycles')
stanford_discharge_NMC.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Gold_Stf_NMC_discharge_cycles')
