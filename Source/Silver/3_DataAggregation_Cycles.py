# Databricks notebook source
# MAGIC %md
# MAGIC # Create Cycle Dataframes

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
