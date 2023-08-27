# Databricks notebook source
# MAGIC %sql
# MAGIC USE SCHEMA LION

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Bronze_NASA_charge
# MAGIC (
# MAGIC Id BIGINT GENERATED ALWAYS AS IDENTITY, 
# MAGIC VoltageMeasured DOUBLE, 
# MAGIC CurrentMeasured DOUBLE, 
# MAGIC TemperatureMeasured DOUBLE, 
# MAGIC CurrentCharge DOUBLE, 
# MAGIC VoltageCharge DOUBLE, 
# MAGIC Duration DOUBLE, 
# MAGIC Profile STRING, 
# MAGIC Idx INT, 
# MAGIC FileName STRING, 
# MAGIC AmbientTemperature INT
# MAGIC ) 
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'bronze')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Bronze_NASA_discharge
# MAGIC (
# MAGIC Id BIGINT GENERATED ALWAYS AS IDENTITY, 
# MAGIC VoltageMeasured DOUBLE, 
# MAGIC CurrentMeasured DOUBLE, 
# MAGIC TemperatureMeasured DOUBLE, 
# MAGIC CurrentLoad DOUBLE, 
# MAGIC VoltageLoad DOUBLE
# MAGIC Duration DOUBLE, 
# MAGIC Profile STRING, 
# MAGIC Idx INT, 
# MAGIC FileName STRING, 
# MAGIC AmbientTemperature INT
# MAGIC ) 
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'bronze')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Bronze_NASA_impedance
# MAGIC (
# MAGIC Id BIGINT GENERATED ALWAYS AS IDENTITY, 
# MAGIC VoltageMeasured DOUBLE, 
# MAGIC CurrentMeasured DOUBLE, 
# MAGIC TemperatureMeasured DOUBLE, 
# MAGIC CurrentLoad DOUBLE, 
# MAGIC VoltageLoad DOUBLE
# MAGIC Duration DOUBLE, 
# MAGIC Profile STRING, 
# MAGIC Idx INT, 
# MAGIC FileName STRING, 
# MAGIC AmbientTemperature INT
# MAGIC ) 
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'bronze')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Bronze_Stf_LFP_discharge
# MAGIC (
# MAGIC Id BIGINT GENERATED ALWAYS AS IDENTITY, 
# MAGIC Time TIMESTAMP, 
# MAGIC TestTime DOUBLE, 
# MAGIC StepTime DOUBLE, 
# MAGIC StepIndex BIGINT, 
# MAGIC Voltage DOUBLE, 
# MAGIC Current DOUBLE, 
# MAGIC SurfaceTemperature DOUBLE, 
# MAGIC Temperature STRING, 
# MAGIC Idx INT, 
# MAGIC FileName STRING
# MAGIC ) 
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'bronze')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Bronze_Stf_NCA_discharge
# MAGIC (
# MAGIC Id BIGINT GENERATED ALWAYS AS IDENTITY, 
# MAGIC Time TIMESTAMP, 
# MAGIC TestTime DOUBLE, 
# MAGIC StepTime DOUBLE, 
# MAGIC StepIndex BIGINT, 
# MAGIC Voltage DOUBLE, 
# MAGIC Current DOUBLE, 
# MAGIC SurfaceTemperature DOUBLE, 
# MAGIC Temperature STRING, 
# MAGIC Idx INT, 
# MAGIC FileName STRING
# MAGIC ) 
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'bronze')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Bronze_Stf_NMC_discharge
# MAGIC (
# MAGIC Id BIGINT GENERATED ALWAYS AS IDENTITY, 
# MAGIC Time TIMESTAMP, 
# MAGIC TestTime DOUBLE, 
# MAGIC StepTime DOUBLE, 
# MAGIC StepIndex BIGINT, 
# MAGIC Voltage DOUBLE, 
# MAGIC Current DOUBLE, 
# MAGIC SurfaceTemperature DOUBLE, 
# MAGIC Temperature STRING, 
# MAGIC Idx INT, 
# MAGIC FileName STRING
# MAGIC ) 
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'bronze')

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Silver_NASA_charge
# MAGIC (
# MAGIC Id BIGINT GENERATED ALWAYS AS IDENTITY, 
# MAGIC VoltageMeasured DOUBLE, 
# MAGIC CurrentMeasured DOUBLE, 
# MAGIC TemperatureMeasured DOUBLE, 
# MAGIC CurrentCharge DOUBLE, 
# MAGIC VoltageCharge DOUBLE, 
# MAGIC Duration DOUBLE, 
# MAGIC Profile STRING, 
# MAGIC Idx INT, 
# MAGIC FileName STRING, 
# MAGIC AmbientTemperature INT
# MAGIC ) 
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'bronze')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Silver_NASA_discharge
# MAGIC (
# MAGIC Id BIGINT GENERATED ALWAYS AS IDENTITY, 
# MAGIC VoltageMeasured DOUBLE, 
# MAGIC CurrentMeasured DOUBLE, 
# MAGIC TemperatureMeasured DOUBLE, 
# MAGIC CurrentLoad DOUBLE, 
# MAGIC VoltageLoad DOUBLE
# MAGIC Duration DOUBLE, 
# MAGIC Profile STRING, 
# MAGIC Idx INT, 
# MAGIC FileName STRING, 
# MAGIC AmbientTemperature INT
# MAGIC ) 
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'bronze')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Silver_NASA_impedance
# MAGIC (
# MAGIC Id BIGINT GENERATED ALWAYS AS IDENTITY, 
# MAGIC VoltageMeasured DOUBLE, 
# MAGIC CurrentMeasured DOUBLE, 
# MAGIC TemperatureMeasured DOUBLE, 
# MAGIC CurrentLoad DOUBLE, 
# MAGIC VoltageLoad DOUBLE
# MAGIC Duration DOUBLE, 
# MAGIC Profile STRING, 
# MAGIC Idx INT, 
# MAGIC FileName STRING, 
# MAGIC AmbientTemperature INT
# MAGIC ) 
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'bronze')
