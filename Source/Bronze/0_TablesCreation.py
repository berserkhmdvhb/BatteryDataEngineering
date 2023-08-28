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
# MAGIC VoltageLoad DOUBLE,
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
# MAGIC SenseCurrent STRING, 
# MAGIC BatteryCurrent STRING, 
# MAGIC CurrentRatio STRING, 
# MAGIC BatteryImpedance STRING, 
# MAGIC RectifiedImpedance STRING, 
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
# MAGIC Id BIGINT,
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
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'silver')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Silver_NASA_discharge
# MAGIC (
# MAGIC Id BIGINT,
# MAGIC VoltageMeasured DOUBLE, 
# MAGIC CurrentMeasured DOUBLE, 
# MAGIC TemperatureMeasured DOUBLE, 
# MAGIC CurrentLoad DOUBLE, 
# MAGIC VoltageLoad DOUBLE,
# MAGIC Duration DOUBLE, 
# MAGIC Profile STRING, 
# MAGIC Idx INT, 
# MAGIC FileName STRING, 
# MAGIC AmbientTemperature INT
# MAGIC ) 
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'silver')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Silver_NASA_impedance
# MAGIC (
# MAGIC Id BIGINT, 
# MAGIC Profile STRING, 
# MAGIC Idx INT, 
# MAGIC FileName STRING, 
# MAGIC AmbientTemperature INT, 
# MAGIC SenseCurrent_Real DOUBLE, 
# MAGIC SenseCurrent_Imag DOUBLE, 
# MAGIC SenseCurrent_Magnitude DOUBLE, 
# MAGIC SenseCurrent_Phase DOUBLE, 
# MAGIC BatteryCurrent_Real DOUBLE, 
# MAGIC BatteryCurrent_Imag DOUBLE, 
# MAGIC BatteryCurrent_Magnitude DOUBLE, 
# MAGIC BatteryCurrent_Phase DOUBLE, 
# MAGIC CurrentRatio_Real DOUBLE, 
# MAGIC CurrentRatio_Imag DOUBLE, 
# MAGIC CurrentRatio_Magnitude DOUBLE, 
# MAGIC CurrentRatio_Phase DOUBLE, 
# MAGIC BatteryImpedance_Real DOUBLE, 
# MAGIC BatteryImpedance_Imag DOUBLE, 
# MAGIC BatteryImpedance_Magnitude DOUBLE, 
# MAGIC BatteryImpedance_Phase DOUBLE, 
# MAGIC RectifiedImpedance_Real DOUBLE, 
# MAGIC RectifiedImpedance_Imag DOUBLE, 
# MAGIC RectifiedImpedance_Magnitude DOUBLE, 
# MAGIC RectifiedImpedance_Phase DOUBLE
# MAGIC ) 
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'silver')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Silver_Stf_LFP_discharge
# MAGIC (
# MAGIC Id BIGINT, 
# MAGIC Time TIMESTAMP, 
# MAGIC TestTime DOUBLE, 
# MAGIC StepTime DOUBLE, 
# MAGIC StepIndex BIGINT, 
# MAGIC Voltage DOUBLE, 
# MAGIC Current DOUBLE, 
# MAGIC SurfaceTemperature DOUBLE, 
# MAGIC Temperature INT, 
# MAGIC Idx INT, 
# MAGIC FileName STRING,
# MAGIC Profile STRING
# MAGIC ) 
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'silver')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Silver_Stf_NCA_discharge
# MAGIC (
# MAGIC Id BIGINT, 
# MAGIC Time TIMESTAMP, 
# MAGIC TestTime DOUBLE, 
# MAGIC StepTime DOUBLE, 
# MAGIC StepIndex BIGINT, 
# MAGIC Voltage DOUBLE, 
# MAGIC Current DOUBLE, 
# MAGIC SurfaceTemperature DOUBLE, 
# MAGIC Temperature INT, 
# MAGIC Idx INT, 
# MAGIC FileName STRING,
# MAGIC Profile STRING
# MAGIC ) 
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'silver')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Silver_Stf_NMC_discharge
# MAGIC (
# MAGIC Id BIGINT, 
# MAGIC Time TIMESTAMP, 
# MAGIC TestTime DOUBLE, 
# MAGIC StepTime DOUBLE, 
# MAGIC StepIndex BIGINT, 
# MAGIC Voltage DOUBLE, 
# MAGIC Current DOUBLE, 
# MAGIC SurfaceTemperature DOUBLE, 
# MAGIC Temperature INT, 
# MAGIC Idx INT, 
# MAGIC FileName STRING,
# MAGIC Profile STRING
# MAGIC ) 
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'silver')

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Gold_NASA_charge_cycles
# MAGIC (
# MAGIC CycleNumber INT, 
# MAGIC CycleCurrent_Median DOUBLE, 
# MAGIC CycleCurrent_Total DOUBLE, 
# MAGIC CycleVoltage_Median DOUBLE, 
# MAGIC CycleChargeCurrent_Median DOUBLE, 
# MAGIC CycleChargeVoltage_Median DOUBLE, 
# MAGIC CycleTemperatue_Min DOUBLE, 
# MAGIC CycleTemperatue_Max DOUBLE, 
# MAGIC CycleTemperatue_Median DOUBLE, 
# MAGIC CycleDuration FLOAT, 
# MAGIC CycleAmbientTemperature INT,
# MAGIC Profile STRING
# MAGIC ) 
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'gold')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Gold_NASA_discharge_cycles
# MAGIC (
# MAGIC CycleNumber INT, 
# MAGIC CycleCurrent_Median DOUBLE, 
# MAGIC CycleCurrent_Total DOUBLE, 
# MAGIC CycleVoltage_Median DOUBLE, 
# MAGIC CycleLoadCurrent_Median DOUBLE, 
# MAGIC CycleLoadVoltage_Median DOUBLE, 
# MAGIC CycleTemperatue_Min DOUBLE, 
# MAGIC CycleTemperatue_Max DOUBLE, 
# MAGIC CycleTemperatue_Median DOUBLE,
# MAGIC CycleDuration FLOAT, 
# MAGIC CycleAmbientTemperature INT,
# MAGIC Profile STRING
# MAGIC ) 
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'gold')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Gold_Stf_LFP_discharge_cycles
# MAGIC (
# MAGIC CycleNumber INT, 
# MAGIC CycleCurrent_Median DOUBLE, 
# MAGIC CycleVoltage_Median DOUBLE, 
# MAGIC CycleTemperatue_Min DOUBLE, 
# MAGIC CycleTemperatue_Max DOUBLE, 
# MAGIC CycleTemperatue_Median DOUBLE,
# MAGIC CycleDuration FLOAT, 
# MAGIC CycleStaticTemperatue INT,
# MAGIC Profile STRING
# MAGIC ) 
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'gold')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Gold_Stf_NCA_discharge_cycles
# MAGIC (
# MAGIC CycleNumber INT, 
# MAGIC CycleCurrent_Median DOUBLE, 
# MAGIC CycleVoltage_Median DOUBLE, 
# MAGIC CycleTemperatue_Min DOUBLE, 
# MAGIC CycleTemperatue_Max DOUBLE, 
# MAGIC CycleTemperatue_Median DOUBLE,
# MAGIC CycleDuration FLOAT, 
# MAGIC CycleStaticTemperatue INT,
# MAGIC Profile STRING
# MAGIC ) 
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'gold')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Gold_Stf_NMC_discharge_cycles
# MAGIC (
# MAGIC CycleNumber INT, 
# MAGIC CycleCurrent_Median DOUBLE, 
# MAGIC CycleVoltage_Median DOUBLE, 
# MAGIC CycleTemperatue_Min DOUBLE, 
# MAGIC CycleTemperatue_Max DOUBLE, 
# MAGIC CycleTemperatue_Median DOUBLE,
# MAGIC CycleDuration FLOAT, 
# MAGIC CycleStaticTemperatue INT,
# MAGIC Profile STRING
# MAGIC ) 
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'gold')
