# Databricks notebook source
# MAGIC %md
# MAGIC # Setup and Imports

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace, regexp_extract, split, when, concat_ws, udf, size, expr, trim, lit, element_at, to_date, date_format, lower, length, to_timestamp, monotonically_increasing_id, expr, atan2, sqrt

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

nasa_charge = spark.table(f"{schema}.Bronze_NASA_charge")
nasa_discharge = spark.table(f"{schema}.Bronze_NASA_discharge")
nasa_impedance = spark.table(f"{schema}.Bronze_NASA_impedance")

stanford_discharge_LFP = spark.table(f"{schema}.Bronze_Stf_LFP_discharge")
stanford_discharge_NCA = spark.table(f"{schema}.Bronze_Stf_NCA_discharge")
stanford_discharge_NMC = spark.table(f"{schema}.Bronze_Stf_NMC_discharge")

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC selected_columns = [col(c) for c in nasa_charge.columns if c != "Id"]
# MAGIC nasa_charge = nasa_charge.select(*selected_columns)
# MAGIC selected_columns = [col(c) for c in nasa_discharge.columns if c != "Id"]
# MAGIC nasa_discharge = nasa_discharge.select(*selected_columns)
# MAGIC selected_columns = [col(c) for c in nasa_impedance.columns if c != "Id"]
# MAGIC nasa_impedance = nasa_impedance.select(*selected_columns)
# MAGIC selected_columns = [col(c) for c in stanford_discharge_LFP.columns if c != "Id"]
# MAGIC stanford_discharge_LFP = stanford_discharge_LFP.select(*selected_columns)
# MAGIC selected_columns = [col(c) for c in stanford_discharge_NCA.columns if c != "Id"]
# MAGIC stanford_discharge_NCA = stanford_discharge_NCA.select(*selected_columns)
# MAGIC selected_columns = [col(c) for c in stanford_discharge_NMC.columns if c != "Id"]
# MAGIC stanford_discharge_NMC = stanford_discharge_NMC.select(*selected_columns)
# MAGIC ```

# COMMAND ----------

nasa_impedance = spark.table(f"{schema}.nasa_batteries_impedance")

# COMMAND ----------

#nasa_discharge._jdf.schema().toDDL()

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaning

# COMMAND ----------

# MAGIC %md
# MAGIC ## Numeric Columns

# COMMAND ----------

# DBTITLE 0,Correct Stanford Temperatures
def RemoveWhitespace(sdf: DataFrame, colname: str, cond_col: str = None, cond: str = "y") -> DataFrame:
    """
    Applies two operations on the input column of the input spark dataframe, first is using `regexp_replace(colname, r"\s{2,}", " ")` to remove any more than two spaces that are consecutive.
    Secondly, the trim function is used to remove spaces at beginning and end of the strings of a column.

        Parameters:
            sdf (pyspark.sql.dataframe.DataFrame): Spark Dataframe.
            colname (str): Name of column to be modified.
            cond_col (str): Name of column to be conditioned on. If no value is given, no conditioning will occur. Default value is "None".
            cond (str): Whether the condition should be on the trueness of `cond_col` ("y") or falseness of `cond_col` (any other value). Default value is "y".

        Returns:
            res (pyspark.sql.dataframe.DataFrame): The input spark dataframe `sdf` but with modified column `colname`. Whitespaces are removed from this column.
    """

    res = sdf
    if cond_col == None:
        res = res.withColumn(colname, regexp_replace(colname, r"\s{2,}", " "))
        res = res.withColumn(colname, trim(col(colname)))
    else:
        if cond_col in res.columns:
            if cond.lower() != "y":
                res = res.withColumn(
                    colname,
                    when(
                        ~col(cond_col), regexp_replace(colname, r"\s{2,}", " ")
                    ).otherwise(col(colname)),
                )
                res = res.withColumn(
                    colname,
                    when(~col(cond_col), trim(col(colname))).otherwise(col(colname)),
                )
            else:
                res = res.withColumn(
                    colname,
                    when(
                        col(cond_col), regexp_replace(colname, r"\s{2,}", " ")
                    ).otherwise(col(colname)),
                )
                res = res.withColumn(
                    colname,
                    when(col(cond_col), trim(col(colname))).otherwise(col(colname)),
                )
    return res

# COMMAND ----------

def HandleNullVals(sdf: DataFrame, colname: str, cond_col: str = None, cond: str = "y") -> DataFrame:
    """
    Replaces values of of the input column of the input spark dataframe that implies missing data with null values. The value implying null value differs on columns, but to use the same function, all the following values are replaced with syntactic null:
     `unknown`, `null`, `na`, `""`, `" "`. The lowercase version of the string under study is compared with the mentioned values so that lowercase and uppercase differences don't play any role.
     Usually `RemoveWhitespace` should be applied before `HandleNullVals` so that all empty strings all properly replaced with null values.

        Parameters:
            sdf (pyspark.sql.dataframe.DataFrame): Spark Dataframe.
            colname (str): Name of column to be modified.
            cond_col (str): Name of column to be conditioned on. If no value is given, no conditioning will occur. Default value is "None".
            cond (str): Whether the condition should be on the trueness of `cond_col` ("y") or falseness of `cond_col` (any other value). Default value is "y".

        Returns:
            res (pyspark.sql.dataframe.DataFrame): The input spark dataframe `sdf` but with modified column `colname`. Any value in column implying missing value is now replaced with syntactic null.
    """

    res = sdf
    if cond_col == None:
        res = res.withColumn(
            colname,
            when(
                (lower(col(colname)) == "unknown")
                | (lower(col(colname)) == "unavailable")
                | (lower(col(colname)) == "null")
                | (lower(col(colname)) == "na")
                | (col(colname) == "")
                | (col(colname) == " "),
                lit(None),
            ).otherwise(col(colname)),
        )
    else:
        if cond_col in res.columns:
            if cond.lower() != "y":
                res = res.withColumn(
                    colname,
                    when(
                        ~col(cond_col) & (lower(col(colname)) == "unknown")
                        | (lower(col(colname)) == "unavailable")
                        | (lower(col(colname)) == "null")
                        | (lower(col(colname)) == "na")
                        | (col(colname) == "")
                        | (col(colname) == " "),
                        lit(None),
                    ).otherwise(col(colname)),
                )
            else:
                res = res.withColumn(
                    colname,
                    when(
                        col(cond_col) & (lower(col(colname)) == "unknown")
                        | (lower(col(colname)) == "unavailable")
                        | (lower(col(colname)) == "null")
                        | (lower(col(colname)) == "na")
                        | (col(colname) == "")
                        | (col(colname) == " "),
                        lit(None),
                    ).otherwise(col(colname)),
                )
    return res

# COMMAND ----------

def RemoveUnwantedChars(sdf: DataFrame, colname: str, unwanted_char_pattern: str, cond_col: str = None, cond: str = "y") -> DataFrame:
    """
    Removes unexpected characters from the input column of the input spark dataframe using `regexp_replace(colname, unwanted_char_pattern, ""))`.
    In `unwanted_char_pattern`, usually instead of specifying characters to remove, the characters that are intended to be kept are stated, and they are followed by a `^` at beginning of the `unwanted_char_pattern` pattern, so as to exclude the desired characters,
    and thus everything other than these characters are removed. The decision of which kind of characters should be expected and hence kept, and which should be removed depend on the column under study.
    For instance, for cleaning names, more precisely, cleaning the columns `firstname` and `lastname`, the pattern `[^a-zA-Z\s]` is used, which meaning alphabets (`a-zA-Z`), and whitespaces (`\s`) are only allowed.
    On the other hand, for column `DateOfBirthDateOfBirth` which contains birthdates, the pattern `[^a-zA-Z\d\s\d\\\\\/]` is used, as some birthdates contain the symbols `\` or `/`, so these symbols are kept.

        Parameters:
            sdf (pyspark.sql.dataframe.DataFrame): Spark Dataframe.
            colname (str): Name of column to be modified.
            unwanted_char_pattern (str): Regex pattern of unwanted characters. The function will remove any substring of the input column that matches this pattern.
            cond_col (str): Name of column to be conditioned on. If no value is given, no conditioning will occur. Default value is "None".
            cond (str): Whether the condition should be on the trueness of `cond_col` ("y") or falseness of `cond_col` (any other value). Default value is "y".

        Returns:
            res (pyspark.sql.dataframe.DataFrame): The input spark dataframe `sdf` but with modified column `colname`. Unwanted (unexpected) characters are removed from this column based on user's regex pattern containing what to remove.
    """

    res = sdf
    if cond_col == None:
        res = res.withColumn(
            colname, regexp_replace(colname, unwanted_char_pattern, "")
        )
    else:
        if cond_col in res.columns:
            if cond.lower() != "y":
                res = res.withColumn(
                    colname,
                    when(
                        ~col(cond_col),
                        regexp_replace(colname, unwanted_char_pattern, ""),
                    ).otherwise(col(colname)),
                )
            else:
                res = res.withColumn(
                    colname,
                    when(
                        col(cond_col),
                        regexp_replace(colname, unwanted_char_pattern, ""),
                    ).otherwise(col(colname)),
                )
    return res

# COMMAND ----------

def clean_string_cols_prepare(sdf: DataFrame, colname: str) -> DataFrame:
    """
    Cleans the input column (number as string). The allowed characters are digits, letter "E", and symbols `.`, `-`. This function is for string columns that should only contain numbers (either integer or float).

        Parameters:
            sdf (pyspark.sql.dataframe.DataFrame): Spark Dataframe.
            colname (str): Name of column to be modified (string).

        Returns:
            res (pyspark.sql.dataframe.DataFrame): The input spark dataframe `sdf` but with cleaned column `colname`. 
   """

    res = sdf
    res = RemoveWhitespace(sdf = res, colname = colname)
    unwanted_char_pattern = "[^\d\-\.E]" 
    res = RemoveUnwantedChars(sdf = res, colname = colname, unwanted_char_pattern = unwanted_char_pattern)
    res = RemoveWhitespace(sdf = res, colname = colname)
    res = HandleNullVals(sdf = res, colname = colname)
    return res

# COMMAND ----------

def clean_numeric_cols(sdf: DataFrame, colnames: List[str]) -> DataFrame: 
    """
    Cleans the numeric columns of a given spark dataframe using the function `clean_numeric_cols_prepare`.

        Parameters:
            sdf (pyspark.sql.dataframe.DataFrame): Spark Dataframe.
            colnames (str list): List of the names of the columns to be modified (numeric).

        Returns:
            res (pyspark.sql.dataframe.DataFrame): The input spark dataframe `sdf` but with the numeric columns cleaned.
    """ 

    res = sdf
    num_cols = [c for c in res.columns if any(word == c for word in colnames)]
    if len(num_cols) != 0:
        for colname in num_cols:
            res = clean_numeric_cols_prepare(sdf = res, colname = colname)
    return(res)    

# COMMAND ----------

#nasa_charge = clean_numeric_cols(nasa_charge, colnames = ['VoltageMeasured', 'CurrentMeasured', 'TemperatureMeasured', 'CurrentCharge', 'VoltageCharge', 'Duration', 'AmbientTemperature'])
#nasa_discharge = clean_numeric_cols(nasa_discharge, colnames = ['VoltageMeasured', 'CurrentMeasured', 'TemperatureMeasured', 'CurrentCharge', 'VoltageCharge', 'Duration', 'AmbientTemperature'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Correct Stanford Temperatures

# COMMAND ----------

unwanted_char_pattern = "[^\d]" 
stanford_discharge_LFP = RemoveUnwantedChars(sdf = stanford_discharge_LFP, colname = "Temperature", unwanted_char_pattern = unwanted_char_pattern).withColumn("Temperature", col("Temperature").cast(IntegerType()))
stanford_discharge_NCA = RemoveUnwantedChars(sdf = stanford_discharge_NCA, colname = "Temperature", unwanted_char_pattern = unwanted_char_pattern).withColumn("Temperature", col("Temperature").cast(IntegerType()))
stanford_discharge_NMC = RemoveUnwantedChars(sdf = stanford_discharge_NMC, colname = "Temperature", unwanted_char_pattern = unwanted_char_pattern).withColumn("Temperature", col("Temperature").cast(IntegerType()))

# COMMAND ----------

#stanford_discharge_LFP = clean_numeric_cols(stanford_discharge_LFP, colnames = ['StepTime', 'TestTime', 'StepIndex', 'Current', 'SurfaceTemperature', 'Temperature'])
#stanford_discharge_NCA = clean_numeric_cols(stanford_discharge_NCA, colnames = ['StepTime', 'TestTime', 'StepIndex', 'Current', 'SurfaceTemperature', 'Temperature'])
#stanford_discharge_NMC = clean_numeric_cols(stanford_discharge_NMC, colnames = ['StepTime', 'TestTime', 'StepIndex', 'Current', 'SurfaceTemperature', 'Temperature'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handle Complex Numbers

# COMMAND ----------


# Regular expression pattern to extract real and imaginary parts
pattern = r"\((-?\d+\.\d+)([+-]\d+\.\d+)j\)"
# User-defined function to process complex strings
def prase_complex_real_part(complex_str):
    if isinstance(complex_str, str):
        match = re.match(pattern, complex_str)
        if match:
            real_part = float(match.group(1).replace('j', ''))
            if real_part != "":
                return real_part
            else:
                return(None)
        else: 
            return(None)    


def prase_complex_imag_part(complex_str):
    if isinstance(complex_str, str):
        match = re.match(pattern, complex_str)
        if match:
            imag_part = float(match.group(2).replace('j', ''))
            if imag_part != "":
                return imag_part
            else:
                return(None)
        else:
            return(None)    
            
# Calculate magnitude and phase
magnitude_udf = udf(lambda real, imag: math.sqrt(real**2 + imag**2))
phase_udf = udf(lambda real, imag: math.atan2(imag, real))

parse_complex_real_part_udf = udf(prase_complex_real_part, returnType = DoubleType())
parse_complex_imag_part_udf = udf(prase_complex_imag_part, returnType = DoubleType())

# Columns containing complex numbers
complex_columns = ['SenseCurrent', 'BatteryCurrent', 'CurrentRatio', 'BatteryImpedance', 'RectifiedImpedance']

# Loop through complex columns and create new columns for real and imaginary parts
for colname in complex_columns:
    newcolname_real = colname + '_Real'
    newcolname_imag = colname + '_Imag'
    unwanted_char_pattern = "[^\d\.\-\+\(\)j]" 
    nasa_impedance = RemoveUnwantedChars(sdf = nasa_impedance, colname = colname, unwanted_char_pattern = unwanted_char_pattern)
    nasa_impedance = nasa_impedance.withColumn(newcolname_real, parse_complex_real_part_udf(colname).cast(DoubleType()))
    nasa_impedance = nasa_impedance.withColumn(newcolname_imag, parse_complex_imag_part_udf(colname).cast(DoubleType()))
    nasa_impedance = nasa_impedance.withColumn(colname + "_Magnitude", sqrt(expr(f"{newcolname_real} * {newcolname_real} + {newcolname_imag} * {newcolname_imag}")))\
    .withColumn(colname + "_Phase", atan2(expr(f"{newcolname_imag}"), expr(f"{newcolname_real}")))


# COMMAND ----------

# DBTITLE 1,Store as delta tables
nasa_charge.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Silver_NASA_charge')
nasa_discharge.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Silver_NASA_discharge')
nasa_impedance.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Silver_NASA_impedance')


# COMMAND ----------

stanford_discharge_LFP.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Silver_Stf_LFP_discharge')
stanford_discharge_NCA.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Silver_Stf_NCA_discharge')
stanford_discharge_NMC.write.format('delta').mode('overwrite').saveAsTable(f'{schema}.Silver_Stf_NMC_discharge')
