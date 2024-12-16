# Databricks notebook source
# MAGIC %md-sandbox 
# MAGIC
# MAGIC ## Fitbit DLT Pipeline

# COMMAND ----------

catalog = 'ahtsa'
schema = 'fitbit'
vol = '/Volumes/ahtsa/fitbit/raw_fitbitapi'

# COMMAND ----------

import dlt
from pyspark.sql.functions import explode, struct, from_json,col,first,arrays_zip,col,map_from_entries,expr,to_timestamp,to_date,array

# COMMAND ----------

@dlt.table(
  comment="Parsing fitbit sleep data from JSON delivery - using volumes"
)
def bronze_sleep():
  return (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(f"{vol}/sleep")
    .filter(col("sleep") !=array())
    .select('summary',explode('sleep').alias('sleep'))
    )

# COMMAND ----------

@dlt.table(
  comment="Parsing bronze to silver, exploding structs "
)
def silver_sleep():
  return (
    dlt.read('bronze_sleep')
    .select(to_date(col('sleep.dateOfSleep'), 'yyyy-MM-dd').alias('dateOfSleep')
            ,to_timestamp(col('sleep.startTime')).alias('startTime')
            ,to_timestamp(col('sleep.endTime')).alias('endTime')
            ,col('summary.totalMinutesAsleep').cast('int').alias('totalMinutesAsleep')
            ,col('summary.totalSleepRecords').cast('int').alias('totalSleepRecords')
            ,col('summary.totalTimeInBed').cast('int').alias('totalTimeInBed')
            ,col('sleep.efficiency').cast('int').alias('efficiency')
            ,col('sleep.isMainSleep').alias('isMainSleep')
            ,'summary.stages'
            )
    .sort("dateOfSleep", ascending=False)
    )
