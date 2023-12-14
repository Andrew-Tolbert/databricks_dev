# Databricks notebook source
# MAGIC %md-sandbox 
# MAGIC
# MAGIC ## Fitbit sleep DLT Pipeline

# COMMAND ----------

catalog = 'ahtsa'
schema = 'fitbit'
vol = '/Volumes/ahtsa/fitbit/raw_fitbitapi'

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

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
    .filter(F.col("sleep") !=F.array())
    .select('summary',F.explode('sleep').alias('sleep'))
    )

# COMMAND ----------

@dlt.table(
  comment="Parsing bronze to silver, exploding structs "
)
def silver_sleep():
  return (
    dlt.read('bronze_sleep')
    .select(F.to_date(F.col('sleep.dateOfSleep'), 'yyyy-MM-dd').alias('dateOfSleep')
            ,F.to_timestamp(F.col('sleep.startTime')).alias('startTime')
            ,F.to_timestamp(F.col('sleep.endTime')).alias('endTime')
            ,F.col('summary.totalMinutesAsleep').cast('int').alias('totalMinutesAsleep')
            ,F.col('summary.totalSleepRecords').cast('int').alias('totalSleepRecords')
            ,F.col('summary.totalTimeInBed').cast('int').alias('totalTimeInBed')
            ,F.col('sleep.efficiency').cast('int').alias('efficiency')
            ,F.col('sleep.isMainSleep').alias('isMainSleep')
            ,'summary.stages'
            )
    .sort("dateOfSleep", ascending=False)
    )

# COMMAND ----------

# @dlt.table(
#   comment="Parsing fitbit activity Log data from JSON delivery - using volumes - there WILL be duplicates in this table"
# )
# def bronze_activities():
#   return (
#     spark.readStream.format("cloudFiles")
#     .option("cloudFiles.format", "json")
#     .option("cloudFiles.inferColumnTypes", "true")
#     .load(f"{vol}/sleep")
#     #.filter(F.col("sleep") !=F.array())
#     #.select('summary',F.explode('sleep').alias('sleep'))
#     )
