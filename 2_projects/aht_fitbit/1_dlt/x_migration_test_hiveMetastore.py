# Databricks notebook source
# MAGIC %md-sandbox 
# MAGIC ## My UC locations
# MAGIC
# MAGIC * External Location: [***`one-env-uc-external-location`***](https://e2-demo-field-eng.cloud.databricks.com/explore/locations/one-env-uc-external-location/browse?o=1444828305810485) -> ***`s3://one-env-uc-external-location/shared_location/ahtsa`***
# MAGIC * this External Location is backed by Credential: [***`one_env_external_location`***](https://e2-demo-field-eng.cloud.databricks.com/explore/credentials/one_env_external_location?o=1444828305810485)
# MAGIC * Fitbit Managed Volume is [***`/Volumes/ahtsa/fitbit/raw_fitbitapi`***](https://e2-demo-field-eng.cloud.databricks.com/explore/data/ahtsa/fitbit?o=1444828305810485)
# MAGIC * This Volume is backed by  ***`s3://one-env-uc-external-location/shared_location/ahtsa/__unitystorage/catalogs/7bd24d57-0a93-4df1-9bcb-40f629f65b45/`***
# MAGIC * Catalog with External Location for Managed Tables [***`ahtsa`***](https://e2-demo-field-eng.cloud.databricks.com/explore/data/ahtsa?o=1444828305810485)
# MAGIC * This Catalog is backed by ***`s3://one-env-uc-external-location/shared_location/ahtsa`***

# COMMAND ----------

#path = '/Volumes/ahtsa/fitbit/raw_fitbitapi'
#dbutils.fs.ls(path)
#dbutils.fs.cp('/Volumes/ahtsa/fitbit/raw_fitbitapi','dbfs:/mnt/ahtsa',True)

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

data_dir = 'dbfs:/mnt/ahtsa'

@dlt.table(
  comment="Clone API Test - Parsing fitbit sleep data from JSON delivery - using volumes",
  path = f"{data_dir}/dlt_tables/bronze_sleep"
)
def bronze_sleep_clone():
  return (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(f"{data_dir}/sleep")
    .filter(F.col("sleep") !=F.array())
    .select('summary',F.explode('sleep').alias('sleep'))
    )
  
@dlt.table(
  comment="Clone API Test - Parsing bronze to silver, exploding structs ",
  path = f"{data_dir}/dlt_tables/silver_sleep"
)
def silver_sleep_clone():
  return (
    dlt.read('bronze_sleep_clone')
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
