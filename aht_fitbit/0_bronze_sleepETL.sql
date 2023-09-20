-- Databricks notebook source
USE CATALOG aht_sa;
USE SCHEMA fitbit; 

-- COMMAND ----------

-- CREATE OR REPLACE TABLE bronze_sleep (
-- dateOfSleep DATE, 
-- startTime TIMESTAMP, 
-- endTime TIMESTAMP, 
-- efficiency INT, 
-- isMainSleep BOOLEAN, 
-- stages STRUCT<deep: BIGINT, light: BIGINT, rem: BIGINT, wake: BIGINT>, 
-- totalMinutesAsleep INT,
-- totalSleepRecords INT ,
-- totalTimeInBed INT
-- )  TBLPROPERTIES(delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

-- COMMAND ----------

-- COPY INTO bronze_sleep 
-- FROM (SELECT    
--   TO_DATE(sleep[0].dateOfSleep) as  dateOfSleep, 
--   TO_TIMESTAMP(sleep[0].startTime) as startTime,
--   TO_TIMESTAMP(sleep[0].endTime) as  endTime, 
--   CAST(sleep[0].efficiency as INT) as efficiency,
--   sleep[0].isMainSleep as isMainSleep,
--   summary.stages as stages, 
--   CAST(summary.totalMinutesAsleep as INT) as totalMinutesAsleep, 
--   CAST(summary.totalSleepRecords as INT) as totalSleepRecords,
--   CAST(summary.totalTimeInBed as INT) as totalTimeInBed
--   FROM 's3://aht-fitbit/sleep/') 
--   FILEFORMAT = JSON

-- COMMAND ----------

SELECT 
*
FROM
bronze_sleep ORDER BY dateOfSleep desc

-- COMMAND ----------

Describe extended bronze_sleep

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC display(dbutils.fs.ls('s3://databricks-e2demofieldengwest/b169b504-4c54-49f2-bc3a-adf4b128f36d/tables/aaf344af-b5f8-4230-b984-bf1ea5451165'))

-- COMMAND ----------

--OPTIMIZE bronze_sleep ZORDER BY (dateOfSleep)

-- COMMAND ----------

--ANALYZE TABLE bronze_sleep COMPUTE STATISTICS FOR COLUMNS dateOfSleep

-- COMMAND ----------

--DESCRIBE HISTORY bronze_sleep

-- COMMAND ----------

-- MAGIC %sh ls
