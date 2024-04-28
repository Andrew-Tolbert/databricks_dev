# Databricks notebook source
# MAGIC %md #### Incremental Ingestion

# COMMAND ----------

# MAGIC %run ./0_ingestMain

# COMMAND ----------

import pyspark.sql.functions as f
import re

def getMaxDatefromDir(dir):
  df = spark.createDataFrame(dbutils.fs.ls(dir))
  _max = df.select(f.max(df.name)).collect()[0]['max(name)'] 
  pattern = r'[^0-9]'
  # Match all digits in the string and replace them with an empty string
  strMax = re.sub(pattern, '', _max)
  date_obj = dt.datetime.strptime(strMax, '%Y%m%d').date()
  return date_obj,_max

# COMMAND ----------

sleep_dir = f"dbfs:{vol}/raw_fitbitapi/sleep"
act_dir = f"dbfs:{vol}/raw_fitbitapi/activities"
actLogs_dir = f"dbfs:{vol}/raw_fitbitapi/activitylog"

# COMMAND ----------

_maxSleepDate,_maxSleepFile = getMaxDatefromDir(sleep_dir)
_maxActDate,_maxActFile = getMaxDatefromDir(act_dir)
_maxActLogsDate, _maxLogsFile = getMaxDatefromDir(actLogs_dir)

# COMMAND ----------

#REST MAX Activity Log Date - This API comes to us in a different way, 
# feed is not 1:1 with data, one day file can contain up to 100 acitivity logs, 
# we will get the max available date and add a buffer of -1 days 

def getMaxActLogDate(file): 
  actLogDf = spark.read.json(f'{actLogs_dir}/{file}')
  actLogDf= (actLogDf.select("activities")
            .select("*", explode(actLogDf.activities).alias("activities_exploded"))
  )
  actLogDT =  actLogDf.select("activities.startTime").distinct().collect()[0][0][-1][0:10]
  __maxActLogsDate =  (dt.datetime.strptime(actLogDT, '%Y-%m-%d').date()  - dt.timedelta(days = 1))
  return __maxActLogsDate

try: 
  __maxActLogsDate = getMaxActLogDate(_maxLogsFile)
  print("original input worked")
except: 
  print("original input did not work")
  dbutils.fs.rm(f"{actLogs_dir}/{_maxLogsFile}")
  _maxActLogsDate, _maxLogsFile = getMaxDatefromDir(actLogs_dir)
  __maxActLogsDate = getMaxActLogDate(_maxLogsFile)


# COMMAND ----------

 print(__maxActLogsDate)

# COMMAND ----------

print("_maxSleepDate: " f"{_maxSleepDate}")
print("_maxActDate: " f"{_maxActDate}")
print("_maxActLogsDate: " f"{__maxActLogsDate}")

# COMMAND ----------


(access_token, refresh_token,user_id) = get_creds()

s3_interval_update('sleep',_maxSleepDate,date_0,10,access_token)
print('~~~~~~~~~~~~~~~~~~~~~~~~~SLEEP COMPLETE~~~~~~~~~~~~~~~~~~~~~~~~~')
s3_interval_update('activities',_maxActDate,date_1,10,access_token)
print('~~~~~~~~~~~~~~~~~~~~~~~~~ACTIVITIES COMPLETE~~~~~~~~~~~~~~~~~~~~~')
s3_interval_activities(__maxActLogsDate,date_0,10,10,access_token)
print('~~~~~~~~~~~~~~~~~~~~~~~~~ACTIVITY LOGS AND TCX COMPLETE~~~~~~~~~~')

# COMMAND ----------

# MAGIC %md
# MAGIC #### ~~~~~~~~~~~DONE FILE INGESTION~~~~~~~~~~~~~~~~~~~~

# COMMAND ----------

def bulk_restore(start,end):
  """
  This function takes a 2 strings, a start and end date of format '2023-01-01' and performs a bulk reload of fitbit data
  """
  start= dt.datetime.strptime(start, '%Y-%m-%d').date()
  end = dt.datetime.strptime(end, '%Y-%m-%d').date()


  (access_token, refresh_token,user_id) = get_creds()

  #####################################################
  ##                   RESTORES                     ###
  #####################################################
  #s3_interval_update('sleep',start,end,20,access_token)
  #s3_interval_update('activities',start,end,20,access_token)
  s3_interval_activities(start,end,10,20,access_token)
