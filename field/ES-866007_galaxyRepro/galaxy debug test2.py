# Databricks notebook source
# MAGIC %md 
# MAGIC ##### This notebook is designed to run and execute print statements over a long period of time to test the scenario in [ES-866007](https://databricks.atlassian.net/browse/ES-866007)

# COMMAND ----------

import datetime as dt
from time import sleep
import pytz

#params 
_min = 10 
_sec = 30 
_start = dt.datetime.now()

# COMMAND ----------

import datetime as dt
from time import sleep


def galaxyPrintSleep(minutes,interval):
  t = dt.datetime.now()
  minute_count = minutes
  while True:
      delta_minutes = round((dt.datetime.now() - t).seconds / 60,2)     
      _totalExec = round((dt.datetime.now()-_start).seconds / 60,2) 

      # using now() to get current time
      _cTime = dt.datetime.now(pytz.timezone('US/Eastern'))

      if minute_count >= delta_minutes:
        print(f"Total run time ~ {_totalExec} min| Currently {_cTime}")
        sleep(interval) # Stop maxing out CPU
      else: 
        print("~~~~~~~~~~~~~~~~~~this concludes the output~~~~~~~~~~~~~~~~~~")
        break 

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)

# COMMAND ----------

galaxyPrintSleep(_min,_sec)
