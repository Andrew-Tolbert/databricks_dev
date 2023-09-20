# Databricks notebook source
# MAGIC %md-sandbox ### Batch Ingestion

# COMMAND ----------

# MAGIC %run ./0_ingestMaster 

# COMMAND ----------

from dateutil.relativedelta import relativedelta

date_0 = dt.date.today()
date_1 = (date_0 - dt.timedelta(days = 1))
date_7 = (date_0 - dt.timedelta(days = 7))
# for bulk data 
date_120 = (date_0 - dt.timedelta(days = 150))
date_monthStart = (date_0.replace(day=1))
date_5Mo = date_monthStart - relativedelta(months = 5)

# COMMAND ----------

def s3_interval_update(_type,start,end,sleep,_access_token):
    while start <= end:
      _day = start.strftime('%Y-%m-%d')
      match _type:
        case 'sleep':
            filename,data,err = get_sleep(_day,_access_token)
        case 'activities':
            filename,data,err = get_activities(_day,_access_token)
      if err == 0:
          write_json(f'raw_fitbitapi/{_type}',filename,data) 
          #print(f"{filename}")
          print(_day)
          time.sleep(sleep)
          start += dt.timedelta(days=1)
      else: 
          print('Error Authenticating with API')
          break

# COMMAND ----------

(access_token, refresh_token,user_id) = get_creds()

#s3_interval_update('sleep',date_5Mo,date_0,30,access_token)

s3_interval_update('activities',date_5Mo,date_1,20,access_token)
