# Databricks notebook source
# MAGIC %md #### Global Variable and Function Configs

# COMMAND ----------

client_id = '2395FL' 
base_url = 'https://api.fitbit.com/'
vol = '/Volumes/ahtsa/fitbit' 

import json
import csv
import datetime as dt
import requests
import json 
import time
from xml.etree import ElementTree as ET
from pyspark.sql.functions import explode, col, regexp_replace,to_json

# COMMAND ----------

# DBTITLE 1,DECLARE TIME VARIABLES
# RELEVANT TIME VARS FOR INCREMENTAL AND BATCH LOADS
from dateutil.relativedelta import relativedelta

date_0 = dt.date.today()
date_1 = (date_0 - dt.timedelta(days = 1))
date_7 = (date_0 - dt.timedelta(days = 7))
# for bulk data 
date_120 = (date_0 - dt.timedelta(days = 150))
date_monthStart = (date_0.replace(day=1))
date_5Mo = date_monthStart - relativedelta(months = 5)

# COMMAND ----------

# DBTITLE 1,CREDENTIAL FUNCTIONS
# LOAD CREDS FROM JSON
def get_creds():
    with open(f'{vol}/_configs/creds.json') as f:
      JSONData = json.load(f)
    access_token = JSONData['access_token']
    refresh_token = JSONData['refresh_token']
    user_id = JSONData['user_id']
    return access_token, refresh_token,user_id
  
  
def refresh_access_token(refresh_token): 

  url = f"{base_url}oauth2/token"
  
  payload = {
      'refresh_token': refresh_token,
      'grant_type': 'refresh_token', 
      'client_id': client_id
  }
  headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
  }
  response = requests.request("POST", url, headers=headers, data=payload)
  
  #REWRITE TOKEN FILE
  with open(f'{vol}/_configs/creds.json', 'w') as json_file:
      json.dump(json.loads(response.text), json_file)
      print('rewrote file')

# COMMAND ----------

# DBTITLE 1,API CALL FUNCTIONS
def get_sleep(date,access_token): 
    url = f"{base_url}1.2/user/{user_id}/sleep/date/{date}.json"
    
    payload={
        }
    headers = {
      'Authorization': f"Bearer {access_token}"
    }
    
    err = 0
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        response.raise_for_status()
        filename = "sleep_" + date.replace("-","") + ".json"
        return filename,response.text,err
    except:
        err += 1 
        return 'NA','NA',err
      
def get_activities(date,access_token): 
    url = f"{base_url}1/user/{user_id}/activities/date/{date}.json"
    
    payload={
        }
    headers = {
      'Authorization': f"Bearer {access_token}"
    }
    
    err = 0
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        response.raise_for_status()
        filename = "activities_" + date.replace("-","") + ".json"
        return filename,response.text,err
    except:
        err += 1 
        return 'NA','NA',err

def get_activityLogList(date,access_token,limit): 
    """
    THIS API OPERATES WITH A START DATE AND A LIMIT 
    EX - START ON 2022-09-20 AND GO TO 2022-09-30
    https://dev.fitbit.com/build/reference/web-api/activity/get-activity-log-list/
    11/18/23 - Return Max Date within the file
    """
    
    url = f"{base_url}1/user/{user_id}/activities/list.json?afterDate={date}&sort=asc&offset=0&limit={limit}"
    
    payload={
        }
    headers = {
      'Authorization': f"Bearer {access_token}"
    }
    
    err = 0
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        response.raise_for_status()
        filename = "activityLog_" + date.replace("-","") + ".json"               
        return filename,response.text,err
      
    except:
        err += 1 
        return 'NA','NA',err
      
def getTCX(url):
    payload={
        }
    headers = {
      'Authorization': f"Bearer {access_token}"
    }
    err=0
    try:
      response = requests.request("GET", url, headers=headers, data=payload)
      response.raise_for_status()
      filename = url[-15:-4] + '.xml'
    except:
      err += 1 
      return 'NA','NA',err
    return response.text,filename,err

# COMMAND ----------

# DBTITLE 1,API ORCHESTRATION FUNCTIONS
def s3_interval_update(endpoint,start,end,sleep,_access_token):
    while start <= end:
      _day = start.strftime('%Y-%m-%d')
      match endpoint:
        case 'sleep':
            filename,data,err = get_sleep(_day,_access_token)
        case 'activities':
            filename,data,err = get_activities(_day,_access_token)
        case _:
            print("Provide a valid service to call")
      if err == 0:
          write_json(f'raw_fitbitapi/{endpoint}',filename,data) 
          #print(f"{filename}")
          print(f"{filename}")
          time.sleep(sleep)
          start += dt.timedelta(days=1)
      else: 
          print('Error Authenticating with API')
          break


def s3_interval_activities(start,end,limit,sleep,_access_token):
    buffer = 4
    while start <= end:
      _day = start.strftime('%Y-%m-%d')
      filename,data,err = get_activityLogList(_day,access_token,limit) 
      if err == 0:
          write_json(f'raw_fitbitapi/activitylog',filename,data) 
          print(f"{filename}")
          time.sleep(sleep)
          start += dt.timedelta(days=(limit-buffer))
          
          #NOW GET TCX DATA FROM RECENTLY WRITTEN JSON 
          write_json('temp','temp_json',data)
          tcxDf = spark.read.json(f'{vol}/raw_fitbitapi/activitylog/{filename}')

          if "tcxLink:" in tcxDf.schema.simpleString(): 
            tcxDf= (tcxDf.select("activities")
                  .select("*", explode(tcxDf.activities).alias("activities_exploded"))
                  .select('activities_exploded.tcxLink')
                  .withColumn('tcxLink',regexp_replace('tcxLink', 'user/-/', f'user/{user_id}/'))
            )

            tcxList = tcxDf.select("tcxLink").distinct().collect()

            for x in tcxList:
              tcxResponse,tcxFilename,tcxErr = getTCX(x[0])
              if tcxErr ==0:
                write_xml('raw_fitbitapi/TCX',tcxFilename,tcxResponse)
                print(tcxFilename)
                time.sleep(3)
              if tcxErr ==1:
                continue 
            #DONE WRITING TCX
      else: 
          print('Error Authenticating with API')
          break

# COMMAND ----------

# DBTITLE 1,VOLUME FUNCTIONS
def write_json(path,filename,data):
    """
    writes data to volume
    """
    with open(f'{vol}/{path}/{filename}', 'w') as json_file:
      json.dump(json.loads(data), json_file)

# COMMAND ----------

def write_xml(path,filename,data):
    """
    writes data to volume
    """
    with open(f'{vol}/{path}/{filename}', 'w') as xml_file:
      xml_file.write(data)
