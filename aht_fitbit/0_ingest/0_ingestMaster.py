# Databricks notebook source
# MAGIC %md #### Global Variable and Function Configs

# COMMAND ----------

client_id = '2395GM' 
base_url = 'https://api.fitbit.com/'
vol = '/Volumes/aht_sa/fitbit' 

import json
import csv
import datetime as dt
import requests
import json 
import time

# COMMAND ----------

# LOAD CREDS FROM JSON
def get_creds():
    with open(f'{vol}/_configs/creds.json') as f:
      JSONData = json.load(f)
    access_token = JSONData['access_token']
    refresh_token = JSONData['refresh_token']
    user_id = JSONData['user_id']
    return access_token, refresh_token,user_id

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

def s3_sleep_batch_update(days,sleep):
    for n in range(0,days):
        day = (date.today() - timedelta(days = n)).strftime('%Y-%m-%d')
        filename,data,err = get_sleep(day)
        if err == 0:
            write_json('sleep',filename,data) 
            print(f"{filename}")
            time.sleep(sleep)
        else: 
            print('Error Authenticating with API')
            break

# COMMAND ----------

def s3_act_batch_update(days,sleep):
    for n in range(1,days):
        day = (date.today() - timedelta(days = n)).strftime('%Y-%m-%d')
        filename,data,err = get_activities(day)
        if err == 0:
            write_json('activities',filename,data)
            print(f"{filename}")
            time.sleep(sleep)
        else: 
            print('Error Authenticating with API')
            break

# COMMAND ----------

def write_json(path,filename,data):
    """
    writes data to volume
    """
    with open(f'{vol}/{path}/{filename}', 'w') as json_file:
      json.dump(json.loads(data), json_file)
