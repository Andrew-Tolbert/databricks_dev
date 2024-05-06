# Databricks notebook source
import requests
from pyspark.sql import Row
import json 

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG shared; 
# MAGIC USE SCHEMA sra;

# COMMAND ----------

token = dbutils.secrets.get("sra_tf","git_api")

# COMMAND ----------

def response_2_df(repo,endpoint):
  url = f"https://api.github.com/repos/databricks/{repo}/traffic/{endpoint}"
  payload = {}
  headers = {
    'Authorization': f'Bearer {token}'
  }
  response = requests.request("GET", url, headers=headers, data=payload)

  json_data = [json.loads(response.text)]
  if 'popular' in endpoint:
    json_data = json_data[0]
  rows = [Row(**json_dict) for json_dict in json_data]
  df = spark.createDataFrame(rows)
  # Return  the DataFrame
  return(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temporary function with no parameter.
# MAGIC CREATE FUNCTION IF NOT EXISTS date_est() 
# MAGIC RETURNS DATE 
# MAGIC RETURN CAST(timestampadd(HOUR,-4,current_timestamp()) as DATE) ; 
# MAGIC
# MAGIC SELECT date_est() as Update_Time

# COMMAND ----------


