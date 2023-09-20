# Databricks notebook source
# MAGIC %sql 
# MAGIC use catalog main; 
# MAGIC use schema aht_sa;

# COMMAND ----------

import requests
from delta.tables import * 
import math
import json
import datetime
from pyspark.sql.functions import from_json,col

# COMMAND ----------

secret = dbutils.secrets.get("at_secret_bucket","pat")
base_url = "https://e2-demo-field-eng.cloud.databricks.com"

# COMMAND ----------

def __databricksApi_generic_get(url: str, params: dict, headers: dict, api_command: str):
  """
  Return the response from the request module
  """
  url = f"{url}{api_command}"
  try:
    response = requests.get(
      headers = headers,
      url = url,
      params = params
    )
  except Exception as e:
    response = e
  
  return response 

# COMMAND ----------

def __map_reponse_to_list_dict_dt_schema(response: list, request_page: int):
  results = []
  for payload in response:
      results.append( {
        'object_id': payload['id'],
        'owner_email': payload['user']['email'],
        'payload': json.dumps(payload),
        'request_page': request_page,
        'timestamp': datetime.datetime.now()
      })
  return results

def __map_warehouse_reponse_to_list_dict(response: list):
  results = []
  for payload in response:
      results.append( {
        'warehouse_id': payload['id'],
        'warehouse_creator_name': payload['creator_name'],
        'warehouse_payload': payload
      })
  return results
  
def __map_data_source_reponse_to_list_dict(response: list):
  results = []
  for payload in response:
      results.append( {
        'data_source_object_id': payload['id'],
        'warehouse_id': payload['warehouse_id'],
        'data_source_name': payload['name'],
        'data_source_endpoint_id': payload['endpoint_id']
      })
  return results

def __map_response_payload_to_struct(df):
  json_schema = spark.read.json(df.rdd.map(lambda row: row.payload)).schema
  df = df.withColumn('payload_struct', from_json(col('payload'), json_schema)) 
  return df

# COMMAND ----------

def get_resources_metadata(instance_id: str, params: dict, header: dict, api_command: str, metadata_store_dt: str,how: str):
  try:
    #get the fist page 
    r = __databricksApi_generic_get(instance_id, params, header, api_command)
    
    if (r.status_code == 200):
      
      #Verify if API is paginated
      if ('page' in r.json()): 
        # retrieve remaining pages
        pages = math.ceil(int(r.json()['count'])/int(r.json()['page_size']))

        # convert response to dt schema
        results = __map_reponse_to_list_dict_dt_schema(r.json()['results'], int(r.json()['page']))
      else:
        pages = 1 
        # convert response to dt schema
        results = __map_reponse_to_list_dict_dt_schema(r.json(), pages)      

      #insert current page into delta table
      fullDf = spark.createDataFrame(results)
      #df.writeTo(table=metadata_store_dt).append()

      if (pages >= 2):
        for page in range(2,pages+1):
          params['page'] = page
          r = __databricksApi_generic_get(instance_id, params, header, api_command)

          # convert response to dt schema
          results = __map_reponse_to_list_dict_dt_schema(r.json()['results'], int(r.json()['page']))

          #insert current page into delta table
          incDf = spark.createDataFrame(results)
          fullDf = fullDf.union(incDf)
          #df.writeTo(table=metadata_store_dt).append()
      
      #made it to table save
       
      fullDf = __map_response_payload_to_struct(fullDf) #Add payload struct 
      print('MAPPED PAYLOAD TO STRUCT - CREATING DELTA TABLE!')
      if (how == 'full_append'):
         print('FULLY APPENDING')
         fullDf.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(metadata_store_dt)
      else:
        print('MERGING')
        sourceDF = spark.table(spark.table(metadata_store_dt)).alias('source')
        targetDF = fullDf.alias('target')
        (sourceDF
          .merge(targetDF, "source.object_id = target.object_id")
          .whenMatchedUpdateAll()
          .whenNotMatchedInsertAll()
          .execute()
        )
    else:
      print('Request response: ', r)
    
  except Exception as e:
    print(e)

# COMMAND ----------

# Get Queries 
payload = {}
headers = {
  'Authorization': f"Bearer {secret}"
}
params = { 
           'page_size': 100
         }
api_command = '/api/2.0/preview/sql/queries/admin'
metadata_store_dt = "metadata_queries_silver"

(DeltaTable.createOrReplace(spark) 
.tableName( metadata_store_dt) 
.addColumn("object_id", "STRING") 
.addColumn("owner_email", "STRING") 
.addColumn("payload","STRING") 
.addColumn("request_page", "BIGINT") 
.addColumn("timestamp", "TIMESTAMP") 
.execute()
)

# COMMAND ----------

request = get_resources_metadata(instance_id=base_url, 
                       header=headers, 
                       params=params,
                       api_command=api_command,
                       metadata_store_dt= metadata_store_dt ,
                       how = 'full_append')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata_queries_silver

# COMMAND ----------


