# Databricks notebook source
catalog = 'main'
schema = 'aht_sa'
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

def src_json(cloud):
  return f"https://www.databricks.com/data/pricing/{cloud}.json"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from urllib.request import urlopen

cloud_list = ['AWS','GCP']

df = spark.createDataFrame([],StructType([]))

for i,j in enumerate(cloud_list):
  print(src_json(j))
  jsonData = urlopen(src_json(j)).read().decode('utf-8')
  rdd = spark.sparkContext.parallelize([jsonData])
  df2 = spark.read.json(rdd)
  df = df.unionByName(df2,allowMissingColumns = True)
  
df.write.format('delta').mode("overwrite").saveAsTable(f"pricing_all")  

# COMMAND ----------

display(
  'AWS Count: ' + 
  str(df.filter(F.col('cloud') == 'AWS').count()) + ' | ' + 
 'GCP Count: ' + 
  str(df.filter(F.col('cloud') == 'GCP').count())
)

# COMMAND ----------

df = spark.sql("select * from main.aht_sa.pricing_all")

# COMMAND ----------

def new_display(_df):
  return display(_df.limit(1000))
new_display(df)

# COMMAND ----------

display(df)

# COMMAND ----------


