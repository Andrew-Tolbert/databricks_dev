# Databricks notebook source
# MAGIC %run "./0_getData"

# COMMAND ----------

# MAGIC %sql 
# MAGIC USE CATALOG shared; 
# MAGIC USE SCHEMA sra; 

# COMMAND ----------

dbutils.widgets.text("Repo","terraform-databricks-sra",label="Repo")

# COMMAND ----------

repo = dbutils.widgets.get("Repo")

df = response_2_df(repo,'clones')

# COMMAND ----------

from pyspark.sql.functions import explode
from delta.tables import *

total_df = df.selectExpr("count as total_count","uniques as total_uniques")

daily_df = (df
            .selectExpr("explode(clones) as metrics_daily")
            .selectExpr("date_est() as ingestDate","cast(metrics_daily.timestamp as date) AS date" ,"cast(metrics_daily.count as int) AS count", "cast(metrics_daily.uniques as int) AS uniques")
)

ingest_df = daily_df.join(total_df,how = "full")

ingest_df.createOrReplaceTempView("ingest")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO clones
# MAGIC USING ingest ON ingest.date = clones.date 
# MAGIC WHEN MATCHED AND date_add(clones.date,1) >= ingest.ingestDate THEN UPDATE SET * 
# MAGIC WHEN NOT MATCHED THEN INSERT * 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from clones
