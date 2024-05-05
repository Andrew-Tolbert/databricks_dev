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

df = response_2_df(repo,'popular/referrers')

# COMMAND ----------

from pyspark.sql.functions import explode, current_date
from delta.tables import *

daily_df = (df
            .selectExpr("current_date() as ingestDate","*")
)

ingest_df = daily_df

ingest_df.createOrReplaceTempView("ingest")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO referrers
# MAGIC USING ingest ON ingest.ingestDate = referrers.ingestDate 
# MAGIC WHEN MATCHED THEN UPDATE SET * 
# MAGIC WHEN NOT MATCHED THEN INSERT * 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from referrers order by count desc

# COMMAND ----------

path_df = response_2_df(repo,'popular/paths')

# COMMAND ----------

path_daily_df = (path_df
            .selectExpr("current_date() as ingestDate","*")
)

path_ingest_df = path_daily_df

path_ingest_df.createOrReplaceTempView("paths_ingest")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO referral_paths
# MAGIC USING paths_ingest ON paths_ingest.ingestDate = referral_paths.ingestDate 
# MAGIC WHEN MATCHED THEN UPDATE SET * 
# MAGIC WHEN NOT MATCHED THEN INSERT * 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from referral_paths order by count desc

# COMMAND ----------


