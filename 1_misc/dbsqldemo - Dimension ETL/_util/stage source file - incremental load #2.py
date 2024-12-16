# Databricks notebook source
# DBTITLE 1,initialize config for sgc
# MAGIC %run "./initialize-staging"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage the source file for second incremental load

# COMMAND ----------

# DBTITLE 1,incremental file name (2)
file_name = "patients_incr2.csv"

# COMMAND ----------

# DBTITLE 1,stage the file for initial load
# download to volume staging path
get_file(file_name)
