# Databricks notebook source
# MAGIC %md 
# MAGIC ### ## This notebook is not necessary as of now but contains the starting point to creating a SP to run the workflow

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam

scope = 'ahtsas'
key = 'current_pat'

TOKEN = dbutils.secrets.get(scope,key)

w = WorkspaceClient(
  host  = 'https://e2-demo-field-eng.cloud.databricks.com/',
  token = TOKEN
)
spn = w.service_principals.create(id = 'sra-jobRunner', 
                                  display_name='sra-jobRunner')

# COMMAND ----------

obo = w.token_management.create_obo_token(application_id=spn.application_id,lifetime_seconds='')

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT ALL PRIVILEGES on CATALOG shared to `sra-jobRunner`

# COMMAND ----------


