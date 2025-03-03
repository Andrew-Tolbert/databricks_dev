# Databricks notebook source
import os
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

jobs = w.jobs.list() # a Python generator

df_jobs = spark.createDataFrame(
  data=jobs)

# COMMAND ----------

jobs.settings.name

# COMMAND ----------

for job in w.jobs.list():
    print(f'Found job: {job.settings.email_notifications} (id: {job.job_id})')

# COMMAND ----------

for job in w.jobs.list():
    if job.settings.schedule is None: # Skip jobs that are not scheduled
        continue 
    else: 
       print(f'Found job: {job.settings.email_notifications} (id: {job.job_id})')

# COMMAND ----------


