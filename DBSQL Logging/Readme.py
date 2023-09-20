# Databricks notebook source
# MAGIC %md
# MAGIC # Readme
# MAGIC
# MAGIC This notebook collection pulls together data from 4 different APIs to produce useful metrics to monitor DBSQL usage:
# MAGIC * [SQL Warehouses APIs 2.0](https://docs.databricks.com/sql/api/sql-endpoints.html), referred to as the Warehouse API
# MAGIC * [Query History API 2.0](https://docs.databricks.com/sql/api/query-history.html), referred to as the Queries API
# MAGIC * [Jobs API 2.1](https://docs.databricks.com/dev-tools/api/latest/jobs.html), referred to as the Workflows API
# MAGIC * [Queries and Dashboards API](https://docs.databricks.com/sql/api/queries-dashboards.html), referred to as the Dashboards API  - ❗️in preview, known issues, deprecated soon❗️
# MAGIC
# MAGIC Creator: holly.smith@databricks.com
# MAGIC
# MAGIC ## Setup
# MAGIC Cluster config: 
# MAGIC * 11.3 LTS 
# MAGIC * Driver: i3.xlarge
# MAGIC * Workers: 2 x i3.xlarge  - the data here is fairly small
# MAGIC
# MAGIC Profile:
# MAGIC Must be an **admin** in your workspace for the Dashboards API
# MAGIC
# MAGIC ## Contents
# MAGIC
# MAGIC #### 00-Config
# MAGIC This is the configuration of:
# MAGIC * Workspace URL
# MAGIC * Authetication options
# MAGIC * Database and Table storage
# MAGIC * `OPTIMIZE`, `ZORDER` and `VACUUM` settings
# MAGIC
# MAGIC #### 01-Functions
# MAGIC * Reuable functions created, all pulled out for code readability
# MAGIC
# MAGIC #### 02-Initialisation
# MAGIC * Creates the database if it doesn't exist
# MAGIC * Optional: specify a location for the Database
# MAGIC Dependent on: `00-Config`
# MAGIC
# MAGIC #### 03-APIs_to_Delta
# MAGIC
# MAGIC **Warehouses API:** Appends each call of the api and uses a snapshot time to identify 
# MAGIC
# MAGIC **Query History API:** Upserts / merges new queries to the original table
# MAGIC
# MAGIC **Workflows API:** Upserts / merges new workflows to the original table
# MAGIC
# MAGIC **Dashboards API:**  . I have tried my best to refer to it as a preview in every step of the code to reflect how this is a preview
# MAGIC
# MAGIC Dependent on: `00-Config`, `01-Functions`, `02-Initialisation`
# MAGIC
# MAGIC #### 04-Metrics
# MAGIC * Dashbaords & Queries with owner, useful for finding orphaned records
# MAGIC * Queries to Optimise
# MAGIC * Warehouse Metrics
# MAGIC * Per User Metrics
# MAGIC
# MAGIC
# MAGIC Dependent on: `00-Config`
# MAGIC
# MAGIC
# MAGIC #### 99-Maintenance
# MAGIC Runs `OPTIMIZE`, `ZORDER` and `VACUUM` against tables
# MAGIC
# MAGIC Dependent on: `00-Config`
# MAGIC
# MAGIC
# MAGIC ## Troubleshooting
# MAGIC
# MAGIC #### Cluster OOM
# MAGIC The data used here was very small, even for a Databricks demo workspace with thousands of users. Parts of 03-APIs_to_Delta involves pulling a json to the driver, in the highly unlikely event of the driver OOM you have two choices:
# MAGIC 1. The quick option: select a larger driver
# MAGIC 2. The robust option: loop through reading in only one page at a time and write to a spark dataframe at a time
# MAGIC
# MAGIC #### Dashboards API not sorting in new queries
# MAGIC There are known issues with the API. Where possible, try to use the 
# MAGIC
# MAGIC #### Dashboards API has stopped working
# MAGIC This API will go through stages of deprecation, unfortunately with no hard timelines as of yet. Here is the rough process:
# MAGIC 1. When DBSQL Pro comes out, the API will be officially deprecated
# MAGIC 2. It should (*should*) be removed from the documentation at that point
# MAGIC 3. Later it will become totally unavailable 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Warehouses Data
# MAGIC
# MAGIC Responds with information pertaining to these views
# MAGIC
# MAGIC ![Warehouses](https://i.imgur.com/lE7bIu5.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query History
# MAGIC
# MAGIC Responds with information pertaining to this view
# MAGIC
# MAGIC ![Warehouses](https://i.imgur.com/fZaQYzT.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflows API 
# MAGIC
# MAGIC Responds with information pertaining to these views, especially the new multi task nature of the jobs
# MAGIC
# MAGIC ![Warehouses](https://i.imgur.com/lu8he8P.png)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Dashboards API
# MAGIC
# MAGIC Responds with information pertaining to these views
# MAGIC
# MAGIC ❗️in preview, deprecated soon❗️
# MAGIC
# MAGIC
# MAGIC ![image](https://i.imgur.com/d9MJKcg.png)
