-- Databricks notebook source
-- DBTITLE 1,set the values
-- MAGIC %run "./Configure"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Rough edge:**
-- MAGIC <br>
-- MAGIC [Performance of SQL Variables in Notebook task](https://community.databricks.com/t5/data-engineering/databricks-sql-script-slow-execution-in-workflows-using/td-p/78333)

-- COMMAND ----------

declare variable dim_catschema string = dim_catalog || '.' || dim_schema;
declare variable conf_catschema string = conf_catalog || '.' || conf_schema;

-- COMMAND ----------

declare variable conf_table string = conf_catschema || '.' || 'elt_config';

-- COMMAND ----------

declare variable volume_name string = 'staging';
declare variable dir_path string = '/patient';

-- COMMAND ----------

declare variable file_stage string = '/Volumes/' || dim_catalog || '/' || dim_schema || '/' || volume_name || dir_path;
