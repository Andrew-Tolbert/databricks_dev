-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC **Specify** Catalog and Schema to create **patient tables** <br>
-- MAGIC
-- MAGIC <u>NOTE:</u>
-- MAGIC The catalog and schema can be create beforehand.  If not, ensure that the user running the workflow has permissions to create catalog and schema.

-- COMMAND ----------

-- DBTITLE 1,dimension schema
/*
Manually update the following, to use a different catalog / schema:
- dim_catalog
- dim_schema
*/

declare variable dim_catalog string = 'dbsqldemos';
declare variable dim_schema string = 'clinical';

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC **Specify** Catalog and Schema to create **config/log and exceptions tables** <br>
-- MAGIC
-- MAGIC <u>NOTE:</u>
-- MAGIC The catalog and schema can be create beforehand.  If not, ensure that the user running the workflow has permissions to create catalog and schema.

-- COMMAND ----------

-- DBTITLE 1,config schema
/*
Manually update the following, to use a different catalog / schema:
- conf_catalog
- conf_schema
*/

declare variable conf_catalog string = 'dbsqldemos';
declare variable conf_schema string = 'conf';
