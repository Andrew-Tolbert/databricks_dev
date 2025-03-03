-- Databricks notebook source
-- MAGIC %run "./Initialize"

-- COMMAND ----------

declare variable sqlstr string; -- variable to hold any sql statement for EXECUTE IMMEDIATE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Catalog(s) and Schema(s) if required

-- COMMAND ----------

set variable sqlstr = "create catalog if not exists " || dim_catalog;
execute immediate sqlstr;

-- COMMAND ----------

set variable sqlstr = "create schema if not exists " || dim_catschema;
execute immediate sqlstr;

-- COMMAND ----------

set variable sqlstr = "create catalog if not exists " || conf_catalog;
execute immediate sqlstr;

-- COMMAND ----------

set variable sqlstr = "create schema if not exists " || conf_catschema;
execute immediate sqlstr;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Volume for staging source data files

-- COMMAND ----------

set variable sqlstr = "create volume if not exists " || dim_catschema || "." || volume_name;
execute immediate sqlstr;
