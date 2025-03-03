-- Databricks notebook source
-- MAGIC %run "../00-Setup/Setup"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Config/Log Table for ELT
-- MAGIC This table captures the metadata for a given table that includes the table name, load start time and load end time.

-- COMMAND ----------

drop table if exists identifier(conf_catschema || '.elt_config');

-- COMMAND ----------

create table identifier(conf_catschema || '.elt_config') (data_source string, table_name string, load_start_time timestamp, locked boolean, load_end_time timestamp, process_id string)
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Common Exceptions Table
-- MAGIC This table captures any data quality exceptions for the table (for e.g., patient silver table) along with the corresponding record Id (for e.g., patient_src_id) and an error code that indicates the violation (for e.g., SOURCE_CHANGED_ON_DATE_NULL)

-- COMMAND ----------

drop table if exists identifier(conf_catschema || '.elt_error_table');

-- COMMAND ----------

create table identifier(conf_catschema || '.elt_error_table') (table_name string, src_id string, src_changed_on_dt timestamp, file_name string, error_code string, data_source string, insert_dt timestamp, process_id string)
;
