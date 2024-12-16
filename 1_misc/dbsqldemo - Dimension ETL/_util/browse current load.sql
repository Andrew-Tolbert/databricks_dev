-- Databricks notebook source
-- MAGIC %run "../00-Setup/Initialize"

-- COMMAND ----------

declare or replace variable br_table string; -- bronze table
declare or replace variable si_table string; -- silver table
declare or replace variable gd_table string; -- gold dimension table

-- COMMAND ----------

set variable br_table = dim_catschema || '.' || 'patient_br';
set variable si_table = dim_catschema || '.' || 'patient_si';
set variable gd_table = dim_catschema || '.' || 'g_patient_d';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Config/Log table**

-- COMMAND ----------

select * from identifier(conf_table) order by load_start_time;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Patient Bronze table**

-- COMMAND ----------

-- DBTITLE 1,Bronze
select id, src_changed_on_dt, data_source, * except(id, src_changed_on_dt, data_source) from identifier(br_table)
where process_id = (select process_id from identifier(conf_table) where table_name = br_table)
order by data_source, id, src_changed_on_dt

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Exceptions table**

-- COMMAND ----------

-- DBTITLE 1,Exception
select * from identifier(conf_catschema || '.elt_error_table')
where process_id = (select process_id from identifier(conf_table) where table_name = br_table)
order by data_source, table_name;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Patient Silver table**

-- COMMAND ----------

-- DBTITLE 1,Silver
select patient_src_id, src_changed_on_dt, data_source, * except(patient_src_id, src_changed_on_dt, data_source) from identifier(si_table)
where process_id = (select process_id from identifier(conf_table) where table_name = si_table)
order by data_source, patient_src_id, src_changed_on_dt

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Patient Dimension table**

-- COMMAND ----------

-- DBTITLE 1,Gold
select patient_sk, patient_src_id, effective_start_date, effective_end_date, data_source, * except(patient_sk, patient_src_id, effective_start_date, effective_end_date, data_source) from identifier(gd_table)
where process_id = (select process_id from identifier(conf_table) where table_name = gd_table)
order by data_source, patient_src_id, effective_start_date
