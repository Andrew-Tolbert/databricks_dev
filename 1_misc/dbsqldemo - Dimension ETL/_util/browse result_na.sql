-- Databricks notebook source
-- MAGIC %run "../00-setup/00-setup"

-- COMMAND ----------

declare or replace variable br_table string; -- bronze table
declare or replace variable si_table string; -- silver table
declare or replace variable gd_table string; -- gold dimension table

-- COMMAND ----------

set variable br_table = dim_catschema || '.' || 'patient_br';
set variable si_table = dim_catschema || '.' || 'patient_si';
set variable gd_table = dim_catschema || '.' || 'g_patient_d';

-- COMMAND ----------

declare or replace variable initial_load_update_dt timestamp;
declare or replace variable incremental_load_1_update_dt timestamp;
declare or replace variable incremental_load_2_update_dt timestamp;

-- COMMAND ----------

set variable (initial_load_update_dt, incremental_load_1_update_dt, incremental_load_2_update_dt) = (with a as (select distinct update_dt from identifier(gd_table)) select (select update_dt from a qualify row_number() over (order by update_dt) = 1), (select distinct update_dt from a qualify row_number() over (order by update_dt) = 2), (select distinct update_dt from a qualify row_number() over (order by update_dt) = 3))

-- COMMAND ----------

select initial_load_update_dt, incremental_load_1_update_dt, incremental_load_2_update_dt

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Initial Load

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Bronze

-- COMMAND ----------

-- DBTITLE 1,Bronze
select id, src_changed_on_dt, data_source, * except(id, src_changed_on_dt, data_source) from identifier(br_table)
where update_dt = initial_load_update_dt
order by data_source, id, src_changed_on_dt

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Silver

-- COMMAND ----------

-- DBTITLE 1,Silver
select patient_src_id, src_changed_on_dt, data_source, * except(patient_src_id, src_changed_on_dt, data_source) from identifier(si_table)
where update_dt = initial_load_update_dt
order by data_source, patient_src_id, src_changed_on_dt

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Gold

-- COMMAND ----------

-- DBTITLE 1,Gold
select patient_sk, patient_src_id, effective_start_date, effective_end_date, data_source, * except(patient_sk, patient_src_id, effective_start_date, effective_end_date, data_source) from identifier(gd_table)
where update_dt = initial_load_update_dt
order by data_source, patient_src_id, effective_start_date

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Incremental Load 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Bronze

-- COMMAND ----------

-- DBTITLE 1,Bronze
select id, src_changed_on_dt, data_source, * except(id, src_changed_on_dt, data_source) from identifier(br_table)
where update_dt = incremental_load_1_update_dt
order by id, src_changed_on_dt, data_source

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Exception table

-- COMMAND ----------

-- DBTITLE 1,Exception
select * from identifier(conf_catschema || '.elt_error_table')
where update_dt = incremental_load_1_update_dt
order by data_source, table_name;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Silver

-- COMMAND ----------

-- DBTITLE 1,Silver
select patient_src_id, src_changed_on_dt, data_source, * except(patient_src_id, src_changed_on_dt, data_source) from identifier(si_table)
where update_dt = incremental_load_1_update_dt
order by data_source, patient_src_id, src_changed_on_dt

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Gold

-- COMMAND ----------

-- DBTITLE 1,Gold
select patient_sk, patient_src_id, effective_start_date, effective_end_date, data_source, * except(patient_sk, patient_src_id, effective_start_date, effective_end_date, data_source) from identifier(gd_table)
where update_dt = incremental_load_1_update_dt
order by data_source, patient_src_id, effective_start_date

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Incremental Load 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Bronze

-- COMMAND ----------

-- DBTITLE 1,Bronze
select id, src_changed_on_dt, data_source, * except(id, src_changed_on_dt, data_source) from identifier(br_table)
where update_dt = incremental_load_2_update_dt
order by id, src_changed_on_dt, data_source

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Exception table

-- COMMAND ----------

-- DBTITLE 1,Exception
select * from identifier(conf_catschema || '.elt_error_table')
where update_dt = incremental_load_2_update_dt
order by data_source, table_name;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Silver

-- COMMAND ----------

-- DBTITLE 1,Silver
select patient_src_id, src_changed_on_dt, data_source, * except(patient_src_id, src_changed_on_dt, data_source) from identifier(si_table)
where update_dt = incremental_load_2_update_dt
order by data_source, patient_src_id, src_changed_on_dt

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Gold

-- COMMAND ----------

-- DBTITLE 1,Gold
select patient_sk, patient_src_id, effective_start_date, effective_end_date, data_source, * except(patient_sk, patient_src_id, effective_start_date, effective_end_date, data_source) from identifier(gd_table)
where update_dt = incremental_load_2_update_dt
order by data_source, patient_src_id, effective_start_date
