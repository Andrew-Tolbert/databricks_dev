-- Databricks notebook source
-- MAGIC %run "../00-Setup/Setup"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Master Data
-- MAGIC Standardized codes used for coded attributes

-- COMMAND ----------

drop table if exists identifier(conf_catschema || '.code_m');

-- COMMAND ----------

create table identifier(conf_catschema || '.code_m') (
  m_code string comment 'code',
  m_desc string comment 'name or description for the code',
  m_type string comment 'attribute type utilizing code'
)
comment 'master table for coded attributes'

-- COMMAND ----------

insert into identifier(conf_catschema || '.code_m')
values
  ('M', 'Male', 'GENDER'),
  ('F', 'Female', 'GENDER'),
  ('hispanic', 'Hispanic', 'ETHNICITY'),
  ('nonhispanic', 'Not Hispanic', 'ETHNICITY')e
;
