-- Databricks notebook source
-- MAGIC %run "../00-Setup/Setup"

-- COMMAND ----------

declare or replace variable br_table string; -- bronze table identifier
declare or replace variable si_table string; -- silver table identifier
declare or replace variable gd_table string; -- gold dimension table identifier
declare or replace variable catalog_schema string; -- catalog.schema

-- COMMAND ----------

-- catalog.schema where the tables will be created
set variable catalog_schema = dim_catschema;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Create Tables
-- MAGIC Create the bronze, silver, and gold tables for patient.<br>
-- MAGIC The gold table is the patient dimension which is part of the clinical star schema.
-- MAGIC
-- MAGIC <u>NOTE:</u> By default, the tables are created in the **catalog dbsqldemos**.  To change this, or specify an existing catalog / schema, please see [Configure notebook]($../00-Setup/Configure) for more context.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Bronze Table
-- MAGIC The schema for the bronze table will be derived from the source data file(s)

-- COMMAND ----------

-- three-level name
set variable br_table =  catalog_schema || '.' || 'patient_br';

-- COMMAND ----------

drop table if exists identifier(br_table);

-- COMMAND ----------

create table if not exists identifier(br_table)
comment 'Patient bronze table ingesting initial and incremental master data from csv files'
tblproperties (delta.enableChangeDataFeed = true)
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Silver Table

-- COMMAND ----------

-- three-level name
set variable si_table = catalog_schema || '.' || 'patient_si';

-- COMMAND ----------

drop table if exists identifier(si_table);

-- COMMAND ----------

-- potential clustering columns - data_source, src_changed_on_dt, patient_src_id

-- src_changed_on_dt will be naturally ordered (ingestion-time clustering)
-- data_source will typically be the same for all records in a source file
-- liquid-clustering to depend on other uses of silver table
--   (apart from sourcing the dimension table)

create table if not exists identifier(si_table) (
  patient_src_id string comment 'ID of the record in the source',
  date_of_birth date comment 'date of birth',
  ssn string comment 'social security number',
  drivers_license string comment 'driver\'s license',
  name_prefix string comment 'name prefix',
  first_name string comment 'first name of patient',
  last_name string comment 'last name of patient',
  name_suffix string comment 'name suffix',
  maiden_name string comment 'maiden name',
  gender_cd string comment 'code for patient\'s gender',
  gender_nm string comment 'description of patient\'s gender',
  marital_status string comment 'marital status',
  ethnicity_cd string comment 'code for patient\'s ethnicity',
  ethnicity_nm string comment 'description of patient\'s ethnicity',
  src_changed_on_dt timestamp comment 'date of last change to record in source',
  data_source string comment 'code for source system',
  insert_dt timestamp comment 'date record inserted',
  update_dt timestamp comment 'date record updated',
  process_id string comment 'Process ID for run'
)
comment 'curated silver table for patient data'
tblproperties (delta.enableChangeDataFeed = true)
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Dimension

-- COMMAND ----------

-- three-level name
set variable gd_table = catalog_schema || '.' || 'g_patient_d';

-- COMMAND ----------

drop table if exists identifier(gd_table);

-- COMMAND ----------

create table if not exists identifier(gd_table) (
  patient_sk bigint generated always as identity comment 'Primary Key (ID)',
  last_name string NOT NULL  comment 'Last name of the person',
  first_name string NOT NULL  comment 'First name of the person',
  name_prefix string  comment 'Prefix of person name',
  name_suffix string  comment 'Suffix of person name',
  maiden_name string comment 'Maiden name',
  gender_code string  comment 'Gender code',
  gender string  comment 'gender description',
  date_of_birth timestamp  comment 'Birth date and time',
  marital_status string  comment 'Marital status',
  ethnicity_code string,
  ethnicity string,
  ssn string  comment 'Patient SSN',
  other_identifiers map <string, string>  comment 'Identifier type (passport number, license number except mrn, ssn) and value',
  uda map <string, string>  comment 'User Defined Attributes',
  patient_src_id string  comment 'Unique reference to the source record',
  effective_start_date timestamp  comment 'SCD2 effective start date for version',
  effective_end_date timestamp  comment 'SCD2 effective start date for version',
  checksum string  comment 'Checksum for the record',
  data_source string  comment 'Code for source system',
  insert_dt timestamp comment 'record inserted time',
  update_dt timestamp comment 'record updated time',
  process_id string  comment 'Process ID for run',
  constraint g_patient_d_pk primary key (patient_sk)
)
comment 'Patient dimension' cluster by (last_name, gender_code, date_of_birth)
tblproperties (
  delta.deletedFileRetentionDuration = 'interval 30 days'
)
;

