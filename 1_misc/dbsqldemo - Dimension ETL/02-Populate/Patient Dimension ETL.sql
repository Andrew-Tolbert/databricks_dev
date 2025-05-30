-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Patient Dimension ELT
-- MAGIC This notebook contains the code to load the patient dimension which is part of the clinical star schema.<br>
-- MAGIC The same pattern can be used to load any of your business dimensions.<br>
-- MAGIC
-- MAGIC **<u>Initial and Incremental load of Patient dimension</u>**<br>
-- MAGIC The notebook performs the following tasks:<br>
-- MAGIC -> Load bronze table<br>
-- MAGIC -> Curate and load silver table<br>
-- MAGIC -> Transform and load dimension table using SCD2
-- MAGIC
-- MAGIC The bronze table is loaded from files extracted to cloud storage. 
-- MAGIC  These files contain incremental data extracts.
-- MAGIC  One or more new files are loaded during each run.<br>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### The code incorporates the following design elements:
-- MAGIC - Versioning of data
-- MAGIC - Checksum​
-- MAGIC - Master data lookup​
-- MAGIC - Data Validation & Exception handling
-- MAGIC
-- MAGIC Simply re-run to recover from a runtime error.
-- MAGIC <br>
-- MAGIC <br>
-- MAGIC _The code uses temporary views and single DML for each of the bronze, silver, and gold tables._

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Configuration Settings
-- MAGIC Set the catalog and schema where the tables will be created.

-- COMMAND ----------

-- MAGIC %run "../00-Setup/Initialize"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Set variables
-- MAGIC Variables used in the queries.

-- COMMAND ----------

declare or replace variable br_table string = dim_catschema || '.' || 'patient_br'; -- bronze table identifier
declare or replace variable si_table string = dim_catschema || '.' || 'patient_si'; -- silver table
declare or replace variable gd_table string = dim_catschema || '.' || 'g_patient_d'; -- gold dimension table

-- COMMAND ----------

declare or replace variable error_table string = conf_catschema || '.' || 'elt_error_table'; -- elt error table
declare or replace variable conf_table string = conf_catschema || '.' || 'elt_config'; -- config/log table
declare or replace variable code_table string = conf_catschema || '.' || 'code_m'; -- code master table

-- COMMAND ----------

declare or replace variable data_source string = 'ABC Systems'; -- source system code
declare or replace variable process_id string; -- a process id to associate with the load, for e.g., session id, run id

-- pass Workflows {{job.id}}-{{job.run_id}} to notebook parameter
-- to set process_id
-- Optional
set variable process_id = :p_process_id;

-- COMMAND ----------

declare or replace variable sqlstr string; -- variable to hold any sql statement for EXECUTE IMMEDIATE

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC **Set variables for incremental load to Silver and Gold tables.**<br>
-- MAGIC
-- MAGIC
-- MAGIC <u>Note:</u> <br>
-- MAGIC The code guards against runtime failures.  If the load end time failed to update in the config/log table, the next time round, the target table is checked to detrmine the last load time.<br>
-- MAGIC
-- MAGIC To set the last load date-<br>
-- MAGIC 1. Query log table
-- MAGIC 2. If no value found, query actual table to get the largest Update Date value
-- MAGIC 3. If it is the initial load, proceed with default value
-- MAGIC

-- COMMAND ----------

declare or replace variable si_last_load_date timestamp default '1990-01-01';
declare or replace variable gd_last_load_date timestamp default '1990-01-01';

-- COMMAND ----------

-- to get table_changes since silver table last loaded
set variable si_last_load_date = coalesce((select load_end_time from identifier(session.conf_table) where data_source = session.data_source and table_name = session.br_table), (select max(update_dt) from identifier(session.si_table)), session.si_last_load_date);

-- to get table_changes since gold table last loaded
set variable gd_last_load_date = coalesce((select load_end_time from identifier(session.conf_table) where data_source = session.data_source and table_name = session.gd_table), (select max(update_dt) from identifier(session.gd_table)), session.gd_last_load_date);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Load bronze table
-- MAGIC **Load the incremental (cdc) source files to bronze table**<br>
-- MAGIC
-- MAGIC The initial and incremental source CSV files are uploaded to a staging location.<br>
-- MAGIC
-- MAGIC The bronze table is insert-only.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Log load start
-- MAGIC Update load start time for bronze table in the config/log table.<br>
-- MAGIC The code inserts a record if none found for the table.<br>
-- MAGIC
-- MAGIC Note: You can insert a log record each time if you wish. 
-- MAGIC Currently using a single record per target table.

-- COMMAND ----------


merge into identifier(session.conf_table) as t
using (select * from values (session.data_source, session.br_table, current_timestamp(), null, null, session.process_id) as source(data_source, table_name, load_start_time, locked, load_end_time, process_id))
on (t.data_source = source.data_source and t.table_name = source.table_name)
when matched then update set t.load_start_time = source.load_start_time, t.locked = source.locked, t.load_end_time = source.load_end_time, t.process_id = source.process_id
when not matched then insert *
;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## COPY INTO bronze table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _**Rough edge**_<br>
-- MAGIC ** Cannot use IDENTIFIER for target of COPY INTO<br>
-- MAGIC ** Cannot user variable to specify file location (in from clause)<br>
-- MAGIC Hence using execute immediate.

-- COMMAND ----------

set variable sqlstr = "
copy into " || session.br_table || "
from (
  select
    *,
    session.data_source as data_source,
    _metadata.file_name as file_name,
    current_timestamp() as insert_dt,
    session.process_id as process_id
  from '" || session.file_stage || "'
)
fileformat = CSV
format_options ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
copy_options ('mergeSchema' = 'true')
;
"
;

-- load bronze table
execute immediate sqlstr;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Log load end
-- MAGIC Update load end time in the config/log table.

-- COMMAND ----------

merge into identifier(session.conf_table) as t
using (select * from values (session.data_source, session.br_table, current_timestamp()) as source(data_source, table_name, load_end_time))
on (t.data_source = source.data_source and t.table_name = source.table_name)
when matched then update set t.load_end_time = source.load_end_time
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Workaround - artificially increase version of bronze table**
-- MAGIC
-- MAGIC ** TABLE_CHANGES will throw an error if no (new) files were loaded. This is because the version of the (bronze) table doesn't change. Hence the workaround below.<br>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _**Rough edge**_
-- MAGIC ** Cannot get the results of DML, like num_affected_rows, num_inserted_rows, num_skipped_corrupt_files<br>
-- MAGIC ** And, don't have a procedural IF/ELSE.<br>
-- MAGIC This would have helped check if no records were copied.

-- COMMAND ----------

-- Workaround to increase version of bronze table to handle the case when no new files are copied
-- By setting a custom table property - "latest_run" time
-- (The property could be set for silver and gold as well, for informational purposes)

declare or replace variable sqlstr string;
set variable sqlstr = 'alter table identifier(br_table) set tblproperties (\'elt.latest_run\' = \'' || current_timestamp() || '\')';
execute immediate sqlstr;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Populate silver table
-- MAGIC Validate, curate, and load incremental data into the silver table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Validate incoming data
-- MAGIC Handle errors in the newly inserted data, before populating the curated data in the silver table.<br>
-- MAGIC Exception records (refs) are captured in common table elt_error_table.<br>
-- MAGIC <br>
-- MAGIC **The following conditions are being evaluated. If these are violated, the exception is logged and the record is omitted from the load.**<br>
-- MAGIC 1. The incoming data is expected to contain the following two columns
-- MAGIC > - Source Identifier (natural key)<br>
-- MAGIC > - Source Changed On Date (used for SCD2 versioning)<br>
-- MAGIC
-- MAGIC 2. Late arriving dimension version is not allowed (typically).
-- MAGIC > Checking to ensure that newer versions have a src_changed_on_dt > that of current version
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC _Note: Source Changed On Date could be a) business effective date b) date the record was inserted/updated in the source c) date of the source file (derived during COPY INTO)_
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create table_changes view**<br>
-- MAGIC Create a temporary view selecting incremental records from the bronze table.<br>
-- MAGIC Note that the bronze table has Change Data Feed enabled.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _**Rough edge**_<br>
-- MAGIC
-- MAGIC ** Cannot use variable as parameter to table_changes table name (string) parameter.

-- COMMAND ----------

-- creating view to overcome for table_changes param issue
-- bronze table changes
set var sqlstr = "create or replace temporary view tc_br_table_tv as with dummy as (select session.si_last_load_date) select * from table_changes('" || session.br_table || "', session.si_last_load_date)";

execute immediate sqlstr;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Quarantine exceptions

-- COMMAND ----------

-- Exception list:
-- src id cannot be null
-- src changed on dt cannot be null
-- dont allow src_changed_on_dt < any existing ones (this can potentially cascade to fact table)

insert into identifier(error_table)
select
  session.br_table,
  id,
  src_changed_on_dt,
  file_name,
  case when id is null then 'SOURCE_ID_NULL'
    when src_changed_on_dt is null then 'SOURCE_CHANGED_ON_DATE_NULL'
    else 'OLDER_VERSION'
    end as error_code,
  data_source,
  current_timestamp(),
  session.process_id
from tc_br_table_tv br_tc
where
  (
    id is null or
    src_changed_on_dt is null or
    exists(select 1
      from identifier(si_table) si
      where si.patient_src_id = br_tc.id
        and si.src_changed_on_dt >= br_tc.src_changed_on_dt
        and si.data_source = br_tc.data_source)
  )
  -- make it idempotent
  and not exists(select 1
    from identifier(error_table) err
    where ifnull(err.src_id, 'x') = ifnull(br_tc.id, 'x')
      and ifnull(err.src_changed_on_dt, '9999-01-01') = ifnull(br_tc.src_changed_on_dt, '9999-01-01')
      and err.data_source = br_tc.data_source
      and err.file_name = br_tc.file_name)
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Log load start
-- MAGIC
-- MAGIC Update load start time for silver table in the config/log table.

-- COMMAND ----------

merge into identifier(session.conf_table) as t
using (select * from values (session.data_source, session.si_table, current_timestamp(), null, null, session.process_id) as source(data_source, table_name, load_start_time, locked, load_end_time, process_id))
on (t.data_source = source.data_source and t.table_name = source.table_name)
when matched then update set t.load_start_time = source.load_start_time, t.locked = source.locked, t.load_end_time = source.load_end_time, t.process_id = source.process_id
when not matched then insert *
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create transformation view
-- MAGIC The temporary view curates the data as follows:<br>
-- MAGIC - Transforms columns
-- MAGIC - Standardizes code description for gender and ethnicity
-- MAGIC - Omits exception records
-- MAGIC - Ignores (duplicate) records already loaded in the silver table.  This helps with idempotency if there is a run error and existing records are re-processed.<br>
-- MAGIC
-- MAGIC The silver table is being treated as insert only.<br>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _**Rough edge**_<br>
-- MAGIC ** Variables can be used in other clauses only if they are present in a SELECT clause. Hence the dummy CTE.

-- COMMAND ----------

-- transform ingested source data
create or replace temporary view si_transform_tv
as
with br_cdc as (
  -- cannot use variable for table name parameter in table_changes
  select * from tc_br_table_tv
),
dummy as (select session.si_table, session.br_table, session.si_last_load_date, session.code_table) -- required for identity(si_table) to work
select
  id as patient_src_id,
  birthdate as date_of_birth,
  ssn as ssn,
  drivers as drivers_license,
  initcap(prefix) as name_prefix,
  first as first_name,
  last as last_name,
  suffix as name_suffix,
  maiden as maiden_name,
  gender as gender_cd,
  ifnull(code_gr.m_desc, gender) as gender_nm,
  marital as marital_status,
  ethnicity as ethnicity_cd,
  ifnull(code_ethn.m_desc, ethnicity) as ethnicity_nm,
  src_changed_on_dt,
  data_source,
  current_timestamp() as insert_dt,
  current_timestamp() as update_dt,
  session.process_id as process_id
from br_cdc
left outer join identifier(session.code_table) code_gr on code_gr.m_code = br_cdc.gender and code_gr.m_type = 'GENDER'
left outer join identifier(session.code_table) code_ethn on code_ethn.m_code = br_cdc.ethnicity and code_ethn.m_type = 'ETHNICITY'
where
  id is not null and src_changed_on_dt is not null and
  not exists(select 1
     from identifier(session.si_table) si
     where si.patient_src_id = br_cdc.id
       and si.src_changed_on_dt >= br_cdc.src_changed_on_dt
       and si.data_source = br_cdc.data_source)
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Insert data
-- MAGIC Insert data into the silver table using transformation view.

-- COMMAND ----------


-- Insert new and changed data
insert into identifier(si_table)
select * from si_transform_tv
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Log load end
-- MAGIC Update load end time in the config/log table.

-- COMMAND ----------

merge into identifier(session.conf_table) as t
using (select * from values (session.data_source, session.si_table, current_timestamp()) as source(data_source, table_name, load_end_time))
on (t.data_source = source.data_source and t.table_name = source.table_name)
when matched then update set t.load_end_time = source.load_end_time
;
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Populate gold table
-- MAGIC The gold table g_patient_d is created as a SCD2 dimension.<br>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Log load start
-- MAGIC
-- MAGIC Update load start time for dimension table in the config/log table.

-- COMMAND ----------

merge into identifier(session.conf_table) as t
using (select * from values (session.data_source, session.gd_table, current_timestamp(), null, null, session.process_id) as source(data_source, table_name, load_start_time, locked, load_end_time, process_id))
on (t.data_source = source.data_source and t.table_name = source.table_name)
when matched then update set t.load_start_time = source.load_start_time, t.locked = source.locked, t.load_end_time = source.load_end_time, t.process_id = source.process_id
when not matched then insert *
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create table_changes view**<br>
-- MAGIC Create a temporary view selecting incremental records from the silver table.<br>
-- MAGIC Note that the silver table has Change Data Feed enabled.

-- COMMAND ----------

-- creating view to overcome for table_changes param limitation
-- silver table changes
set var sqlstr = "create or replace temporary view tc_si_table_tv as with dummy as (select session.gd_last_load_date) select * from table_changes('" || session.si_table || "', session.gd_last_load_date)";

execute immediate sqlstr;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create transformation view
-- MAGIC The view performs multiple functions:<br>
-- MAGIC - Transform incoming rows as required
-- MAGIC - Create checksum
-- MAGIC - Handle new and changed instances
-- MAGIC - Handle multiple changes in a single batch
-- MAGIC - Ignore consecutive versions if no changes to business attributes of interest

-- COMMAND ----------

create or replace temporary view dim_transform_tv
as
with dummy as (select session.gd_table, session.gd_last_load_date), -- select the variables for use in later clauses
si_tc as (
  select
    last_name,
    first_name,
    name_prefix,
    name_suffix,
    maiden_name,
    gender_cd as gender_code,
    gender_nm as gender,
    date_of_birth,
    nvl(marital_status, 'Not Available') as marital_status,
    ethnicity_cd as ethnicity_code,
    ethnicity_nm as ethnicity,
    ssn,
    null as other_identifiers,
    null as uda,
    patient_src_id,
    src_changed_on_dt as effective_start_date,
    md5(ifnull(last_name, '#') || ifnull(first_name, '#') || ifnull(name_prefix, '#') || ifnull(name_suffix, '#') || ifnull(maiden_name, '#') ||
        ifnull(gender_cd, '#') || ifnull(gender_nm, '#') || ifnull(date_of_birth, '#') || ifnull(marital_status, '#') || ifnull(ethnicity_cd, '#') || ifnull(ethnicity_nm, '#') || ifnull(ssn, '#')) as checksum,
    data_source
  from tc_si_table_tv
),
curr_v as (
  -- GET current version records in dimension table, if any, corresponding to incoming data
  select gd.* except (effective_end_date, insert_dt, update_dt, process_id)
  from identifier(session.gd_table) gd
  where effective_end_date is null and
    exists (select 1 from si_tc where si_tc.patient_src_id = gd.patient_src_id AND si_tc.data_source = gd.data_source)
),
ins_upd_rows as (
  -- ISOLATE new patients and new versions
  -- for insert
  select
    null as patient_sk,
    *
  from si_tc
  union all
  -- existing version
  -- use this to update effective_end_date of existing version in gd
  select *
  from curr_v
),
no_dup_ver as (
  -- IGNORE consecutive versions if no changes to business attributes of interest, from
  -- new patients and new versions
  select
    *,
    lag(checksum, 1, null) over (partition by patient_src_id, data_source
                                 order by effective_start_date asc) as checksum_next
  from ins_upd_rows
  qualify checksum <> ifnull(checksum_next, '#') -- initially no records (for checksum_next)
)
-- FINAL set (new patients and new versions, existing versions for updating effective_end_date)
select
  *,
  lead(effective_start_date, 1, null) over (partition by patient_src_id
                                            order by effective_start_date asc) as effective_end_date
from no_dup_ver
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Merge data
-- MAGIC Update the dimension table by:<br>
-- MAGIC - Merge new and changed records.<br>
-- MAGIC - Version existing patient records (by updating effective_end_date).<br>

-- COMMAND ----------

merge into identifier(session.gd_table) d
using dim_transform_tv tr
on d.patient_sk = tr.patient_sk
when matched then update
  -- update end date for existing version of patient
  set d.effective_end_date = tr.effective_end_date,
    update_dt = current_timestamp(),
    process_id = session.process_id
when not matched then insert (
  -- insert new vesrions and new patients
  last_name,
  first_name,
  name_prefix,
  name_suffix,
  maiden_name,
  gender_code,
  gender,
  date_of_birth,
  marital_status,
  ethnicity_code,
  ethnicity,
  ssn,
  patient_src_id,
  effective_start_date,
  effective_end_date,
  checksum,
  data_source,
  insert_dt,
  update_dt,
  process_id)
  values (last_name, first_name, name_prefix, name_suffix, maiden_name, gender_code, gender, date_of_birth, marital_status, ethnicity_code,
    ethnicity, ssn, patient_src_id, effective_start_date, effective_end_date, checksum, data_source, current_timestamp(), current_timestamp(), session.process_id)
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Log load end
-- MAGIC Update load end time in the config/log table.

-- COMMAND ----------

merge into identifier(session.conf_table) as t
using (select * from values (session.data_source, session.gd_table, current_timestamp()) as source(data_source, table_name, load_end_time))
on (t.data_source = source.data_source and t.table_name = source.table_name)
when matched then update set t.load_end_time = source.load_end_time
;
