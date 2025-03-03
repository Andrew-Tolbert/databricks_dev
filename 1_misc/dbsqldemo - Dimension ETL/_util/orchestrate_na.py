# Databricks notebook source
nb_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
nb_dir = "/Workspace" + "/".join(nb_path.split("/")[:-1]) + "/"

# COMMAND ----------

seed_dir = "file:" + nb_dir + "seed/"

stage_dir = "dbfs:/FileStore/stage/patient/"

file_initial = "patients50 - patients.csv"
file_incr_1 = "patients10-1 - patients.csv"
file_incr_2 = "patients10-2 - patients.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC Clear staging directory

# COMMAND ----------

dbutils.fs.rm(stage_dir, True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create All Tables

# COMMAND ----------

# MAGIC %run "../create/config table"

# COMMAND ----------

# MAGIC %run "../create/code table"

# COMMAND ----------

# MAGIC %run "../create/patient dimension"

# COMMAND ----------

# MAGIC %md
# MAGIC # Initial load

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage the source file for initial load
# MAGIC
# MAGIC Copy _patients50 - patients.csv_ from seed folder to dbfs

# COMMAND ----------

dbutils.fs.cp(seed_dir + file_initial, stage_dir + file_initial)
dbutils.fs.ls(stage_dir)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run initial load

# COMMAND ----------

# MAGIC %run "../patient dimension ELT w log"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Browse tables

# COMMAND ----------

# MAGIC %run "./browse current load"

# COMMAND ----------

# MAGIC %md
# MAGIC # Dummy run

# COMMAND ----------

dummy = 1

# COMMAND ----------

# Dummy run
# Run With No New Files to Load - Test!
%run "../patient dimension ELT w log"

# COMMAND ----------

# MAGIC %md
# MAGIC # Incremental load #1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage the source file for incremental load #1

# COMMAND ----------

dbutils.fs.cp(seed_dir + file_incr_1, stage_dir + file_incr_1)
dbutils.fs.ls(stage_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run incremental load #1

# COMMAND ----------

# MAGIC %run "../patient dimension ELT w log"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Browse tables

# COMMAND ----------

# MAGIC %run "./browse current load"

# COMMAND ----------

# MAGIC %md
# MAGIC # Incremental load #2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage the source file for incremental load #2

# COMMAND ----------

dbutils.fs.cp(seed_dir + file_incr_2, stage_dir + file_incr_2)
dbutils.fs.ls(stage_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run incremental load #2

# COMMAND ----------

# MAGIC %run "../patient dimension ELT w log"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Browse tables

# COMMAND ----------

# MAGIC %run "./browse current load"
