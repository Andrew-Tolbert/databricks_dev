#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar  3 12:13:26 2023

@author: Andrew.Tolbert
"""

python migration_pipeline.py -h
usage: migration_pipeline.py [-h] [--profile PROFILE] [--azure or gcp] [--silent] [--no-ssl-verification] [--debug] [--set-export-dir SET_EXPORT_DIR]
                             [--cluster-name CLUSTER_NAME] [--notebook-format {DBC,SOURCE,HTML}] [--overwrite-notebooks] [--archive-missing]
                             [--repair-metastore-tables] [--metastore-unicode] [--skip-failed] [--session SESSION] [--dry-run] [--export-pipeline] [--import-pipeline]
                             [--validate-pipeline] [--validate-source-session VALIDATE_SOURCE_SESSION] [--validate-destination-session VALIDATE_DESTINATION_SESSION]
                             [--use-checkpoint] [--skip-tasks SKIP_TASKS [SKIP_TASKS ...]] [--num-parallel NUM_PARALLEL] [--retry-total RETRY_TOTAL]
                             [--retry-backoff RETRY_BACKOFF] [--start-date START_DATE]
                             [--exclude-work-item-prefixes EXCLUDE_WORK_ITEM_PREFIXES [EXCLUDE_WORK_ITEM_PREFIXES ...]]

Export user(s) workspace artifacts from Databricks

optional arguments for import/export pipeline:
  -h, --help            show this help message and exit
  --profile PROFILE     Profile to parse the credentials
  --azure or --gcp      Run on Azure or GCP (Default is AWS)
  --silent              Silent all logging of export operations.
  --no-ssl-verification
                        Set Verify=False when making http requests.
  --debug               Enable debug logging
  --no-prompt           Skip interactive prompt/confirmation for workspace import.
  --set-export-dir SET_EXPORT_DIR
                        Set the base directory to export artifacts
  --cluster-name CLUSTER_NAME
                        Cluster name to export the metastore to a specific cluster. Cluster will be started.
  --notebook-format {DBC,SOURCE,HTML}
                        Choose the file format to download the notebooks (default: DBC)
  --overwrite-notebooks
                        Flag to overwrite notebooks to forcefully overwrite during notebook imports
  --archive-missing     Import all missing users into the top level /Archive/ directory.
  --repair-metastore-tables
                        Repair legacy metastore tables
  --metastore-unicode   log all the metastore table definitions including unicode characters
  --skip-failed         Skip retries for any failed hive metastore exports.
  --skip-missing-users  Skip failed principles during ACL import; for missing principles, this will result in open ACLs
  --session SESSION     If set, pipeline resumes from latest checkpoint of given session; Otherwise, pipeline starts from beginning and creates a new session.
  --dry-run             Dry run the pipeline i.e. will not execute tasks if true.
  --export-pipeline     Execute all export tasks.
  --import-pipeline     Execute all import tasks.
  --use-checkpoint      use checkpointing to restart from previous state
  --skip-tasks SKIP_TASK [SKIP_TASK ...]
                        Space-separated list of tasks to skip from the pipeline. Valid options are:
                         instance_profiles, users, groups, workspace_item_log, workspace_acls, notebooks, secrets,
                         clusters, instance_pools, jobs, metastore, metastore_table_acls, mlflow_experiments, mlflow_runs
  --keep-tasks KEEP_TASK [KEEP_TASK ...]
                        Space-separated list of tasks to run from the pipeline. See valid options in --skip-tasks. Overrides skip-tasks.
  --num-parallel NUM_PARALLEL
                        Number of parallel threads to use to export/import
  --retry-total RETRY_TOTAL
                        Total number or retries when making calls to Databricks API
  --retry-backoff RETRY_BACKOFF
                        Backoff factor to apply between retry attempts when making calls to Databricks API
  --start-date START_DATE
                        start-date format: YYYY-MM-DD. If not provided, defaults to past 30 days. Currently, only used for exporting ML runs objects.
  --groups-to-keep group [group ...]
                        List of groups to keep if selectively exporting assets. Only users (and their assets) belonging to these groups will be exported.
                        
options for validation pipeline:
  --validate-pipeline   Validate exported data between source and destination.
  --validate-source-session VALIDATE_SOURCE_SESSION
                        Session used by exporting source workspace. Only used for --validate-pipeline.
  --validate-destination-session VALIDATE_DESTINATION_SESSION
                        Session used by exporting destination workspace. Only used for --validate-pipeline.