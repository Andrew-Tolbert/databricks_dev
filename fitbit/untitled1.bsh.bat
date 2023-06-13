#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar  3 10:39:53 2023

@author: Andrew.Tolbert
"""

python3 migration_pipeline.py --profile newWS --import-pipeline --use-checkpoint --session 20230302_4 --retry-total=30 --num-parallel=8 --retry-backoff=1.0 --azure --notebook-format SOURCE --archive-missing --keep-tasks jobs --skip-tasks instance_profiles users groups secrets clusters instance_pools metastore mlflow_experiments mlflow_runs workspace_item_log workspace_acls notebooks metastore_table_acls
Using the session id: 20230302_4
Import from `https://adb-7774174877108386.6.azuredatabricks.net` into `https://dbc-d985cd0b-f131.cloud.databricks.com`? (y/N) y
2023-03-02,16:48:59;INFO;Start import_instance_profiles
2023-03-02,16:48:59;INFO;import_instance_profiles Skipped.
2023-03-02,16:48:59;INFO;Start import_users
2023-03-02,16:48:59;INFO;import_users Skipped.
2023-03-02,16:48:59;INFO;Start import_groups
2023-03-02,16:48:59;INFO;import_groups Skipped.
2023-03-02,16:48:59;INFO;Start import_notebooks
2023-03-02,16:48:59;INFO;import_notebooks Skipped.
2023-03-02,16:48:59;INFO;Start import_workspace_acls
2023-03-02,16:48:59;INFO;import_workspace_acls Skipped.
2023-03-02,16:48:59;INFO;Start import_secrets
2023-03-02,16:48:59;INFO;import_secrets Skipped.
2023-03-02,16:48:59;INFO;Start import_instance_pools
2023-03-02,16:48:59;INFO;import_instance_pools Skipped.
2023-03-02,16:48:59;INFO;Start import_clusters
2023-03-02,16:48:59;INFO;import_clusters Skipped.
2023-03-02,16:48:59;INFO;Start import_jobs
Traceback (most recent call last):
  File "/home/anelson/git/migrate/migration_pipeline.py", line 349, in <module>
    main()
  File "/home/anelson/git/migrate/migration_pipeline.py", line 345, in main
    pipeline.run()
  File "/home/anelson/git/migrate/pipeline/pipeline.py", line 64, in run
    future.result()
  File "/usr/lib/python3.10/concurrent/futures/_base.py", line 458, in result
    return self.__get_result()
  File "/usr/lib/python3.10/concurrent/futures/_base.py", line 403, in __get_result
    raise self._exception
  File "/usr/lib/python3.10/concurrent/futures/thread.py", line 58, in run
    result = self.fn(*self.args, **self.kwargs)
  File "/home/anelson/git/migrate/pipeline/pipeline.py", line 73, in _run_task
    task.run()
  File "/home/anelson/git/migrate/tasks/tasks.py", line 279, in run
    jobs_c.import_job_configs()
  File "/home/anelson/git/migrate/dbclient/JobsClient.py", line 164, in import_job_configs
    old_2_new_policy_ids = self.get_new_policy_id_dict()  # dict { old_policy_id : new_policy_id }
  File "/home/anelson/git/migrate/dbclient/ClustersClient.py", line 255, in get_new_policy_id_dict
    policy_id_dict[old_policy_id] = current_policies_dict[policy_name] # old_id : new_id
KeyError: 'SS_All'