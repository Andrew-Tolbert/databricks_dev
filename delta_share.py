#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 11 11:44:34 2023

@author: Andrew.Tolbert
"""

import delta_sharing

# Point to the profile file. It can be a file on the local file system or a file on a remote storage.
profile_file = "config.share"

# Create a SharingClient.
client = delta_sharing.SharingClient(profile_file)

# List all shared tables.
client.list_all_tables()

schema = '#aht_sa.aht_bm3'

# Create a url to access a shared table.
# A table path is the profile file path following with `#` and the fully qualified name of a table 
# (`<share-name>.<schema-name>.<table-name>`).
table_url = profile_file + f"{schema}.gold_ds_sensor"

# Fetch 10 rows from a table and convert it to a Pandas DataFrame. This can be used to read sample data 
# from a table that cannot fit in the memory.
delta_sharing.load_as_pandas(table_url, limit=10)

# Load a table as a Pandas DataFrame. This can be used to process tables that can fit in the memory.
df = delta_sharing.load_as_pandas(table_url)

# If the code is running with PySpark, you can use `load_as_spark` to load the table as a Spark DataFrame.
delta_sharing.load_as_spark(table_url)