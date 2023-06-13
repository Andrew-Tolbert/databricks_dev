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

schema = '#aht_sa.aht_sa' 

# Create a url to access a shared table.
# A table path is the profile file path following with `#` and the fully qualified name of a table 
# (`<share-name>.<schema-name>.<table-name>`).
table_url = profile_file + f"{schema}.fitbit_sleep_silver"

# Load a table as a Pandas DataFrame. This can be used to process tables that can fit in the memory.
df = delta_sharing.load_as_pandas(table_url)

df = df.drop(columns=['name', 'endTime'])
df = df.set_index(list(df)[0])

ax = df.plot(kind='bar',alpha=0.75, rot='vertical')