#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 11 13:45:18 2023

@author: Andrew.Tolbert
"""
#GLOBAL VARS 
client_id = '2395GM' 
base_url = 'https://api.fitbit.com/'
bucket = 'aht-fitbit' 

import json
import csv
from datetime import date
from datetime import timedelta
 

date_0 = date.today().strftime('%Y-%m-%d')
date_1 = (date.today() - timedelta(days = 1)).strftime('%Y-%m-%d')
date_7 = (date.today() - timedelta(days = 7)).strftime('%Y-%m-%d')
# for bulk data 
date_120 = (date.today() - timedelta(days = 120)).strftime('%Y-%m-%d') 

# Define JSON data
def get_creds():
    with open('0_admin/creds.json') as f:
      JSONData = json.load(f)
    access_token = JSONData['access_token']
    refresh_token = JSONData['refresh_token']
    user_id = JSONData['user_id']
    return access_token, refresh_token,user_id

def get_iam_creds():
    with open('0_admin/s3_writer_accessKeys.csv', 'r') as f:
        dict_reader = csv.DictReader(f)
        iam_creds = list(dict_reader)
    return iam_creds[0]['Access key ID'],iam_creds[0]['Secret access key']

(access_token, refresh_token,user_id) = get_creds()

(aws_access_key, aws_secret_key) = get_iam_creds()

print('imported: requests, client_id, url, access_token, refresh_token, user_id, \
      aws_access_key, aws_secret_key')
