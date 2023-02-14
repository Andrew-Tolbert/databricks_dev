#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 11 13:21:19 2023

@author: Andrew.Tolbert
"""
from var_config import client_id, base_url, get_creds

access_token, refresh_token, user_id = get_creds()

import requests
import json 

url = f"{base_url}oauth2/token"

payload = {
    'refresh_token': refresh_token,
    'grant_type': 'refresh_token', 
    'client_id': client_id
}
headers = {
  'Content-Type': 'application/x-www-form-urlencoded'
}

response = requests.request("POST", url, headers=headers, data=payload)

#rewrite token file

with open('0_admin/creds.json', 'w') as json_file:
    json.dump(json.loads(response.text), json_file)
