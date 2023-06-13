#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 12 20:51:58 2023

@author: Andrew.Tolbert
"""
from datetime import date
from datetime import timedelta

import var_config as var

from get_data import get_activities, get_sleep, write_json

def s3_sleep_batch_update(days):
    for n in range(days):
        day = (date.today() - timedelta(days = n)).strftime('%Y-%m-%d')
        filename,data = get_sleep(day)
        write_json('sleep',filename,data) 
        print(f"{filename}")
        
def s3_act_batch_update(days):
    for n in range(days):
        day = (date.today() - timedelta(days = n)).strftime('%Y-%m-%d')
        filename,data = get_activities(day)
        write_json('activities',filename,data)
        print(f"{filename}")