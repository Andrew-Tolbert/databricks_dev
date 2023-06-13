#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 11 14:05:19 2023

@author: Andrew.Tolbert
"""
import requests
import boto3 
from datetime import date
from datetime import timedelta
 

from var_config import base_url, bucket, date_0, date_1

from var_config import get_creds,get_iam_creds


(access_token, refresh_token,user_id) = get_creds()
(aws_access_key, aws_secret_key) = get_iam_creds()

def get_activities(date): 
    url = f"{base_url}1/user/{user_id}/activities/date/{date}.json"
    
    payload={
        }
    headers = {
      'Authorization': f"Bearer {access_token}"
    }
    
    response = requests.request("GET", url, headers=headers, data=payload)
    
    filename = "activities_" + date.replace("-","") + ".json"
    
    return filename,response.text


def get_sleep(date): 
    url = f"{base_url}1.2/user/{user_id}/sleep/date/{date}.json"
    
    payload={
        }
    headers = {
      'Authorization': f"Bearer {access_token}"
    }
    
    response = requests.request("GET", url, headers=headers, data=payload)
    
    filename = "sleep_" + date.replace("-","") + ".json"
    
    return filename,response.text



s3 = boto3.client("s3", 
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key)

def write_json(path,filename,data):
    """
    Uploads file to S3 bucket using S3 client object
    :return: None
    """
    s3.put_object(
        Body=data, 
        Bucket=bucket, 
        Key=f"{path}/{filename}"
        )
