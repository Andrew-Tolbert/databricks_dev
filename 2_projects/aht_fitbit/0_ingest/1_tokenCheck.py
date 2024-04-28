# Databricks notebook source
# MAGIC %run ./0_ingestMain

# COMMAND ----------

#LOAD EXISTING CREDS 
(access_token, refresh_token,user_id) = get_creds()

#CHECK WITH KNOWN DATE OF ACTIVITY
filename,response,err = get_activities('2023-09-17',access_token)

#ERROR HANDLING
if err == 1: 
  refresh_access_token(refresh_token)
  time.sleep(5)
  (new_access_token, new_refresh_token,user_id) = get_creds()
  #TEST NEW ACCESS TOKEN
  filename,response,err = get_activities('2023-09-17',new_access_token)
  if err == 1: 
    print("Error - Token not Refreshed")
    raise Exception("Error - Token not Refreshed")
  else:
    print("Token Refreshed!")
else: 
  print("Token is Valid!   ")

