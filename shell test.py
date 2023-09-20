# Databricks notebook source
dbutils.fs.put("multi-line.json", """[
    {"string":"string1","int":1,"array":[1,2,3],"dict": {"key": "value1"}},
    {"string":"string2","int":2,"array":[2,4,6],"dict": {"key": "value2"}},
    {
        "string": "string3",
        "int": 3,
        "array": [
            3,
            6,
            9
        ],
        "dict": {
            "key": "value3",
            "extra_key": "extra_value3"
        }
    }
]""")

# COMMAND ----------

# MAGIC %sh 
# MAGIC value=$(<multi-line.json)  
# MAGIC echo "$value"  

# COMMAND ----------

# MAGIC %sh 
# MAGIC value=$(</dbfs/multi-line.json)  
# MAGIC echo "$value"  

# COMMAND ----------

dbutils.fs.cp('dbfs:/mnt/field-demos/retail/fgforecast/','/Users/andrew.tolbert@databricks.com/demos',recurse = True)

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/Users/andrew.tolbert@databricks.com/demos'))

# COMMAND ----------


