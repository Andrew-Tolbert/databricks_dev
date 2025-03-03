import logging
import os
import streamlit as st
from databricks.sdk import WorkspaceClient
from functions import deploy_time
from pyspark.sql.functions import min,sum

st.set_page_config(
    page_title="PySpark",
    page_icon="✨",
)


from databricks.connect import DatabricksSession as SparkSession
var_host = 'e2-demo-field-eng.cloud.databricks.com'
var_cluster_id = '0303-153924-q0hq3d9a'
secret_value = os.getenv('SECRET_KEY')


#spark =  SparkSession.builder.remote(serverless=True).getOrCreate() # - For Serverless
spark = SparkSession.builder.remote(
host       = f"https://{var_host}",
cluster_id = f"{var_cluster_id}"
).getOrCreate()


df = spark.sql("select * from ahtsa.fitbit.silver_activities").groupBy("id_activity").agg(sum("steps").alias("total_steps")
                                         ,sum("calories").alias("total_calories")
                                         ,min("startTime").alias("min")
)

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = df.toPandas()

# Display the DataFrame in Streamlit
st.dataframe(pandas_df)
