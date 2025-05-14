import logging
import os
import streamlit as st
from databricks.sdk import WorkspaceClient
from functions import deploy_time
from pyspark.sql.functions import min,sum
from databricks.connect import DatabricksSession as SparkSession

st.set_page_config(
    page_title="PySpark",
    page_icon="✨",
)
st.header("# PySpark on Databricks Apps: ✨")

def get_spark_session():
    # Check if the environment variable is set
    if os.getenv('USE_SERVERLESS') == True:
        spark  = SparkSession.builder.remote(serverless=True).getOrCreate()
        st.subheader("Serverless Spark Session")
        return spark
    else:
        #- For Classic (you will need to provision SP to have can manage permissions)
        w = WorkspaceClient()
        var_host = w.config.host
        var_cluster_id = os.getenv('CLASSIC_CLUSTER_ID')
        spark = SparkSession.builder.remote(
        host       = f"{var_host}", 
        cluster_id = f"{var_cluster_id}"
        ).getOrCreate()
        st.subheader("Classic Spark Session")
        return spark

# - For Serverless
spark =  get_spark_session()


df = spark.sql("select * from ahtsa.fitbit.silver_activities limit 1000").groupBy("id_activity").agg(sum("steps").alias("total_steps")
                                         ,sum("calories").alias("total_calories")
                                         ,min("startTime").alias("min")

)

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = df.toPandas()

# Display the DataFrame in Streamlit
st.dataframe(pandas_df)
