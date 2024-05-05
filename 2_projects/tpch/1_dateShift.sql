-- Databricks notebook source
USE CATALOG shared; 

USE SCHEMA tpch; 


-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW maxdates as (select 
max(o_orderdate) as max_orderdate
,max(l_commitdate) as max_commitdate
,max(l_shipdate) as max_shipdate
,max(l_receiptdate) as max_receiptdate
from orders full join lineitem on o_orderkey = l_orderkey
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import datediff, current_date, col
-- MAGIC
-- MAGIC datedf = (spark.table('maxdates')
-- MAGIC           .withColumn("shiftOrders",datediff(current_date(), col("max_orderdate")))
-- MAGIC           .withColumn("shiftCommits",datediff(current_date(), col("max_commitdate")))
-- MAGIC           .withColumn("shiftShipments",datediff(current_date(), col("max_shipdate")))
-- MAGIC           .withColumn("shiftReceipts",datediff(current_date(), col("max_receiptdate")))
-- MAGIC )
-- MAGIC
-- MAGIC datedf.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC shift = datedf.select("shiftOrders").first()[0] - 1
-- MAGIC
-- MAGIC spark.sql(f"""
-- MAGIC           UPDATE orders SET o_orderdate = o_orderdate + INTERVAL {shift} DAY;
-- MAGIC           """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC shiftCommits = datedf.select("shiftCommits").first()[0] - 1
-- MAGIC shiftShipments = datedf.select("shiftShipments").first()[0] - 1
-- MAGIC shiftReceipts = datedf.select("shiftReceipts").first()[0] - 1
-- MAGIC
-- MAGIC spark.sql(f"""
-- MAGIC           UPDATE lineitem SET l_commitdate = l_commitdate + INTERVAL {shiftCommits} DAY;
-- MAGIC           """)
-- MAGIC
-- MAGIC spark.sql(f"""
-- MAGIC           UPDATE lineitem SET l_shipdate = l_shipdate + INTERVAL {shiftShipments} DAY;
-- MAGIC           """)
-- MAGIC
-- MAGIC spark.sql(f"""
-- MAGIC           UPDATE lineitem SET l_receiptdate = l_receiptdate + INTERVAL {shiftReceipts} DAY;
-- MAGIC           """)

-- COMMAND ----------

-- Update the shifted_date column by shifting the date_field by the specified number of days
SELECT * FROM orders;

-- COMMAND ----------

SELECT * from lineitem

-- COMMAND ----------


