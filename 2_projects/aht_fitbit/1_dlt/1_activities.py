# Databricks notebook source
catalog = 'ahtsa'
schema = 'fitbit'
vol = '/Volumes/ahtsa/fitbit/raw_fitbitapi'

# COMMAND ----------

from pyspark.sql.functions import explode, struct, from_json,col,first,arrays_zip 

# COMMAND ----------



actLogDf = spark.read.json(f'{vol}/activitylog')
actLogDf= (actLogDf.select("activities")
          .select(explode(actLogDf.activities).alias("activities_exploded"))
          .select('activities_exploded.*')
          #.select('*','activeZoneMinutes.*','activityLevel.*')
          .drop('source')
           
)

# COMMAND ----------

display(actLogDf.distinct().withColumn("activityLevels", explode("activityLevel").pivot(groupBy))
        )

# COMMAND ----------

display(actLogDf.distinct())

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.types as t 

actMinutes = lambda name: f.expr(f"transform(filter(activityLevel, x -> x.name = '{name}'), value -> value.minutes)")

newDf = (
  actLogDf
  .select('*')
  .withColumn('minutesSedentary',actMinutes('sedentary')[0].cast(t.IntegerType()))
  .withColumn('minutesLightly',actMinutes('lightly')[0].cast(t.IntegerType()))
  .withColumn('minutesFairly',actMinutes('fairly')[0].cast(t.IntegerType())) 
  .withColumn('minutesVery',actMinutes('very')[0].cast(t.IntegerType())) 
        
)

display(newDf)

# COMMAND ----------

df = actLogDf.select(
  #arrays_zip(col('activityLevel')).alias('zipped'))
  f.explode(col("activityLevel")).alias("exploded")
).select(f.expr("concat_ws(',', exploded.*)").alias("single_col"))

display(df)

# COMMAND ----------


