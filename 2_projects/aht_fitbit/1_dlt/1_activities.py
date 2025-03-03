# Databricks notebook source
catalog = 'ahtsa'
schema = 'fitbit'
vol = '/Volumes/ahtsa/fitbit/raw_fitbitapi'

# COMMAND ----------

# Load the necessary library for XML parsing
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

# # Define the schema for the XML data
# schema = StructType([
#     StructField("field1", StringType(), True),
#     StructField("field2", StringType(), True),
#     # Add more fields as per your XML structure
# ])

# Load the XML file into a DataFrame
xml_df = (spark.read
          .format("com.databricks.spark.xml")
          .option("rowTag", "Activities")  # Replace with the appropriate row tag
          .load(f'{vol}/TCX/*.xml'))

# Display the DataFrame
display(xml_df)

# COMMAND ----------

xml_df

# COMMAND ----------

from pyspark.sql.functions import explode, struct, from_json,col,first,arrays_zip,col,map_from_entries,expr

# COMMAND ----------



#bronze load 
actLogDf = spark.read.json(f'{vol}/activitylog')
actLogDf= (actLogDf.select("activities")
          .select(explode(actLogDf.activities).alias("activities_exploded"))
          .select('activities_exploded.*')
          .drop('source')
           
)

actLogDf.display()

# COMMAND ----------



#silver load 

display(
    actLogDf
    .distinct()
    .withColumn(
        "activityLevelMap",
        map_from_entries(
            expr("transform(activityLevel, x -> struct(x.name, x.minutes))")
        )
    )
    .withColumn(
        "heartRateZonesMap",
        map_from_entries(
            expr("transform(heartRateZones, x -> struct(x.name, struct(x.caloriesOut, x.max, x.min, x.minutes)))")
        )
    )
    .selectExpr(
            "logId", 
            "startTime",
            "activityName",
            "steps",
            "speed",
            "pace",
            "duration",
            "logType",
            "averageHeartRate",
            "calories",
            "distance",
            "distanceUnit",
            "duration",
            "hasActiveZoneMinutes",
            "hasGps",
             "activityLevelMap as activityLevels", 
             "heartRateZonesMap as heartRateZones")
)

# COMMAND ----------

import pyspark.sql.functions as f

# Convert the 'activityLevel' array to a map (object)
actLogDf_with_map = (
    actLogDf.select('logId', 'activityLevel', 'heartRateZones')
    .withColumn(
        "activityLevelMap",
        f.map_from_entries(
            f.expr("transform(activityLevel, x -> struct(x.name, x.minutes))")
        )
    )
    .withColumn(
        "heartRateZonesMap",
        f.map_from_entries(
            f.expr("transform(heartRateZones, x -> struct(x.name, struct(x.caloriesOut, x.max, x.min, x.minutes)))")
        )
    )
)

display(actLogDf_with_map.select('logId', 'activityLevel', 'activityLevelMap', 'heartRateZones', 'heartRateZonesMap'))

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


