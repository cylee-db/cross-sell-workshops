# Databricks notebook source
from pyspark.sql.functions import col, lit, expr, when
from pyspark.sql.types import *
from datetime import datetime
import time
 
# Define schema
nyc_schema = StructType([
  StructField('Vendor', StringType(), True),
  StructField('Pickup_DateTime', TimestampType(), True),
  StructField('Dropoff_DateTime', TimestampType(), True),
  StructField('Passenger_Count', IntegerType(), True),
  StructField('Trip_Distance', DoubleType(), True),
  StructField('Pickup_Longitude', DoubleType(), True),
  StructField('Pickup_Latitude', DoubleType(), True),
  StructField('Rate_Code', StringType(), True),
  StructField('Store_And_Forward', StringType(), True),
  StructField('Dropoff_Longitude', DoubleType(), True),
  StructField('Dropoff_Latitude', DoubleType(), True),
  StructField('Payment_Type', StringType(), True),
  StructField('Fare_Amount', DoubleType(), True),
  StructField('Surcharge', DoubleType(), True),
  StructField('MTA_Tax', DoubleType(), True),
  StructField('Tip_Amount', DoubleType(), True),
  StructField('Tolls_Amount', DoubleType(), True),
  StructField('Total_Amount', DoubleType(), True)
])
 
rawDF = spark.read.format('csv').options(header=True).schema(nyc_schema).load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS default.yellow_taxi
# MAGIC (Vendor string,
# MAGIC Pickup_DateTime timestamp,
# MAGIC Dropoff_DateTime timestamp,
# MAGIC Passenger_Count integer,
# MAGIC Trip_Distance double,
# MAGIC Pickup_Longitude double,
# MAGIC Pickup_Latitude double,
# MAGIC Rate_Code string,
# MAGIC Store_And_Forward string,
# MAGIC Dropoff_Longitude double,
# MAGIC Dropoff_Latitude double,
# MAGIC Payment_Type string,
# MAGIC Fare_Amount double,
# MAGIC Surcharge double,
# MAGIC MTA_Tax double,
# MAGIC Tip_Amount double,
# MAGIC Tolls_Amount double,
# MAGIC Total_Amount double
# MAGIC )
# MAGIC using csv
# MAGIC location 'dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE default.yellow_taxi_delta
# MAGIC AS
# MAGIC SELECT * FROM default.yellow_taxi
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE default.yellow_taxi
