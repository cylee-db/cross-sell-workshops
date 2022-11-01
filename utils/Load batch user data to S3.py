# Databricks notebook source
# MAGIC %md
# MAGIC # Load data to S3
# MAGIC Load `user` data into S3 bucket as parquet files.

# COMMAND ----------

import pyspark.sql.functions as f

import time

# COMMAND ----------

# MAGIC %md
# MAGIC We are writing parquet files to S3 once every 100 seconds.

# COMMAND ----------

# Define constants
batch_col = 'batch_num'
batch_num = 1  # Initialize batch number
max_batch = 100  # Max number of batches to generate
interval = 100  # Interval betwen batches in seconds

s3_path = 's3://path_to_s3_location/retail_raw'

# COMMAND ----------

# MAGIC %md
# MAGIC ## User data
# MAGIC This data is recieved regularly. We simulate this by repeatedly writing out batches to S3.

# COMMAND ----------

# Read the source table with the data

user_df = spark.read.table('retail_chiayui_lee.user_bronze')
user_cols = [batch_col] + user_df.columns

display(user_df)

# COMMAND ----------

# Write the data as batches of parquet files to S3

while batch_num <= max_batch:
  user_batch_df = user_df.withColumn(batch_col, f.lit(batch_num)) \
                         .select(user_cols) \
                         .drop('_rescued_data')
  
  user_batch_df.write.parquet(s3_path + '/user', 'append', partitionBy=batch_col)
  
  batch_num += 1
  time.sleep(interval)

# COMMAND ----------

# Sanity check - Read the files written to S3 and display them
spark.read.parquet(s3_path + '/user').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spend data
# MAGIC This is done once only scine we assume this data is static.

# COMMAND ----------

# Write the spend parquet files
# Done once only since we assume the spend data is static for simplicity

spend_df = spark.read.table('retail_chiayui_lee.spend_silver')
spend_batch_df = spend_df.drop('_rescued_data')

spend_batch_df.write.parquet(s3_path + '/spend', 'append')

# COMMAND ----------

# Sanity check - Read the spend data files on S3 and display the data
spark.read.parquet(s3_path + '/spend').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Reset demo
# MAGIC 
# MAGIC Delete data in S3 bucket, clean up checkpoints and delete Delta Tables.

# COMMAND ----------

dbutils.fs.rm(f'{s3_path}/user', recurse=True)
dbutils.fs.rm(f'{s3_path}/checkpoints/user_bronze', recurse=True)
dbutils.fs.rm(f'{s3_path}/checkpoints/user_silver', recurse=True)
dbutils.fs.rm(f'{s3_path}/checkpoints/user_gold', recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS field_demos.cross_sell_cyl.users_bronze_stream
# MAGIC ;
# MAGIC DROP TABLE IF EXISTS field_demos.cross_sell_cyl.user_silver_stream
# MAGIC ;
# MAGIC DROP TABLE IF EXISTS field_demos.cross_sell_cyl.user_gold_stream
# MAGIC ;
