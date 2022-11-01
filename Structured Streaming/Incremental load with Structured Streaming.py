# Databricks notebook source
# MAGIC %md-sandbox ## Incremental load with Structured Streaming on Delta Lake
# MAGIC 
# MAGIC This notebook implements an incremental load pipeline with Spark Structured Streaming.
# MAGIC 
# MAGIC The pipeline processes batches of data produced by a producer in a an S3 location called `retail_raw`. The producer writes the data in the `user` location under `retail_raw`.
# MAGIC 
# MAGIC The pipeline is scheduled to run using Datatricks Workflows either on a schedule (e.g. every 30 minutes, hour, ...) or as an "always on" pipeline.

# COMMAND ----------

# Import libraries
import pyspark.sql.functions as f

# Define constants
s3_path = 's3://databricks-e2demofieldengwest/chiayui_lee_sandbox/retail_raw'
s3_path = 's3://path_to_s3_location/retail_raw'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the Delta table for Spend data
# MAGIC -- The raw data is available as parquet files on S3
# MAGIC -- For simplicity, we assume this data is static and we read it only once
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS field_demos.cross_sell_cyl.spend
# MAGIC AS
# MAGIC   SELECT * FROM parquet.`s3://path_to_s3_location/retail_raw/spend`

# COMMAND ----------

### Step 1 ###
# Incrementally load the data using Structured Streaming.
# We write a function that will be used to ingest incoming files.

# Auto Loader provides a Structured Streaming source called 'cloudFiles'.
# Auto Loader incrementally and efficiently processes new data files as they arrive in cloud storage.

def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  .option("cloudFiles.schemaLocation", checkpoint_directory)
                  .option("cloudFiles.schemaEvolutionMode", 'rescue')
                  .load(data_source)
                  .writeStream
                  .option("checkpointLocation", checkpoint_directory)
                  .option("mergeSchema", "false")
                  .trigger(availableNow=True)
                  .table(table_name))
    return query

# COMMAND ----------

# Read the incoming parquet files
# Write the data in a Delta table - users_bronze_stream

query = autoload_to_table(data_source = f"{s3_path}/user",
                          source_format = "parquet",
                          table_name = "field_demos.cross_sell_cyl.users_bronze_stream",
                          checkpoint_directory = f"{s3_path}/checkpoints/user_bronze")


# COMMAND ----------

### Step 2 ###
# Read the upstream Delta table as a Structured Streaming source
# Clean the data: Mask email addresses, format names, convert dates, drop unused column
# Write the cleaned data in a Delta table - user_silver_stream

(spark.readStream.table('field_demos.cross_sell_cyl.users_bronze_stream')
          .withColumn("email", f.sha1(f.col("email")))
          .withColumn("firstname", f.initcap(f.col("firstname")))
          .withColumn("lastname", f.initcap(f.col("lastname")))
          .withColumn("creation_date", f.to_timestamp(f.col("creation_date"), "MM-dd-yyyy HH:mm:ss"))
          .withColumn("last_activity_date", f.to_timestamp(f.col("last_activity_date"), "MM-dd-yyyy HH:mm:ss"))
          .drop(f.col("_rescued_data"))
     .writeStream
        .option("checkpointLocation", f'{s3_path}/checkpoints/user_silver')
        .trigger(availableNow=True)
        .table("field_demos.cross_sell_cyl.user_silver_stream")
)

# COMMAND ----------

### Step 3 ###
# Read the upstream Delta table as a Structured Streaming source
# Enrich the data: Join with a static table ('spend')
# Write the enriched data in a Delta table - user_gold_stream

spend = spark.read.table("field_demos.cross_sell_cyl.spend")
(spark.readStream.table("field_demos.cross_sell_cyl.user_silver_stream") 
     .join(spend, "id") 
     .writeStream
        .option("checkpointLocation", s3_path+"/checkpoints/user_gold")
        .trigger(availableNow=True)
        .table("field_demos.cross_sell_cyl.user_gold_stream")
)
