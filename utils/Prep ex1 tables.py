# Databricks notebook source
# MAGIC %md
# MAGIC # EX1 pipeline

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Explore table -- Find user count for each postcode
# MAGIC with ranked as (
# MAGIC select
# MAGIC   postcode,
# MAGIC   count(id) as user_cnt
# MAGIC from
# MAGIC   retail_chiayui_lee.user_bronze
# MAGIC group by
# MAGIC   postcode
# MAGIC )
# MAGIC select
# MAGIC   postcode, user_cnt,
# MAGIC   dense_rank(user_cnt) over (order by user_cnt desc) postcode_rank
# MAGIC from ranked
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write data
# MAGIC 
# MAGIC - Write parquet files to external location.
# MAGIC - Create external table for parquet files.

# COMMAND ----------

# Write parquet files for ex1 pipeline
s3_path = 's3://path_to_s3_location/retail_raw'
user_df = spark.read.table('retail_chiayui_lee.user_bronze')
user_df.write.parquet(s3_path + '/user_parquet', 'overwrite')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Initialize parquet table in HMS for workshop
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS retail_chiayui_lee.user_parquet
# MAGIC USING parquet
# MAGIC LOCATION '/Users/chiayui.lee@databricks.com/demos/retail/delta/user_parquet'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tear down tables
# MAGIC 
# MAGIC Tear down tables to re-run pipeline.

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists field_demos.cross_sell_cyl.ex1_users_bronze;
# MAGIC drop table if exists field_demos.cross_sell_cyl.ex1_postcode_user_cnt;
# MAGIC drop table if exists field_demos.cross_sell_cyl.ex1_user_tenure;
# MAGIC drop table if exists field_demos.cross_sell_cyl.ex1_user_features;
