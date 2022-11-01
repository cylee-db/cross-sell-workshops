-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC # Delta Lake Internals
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo-whitebackground.png" style="width:200px; float: right"/>
-- MAGIC 
-- MAGIC Delta Lake is an open format and can be read using multiple engine or with standalone libraries (java, python, rust)...
-- MAGIC 
-- MAGIC It's then easy to subscribe to modifications stream on one of your table to propagage the changes downstream in a medaillong architecture.
-- MAGIC 
-- MAGIC See the [documentation](https://docs.databricks.com/delta/delta-change-data-feed.html) for more details.
-- MAGIC 
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdelta_lake%2Fcdf&dt=FEATURE_DELTA">
-- MAGIC <!-- [metadata={"description":"Quick introduction to Delta Lake. <br/><i>Use this content for quick Delta demo.</i>",
-- MAGIC  "authors":["quentin.ambard@databricks.com"],
-- MAGIC  "db_resources":{}}] -->

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## CDF for Data Mesh & Delta Sharing
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/delta-cdf-datamesh.png" style="float:right; margin-right: 50px" width="300px" />
-- MAGIC 
-- MAGIC When sharing data within a Datamesh and/or to external organization with Delta Sharing, you not only need to share existing data, but also all modifications, so that your consumer can capture apply the same changes.
-- MAGIC 
-- MAGIC CDF makes **Data Mesh** implementation easier. Once enabled by an organisation, data can be shared with other. It's then easy to subscribe to the modification stream and propagage GDPR DELETE downstream.
-- MAGIC 
-- MAGIC To do so, we need to make sure the CDF are enabled at the table level. Once enabled, it'll capture all the table modifications using the `table_changes` function.
-- MAGIC 
-- MAGIC For more details, visit the [CDF documentation](https://docs.databricks.com/delta/delta-change-data-feed.html)

-- COMMAND ----------

-- DBTITLE 1,Enable CDF at the table level
ALTER TABLE user_delta SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

-- MAGIC %md #### Delta CDF table_changes output
-- MAGIC In addition to the row details, `table_changes` provides back 4 cdc types in the "_change_type" column:
-- MAGIC 
-- MAGIC | CDC Type             | Description                                                               |
-- MAGIC |----------------------|---------------------------------------------------------------------------|
-- MAGIC | **update_preimage**  | Content of the row before an update                                       |
-- MAGIC | **update_postimage** | Content of the row after the update (what you want to capture downstream) |
-- MAGIC | **delete**           | Content of a row that has been deleted                                    |
-- MAGIC | **insert**           | Content of a new row that has been inserted                               |
-- MAGIC 
-- MAGIC Let's query the changes of the Delta Version 12 which should be our MERGE operation (you can run a `DESCRIBE HISTORY user_data_bronze` to see the version numbers).
-- MAGIC 
-- MAGIC As you can see 1 row has been UPDATED (we get the old and new value), 1 DELETED and one INSERTED.

-- COMMAND ----------

-- DBTITLE 1,Let's make sure we have some changes in our table
-- Make sure you run the first notebook to load all the data.
UPDATE user_delta SET firstname = 'John' WHERE ID < 10;
DELETE FROM user_delta WHERE ID > 1000;

-- COMMAND ----------

select * from table_changes("user_delta", 1);

-- COMMAND ----------

select distinct(_change_type) from table_changes("user_delta", 1)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Using CDF to capture incremental change (stream)
-- MAGIC 
-- MAGIC To capture the last changes from your table, you can leverage Spark Streaming API. 
-- MAGIC 
-- MAGIC It's then easy to subscribe to modifications stream on one of your table to propagage GDPR DELETE downstream

-- COMMAND ----------

-- MAGIC %python
-- MAGIC stream = spark.readStream.format("delta") \
-- MAGIC               .option("readChangeFeed", "true") \
-- MAGIC               .option("startingVersion", 1) \
-- MAGIC               .table("user_delta")
-- MAGIC 
-- MAGIC display(stream)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Easier CDF with Delta Live Table APPY CHANGES
-- MAGIC 
-- MAGIC Delta Lake CDF is a low level API. To implement simple CDC pipeline using pure SQL (including SCDT2 tables), you can leverage the Delta Live Table engine! See the [documentation](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-cdc.html) for more details.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC That's it, we covered the main capabilities provided by Delta Lake.
-- MAGIC 
-- MAGIC If you want to know more about the technical implementation, you can have a look to the [internal structure of Delta Lake]($./03-Advanced-Delta-Lake-Internal) (optional).

-- COMMAND ----------

-- MAGIC %md ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Exploring delta structure
-- MAGIC 
-- MAGIC Delta is composed of parquet files and a transactional log

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC DESCRIBE DETAIL user_delta

-- COMMAND ----------

-- DBTITLE 1,Delta is composed of parquet 
-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(raw_data_location+"/user_delta"))

-- COMMAND ----------

-- DBTITLE 1,And with a transactional log
-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(raw_data_location+"/user_delta/_delta_log"))

-- COMMAND ----------

-- DBTITLE 1,Each log contains parquet files stats for efficient data skipping
-- MAGIC %python
-- MAGIC with open("/dbfs/"+raw_data_location+"/user_delta/_delta_log/00000000000000000000.json") as f:
-- MAGIC   for l in f.readlines():
-- MAGIC     print(json.dumps(json.loads(l), indent = 2))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC stop_all_streams()
