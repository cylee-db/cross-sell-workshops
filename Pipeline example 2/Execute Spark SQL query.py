# Databricks notebook source
# MAGIC %md-sandbox ## Execute Spark SQL query
# MAGIC 
# MAGIC This notebook reads a SQL file at a location specified in parameters provided to the notebook and execute it in Spark SQL.
# MAGIC 
# MAGIC The notebook can then be sequenced and scheduled to run as tasks in a Databricks Workflow Job. Each tasks invokes a different SQL file and the entire Workflow Job makes up a pipeline.
# MAGIC 
# MAGIC This approach can be used to run pipelines involving Hive queries in Spark SQL with minimal code change.

# COMMAND ----------

# Initialize widgets to set parameters of Repo path to queries folder and SQL query files

dbutils.widgets.text('repo_path', '/Workspace/Repos/chiayui.lee@databricks.com/cross-sell-workshops/Pipeline example 2/queries/')
dbutils.widgets.text('sql_file', 'users_bronze.sql')

# COMMAND ----------

# Get the widget parameter values

repo_path = dbutils.widgets.get("repo_path") 
sql_file = dbutils.widgets.get("sql_file")
sql_filepath = repo_path + sql_file

# COMMAND ----------

# Read the sql file

with open(sql_filepath, 'r') as file:
  sql = file.read()
  file.close()

# COMMAND ----------

# Print sql file content for sanity check

print(sql)

# COMMAND ----------

# Execute with Spark SQL

spark.sql(sql)
