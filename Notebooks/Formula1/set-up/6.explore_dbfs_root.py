# Databricks notebook source
# MAGIC %md
# MAGIC ## Explore DBFS Root
# MAGIC
# MAGIC 1. List all the folders in DBFS root
# MAGIC 1. Interact with DBFS File Browser
# MAGIC 1. Upload file to DBFS Root (Browse DBFS is located in the Catalog -> Browse DBFS)
# MAGIC
# MAGIC #### Note: that the FileStore is not restricted and is available to all users with access to this Workspace, and all storage /files will be dropped when the Workspace is deleted. Not for customer Data
# MAGIC
# MAGIC #### Usecases for FileStore:
# MAGIC 1. For small data sets that do not require authentication, testing, config
# MAGIC 1. For objects that are going to be used in Markdown

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))

# COMMAND ----------

