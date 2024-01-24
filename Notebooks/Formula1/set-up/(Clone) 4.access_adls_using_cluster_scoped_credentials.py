# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Access Azure Data Lake using cluster scoped credentials
# MAGIC 1. Set the spark config fs.azure.account.key in cluster
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuts.csv file
# MAGIC
# MAGIC #### Note: This notbook does not have permission to access the DL and was created to test the cluster-wide authentication

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlkaiser.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlkaiser.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@formula1dlkaiser.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlkaiser.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

