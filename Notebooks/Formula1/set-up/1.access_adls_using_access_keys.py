# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuts.csv file

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope='formula1-scope', key='formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlkaiser.dfs.core.windows.net",
    formula1dl_account_key
)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlkaiser.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlkaiser.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@formula1dlkaiser.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlkaiser.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

