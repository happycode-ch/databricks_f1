# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config for SAS Token (scope was not included in this authentication session)
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuts.csv file
# MAGIC
# MAGIC #### Note: the SAS Token expires after 8 hours and needs to be regenerated/ new version in Key Vault

# COMMAND ----------

formula1dl_sas_token = dbutils.secrets.get(scope='formula1-scope', key='formula1dl-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlkaiser.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlkaiser.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlkaiser.dfs.core.windows.net", formula1dl_sas_token)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlkaiser.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlkaiser.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@formula1dlkaiser.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlkaiser.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

