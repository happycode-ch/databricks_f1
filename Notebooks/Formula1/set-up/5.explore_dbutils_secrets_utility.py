# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Explore the capabilities of the dbutils.secrets utility
# MAGIC
# MAGIC
# MAGIC 1. Create scope (from the home page, add the following to the URL) ...#"/secrets/createScope"

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='formula1-scope')

# COMMAND ----------

dbutils.secrets.get(scope='formula1-scope', key='formula1dl-account-key')

# COMMAND ----------

