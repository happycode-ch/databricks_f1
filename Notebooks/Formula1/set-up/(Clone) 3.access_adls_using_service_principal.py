# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Access Azure Data Lake using service principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 1. Generate a secret/password for the Application
# MAGIC 1. Set Spark Config with App/Client Id, Directory/ Tenant Id, & Secret
# MAGIC 1. Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

service_principal_client_id = dbutils.secrets.get(scope='formula1-scope', key='service-principal-client-id')
service_principal_tenant_id = dbutils.secrets.get(scope='formula1-scope', key='service-principal-tenant-id')
service_principal_client_secret = dbutils.secrets.get(scope='formula1-scope', key='service-principal-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlkaiser.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlkaiser.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlkaiser.dfs.core.windows.net", service_principal_client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlkaiser.dfs.core.windows.net", service_principal_client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlkaiser.dfs.core.windows.net", f"https://login.microsoftonline.com/{service_principal_tenant_id}/oauth2/token") 
# fstring

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlkaiser.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlkaiser.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@formula1dlkaiser.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlkaiser.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

