# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Mount Azure Data Lake using Service Principal (Not Unity Catalog Method)
# MAGIC #### Steps to follow:
# MAGIC
# MAGIC 1. Get client_id, tenant_id, and client_secret from key vault
# MAGIC 1. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 1. Call file system utility mount to mount the storage
# MAGIC 1. Explore other file system utilities related to mount (list all mounts, unmount)
# MAGIC
# MAGIC #### mount naming standards: 
# MAGIC 1. storage account name followed by the container name, so it is obvious which storage account and the container it is pointing to
# MAGIC     1. e.g.:   mount_point = "/mnt/formula1dlkaiser/demo",
# MAGIC
# MAGIC #### check mounts and unmount commands
# MAGIC 1. mount = display(dbutils.fs.mounts())
# MAGIC 1. unmunt = dbutils.fs.unmount('<mountPoint>')

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope', key='service-principal-client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key='service-principal-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='service-principal-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
    source = "abfss://demo@formula1dlkaiser.dfs.core.windows.net/",
    mount_point = "/mnt/formula1dlkaiser/demo",
    extra_configs = configs)

# COMMAND ----------

mount_adls("demo")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dlkaiser/demo")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dlkaiser/demo"))

# COMMAND ----------

spark.read.csv("/mnt/formula1dlkaiser/demo/circuits.csv")

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dlkaiser/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/formula1dlkaiser/demo')

# COMMAND ----------



# COMMAND ----------

