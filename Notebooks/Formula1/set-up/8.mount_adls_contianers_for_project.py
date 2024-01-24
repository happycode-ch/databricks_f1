# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Mount Azure Data Lake containers for the Project (Not Unity Catalog Method)
# MAGIC
# MAGIC #### mount naming standards: 
# MAGIC 1. storage account name followed by the container name, so it is obvious which storage account and the container it is pointing to
# MAGIC     1. e.g.:   mount_point = "/mnt/formula1dlkaiser/demo",
# MAGIC
# MAGIC #### check mounts and unmount commands
# MAGIC 1. mount = display(dbutils.fs.mounts())
# MAGIC 1. unmunt = dbutils.fs.unmount('<mountPoint>')

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Get screts from Key Vault
    client_id = dbutils.secrets.get(scope='formula1-scope', key='service-principal-client-id')
    tenant_id = dbutils.secrets.get(scope='formula1-scope', key='service-principal-tenant-id')
    client_secret = dbutils.secrets.get(scope='formula1-scope', key='service-principal-client-secret')

    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id": client_id,
                "fs.azure.account.oauth2.client.secret": client_secret,
                "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    # Unmount the mount if it already exists
    if any (mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    # Mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    # Display mounts
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('formula1dlkaiser', 'raw')

# COMMAND ----------

mount_adls('formula1dlkaiser', 'processed')

# COMMAND ----------

mount_adls('formula1dlkaiser', 'presentation')

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dlkaiser/presentation")