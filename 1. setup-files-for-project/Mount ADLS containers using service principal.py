# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake Containers for the Project

# COMMAND ----------

# MAGIC %md
# MAGIC  1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/ password for the Application
# MAGIC 3. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake.

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Get secrets from Key Vault
    client_id            = "9a6b7ee9-8f59-4245-a17b-5a9280d81394"
    tenant_id            = "0e84eb4a-8067-4665-9dbc-a8871b9db2a3"
    client_secret        =  dbutils.secrets.get(scope="formula1-scope", key="formula1-SAS-secret-key")
    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": client_id,
              "fs.azure.account.oauth2.client.secret": client_secret,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount the storage account container
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    
    

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Mount Raw Container

# COMMAND ----------

mount_adls('formula1dl2025project', 'raw')

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dl2025project/raw")

# COMMAND ----------

mount_adls('formula1dl2025project', 'processed')

# COMMAND ----------

mount_adls('formula1dl2025project', 'presentation')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(spark.read.csv('/mnt/formula1dl2025project/raw/circuits.csv'))
