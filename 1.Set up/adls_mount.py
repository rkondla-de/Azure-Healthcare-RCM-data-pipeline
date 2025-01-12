# Databricks notebook source
storageAccountName = "rkadlsdev"
storageAccountAccessKey = dbutils.secrets.get('key-vault-scope', 'adls-access-key')
mountPoints=["gold","silver","bronze","landing","config"]
for mountPoint in mountPoints:
    if not any(mount.mountPoint == f"/mnt/{mountPoint}" for mount in dbutils.fs.mounts()):
        try:
            dbutils.fs.mount(
            source = "wasbs://{}@{}.blob.core.windows.net".format(mountPoint, storageAccountName),
            mount_point = f"/mnt/{mountPoint}",
            extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
            )
            print(f"{mountPoint} mount succeeded!")
        except Exception as e:
            print("mount exception", e)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# Validate the secret is retrieved correctly
storageAccountAccessKey = dbutils.secrets.get('key-vault-scope', 'adls-access-key')
print(f"Key length: {len(storageAccountAccessKey)}")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/bronze/claims/"))

# COMMAND ----------

df = spark.read.parquet("dbfs:/mnt/bronze/hosa/encounters")
df.show()


# COMMAND ----------

df = spark.read.parquet("/mnt/bronze/hosa")
display(df)


# COMMAND ----------

df_hosa = spark.read.parquet("dbfs:/mnt/bronze/hosa/")
df_hosa.printSchema()

df_hosb = spark.read.parquet("dbfs:/mnt/bronze/hosb/")
df_hosb.printSchema()

df_claims = spark.read.parquet("dbfs:/mnt/bronze/claims/")
df_claims.printSchema()


# COMMAND ----------


df_claims = spark.read.parquet("dbfs:/mnt/bronze/claims/")
df_claims.printSchema()
