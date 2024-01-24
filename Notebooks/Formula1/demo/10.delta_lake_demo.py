# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 1. Write data to delta lake (external table)
# MAGIC 1. Read data from the delta lake (Table)
# MAGIC 1. Read data from delta lake (File)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/formula1dlkaiser/demo'

# COMMAND ----------

results_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1dlkaiser/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM f1_demo.results_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/formula1dlkaiser/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formula1dlkaiser/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM f1_demo.results_external;

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/formula1dlkaiser/demo/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update Delta Table
# MAGIC 1. Delete From Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC   SET points = 11 -position
# MAGIC WHERE position <= 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Updates and Deletes SQL method in Delta Lake

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1dlkaiser/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update("position <= 10",
  { "points": "21 - position" }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM f1_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1dlkaiser/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upsert using merge
# MAGIC
# MAGIC Note: A merge statement gives you the ability to:
# MAGIC - insert any new records being recieved, 
# MAGIC - update any existing records for which new data has been recieved, and 
# MAGIC - delete request 
# MAGIC All three completed in this single statement  (Common to data warehousing)

# COMMAND ----------

drivers_day1_df = spark.read \
    .option("inferSchema", True) \
    .json("/mnt/formula1dlkaiser/raw/2021-03-28/drivers.json") \
    .filter("driverId <= 10") \
    .select("driverId", "dob", "name.forename", "name.surname")
    

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
    .option("inferSchema", True) \
    .json("/mnt/formula1dlkaiser/raw/2021-03-28/drivers.json") \
    .filter("driverId BETWEEN 6 AND 15") \
    .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
    .option("inferSchema", True) \
    .json("/mnt/formula1dlkaiser/raw/2021-03-28/drivers.json") \
    .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
    .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day3_df)

# COMMAND ----------

drivers_day3_df.createOrReplaceTempView("drivers_day3")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW drivers_day3;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC #### Day 1 Data (merge)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.createdDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC #### Day 2 Data (merge)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1dlkaiser/demo/drivers_merge')

deltaTable.alias('tgt') \
  .merge(
    drivers_day3_df.alias('upd'),
    'tgt.driverId = upd.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "updatedDate": "current_timestamp()",
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History & Versioning
# MAGIC 1. Time Travel
# MAGIC 1. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM f1_demo.drivers_merge VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM f1_demo.drivers_merge VERSION AS OF 2;

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", '2023-12-22T09:55:00.000+00:00').load("/mnt/formula1dlkaiser/demo/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### VACCUM (cmd) SQL, removes data, but retians of 7 days or less of age. Deletes when older than 7 days. 

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM f1_demo.drivers_merge VERSION AS OF 2;
# MAGIC -- still accessable after 24 hours.

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS ;
# MAGIC -- data is not retained after 0 hours, immediate deletion of history

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;
# MAGIC -- history is visible as a list, though its not searchable (GDPR request) 

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE 
# MAGIC   FROM f1_demo.drivers_merge WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC   FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC   FROM f1_demo.drivers_merge VERSION AS OF 8;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 8 src
# MAGIC   ON (tgt.driverId = src.driverId)
# MAGIC WHEn NOT MATCHED THEN
# MAGIC   INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake Transaction Logs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn (
# MAGIC   driverID INT,
# MAGIC   dob DATE, 
# MAGIC   forename STRING,
# MAGIC   surename STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;
# MAGIC -- parquet file is created in addition to the log files (CRC, JSON)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 2;
# MAGIC -- another version is created as noted in the transaction log

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_txn
# MAGIC WHERE driverId = 1;
# MAGIC -- another version, this time without the records for "driverID = 1"

# COMMAND ----------

for driver_id in range(3, 20):
    spark.sql(f"""INSERT INTO f1_demo.drivers_txn
                  SELECT * FROM f1_demo.drivers_merge
                  WHERE driverId = {driver_id}""")

# COMMAND ----------

# MAGIC %md
# MAGIC Looking in the storage explorer, Delta Lake transaction logs include a "compacted" checkpoint file, JSON file that is keeping the records of the previous X amount of file (every 6, in this example). This aids in speeding up queries. transaction logs kept up to 30 day, can be kept for longer, not reccomended. 

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet to Delta (Parquet tables into Delta tables)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/formula1dlkaiser/demo/drivers_convert_to_delta_new")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- convert to Delta from Parquet
# MAGIC CONVERT TO DELTA parquet.`/mnt/formula1dlkaiser/demo/drivers_convert_to_delta_new`

# COMMAND ----------

