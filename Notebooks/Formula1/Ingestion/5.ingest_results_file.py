# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### 1. Ingest the results.json file using spark dataframe reader
# MAGIC #### 1. Set results schema
# MAGIC #### 1. Select /drop columns and rename col's (.alias)
# MAGIC #### 1. Save as a parquet file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1. Ingest the results.json file using spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)
])

# COMMAND ----------

results_df = spark.read \
.option("header", True) \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Drop statusId col

# COMMAND ----------

results_dropped_df = results_df.drop('statusId')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3. Rename columns and add ingestion date 

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_selected_df = results_dropped_df.withColumnRenamed("resultId", "result_id") \
                                        .withColumnRenamed("raceId", "race_id") \
                                        .withColumnRenamed("driverId", "driver_id") \
                                        .withColumnRenamed("constructorId", "constructor_id") \
                                        .withColumnRenamed("positionText", "position_text") \
                                        .withColumnRenamed("positionOrder", "position_order") \
                                        .withColumnRenamed("FastestLap", "fastest_lap") \
                                        .withColumnRenamed("FastestLapTime", "fastest_lap_time") \
                                        .withColumnRenamed("FastestLapSpeed", "fastest_lap_speed") \
                                        .withColumn("data_source", lit(v_data_source)) \
                                        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

results_add_ingestion_df = add_ingestion_date(results_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4. Write output to parquet file with partition by race_id

# COMMAND ----------

results_final_df = results_add_ingestion_df

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC Note: When using .collect(), be <em>specific</em> as this will collect all of the data specificed and load it into the driver nodes memory (only a small amount of date), Never do a .collect() on a normal dataframe, make sure the aggregations have already taken place.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 1 (Less Performant given cluster scenario)

# COMMAND ----------

# Method-1
# for race_id_list in results_final_df.select("race_id").distinct().collect():
#   if (spark.jsparkSession.catalog().tableExists("f1_processed.results")):
#      spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

#Method-1
# results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2 (More Performant given cluster scenario)

# COMMAND ----------

# overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

merge_conditon = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_conditon, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING COUNT(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.results WHERE race_id = 540 AND driver_id = 229;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC   FROM f1_processed.results
# MAGIC WHERE file_date = '2021-03-21';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1)
# MAGIC   FROM f1_processed.results
# MAGIC   GROUP BY race_id
# MAGIC   ORDER BY race_id DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")