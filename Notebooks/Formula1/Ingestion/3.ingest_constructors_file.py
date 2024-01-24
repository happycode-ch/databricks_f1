# Databricks notebook source
# MAGIC %md
# MAGIC ####  Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1. Read the JSON file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %md
# MAGIC The following schema is using the DDL formatted string method, which uses HIVE datatypes /using HIVE metastore

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Drop an unwanted column from the dataframe 

# COMMAND ----------

# MAGIC %md
# MAGIC constructor_dropped_df = constructor_df.drop(constructor_df['url'])
# MAGIC #### Usefull when you have multiple dataframes and want to reference a specific dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3. Rename columns and add ingestion date 

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructor_renamed_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("data_source", lit(v_data_source)) \
                                             .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructor_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4. Write output to parquet file with partition

# COMMAND ----------

#constructors_final_df.write.mode('overwrite').parquet(f"{processed_folder_path}/constructors")
constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")