# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1. Read the CSV file using the spark dataframe reader

# COMMAND ----------

# MAGIC %md
# MAGIC #### statments used in setup
# MAGIC 1. display(races_df) - displays the dataframe in current state
# MAGIC 1. races_df.printSchema - prints the schema 
# MAGIC 1. races_df.describe().show() - describes the dataframe in its current state as it relates to the schema / data

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("year", IntegerType(), True),
                                    StructField("round", IntegerType(), True),
                                    StructField("circuitId", IntegerType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("date", DateType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("url", StringType(), True),
                                    ])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Add Ingestion date and race_timestamp to dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_with_ingestion_date = add_ingestion_date(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3. Select only the required columns

# COMMAND ----------

races_selected_df = races_with_ingestion_date.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("race_timestamp"), col("ingestion_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitId", "circuit_id") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5. Write data to datalake as a parquet file

# COMMAND ----------

races_final_df = races_renamed_df

# COMMAND ----------

#races_final_df.write.mode('overwrite').partitionBy('race_year').parquet(f"{processed_folder_path}/races")
races_final_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")