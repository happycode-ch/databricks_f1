# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest circuts.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1. Read the CSV file using the spark dataframe reader

# COMMAND ----------

# MAGIC %md
# MAGIC #### statments used in setup
# MAGIC 1. display(circuts_df) - displays the dataframe in current state
# MAGIC 1. circuts_df.printSchema - prints the schema 
# MAGIC 1. circuts_df.describe().show() - describes the dataframe in its current state as it relates to the schema / data

# COMMAND ----------

# MAGIC %md
# MAGIC #### %run "../<folder>/<notebook> command makes everything from the referenced notebook available to this notebook, as can be seen below
# MAGIC Note: must be in its own cell 

# COMMAND ----------

dbutils.widgets.help()

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                    StructField("circuitRef", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("location", StringType(), True),
                                    StructField("country", StringType(), True),
                                    StructField("lat", DoubleType(), True),
                                    StructField("lng", DoubleType(), True),
                                    StructField("alt", IntegerType(), True),
                                    StructField("url", StringType(), True),
                                    ])

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Select only the required columns

# COMMAND ----------

# MAGIC %md
# MAGIC circuts_selected_df = circuts_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

# MAGIC %md
# MAGIC circuts_selected_df = circuts_df.select(circuts_df.circuitId, circuts_df.circuitRef, circuts_df.name, circuts_df.location, circuts_df.country, circuts_df.lat, circuts_df.lng, circuts_df.alt)

# COMMAND ----------

# MAGIC %md
# MAGIC circuts_selected_df = circuts_df.select(circuts_df["circuitId"], circuts_df["circuitRef"], circuts_df["name"], 
# MAGIC                                         circuts_df["location"], circuts_df["country"], circuts_df["lat"], circuts_df["lng"], circuts_df["alt"])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### The from pyspark.sql.functions import col method is good when you need to change column names or perform other transform operations, less verbose
# MAGIC adding .alias to col("country") -> col("country").alias("race_country") changes the name of the column to the aliased value 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Add ingestion data to the dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC from pyspark.sql.functions import current_timestamp, lit
# MAGIC 1. creates a column of literal value

# COMMAND ----------

# MAGIC %md
# MAGIC circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) \
# MAGIC .withColumn("env", lit("Production"))
# MAGIC 1. The column describes the environment, in this case production

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5. Write data to datalake as a parquet file

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

# MAGIC %md
# MAGIC %fs
# MAGIC ls /mnt/formula1dlakiser/processed/circuits

# COMMAND ----------

# MAGIC %md
# MAGIC df = spark.read.parquet("/mnt/formula1dlakiser/processed/circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")