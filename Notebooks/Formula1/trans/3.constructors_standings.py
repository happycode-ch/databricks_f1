# Databricks notebook source
# MAGIC %md
# MAGIC #### Produce Constructor Standings

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC Find the race years for which the data is to be reprocessed

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select Columns

# COMMAND ----------

constructor_results_df = race_results_df.select(
    "race_year",
    "team",
    "position",
    "points"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### OrderBy

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

constructor_standings_df = constructor_results_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"), 
count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Rank, Select Columns and Rename "total_points" Col

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec)) \
.select(
    "rank",
    "race_year",
    "team",
    "wins",
    "total_points"
) \
.withColumnRenamed("total_points", "points")

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")

# COMMAND ----------

# overwrite_partition(final_df, 'f1_presentation', 'constructor_standings', 'race_year')

# COMMAND ----------

merge_conditon = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'constructor_standings', presentation_folder_path, merge_conditon, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM f1_presentation.constructor_standings;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, COUNT(1)
# MAGIC   FROM f1_presentation.constructor_standings
# MAGIC   GROUP BY race_year
# MAGIC   ORDER BY race_year DESC;

# COMMAND ----------

