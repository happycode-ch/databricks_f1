# Databricks notebook source
# MAGIC %md
# MAGIC #### Global Temp Views
# MAGIC 1. Create global temporary views on dataframes
# MAGIC 1. Access the view from SQL cell
# MAGIC 1. Access the view from Python cell
# MAGIC 1. Access the view from another notebook

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

races_results_df.createGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM global_temp.gv_race_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

spark.sql("SELECT * \
FROM global_temp.gv_race_results").show()

# COMMAND ----------

