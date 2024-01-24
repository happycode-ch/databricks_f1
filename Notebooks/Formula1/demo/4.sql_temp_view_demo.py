# Databricks notebook source
# MAGIC %md
# MAGIC #### Access dataframes using SQL
# MAGIC #### Objectives
# MAGIC 1. Create temporary views on dataframes
# MAGIC 1. Access the view from SQL cell
# MAGIC 1. Access the view from the Python cell
# MAGIC
# MAGIC NOTE: view is only valid in the spark session, not in another notebook (if notebbok is detached, view is no longer available)

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

races_results_df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

race_results_2019_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {p_race_year}")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# from antoher notebook (global view)
spark.sql("SELECT * \
FROM global_temp.gv_race_results").show()

# COMMAND ----------

