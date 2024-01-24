# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Filtering
# MAGIC #### Demo filter()
# MAGIC Note: where() is an alias for filter()

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

races_filtered_df = races_df.where("race_year = 2019 and round <= 5")
# SQL way, "where condition, in this case 'filter()'"

# COMMAND ----------

races_filtered_df = races_df.filter(races_df.race_year == 2019)
# pythonic way

# COMMAND ----------

races_filtered_df = races_df.filter((races_df["race_year"] == 2019) & (races_df["round"] <= 5))
# pythonic way

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------

