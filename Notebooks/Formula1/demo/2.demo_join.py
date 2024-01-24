# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Filtering
# MAGIC #### Demo join()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inner Join
# MAGIC Returns records that are statified on both dataframes(left and right), need corresponding id's

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.filter("circuit_id < 70") \
.withColumnRenamed("name", "circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
    .withColumnRenamed("name", "race_name")

# COMMAND ----------

#inner join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

#race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
#.select((circuits_df.name).alias('circuit_name'), circuits_df.location, circuits_df.country, (races_df.name).alias('race_name'), races_df.round)
# .alias() works for renaming columns, best practice is to rename before this stage (as Above)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Outer Join
# MAGIC Note: Can have records that are unique to a df
# MAGIC Left: left_df is fixed (all records), corresponding records that match from the right_df (matching condition)
# MAGIC Right: right_df is fixed (all records), corresponding records that match from the left_df (matching condition)
# MAGIC Full: get everything from both dataframes, if the condition is not satified, returns NULL val's for those columns

# COMMAND ----------

# Left Outer Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# Right Outer Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# Full Outer Join, "full" or "full_outer"
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Semi Joins
# MAGIC Similiar to a inner join, difference: only given the columns from the LEFT side of the dataframe

# COMMAND ----------

# Semi Join, "semi" or "left_semi"
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Anti Joins
# MAGIC Opposite of Semi Join, everything on the left which is not found on the RIGHT df

# COMMAND ----------

# Anti Join, "anti" or "left_anit"
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)

# COMMAND ----------

# Anti Join, "anti" or "left_anit"
race_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "anti")
# no correspondin circuit, 3 results

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cross Joins
# MAGIC Note: CAREFUL, give you a cartition product, every record from the left, join with the right and results
# MAGIC Used with a dimension table, handfull of records that you want to duplicate a fact on all records (memory/data problems - big data)

# COMMAND ----------

# Cross Join,
race_circuits_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

