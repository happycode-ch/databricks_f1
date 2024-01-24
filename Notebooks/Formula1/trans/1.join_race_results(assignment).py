# Databricks notebook source
# MAGIC %md
# MAGIC ### Race Results Join -> presentation
# MAGIC #### Requirements:
# MAGIC 1. 2020 Abu Dhabi Grand Prix (https://www.bbc.com/sport/formula1/2020/abu-dhabi-grand-prix/results) example(this will cover entire dataset)
# MAGIC 1. Ergast 
# MAGIC 1. mount presntation folder
# MAGIC 1. parquet files: races, circuits, drivers, constructors, results, current_timestamp
# MAGIC 1. column names: race_year, race_name, race_date, circuit_location, driver_name, driver_number, driver_nationality, team, grid, fastest_lap, race_time, points, created_date
# MAGIC
# MAGIC Sample Row = id | race_year | driver_name | driver_nationality | driver_number | team | grid | pits | fastest_lap | race_time | points | created_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC #### races

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

display(races_df.count())

# COMMAND ----------

# MAGIC %md 
# MAGIC #### circuits

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("location", "circuit_location")

# COMMAND ----------

display(circuits_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join circuits_df to races_df

# COMMAND ----------

# Left Outer Join
race_circuits_df = races_df.join(circuits_df, races_df.race_id == circuits_df.circuit_id, "outer") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_id, circuits_df.circuit_location)

# COMMAND ----------

# inner join (are there any races that dont have circuits?)
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id) \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_id, circuits_df.circuit_location)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### drivers

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### constructors

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors").withColumnRenamed("name", "team")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### results

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results").withColumnRenamed("fastest_lap_time", "fastest lap") \
.withColumnRenamed("time", "race time")

# COMMAND ----------

results_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### join results to drivers

# COMMAND ----------

# Left Outer Join
driver_results_df = results_df.join(drivers_df, results_df.driver_id == drivers_df.driver_id, "outer") \
.select(drivers_df.driver_id, drivers_df.driver_name, drivers_df.driver_number, drivers_df.driver_nationality, results_df.race_id, results_df.constructor_id, results_df.grid, results_df["fastest lap"], results_df["race time"], results_df.points)

# COMMAND ----------

driver_results_df.count()

# COMMAND ----------

display(driver_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join the drivers_results_df to circuits_df

# COMMAND ----------

driver_result_race_circuits_df = driver_results_df.join(race_circuits_df, driver_results_df.race_id == race_circuits_df.race_id, "outer") \
.select(race_circuits_df.race_year, race_circuits_df.race_name, race_circuits_df.race_date, race_circuits_df.circuit_location, driver_results_df.driver_id, driver_results_df.driver_name, driver_results_df.driver_number, driver_results_df.driver_nationality, driver_results_df.race_id, driver_results_df.constructor_id, driver_results_df.grid, driver_results_df["fastest lap"], driver_results_df["race time"], results_df.points)

# COMMAND ----------

display(driver_result_race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add Constructor (team) to driver_result_race_circuits_df

# COMMAND ----------

driver_team_result_race_circuits_df = driver_result_race_circuits_df.join(
    constructors_df, driver_result_race_circuits_df.constructor_id == constructors_df.constructor_id, "outer"
).select(
    race_circuits_df.race_year,
    race_circuits_df.race_name, 
    race_circuits_df.race_date, 
    race_circuits_df.circuit_location, 
    driver_results_df.driver_id, 
    driver_results_df.driver_name, 
    driver_results_df.driver_number, 
    driver_results_df.driver_nationality, 
    driver_results_df.race_id, 
    driver_results_df.constructor_id,
    constructors_df.team,
    driver_results_df.grid, 
    driver_results_df["fastest lap"], 
    driver_results_df["race time"], 
    results_df.points
)

# COMMAND ----------

driver_team_result_race_circuits_df.count()

# COMMAND ----------

display(driver_team_result_race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select only the required columns

# COMMAND ----------

race_results_final_df = driver_team_result_race_circuits_df.select(
    race_circuits_df.race_year,
    race_circuits_df.race_name, 
    race_circuits_df.race_date, 
    race_circuits_df.circuit_location, 
    driver_results_df.driver_name, 
    driver_results_df.driver_number, 
    driver_results_df.driver_nationality, 
    constructors_df.team,
    driver_results_df.grid, 
    driver_results_df["fastest lap"], 
    driver_results_df["race time"], 
    results_df.points
)

# COMMAND ----------

display(race_results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ingestion date

# COMMAND ----------

final_df = add_ingestion_date(race_results_final_df)

# COMMAND ----------

display(final_df)

# COMMAND ----------

dbutils.notebook.exit("Success")