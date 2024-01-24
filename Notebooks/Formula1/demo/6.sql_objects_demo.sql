-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Lesson Objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 1. Create Database demo
-- MAGIC 1. Data tab in the UI
-- MAGIC 1. SHOW command
-- MAGIC 1. DESCRIBE command
-- MAGIC 1. Find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

DESC DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Leanring Objectives
-- MAGIC 1. Create managed table using Python
-- MAGIC 1. Create managed table using SQL
-- MAGIC 1. Effect of dropping a managed table 
-- MAGIC 1. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

CREATE TABLE demo.race_results_2020_sql
SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2020;

-- COMMAND ----------

DESC EXTENDED demo.race_results_2020_sql;

-- COMMAND ----------

SELECT *
  FROM demo.race_results_2020_sql

-- COMMAND ----------

DROP TABLE demo.race_results_2020_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Learing Objectives
-- MAGIC 1. Create an External Table using Python
-- MAGIC 1. Create and External Table using SQL
-- MAGIC 1. Effect of dropping an External Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_pythonext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_pythonext_py;

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)
USING PARQUET
LOCATION "/mnt/formula1dlkaiser/presentation/race_results_ext_sql";

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md Note: Inserting data into the table, writes to the (in our case) the object storage adls.

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_pythonext_py WHERE race_year = 2020;

-- COMMAND ----------

SELECT count(1) FROM demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on Tables
-- MAGIC 1. Create Temp View
-- MAGIC 1. Create Global Temp View
-- MAGIC 1. Create Permanent View

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS 
SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2018;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS 
SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2012;

-- COMMAND ----------

CREATE OR REPLACE VIEW pv_race_results
AS 
SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2000;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results;

-- COMMAND ----------

