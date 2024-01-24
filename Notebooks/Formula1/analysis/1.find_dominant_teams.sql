-- Databricks notebook source
SELECT current_database();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Find Dominant Teams (Assignment)

-- COMMAND ----------

USE f1_presentation;

-- COMMAND ----------

-- Dominant Teams with 100 or more races
SELECT team_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       avg(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  GROUP BY team_name
  HAVING count(1) >= 100
  ORDER BY avg_points DESC;

-- COMMAND ----------

-- Dominant Teams with 50 or more races
SELECT team_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       avg(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  GROUP BY team_name
  HAVING count(1) >= 50
  ORDER BY avg_points DESC;

-- COMMAND ----------

-- Dominant Teams with 100 or more races between 2001 - 2011
SELECT team_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       avg(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2001 AND 2011
  GROUP BY team_name
  HAVING count(1) >= 100
  ORDER BY avg_points DESC;