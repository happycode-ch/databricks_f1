-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT *
FROM f1_processed.drivers
LIMIT 10;

-- COMMAND ----------

SELECT name, dob AS date_of_birth
FROM f1_processed.drivers
WHERE nationality = "British"
AND dob >= "1990-01-01"
ORDER BY dob DESC
LIMIT 10;

-- COMMAND ----------

SELECT *
FROM f1_processed.drivers
ORDER BY nationality DESC
LIMIT 10;

-- COMMAND ----------

SELECT name, nationality, dob AS date_of_birth
FROM f1_processed.drivers
WHERE (nationality = "British"
AND dob >= "1990-01-01") OR 
nationality = "Indian"
ORDER BY dob DESC
LIMIT 10;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SELECT MAX(dob)
FROM drivers;

-- COMMAND ----------

SELECT *
  FROM f1_processed.drivers
  WHERE dob = '2000-05-11';

-- COMMAND ----------

SELECT COUNT(*)
  FROM f1_processed.drivers
  WHERE nationality = "British";

-- COMMAND ----------

SELECT nationality, COUNT(*)
  FROM f1_processed.drivers
  GROUP BY nationality
  ORDER BY nationality;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 'HAVING' clause lets you restrict the data based on the aggregate fuctions

-- COMMAND ----------

SELECT nationality, COUNT(*)
  FROM f1_processed.drivers
  GROUP BY nationality
  HAVING COUNT(*) > 100
  ORDER BY nationality;
  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 'RANK' Function

-- COMMAND ----------

SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob DESC) AS age_rank
  FROM f1_processed.drivers
  ORDER BY nationality, age_rank;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### JOIN in SQL

-- COMMAND ----------

USE f1_presentation;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2018
AS
SELECT race_year, driver_name, team, total_points, wins, rank
  FROM f1_presentation.driver_standings
  WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_driver_standings_2018;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2020
AS
SELECT race_year, driver_name, team, total_points, wins, rank
  FROM f1_presentation.driver_standings
  WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM v_driver_standings_2020;

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  JOIN v_driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  LEFT JOIN v_driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  RIGHT OUTER JOIN v_driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  FULL OUTER JOIN v_driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### SEMI JOIN

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  SEMI JOIN v_driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ANTI JOIN
-- MAGIC The following drivers, competed in 2018 and not in 2020 (What exists in the left table that does carry over in the right)

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  ANTI JOIN v_driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

SELECT *
  FROM v_driver_standings_2018 d_2018
  CROSS JOIN v_driver_standings_2020 d_2020

-- COMMAND ----------

