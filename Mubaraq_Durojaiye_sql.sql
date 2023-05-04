-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Clearing the dbfs environment & making it ready to accomodate new tables and views

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Clear the cache
-- MAGIC spark.catalog.clearCache()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Setup

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import pandas as pd

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Setting the file directory for the clinicaltrials data

-- COMMAND ----------

-- To switch to a different year of the clinicaltrials data requires only the change of the year in the file directory
SET hivevar:data_dir = "/FileStore/tables/clinicaltrial_2021.csv"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Getting the year from the directory to make the other codes rerunnable

-- COMMAND ----------

SELECT substr(${hivevar:data_dir} FROM -8 FOR 4)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating Tables

-- COMMAND ----------

-- Creating the clinicaltrial data table
DROP TABLE IF EXISTS clinical_data;

CREATE TABLE clinical_data
USING csv
OPTIONS (
  path = ${hivevar:data_dir},
  header = "true",
  delimiter = "|",
  mode = "FAILFAST",
  inferSchema = "true"
);

-- Creating the pharma data table
DROP TABLE IF EXISTS pharma_data;

CREATE TABLE pharma_data
USING csv
OPTIONS (
  path = "/FileStore/tables/pharma.csv",
  header = "true",
  delimiter = ",",
  mode = "PERMISSIVE",
  inferSchema = "true"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CLinical Data

-- COMMAND ----------

SELECT * FROM clinical_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Pharma Data

-- COMMAND ----------

SELECT * FROM pharma_data

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # Question 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### What is the number of studies in the clinical data dataset?

-- COMMAND ----------

-- Select the number of studies in the clinical_data table
SELECT COUNT(id) AS `Number of Studies`
FROM clinical_data;

-- COMMAND ----------

-- Count the distinct number of studies in the clinical_data table
SELECT COUNT(DISTINCT id) AS `Distinct Number of Studies`
FROM clinical_data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Question 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### What are the types of studies in the dataset and their occurrences?

-- COMMAND ----------

-- Select the Type column and count the frequency of each type in clinical_data table
SELECT Type, COUNT(*) AS Frequency
FROM clinical_data
-- Grouping the result set by Type
GROUP BY Type
-- Ordering the result set in descending order of frequency
ORDER BY Frequency DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Question 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### What are the top 5 conditions and their occurrences?

-- COMMAND ----------

-- Select the conditions and count their frequency
SELECT Conditions, COUNT(*) AS Frequency
FROM (
  -- Explode the Conditions column by splitting it at the commas
  SELECT EXPLODE(SPLIT(Conditions, ",")) AS Conditions
  FROM clinical_data
) subquery
-- Group the conditions together and order by their frequency
GROUP BY Conditions
ORDER BY Frequency DESC
-- Limit the output to the top 5 most frequent conditions
LIMIT 5;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Question 4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Which companies are the top 10 most common sponsors that are not pharmaceutical companies?

-- COMMAND ----------

-- Select the sponsor and count the number of times it appears in the combined clinical and pharma data
SELECT Sponsor, COUNT(*) AS Frequency
FROM (
  -- Select the sponsor from the clinical data and perform a left anti join with the parent company from the pharma data
  SELECT c.Sponsor
  FROM clinical_data c
  LEFT ANTI JOIN pharma_data p
  ON c.Sponsor == p.Parent_Company
) subquery
-- Group the result by sponsor and sort in descending order of frequency
GROUP BY Sponsor
ORDER BY Frequency DESC
-- Limit the result to the top 10 sponsors
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Question 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### How many studies are completed per month in the given year?

-- COMMAND ----------

--Creating a temporary view named "Completion_count_temp" that aggregates the clinical data based on the completion status and month.
CREATE OR REPLACE TEMP VIEW Completion_count_temp AS 
(
  SELECT *
  FROM (
    SELECT LEFT(Completion, 3) AS Month, COUNT(*) AS Frequency
    FROM clinical_data
    WHERE Status = "Completed" AND RIGHT(Completion, 4) = (SELECT substr(${hivevar:data_dir} FROM -8 FOR 4))
    GROUP BY Month
    ORDER BY Frequency DESC
  )
  -- Ordering the resulting table in ascending order of the month.
  ORDER BY
    CASE
      WHEN Month = "Jan" THEN 1
      WHEN Month = "Feb" THEN 2
      WHEN Month = "Mar" THEN 3
      WHEN Month = "Apr" THEN 4
      WHEN Month = "May" THEN 5
      WHEN Month = "Jun" THEN 6
      WHEN Month = "Jul" THEN 7
      WHEN Month = "Aug" THEN 8
      WHEN Month = "Sep" THEN 9
      WHEN Month = "Oct" THEN 10
      WHEN Month = "Nov" THEN 11
      ELSE 12
    END
);

-- Querying the temporary view "Completion_count_temp" and displaying its result.
SELECT * FROM Completion_count_temp;

-- COMMAND ----------

-- Pairs to be plot
SELECT * FROM Completion_count_temp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Further Analysis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### What is the distribution of sponsors in the studies?

-- COMMAND ----------

-- Creating a temporary view for the sponsor distribution
CREATE OR REPLACE TEMP VIEW SponsorDistribution AS 
(
-- Selecting non-pharmaceutical companies and getting their total frequency
SELECT 'Non-pharmaceutical Companies' AS SponsorType, SUM(Frequency) AS TotalFrequency
FROM (
  SELECT Sponsor, COUNT(*) AS Frequency
  FROM (
  -- Select sponsors from the clinical data and exclude parent companies from pharma data
    SELECT c.Sponsor
    FROM clinical_data c
    LEFT ANTI JOIN pharma_data p
    ON c.Sponsor = p.Parent_Company
  ) subquery
  GROUP BY Sponsor
) subquery2
-- Combine with pharmaceutical companies and their total frequency
UNION ALL
SELECT 'Pharmaceutical Companies' AS SponsorType, SUM(Frequency) AS TotalFrequency
FROM (
  SELECT Sponsor, COUNT(*) AS Frequency
  -- Select sponsors from clinical data and include parent companies from pharma data
  FROM (
    SELECT c.Sponsor
    FROM clinical_data c
    LEFT JOIN pharma_data p
    ON c.Sponsor = p.Parent_Company
  ) subquery
  GROUP BY Sponsor
) subquery3
);

-- Querying the sponsor distribution view
SELECT * FROM SponsorDistribution

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Visualizing the distribution of sponsors

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Converting the SponsorDistribution view to a Spark DataFrame
-- MAGIC df = spark.sql("SELECT * FROM SponsorDistribution")
-- MAGIC 
-- MAGIC # Convert the Spark DataFrame to a Pandas DataFrame
-- MAGIC df_pandas = df.toPandas()
-- MAGIC 
-- MAGIC # Plot a pie chart using matplotlib
-- MAGIC plt.figure(figsize=(8,6))
-- MAGIC plt.title("Distribution of Sponsors")
-- MAGIC plt.pie(df_pandas['TotalFrequency'], labels=df_pandas['SponsorType'], autopct='%1.1f%%', startangle=90, colors=plt.cm.Pastel1.colors)
-- MAGIC plt.axis('equal')
-- MAGIC plt.show()

-- COMMAND ----------


