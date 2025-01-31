-- Databricks notebook source
-- MAGIC %md
-- MAGIC create processed database.We will be creating managed tables under this db, all table data will be automatically saved to "/mnt/formula1dl2025project/processed" 

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dl2025project/processed"

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------


