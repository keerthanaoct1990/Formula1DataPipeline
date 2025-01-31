# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(
    fields=[StructField("resultId", IntegerType(), False),
            StructField("raceId", IntegerType(), True),
            StructField("driverId", IntegerType(), True),
            StructField("constructorId", IntegerType(), True),
            StructField("number", IntegerType(), True),
            StructField("grid", IntegerType(), True),
            StructField("position", IntegerType(), True),
            StructField("positionText", StringType(), True),
            StructField("positionOrder", IntegerType(), True),
            StructField("points", FloatType(), True),
            StructField("laps", IntegerType(), True),
            StructField("time", StringType(), True),
            StructField("milliseconds", IntegerType(), True),
            StructField("fastestLap", IntegerType(), True),
            StructField("rank", IntegerType(), True),
            StructField("fastestLapTime", StringType(), True),
            StructField("fastestLapSpeed", FloatType(), True),
            StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_with_columns_df = results_df \
    .withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

results_with_ingestion_date_df = add_ingestion_date(results_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = results_with_ingestion_date_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC De-dupe the dataframe

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write to output to processed container in delta table

# COMMAND ----------

# MAGIC %md
# MAGIC merge the data to existing table. if new data insert it into table, if exixting data arrives update the info

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

#dbutils.notebook.exit("Success")

# COMMAND ----------

#%sql
#SELECT COUNT(1)
  #FROM f1_processed.results;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,  COUNT(1) 
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

#%sql SELECT * FROM f1_processed.results WHERE race_id = 540 AND driver_id = 229;

# COMMAND ----------


