# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Streaming ETL Lab
# MAGIC ## Process BPM, Workouts, User Info
# MAGIC
# MAGIC In this lab, you will configure a query to consume and parse raw data from a different topics as it lands in a multiplex bronze table. You'll also validate and quarantine these records before loading them into silver tables.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Describe how filters are applied to streaming jobs
# MAGIC - Use built-in functions to flatten nested JSON data
# MAGIC - Parse and save binary-encoded strings to native types
# MAGIC - Describe and implement a quarantine table
# MAGIC
# MAGIC ## Hint
# MAGIC
# MAGIC Each step of this lab has you define one additional table in the pipeline. Work on this lab one step at a time. Once you think you have a step implemented, run the pipeline to verify whether the table has the correct results. If not, update the code in this notebook. Then in the Pipeline interface, select only the table you're working on for a refresh, and perform a **full refresh** on that table.

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# Under pipeline settings -> configuration, add a unique table prefix (eg <firstname>_<lastname>)
table_prefix = spark.conf.get("table_prefix").strip()

assert table_prefix is not None and table_prefix != ""

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Step 1: Auto Load Raw Json Data into Multiplex Bronze
# MAGIC
# MAGIC The chief architect has decided that rather than connecting directly to Kafka, a source system will send raw records as JSON files to cloud object storage. We will need toingest these records with Auto Loader and store the entire history of this incremental feed. The initial table will store data from all of our topics and have the following schema. 
# MAGIC
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | key | BINARY |
# MAGIC | value | BINARY |
# MAGIC | topic | STRING |
# MAGIC | partition | LONG |
# MAGIC | offset | LONG
# MAGIC | timestamp | LONG |
# MAGIC
# MAGIC This single table will drive the majority of the data through the target architecture, feeding three interdependent data pipelines.
# MAGIC
# MAGIC <!-- <img src="https://files.training.databricks.com/images/ade/ADE_arch_bronze.png" width="60%" /> -->
# MAGIC
# MAGIC **NOTE**: Details on additional configurations for connecting to Kafka are available <a href="https://docs.databricks.com/spark/latest/structured-streaming/kafka.html" target="_blank">here</a>.
# MAGIC

# COMMAND ----------

@dlt.table(
    table_properties={"quality": "bronze"},
    name=f"{table_prefix}_bronze"
)
def bronze():
    return (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema("key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG")
        .load("/Volumes/lakehouse_labs/hrio_ppa_developers_bootcamp/lab/gym")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Step 2: Stream BPM from Multiplex Bronze
# MAGIC
# MAGIC Stream records for the **`bpm`** topic from the multiplex bronze table to create a **`bpm_bronze`** table.
# MAGIC 1. Start a stream against the **`bronze`** table
# MAGIC 1. Filter all records by **`topic = 'bpm'`**
# MAGIC 1. Parse and flatten JSON fields

# COMMAND ----------

bpm_schema = "device_id INT, time FLOAT, heartrate DOUBLE"

@dlt.table(
    table_properties={"quality": "bronze"},
    name=f"{table_prefix}_bpm_bronze"
)
def bpm_bronze():
    return (
        dlt.read_stream(f"{table_prefix}_bronze")
          .filter("topic = 'bpm'")
          .select(F.from_json(F.col("value").cast("string"), bpm_schema).alias("v"))
          .select("v.*")
        )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 3: Promote BPM to Silver
# MAGIC
# MAGIC Process BPM bronze data into a **`bpm_silver`** table with the following schema.
# MAGIC
# MAGIC | field | type |
# MAGIC | --- | --- |
# MAGIC | device_id | INT | 
# MAGIC | timestamp | TIMESTAMP | 
# MAGIC | heartrate | DOUBLE |
# MAGIC
# MAGIC We know that our devices and pipelines can cause duplicate data, so we should cleanse that.
# MAGIC Also, our production device id's start from `102000`, eveything below that Id is a test device. Test devices shouldn't be part of our ingested, production data, but sometimes these devices accidently get registered to the live system. We will add this as a data quality check.
# MAGIC
# MAGIC Validate and promote records from **`bpm_bronze`** to silver by implementing a **`bpm_silver`** table.
# MAGIC 1. Check that **`device_id`** field is not null and equal or above 102000
# MAGIC 1. Cast **`time`** to timestamp field named **`timestamp`**
# MAGIC 1. Deduplicate on **`device_id`** and **`time`**

# COMMAND ----------

rules = {
  "valid_id": "device_id IS NOT NULL and device_id >= 102000"
}

@dlt.table(
    table_properties={"quality": "silver"},
    name=f"{table_prefix}_bpm_silver"
)
@dlt.expect_all_or_drop(rules)
def bpm_silver():
    return (
        dlt.read_stream(f"{table_prefix}_bpm_bronze")
          .select("device_id", F.col("time").cast("timestamp").alias("timestamp"), "heartrate")
          .withWatermark("timestamp", "30 seconds")
          .dropDuplicates(["device_id", "timestamp"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 4: Quarantine Invalid Records
# MAGIC
# MAGIC Implement a **`bpm_quarantine`** table for invalid records from **`bpm_bronze`**.

# COMMAND ----------

quarantine_rules = {"invalid_record": f"NOT({' AND '.join(rules.values())})"}
@dlt.table(
    name=f"{table_prefix}_bpm_quarantine"
)
@dlt.expect_all_or_drop(quarantine_rules)
def bpm_quarantine():
    return (
        dlt.read_stream(f"{table_prefix}_bpm_bronze")    
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 5 & Beyond: Adding Workouts & User Info
# MAGIC
# MAGIC Now add similar pipelines for Workout and User Info data! Use the exploration notebook to help understand the datasets, and the other pipleine tables that you have built as a starting point.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
