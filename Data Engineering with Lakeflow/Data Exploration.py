# Databricks notebook source
# MAGIC %md
# MAGIC ## Explore the source

# COMMAND ----------

# MAGIC %md
# MAGIC The `%fs` magic command can be use to run file operations. The below command lists the source files that we will need to ingest.

# COMMAND ----------

# MAGIC %fs ls /Volumes/lakehouse_labs/hrio_ppa_developers_bootcamp/lab/gym

# COMMAND ----------

# MAGIC %md
# MAGIC `dbutils` provides a series of utiliy functions. The `fs` commands are available under this object too. The `head` call gives us a preview of a file.

# COMMAND ----------

dbutils.fs.head("/Volumes/lakehouse_labs/hrio_ppa_developers_bootcamp/lab/gym/part-00000-tid-1852474742045174727-9d06a407-c09f-4ddd-9354-a7403500e8d1-146-1-c000.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Now lets read and preview the source data files using using python and pyspark.

# COMMAND ----------

spark.read.json("/Volumes/lakehouse_labs/hrio_ppa_developers_bootcamp/lab/gym/").display()

# COMMAND ----------

# MAGIC %md
# MAGIC SQL can also be used to read and display the data. The magic `%sql` overrides the notebook's default language (python) for the cell.
# MAGIC
# MAGIC
# MAGIC Let's see what topics are available. 

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct topic 
# MAGIC from json.`/Volumes/lakehouse_labs/hrio_ppa_developers_bootcamp/lab/gym/`

# COMMAND ----------

# MAGIC %md
# MAGIC Let's enforce the correct schema, and inspect the bmp data. In this case, the binary data is expected to text encoded format, so we can cast it directly to `string`.

# COMMAND ----------

import pyspark.sql.functions as F
df_bpm = (spark.read
 .schema("key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG")
 .json("/Volumes/lakehouse_labs/hrio_ppa_developers_bootcamp/lab/gym/")
 .filter(F.col("topic") == "bpm")
 .withColumn("value", F.col("value").cast("string"))
)
df_bpm.display()

# COMMAND ----------

# MAGIC %md
# MAGIC We can see that the `value` colmun is json data. We can use the `from_json` function to parse this, then un-nest it using a `select`.

# COMMAND ----------

bpm_schema = "device_id INT, time FLOAT, heartrate DOUBLE"
df_bpm_parsed = (df_bpm
 .withColumn("value", F.from_json(F.col("value"), bpm_schema))
 .select("value.*")
 .withColumn("time", F.col("time").cast("timestamp"))
)
df_bpm_parsed.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's check if there's any duplicates in the data, looking at `device_id` and `time`. We will use a temporary view to inspect this using SQL from the `bmp_parsed` dataframe.
# MAGIC
# MAGIC We will also use the `spark.sql` function to show how we can run SQL within `pyspark`. The below SQL code will also work in a `%sql` defined cell (provided the `bpm_parsed` view was created first).

# COMMAND ----------

df_bpm_parsed.createOrReplaceTempView("bpm_parsed")
spark.sql("select device_id, count(*) as count from bpm_parsed group by device_id, time order by count desc").display()

# COMMAND ----------

# MAGIC %md
# MAGIC In our gym business, production (customer) device id's are above `102000`, with anything below reserved for testing. Let's see if we have any test devices in our production data. 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bpm_parsed where device_id < 102000
