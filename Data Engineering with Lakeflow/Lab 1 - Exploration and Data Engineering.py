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
# MAGIC # Data Engineering with Databricks
# MAGIC ## Process BPM, Workouts, User Info
# MAGIC
# MAGIC In this lab, you will be working with synthetic workout data from a high-tech gym. At this gym, we use wearable technology to track user details such as heart rate (bpm), workout sessions, and user profile information. This infromation is streamed in real time to our databases. Our task will be to ingest this data into Unity Catalog as bronze data, split out the bpm data, and built a silver table with only quality data.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Source Data
# MAGIC In these exercises, we will be using data that has been extracted from sources and landed in S3/ADLS as a static dataset. In this Lab 1, we will process this whole dataset in one batch. In Lab 2, we will explore streaming this data incrementally instead.
# MAGIC
# MAGIC The raw data we are ingesting is in the json format, stored as a binary payload. Lets take a look at it...
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC The `%fs` magic command can be use to run file operations. The command below lists the source files in the **volume** that we will be exploring and ingesting.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %fs ls /Volumes/rtlh_lakehouse_labs/bootcamp_oct_2025/resources/data/gym/

# COMMAND ----------

# MAGIC %md
# MAGIC **LAB EXERCISE**
# MAGIC
# MAGIC We can see we are listing the volume contents, but what is a volume in Databricks? Use the assistant on the far right-hand side to help answer this.
# MAGIC
# MAGIC
# MAGIC <img src="./Includes/images/assistant_side_panel.png">

# COMMAND ----------

# MAGIC %md
# MAGIC `dbutils` provides a series of utiliy functions. The `fs` commands are available under this object too. The `head` call gives us a preview of a file.
# MAGIC
# MAGIC **LAB EXERCISE**
# MAGIC
# MAGIC Fill in the full path of one of the json files from the volume below to show the file preview
# MAGIC

# COMMAND ----------

dbutils.fs.head("/Volumes/<path to json file>")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploring with Spark and SQL
# MAGIC
# MAGIC The head operation dumps out the raw JSON, which isn't the most user-friendly to look at. Lets use pyspark to read files as a table instead, and get a sense of the structure and contents

# COMMAND ----------

spark.read.json("/Volumes/rtlh_lakehouse_labs/bootcamp_oct_2025/resources/data/gym/").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Ok, that looks much more readable. Notice how there's a column called "topic". In this data source, there is a combination of different types of data, referenced by the topic. Lets explore what topics are available.
# MAGIC
# MAGIC
# MAGIC We aren't restricted to python and pyspark as a language. SQL can also be used to read and display the data. The magic `%sql` overrides the notebook's default language, python, for the cell. You can configure the default language for the notebook at the top of the notebook; look for the "python" toggle.
# MAGIC
# MAGIC
# MAGIC **LAB EXERCISE**
# MAGIC
# MAGIC Complete the below code to show the distinct topics avialable in the source data. If you get stuck, you can use the inline-assistant for help. Just look for the assistant icon in the cell (next to the delete cell icon). 

# COMMAND ----------

# MAGIC %sql
# MAGIC select <fill this in>
# MAGIC from json.`/Volumes/rtlh_lakehouse_labs/bootcamp_oct_2025/resources/data/gym/`

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's dymistify the binary "value" data by enforcing the correct schema, and inspect the bmp data. In this case, the binary data is expected to be in a text-encoded format, so we can cast it directly to `string`.

# COMMAND ----------

import pyspark.sql.functions as F
df_bpm = (spark.read
 .schema("key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG")
 .json("/Volumes/rtlh_lakehouse_labs/bootcamp_oct_2025/resources/data/gym/")
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
# MAGIC We will also use the `spark.sql` function to demonstrate how to run SQL queries within `pyspark`. The SQL code below will also work in a `%sql` defined cell (provided the `bpm_parsed` view has been created first).
# MAGIC
# MAGIC **LAB EXERCISE**
# MAGIC
# MAGIC Fill in the appropriate select and group by statement to check for duplicates. Remeber, you can use the assistant to help you out.

# COMMAND ----------

df_bpm_parsed.createOrReplaceTempView("bpm_parsed")
spark.sql("""
  select <fill this in>
  from bpm_parsed 
  group by <fill this in>
  order by count desc
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Looks like are some duplicates, so we need think about how to handle these when we build our pipeline.
# MAGIC
# MAGIC Let's do some extra checks for invalid data. In our gym business, production (customer) device ids start from `102000`, with anything below reserved for testing. Let's see if we have any test devices in our production data. 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bpm_parsed where device_id < 102000

# COMMAND ----------

# MAGIC %md
# MAGIC It appears we do have some test data present, so we should filter this out as part of our ingestion pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest and Transform
# MAGIC
# MAGIC Now we have explored our dataset, we will need to ingest it into a medallion structure and write it out to tables. We should create the following:
# MAGIC
# MAGIC - A bronze table with the raw data ingested
# MAGIC - An additional bronze table, that splits out the bpm data
# MAGIC - A silver table that cleans up the bpm data, dropping duplicates, and filtering out invalid records
# MAGIC - A lakeflow job that can schedule this ingestion and transform notebook to run
# MAGIC - If time permits, additional bronze and silver tables that split out the `workout` data and the `user_info` data 

# COMMAND ----------

# MAGIC %md
# MAGIC **Set the default catalog and schema**
# MAGIC
# MAGIC Unity Catalog uses a 3 level namespace: `<catalog>.<schema>.<table>`. To avoid typing out the same catalog and schema every time, lets *use* our catalog and schema. The below code should automate this from the labs environment using your username, ie the catalog should be `rtlh_lakehouse_labs` and the schema should be `labs_<firsname>_<lastname>`
# MAGIC

# COMMAND ----------

user = spark.sql("select current_user()").collect()[0][0]
user = user.split('@')[0]
user = ''.join([c if c.isalnum() else '_' for c in user.lower()])

spark.sql("use catalog rtlh_lakehouse_labs")
spark.sql(f"use schema labs_{user}")

print(f"Default location: {spark.sql('select current_catalog()').collect()[0][0]}.{spark.sql('select current_schema()').collect()[0][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Ingest the bronze data**
# MAGIC
# MAGIC Below, we ingest the raw data into Bronze using pyspark. First we create a pyspark `DataFrame`, then we persist this dataframe in a table to the catalog.
# MAGIC
# MAGIC Spark is *lazy* by nature, so expressions such as `spark.read.schema(raw_schema).json(source)` won't read all the source data by themselves. Actions such as `display` and `write` are the commands that will cause execution of the spark expressions.

# COMMAND ----------

bronze_schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"
source = "/Volumes/rtlh_lakehouse_labs/bootcamp_oct_2025/resources/data/gym/"
df_bronze = spark.read.schema(raw_schema).json(source)
display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC Next we write out the dataframe to a table called bronze. 

# COMMAND ----------

df_bronze.write.mode("overwrite").saveAsTable("bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC **Lab Exercise**
# MAGIC
# MAGIC What is *mode* doing here? Why do you think we setting this to *overwrite*? What would happen if we used *append* instead?

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at the table we wrote out. It should be the same as the dataframe.

# COMMAND ----------

spark.table("bronze").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we have ingested the raw bronze data, lets split out the `bpm` data specifically. Lets keep transforms to a minimum so we get a bronze version of the bmp data.

# COMMAND ----------

bpm_schema = "device_id INT, time FLOAT, heartrate DOUBLE"
df_bronze_bpm = (spark.table("bronze")
          .filter("topic = 'bpm'")
          .select(F.from_json(F.col("value").cast("string"), bpm_schema).alias("v"))
          .select("v.*"))
df_bronze_bpm.display()

# COMMAND ----------

df_bronze_bpm.write.mode("overwirte").saveAsTable("bpm_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC **Lab Exercise**
# MAGIC
# MAGIC Once the data has been written, see if you can find the bpm_bronze table in Unity Catalog (try using the search bar at the top of the screen). Can you explore its lineage?

# COMMAND ----------

# MAGIC %md
# MAGIC Next we can create the cleaned silver bpm table. This should drop duplicates from the bronze data, and only include the production devices (devices from id 102000 onwards)
# MAGIC
# MAGIC **Lab Exercise**
# MAGIC
# MAGIC Update the code below to only include device ids 102000 onwards

# COMMAND ----------

(spark.table("bpm_bronze")
  <add a filter to only include ids from 102000 onwards>
  .select("device_id", F.col("time").cast("timestamp").alias("timestamp"), "heartrate")
  .dropDuplicates(["device_id", "timestamp"])
  .write
  .saveAsTable("bpm_silver")
)

# COMMAND ----------

spark.table("bpm_silver").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orchestration
# MAGIC
# MAGIC **Lab Exercise**
# MAGIC
# MAGIC Now that we have built our ingest and transform, create a daily schedule to run this notebook using serverless compute. Look for the "schedule" button in the top right corner.
# MAGIC
# MAGIC Note - after setting this up, be sure to pause the schedule since we don't actually need to run this notebook daily :)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extending the Transforms
# MAGIC
# MAGIC Now that you have completed the ingestion and transformation for `bpm` have a go at adding in `workout` and `user_info` tables.
# MAGIC Take inspiration from the existing code, and use the assistant for help.
