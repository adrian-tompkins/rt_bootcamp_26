# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab 2: Spark Declarative Pipelines
# MAGIC
# MAGIC ## Objective
# MAGIC In this lab, you will create and configure a Databricks Spark Declactive pipeline to ingest and transform our gym data from Lab 1. This time, we will support _streaming_, bringing the data incrementally, which allows near real time processing of data once it lands!
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Stage Source Data
# MAGIC
# MAGIC In Lab 1, we used the entirety of our dataset `/Volumes/rtlh_lakehouse_labs/bootcamp_oct_2025/resources/data/gym/` and ingested it all at once. But now we are going ot simulate this data being incrementally landed. To do this, we will create a volume in our own schema and create a function that populates it incrementally when run.

# COMMAND ----------

# MAGIC %md
# MAGIC First lets set the default location to be our catalog and schema

# COMMAND ----------

user = spark.sql("select current_user()").collect()[0][0]
user = user.split('@')[0]
user = ''.join([c if c.isalnum() else '_' for c in user.lower()])

spark.sql("use catalog rtlh_lakehouse_labs")
spark.sql(f"use schema labs_{user}")

print(f"Default location: {spark.sql('select current_catalog()').collect()[0][0]}.{spark.sql('select current_schema()').collect()[0][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC Now we need to create a volume

# COMMAND ----------

# MAGIC %sql
# MAGIC drop volume if exists lab2; --cleanup the lab2 volume if we ran this previously
# MAGIC create volume lab2;

# COMMAND ----------

# MAGIC %md
# MAGIC Let's create a function that can copy N rows at a time from our source, simulating and incremental load

# COMMAND ----------

import os

def copy_next_n_rows(
  n,
  source_table="bootcamp_oct_2025.lab2_bronze",
  dest_path=f"/Volumes/rtlh_lakehouse_labs/labs_{user}/lab2/resources/data/gym"):
    
    df = spark.table(source_table)

    # Find latest timestamp in destination
    dest_exists = os.path.exists(dest_path)
    dest_files = os.listdir(dest_path) if dest_exists else []
    if dest_files:
        dest_df = spark.read.json(dest_path)
        if dest_df.count() > 0:
            last_ts = dest_df.agg({"timestamp": "max"}).collect()[0][0]
        else:
            last_ts = None
    else:
        last_ts = None

    # Filter for next N rows after last_ts
    if last_ts:
        next_rows = df.filter(df.timestamp > last_ts).orderBy("timestamp").limit(n)
    else:
        next_rows = df.orderBy("timestamp").limit(n)
    
    # If nothing to write, exit
    if next_rows.count() == 0:
        print("No new rows to copy.")
        return
    
    # Write as JSON, append mode, preserve format
    next_rows.write.mode("append").json(dest_path)
    print(f"Copied {next_rows.count()} rows to {dest_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC Now call the function for 100 rows

# COMMAND ----------

copy_next_n_rows(100)

# COMMAND ----------

# MAGIC %md
# MAGIC Validate 100 rows were copied

# COMMAND ----------

spark.read.json(f"/Volumes/rtlh_lakehouse_labs/labs_{user}/lab2/resources/data/gym").count()

# COMMAND ----------

# MAGIC %md
# MAGIC Lets look at the files to validate its json data.

# COMMAND ----------

spark.createDataFrame(dbutils.fs.ls(f"/Volumes/rtlh_lakehouse_labs/labs_{user}/lab2/resources/data/gym")).filter("name like '%.json'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's copy another 1000 rows

# COMMAND ----------

copy_next_n_rows(1000)

# COMMAND ----------

# MAGIC %md
# MAGIC We should now have 1100 rows

# COMMAND ----------

spark.read.json(f"/Volumes/rtlh_lakehouse_labs/labs_{user}/lab2/resources/data/gym").count()

# COMMAND ----------

# MAGIC %md
# MAGIC Great, we now have a function `copy_next_n_rows` that we can use to simulate newly arriving data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create a Spark Declarative Pipeline
# MAGIC
# MAGIC The pipeline code for processing the `bpm` data has already been written in the `Pipeline` folder. To ingest this with the Spark Declarative Pipeline framework, we will need to create the pipeline in Databricks.
# MAGIC
# MAGIC Navigate to **Jobs & Pipelines** and create an ETL pipeline
# MAGIC
# MAGIC ![Create Pipeline](./Includes/images/lab2/1_create_pipeline.png)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Give it a name, choose the `rtlh_lakehouse_labs` catalog and your personal schema as the default table location.
# MAGIC
# MAGIC Select "Add existing assets" 
# MAGIC
# MAGIC
# MAGIC ![Setup Pipeline](./Includes/images/lab2/2_setup_pipeline.png)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we need to define the code that forms the contents of the pipeline.
# MAGIC
# MAGIC The first entry defines your root folder. Note that the contents of this folder won't be treated as pipeline code by default. The contents here may contain a mix of exploratory notebooks along with active pipeline code. In this box, select the folder **Data Engineering with Lakeflow** as the root of the project.
# MAGIC
# MAGIC The second box defines where your active pipeline code lives. Everything in here will be treated as pipeline code that can be executed by SDP. Add the **Pipeline** folder to this.
# MAGIC
# MAGIC
# MAGIC ![Setup Pipeline](./Includes/images/lab2/3_add_assets.png)

# COMMAND ----------

# MAGIC %md
# MAGIC **Your pipeline should now be created!**
# MAGIC
# MAGIC But we are not finished yet. Remember how our `copy_next_n_rows` function writes data to our staging location? We need to tell our pipeline about this. Take a look at the `.\Pipeline\ingest.py` file. You should see that we are using the `user` config attribute to tell the pipeline about our unique schema for this lab
# MAGIC
# MAGIC `user = spark.conf.get("user")`
# MAGIC
# MAGIC To configure this, open the settings of the pipeline, and add a configuration for user.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ![Open Settings](./Includes/images/lab2/4_open_settings.png)
# MAGIC
# MAGIC ![Settings](./Includes/images/lab2/5_settings.png)
# MAGIC
# MAGIC ![Configuration](./Includes/images/lab2/6_config.png)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Run the Pipeline!
# MAGIC
# MAGIC Now it's time to run the pipeline! Press the **Run Pipeline** button and see what happens!
# MAGIC
# MAGIC You should have 3 tables created by the pipeline. Now take some time getting familiar with the interface and the pipeline code so you can understand how everything works.
# MAGIC
# MAGIC **LAB EXERCISE**
# MAGIC
# MAGIC See if you can answer the following questions
# MAGIC  - Can you find the visual DAG that gets created?
# MAGIC  - Where can you see the compute for the pipeline?
# MAGIC  - Can you find the streaming tables in the catalog?
# MAGIC  - How many rows were ingested? Did it match the number of rows you staged (1100)?
# MAGIC  - Did the pipeline have any dropped records because of failed expectations?
# MAGIC  - Where can you find the code for the tables?
# MAGIC  - How did we manage the requirement of device ids being over 102000
# MAGIC  - Did you see we are ingesting data with "cloudfetch"? What is this, and what does it do?
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Ingest More Data
# MAGIC
# MAGIC Now lets demonstrait how we can incrementally ingest data. Run the below command to ingest some more rows, then re-run the pipeline. Notice how the pipeline streaming tables incrementally ingest new rows?

# COMMAND ----------

copy_next_n_rows(1000) # feel free to update the number of rows to stage at a time if desired!

# COMMAND ----------

# MAGIC %md
# MAGIC **LAB EXERCISE**
# MAGIC - We are manually pressing the "Run Pipeline" button, but how could you update your pipline to always ingest data as soon as its available?
# MAGIC - What if you had a requirement to only ingest new data daily? How would you do that?
# MAGIC - In the pipeline code, can you find what's causing the pipeline tables to be streaming tables?
# MAGIC - Can you add another table to the pipeline that calculates the average bpm per user? What happens if you create this with `read` instead of `readStream`?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Build the Workout and User Info Tables
# MAGIC
# MAGIC Have a go at extending the pipeline so that we build the silver and bronze `workout` and `user_info` tables. Use tools like exploration notebooks and the assistant to help you out.
