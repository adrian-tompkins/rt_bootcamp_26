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
# MAGIC In Lab 1, we used the entirety of our dataset `/Volumes/rtlh_lakehouse_labs/bootcamp_oct_2025/resources/data/gym/` and ingested it all at once. However, we will now simulate the incremental landing of this data. The lab instructor will setup a job to incrementally load 1000 rows of fresh data, every 10 seconds, to this location `/Volumes/rtlh_lakehouse_labs/bootcamp_oct_2025/resources/data/gym_stream/` This lab will use this new location for ingestion.
# MAGIC
# MAGIC **Lab Exercise**
# MAGIC Navigate to the `resources` volume in `rtlh_lakehouse_labs.bootcamp_oct_2025` and see if you can find the `gym_stream` data.

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
# MAGIC Now let's demonstrate how we can incrementally ingest data. The instructor's job is continuously staging more data for us, so we can re-run the pipeline and new data should be loaded.

# COMMAND ----------

# MAGIC %md
# MAGIC **LAB EXERCISE**
# MAGIC - How many new rows were ingested in the previous run?
# MAGIC - We are manually pressing the "Run Pipeline" button, but how could you update your pipline to always ingest data as soon as its available?
# MAGIC - What if you had a requirement to only ingest new data daily? How would you do that?
# MAGIC - In the pipeline code, can you find what's causing the pipeline tables to be streaming tables?
# MAGIC - Can you add another table to the pipeline that calculates the average bpm per user? What happens if you create this with `read` instead of `readStream`?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Build the Workout and User Info Tables
# MAGIC
# MAGIC Have a go at extending the pipeline so that we build the silver and bronze `workout` and `user_info` tables. Use tools like exploration notebooks and the assistant to help you out.
