from pyspark import pipelines as dp
import pyspark.sql.functions as F

# Under pipeline settings -> configuration, add a unique table prefix (eg <firstname>_<lastname>)
table_prefix = spark.conf.get("table_prefix").strip()

assert table_prefix is not None and table_prefix != ""

bpm_schema = "device_id INT, time FLOAT, heartrate DOUBLE"

@dp.table(
    table_properties={"quality": "bronze"},
    name=f"{table_prefix}_bpm_bronze"
)
def bpm_bronze():
    return (
        dp.read_stream(f"{table_prefix}_bronze")
          .filter("topic = 'bpm'")
          .select(F.from_json(F.col("value").cast("string"), bpm_schema).alias("v"))
          .select("v.*")
        )
    
rules = {
  "valid_id": "device_id IS NOT NULL and device_id >= 102000"
}

@dp.table(
    table_properties={"quality": "silver"},
    name=f"{table_prefix}_bpm_silver"
)
@dp.expect_all_or_drop(rules)
def bpm_silver():
    return (
        dp.read_stream(f"{table_prefix}_bpm_bronze")
          .select("device_id", F.col("time").cast("timestamp").alias("timestamp"), "heartrate")
          .withWatermark("timestamp", "30 seconds")
          .dropDuplicates(["device_id", "timestamp"])
    )