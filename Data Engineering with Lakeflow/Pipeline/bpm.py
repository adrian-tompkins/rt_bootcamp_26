from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.table()
def sdp_bpm_bronze():
    return (
        dp.read_stream(f"{table_prefix}_bronze")
          .filter("topic = 'bpm'")
          .select(F.from_json(F.col("value").cast("string"), "device_id INT, time FLOAT, heartrate DOUBLE").alias("v"))
          .select("v.*")
        )
    
rules = {
  "valid_id": "device_id IS NOT NULL and device_id >= 102000"
}

@dp.table()
@dp.expect_all_or_drop(rules)
def sdp_bpm_silver():
    return (
        dp.read_stream(f"{table_prefix}_bpm_bronze")
          .select("device_id", F.col("time").cast("timestamp").alias("timestamp"), "heartrate")
          .withWatermark("timestamp", "30 seconds")
          .dropDuplicates(["device_id", "timestamp"])
    )