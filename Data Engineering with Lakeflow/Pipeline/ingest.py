from pyspark import pipelines as dp
import pyspark.sql.functions as F

# Step 1: Auto Load Raw JSON Data into Multiplex Bronze
@dp.table()
def sdp_bronze():
    return (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema("key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG")
        .load(f"/Volumes/rtlh_lakehouse_labs/bootcamp_oct_2025/resources/data/gym_stream/")
    )