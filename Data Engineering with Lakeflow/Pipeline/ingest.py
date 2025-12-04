from pyspark import pipelines as dp
import pyspark.sql.functions as F


user = spark.conf.get("user")
assert user != "" and user is not None, "Set user in the pipeline config"

# Step 1: Auto Load Raw JSON Data into Multiplex Bronze
@dp.table()
def sdp_bronze():
    return (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema("key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG")
        .load(f"/Volumes/rtlh_lakehouse_labs/labs_{user}/lab2/resources/data/gym")
    )