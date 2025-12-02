from pyspark import pipelines as dp
import pyspark.sql.functions as F

# Under pipeline settings -> configuration, add a unique table prefix (eg <firstname>_<lastname>)
table_prefix = spark.conf.get("table_prefix").strip()

assert table_prefix is not None and table_prefix != ""

@dp.table(
    table_properties={"quality": "bronze"},
    name=f"{table_prefix}_bronze"
)
def bronze():
    return (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema("key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG")
        .load("/Volumes/rtlh_lakehouse_labs/bootcamp_oct_2025/resources/data/gym/")
    )