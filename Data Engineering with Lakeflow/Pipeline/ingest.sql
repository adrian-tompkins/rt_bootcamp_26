-- Step 1: Auto Load Raw JSON Data into Multiplex Bronze

-- Note we are using SQL here, but you can also write python too!
-- Spark Declartive Pipelines supports both languages
-- In this Lab, we are using SQL to ingest from json then python to transform
-- But you could choose whichever you like


CREATE OR REFRESH STREAMING TABLE sdp_bronze AS
SELECT
  key,
  value,
  topic,
  partition,
  offset,
  timestamp
FROM STREAM read_files(
  '/Volumes/rtlh_lakehouse_labs/bootcamp_oct_2025/resources/data/gym_stream/',
  format => 'json',
  schema => 'key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG'
);