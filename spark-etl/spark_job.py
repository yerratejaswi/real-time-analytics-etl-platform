from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

spark = SparkSession.builder \
    .appName("RealTimeETL") \
    .getOrCreate()

schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("watch_time", IntegerType()),
    StructField("timestamp", LongType())
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_events") \
    .load()

parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

query = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "warehouse/curated") \
    .option("checkpointLocation", "warehouse/checkpoint") \
    .partitionBy("event_type") \
    .start()

query.awaitTermination()
