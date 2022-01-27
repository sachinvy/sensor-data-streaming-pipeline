from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("SensorStreaming") \
    .getOrCreate()
spark.conf.set("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
spark.conf.set("spark.cassandra.auth.username", "cassandra")
spark.conf.set("spark.cassandra.auth.password", "cassandra")
input_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "iot") \
    .option("startingOffsets", "latest") \
    .load()

location_schema = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("location", StructType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("vibration", IntegerType(), True),
    StructField("noise", IntegerType(), True),
    StructField("image", StringType(), True)
])

input_as_string = input_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

json_df = input_as_string.select(from_json(col("value"), schema=location_schema).alias("data"))

sensor_stat_df = json_df.select(
                        col("data.id").alias("sensor_id"),
                        col("data.timestamp").alias("creation_time"),
                        col("data.temperature").alias("temperature"),
                        col("data.vibration").alias("vibration"),
                        col("data.noise").alias("noise")
                        )

agg_sensor_data = sensor_stat_df\
    .withWatermark("creation_time", "0 minutes")\
    .groupby(
    window(
        "creation_time",windowDuration="15 minutes", startTime="0 minutes"
    ),
    "sensor_id"
).agg(count("temperature").alias("record_count"),
    max("temperature").alias("max_temperature"),
      min("temperature").alias("min_temperature"),
      mean("temperature").alias("mean_temperature"),
      max("noise").alias("max_noise"),
      min("noise").alias("min_noise"),
      mean("noise").alias("mean_noise"),
      max("vibration").alias("max_vibration"),
      min("vibration").alias("min_vibration"),
      mean("vibration").alias("mean_vibration"))\
    .select(col("sensor_id"),
            col("window.start").alias("start_time"),
            col("window.end").alias("end_time"),
            col("record_count"),
            col("min_temperature"),
            col("max_temperature"),
            col("mean_temperature"),
            col("min_noise"),
            col("max_noise"),
            col("mean_noise"),
            col("min_vibration"),
            col("max_vibration"),
            col("mean_vibration"),
            )\


sensor_stats_write = agg_sensor_data.writeStream\
        .option("checkpointLocation",
                "C:\\Users\\sachin\\Documents\\sachin\\Mtech-davv\\Sem_1_project\\stream-sensor-data\\data\\sensor_stats") \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "streaming_data") \
            .option("table", "sensor_stats_history") \
            .outputMode("append") \
            .start()


# sensor_stats_write_console = agg_sensor_data.writeStream \
#                                 .format("console").outputMode("update").start()
sensor_data_write = sensor_stat_df.writeStream\
.option("checkpointLocation",
        "C:\\Users\\sachin\\Documents\\sachin\\Mtech-davv\\Sem_1_project\\stream-sensor-data\\data\\sensor_data") \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "streaming_data") \
    .option("table", "sensor_data_history") \
    .outputMode("append") \
    .start()

sensor_image_write = json_df.select(col("data.id").alias("sensor_id"),
               col("data.timestamp").alias("creation_time"),
               col("data.image").alias("image_matrix")
               )\
    .writeStream \
    .option("checkpointLocation",
            "C:\\Users\\sachin\\Documents\\sachin\\Mtech-davv\\Sem_1_project\\stream-sensor-data\\data\\image_history\\") \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "streaming_data") \
    .option("table", "sensor_image_history") \
    .outputMode("append") \
    .start()

sensor_image_write.awaitTermination()
sensor_stats_write.awaitTermination()
sensor_data_write.awaitTermination()
# sensor_stats_write_console.awaitTermination()