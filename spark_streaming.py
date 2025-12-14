from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, avg, count, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# --- Schema des messages ---
schema = StructType([
    StructField("timestamp", StringType(), True), #Parce que Kafka transporte du JSON texte.La conversion en timestamp réel vient après. 
    StructField("sensor_id", IntegerType(), True),
    StructField("device_type", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("co2_level", DoubleType(), True),
    StructField("battery_level", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("location_x", DoubleType(), True),
    StructField("location_y", DoubleType(), True)
])

# --- Spark session ---
spark = SparkSession.builder \
    .appName("KafkaToPostgresPipeline") \
    .getOrCreate()

# --- Lecture du topic Kafka ---
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .select("data.*") \
    .withColumn(
        "timestamp",
        to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS") #On transforme une chaine de charcateres en timestamp spark
    ) \
    .withColumn(
        "status",
        when(col("status").isin("normal", "warning", "malfunction"), col("status")).otherwise("normal")#On s'assure que le status est valide avant le stockage
    )


# --- Query 1: Raw events → sensor_data ---
def write_raw(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://db:5432/TR") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "sensor_data") \
        .option("user", "postgres") \
        .option("password", "hana") \
        .mode("append") \
        .save()

raw_query = raw_df.writeStream \
    .foreachBatch(write_raw) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/sensor_raw") \
    .start() #Spark sauvegarde :les offsets Kafka, l’état du stream avec le checkpointing pour garantir une tolérance aux pannes.

# --- Query 2: Aggregates → sensor_data_agg ---

#on accepte les données en retard jusqu’à 2 minutes. Après, on les ignore.
#On regroupe par capteur , par minute réelle
agg_df = raw_df \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(window("timestamp", "1 minute"), "sensor_id") \
    .agg(
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_humidity"),
        avg("co2_level").alias("avg_co2"),
        avg("battery_level").alias("avg_battery"),
        count(when(col("status") == "warning", True)).alias("warning_count"),
        count(when(col("status") == "malfunction", True)).alias("malfunction_count")
    )


def write_agg(batch_df, batch_id):
    batch_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "sensor_id", "avg_temp", "avg_humidity", "avg_co2", "avg_battery",
        "warning_count", "malfunction_count"
    ).write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://db:5432/TR") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "sensor_data_agg") \
        .option("user", "postgres") \
        .option("password", "hana") \
        .mode("append") \
        .save()

agg_query = agg_df.writeStream \
    .foreachBatch(write_agg) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/sensor_agg") \
    .start()

# --- Await termination for both streams ---
spark.streams.awaitAnyTermination()
