from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType

# Schema des messages
schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("humidity", IntegerType(), True)
])

# Spark session
spark = SparkSession.builder \
    .appName("KafkaToPostgres") \
    .getOrCreate()

# Lire le topic Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "latest") \
    .load()

# Convertir la valeur JSON en colonnes
df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Fonction d'écriture dans PostgreSQL
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://db:5432/TR") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "sensor_data") \
        .option("user", "postgres") \
        .option("password", "hana") \
        .mode("append") \
        .save()

# Démarrer le streaming
query = df_parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
