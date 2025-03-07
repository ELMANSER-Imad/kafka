from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialisation de Spark
spark = SparkSession.builder \
    .appName("RandomUserStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Schéma des données JSON
schema = StructType([
    StructField("gender", StringType()),
    StructField("name", StructType([
        StructField("first", StringType()),
        StructField("last", StringType())
    ])),
    StructField("location", StructType([
        StructField("country", StringType())
    ])),
    StructField("dob", StructType([
        StructField("age", IntegerType())
    ])),
    StructField("login", StructType([
        StructField("uuid", StringType())
    ]))
])

# Lecture des données Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "random_user_data") \
    .option("startingOffsets", "latest") \
    .load()

# Transformation des données
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.gender"),
        col("data.name.first").alias("first_name"),
        col("data.name.last").alias("last_name"),
        col("data.location.country").alias("country"),
        col("data.dob.age").alias("age"),
        col("data.login.uuid").alias("user_id"),
        current_timestamp().alias("ingestion_time")
    )

# Enrichissement des données
df = df.withColumn("full_name", col("first_name") + " " + col("last_name"))

# Agrégations (exemple : nombre d'utilisateurs par pays et âge moyen)
agg_df = df.groupBy("country").agg(
    count("*").alias("count_users"),
    avg("age").alias("avg_age")
)

# Écriture dans MongoDB (données brutes)
df.writeStream \
    .format("mongo") \
    .option("uri", "mongodb://localhost:27017") \
    .option("database", "randomUserDB") \
    .option("collection", "randomUserCollection") \
    .outputMode("append") \
    .start()

# Écriture dans Cassandra (agrégations)
agg_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "random_user_keyspace") \
    .option("table", "country_stats") \
    .outputMode("update") \
    .start()

# Démarrage du streaming
spark.streams.awaitAnyTermination()