"""
Kafka Consumer: PySpark Structured Streaming consumer for UHI data.
Reads from 'uhi-data' Kafka topic and writes cleaned data to Parquet.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_date, year as yr, month as mn, dayofyear
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, FloatType
)

KAFKA_BROKER = "localhost:9092"
TOPIC = "uhi-data"
OUTPUT_PATH = "data/kafka_output"
CHECKPOINT_PATH = "data/kafka_checkpoints"

# ── Schema for incoming JSON messages ──────────────────────────────────────
schema = StructType([
    StructField("city", StringType(), True),
    StructField("date", StringType(), True),
    StructField("LST_Day_Urban", DoubleType(), True),
    StructField("LST_Day_Rural", DoubleType(), True),
    StructField("LST_Night_Urban", DoubleType(), True),
    StructField("UHI_Index", DoubleType(), True),
    StructField("timestamp", DoubleType(), True),
])


def main():
    print("=" * 60)
    print("KAFKA CONSUMER: PySpark Structured Streaming")
    print("=" * 60)

    # ── Start Spark with Kafka package ─────────────────────────────────────
    spark = SparkSession.builder \
        .appName("UHI_Kafka_Consumer") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ── Read from Kafka ────────────────────────────────────────────────────
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    print(f"✅ Connected to Kafka topic: {TOPIC}")

    # ── Parse JSON payload ─────────────────────────────────────────────────
    parsed_df = kafka_df \
        .selectExpr("CAST(key AS STRING) as city_key",
                     "CAST(value AS STRING) as json_value") \
        .select(
            col("city_key"),
            from_json(col("json_value"), schema).alias("data")
        ) \
        .select("data.*")

    # ── Transform: add year, month ─────────────────────────────────────────
    transformed_df = parsed_df \
        .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
        .withColumn("year", yr(col("date")).cast("int")) \
        .withColumn("month", mn(col("date")).cast("int")) \
        .withColumn("day_of_year", dayofyear(col("date")).cast("int")) \
        .drop("timestamp") \
        .filter(col("LST_Day_Urban").isNotNull())

    # ── Write to Parquet (partitioned by city) ─────────────────────────────
    query = transformed_df.writeStream \
        .format("parquet") \
        .option("path", OUTPUT_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .partitionBy("city") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    print(f"📥 Writing output to: {OUTPUT_PATH}")
    print(f"📋 Checkpoint at: {CHECKPOINT_PATH}")
    print("\nStreaming active... Press Ctrl+C to stop.\n")

    query.awaitTermination()


if __name__ == "__main__":
    main()
