from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, round as spark_round, when, lit,
    percentile_approx, row_number, desc
)
from pyspark.sql.window import Window
import os

# =========================
# 🔥 SPARK CLUSTER CONFIG
# =========================
spark = SparkSession.builder \
    .master("spark://localhost:7077") \
    .appName("UHI_Hotspot_Detection") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("PHASE 3b: Hotspot Detection (Distributed)")
print("=" * 60)

# =========================
# LOAD DATA
# =========================
df = spark.read.parquet("data/parquet")

print(f"\nLoaded records: {df.count()}")
print(f"Partitions: {df.rdd.getNumPartitions()}")

os.makedirs("data/output", exist_ok=True)

# =========================
# 1. THRESHOLDS
# =========================
print("\n── Percentile Thresholds ──")

thresholds = df.groupBy("city").agg(
    spark_round(percentile_approx("UHI_Index", 0.50), 4).alias("p50_uhi"),
    spark_round(percentile_approx("UHI_Index", 0.75), 4).alias("p75_uhi"),
    spark_round(percentile_approx("UHI_Index", 0.90), 4).alias("p90_uhi"),
    spark_round(percentile_approx("UHI_Index", 0.95), 4).alias("p95_uhi"),
    spark_round(percentile_approx("LST_Day_Urban", 0.90), 2).alias("p90_urban_lst"),
    spark_round(percentile_approx("LST_Day_Urban", 0.95), 2).alias("p95_urban_lst"),
)

thresholds.show()

thresholds.toPandas().to_csv("data/output/uhi_thresholds.csv", index=False)

# =========================
# 2. HOTSPOTS
# =========================
df_with_thresh = df.join(
    thresholds.select("city", "p90_uhi", "p90_urban_lst"),
    on="city"
)

hotspots = df_with_thresh.filter(col("UHI_Index") > col("p90_uhi")) \
    .withColumn("severity",
        when(col("UHI_Index") > col("p90_uhi") * 1.5, lit("CRITICAL"))
        .when(col("UHI_Index") > col("p90_uhi") * 1.2, lit("HIGH"))
        .otherwise(lit("MODERATE"))
    ) \
    .select(
        "city", "date", "year", "month",
        spark_round("LST_Day_Urban", 2).alias("urban_temp_c"),
        spark_round("LST_Day_Rural", 2).alias("rural_temp_c"),
        spark_round("UHI_Index", 4).alias("uhi_index"),
        spark_round("p90_uhi", 4).alias("threshold_p90"),
        "severity"
    )

print(f"Hotspot events: {hotspots.count()}")
hotspots.show(10)

hotspots.toPandas().to_csv("data/output/hotspot_alerts.csv", index=False)

# =========================
# 3. SUMMARY
# =========================
hotspot_summary = hotspots.groupBy("city", "severity").agg(
    count("*").alias("event_count"),
    spark_round(avg("uhi_index"), 4).alias("avg_uhi"),
    spark_round(avg("urban_temp_c"), 2).alias("avg_temp")
)

hotspot_summary.show()

# =========================
# 4. MONTHLY FREQUENCY
# =========================
monthly_hotspots = hotspots.groupBy("city", "month").agg(
    count("*").alias("hotspot_count"),
    spark_round(avg("uhi_index"), 4).alias("avg_uhi")
)

monthly_hotspots.show()

monthly_hotspots.toPandas().to_csv("data/output/monthly_hotspot_freq.csv", index=False)

# =========================
# 5. EXTREME HEAT
# =========================
extreme_heat = df.filter(col("LST_Day_Urban") > 45)

print(f"Extreme heat events: {extreme_heat.count()}")
extreme_heat.show(10)

extreme_heat.toPandas().to_csv("data/output/extreme_heat_events.csv", index=False)

# =========================
# 6. TOP EVENTS
# =========================
window = Window.partitionBy("city").orderBy(desc("UHI_Index"))

top_events = df.withColumn("rank", row_number().over(window)) \
    .filter(col("rank") <= 10)

top_events.show()

top_events.toPandas().to_csv("data/output/top_uhi_events.csv", index=False)

# =========================
# FINAL
# =========================
spark.stop()

print("\n🏁 Phase 3b COMPLETE (Distributed)")