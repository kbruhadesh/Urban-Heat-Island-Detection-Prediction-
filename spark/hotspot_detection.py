"""
Phase 3b: Hotspot Detection
- 90th percentile LST threshold per city
- Flag dates/periods exceeding critical thresholds
- Identify extreme heat events
- Output: data/output/hotspot_alerts.csv
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, round as spark_round, when, lit,
    percentile_approx, row_number, desc
)
from pyspark.sql.window import Window
import os

# Start Spark
spark = SparkSession.builder \
    .appName("UHI_Hotspot_Detection") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("PHASE 3b: Hotspot Detection")
print("=" * 60)

# ── Load cleaned Parquet data ──────────────────────────────────────────────
df = spark.read.parquet("data/parquet")
print(f"\nLoaded {df.count()} records")

os.makedirs("data/output", exist_ok=True)

# ── 1. Compute Percentile Thresholds per City ─────────────────────────────
print("\n── 1. UHI Percentile Thresholds per City ──")

thresholds = df.groupBy("city").agg(
    spark_round(percentile_approx("UHI_Index", 0.50), 4).alias("p50_uhi"),
    spark_round(percentile_approx("UHI_Index", 0.75), 4).alias("p75_uhi"),
    spark_round(percentile_approx("UHI_Index", 0.90), 4).alias("p90_uhi"),
    spark_round(percentile_approx("UHI_Index", 0.95), 4).alias("p95_uhi"),
    spark_round(percentile_approx("LST_Day_Urban", 0.90), 2).alias("p90_urban_lst"),
    spark_round(percentile_approx("LST_Day_Urban", 0.95), 2).alias("p95_urban_lst"),
).orderBy("city")

thresholds.show(11, truncate=False)
thresholds.toPandas().to_csv("data/output/uhi_thresholds.csv", index=False)
print("✅ Saved: data/output/uhi_thresholds.csv")

# ── 2. Flag Hotspot Events (Above 90th percentile) ────────────────────────
print("\n── 2. Hotspot Events (UHI > 90th percentile per city) ──")

# Join thresholds back to main data
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
    ) \
    .orderBy("city", "date")

hotspot_count = hotspots.count()
print(f"Total hotspot events: {hotspot_count}")
hotspots.show(20, truncate=False)

hotspots_pdf = hotspots.toPandas()
hotspots_pdf.to_csv("data/output/hotspot_alerts.csv", index=False)
print(f"✅ Saved: data/output/hotspot_alerts.csv ({len(hotspots_pdf)} rows)")

# ── 3. Hotspot Summary by City ────────────────────────────────────────────
print("\n── 3. Hotspot Summary by City ──")

hotspot_summary = hotspots.groupBy("city", "severity").agg(
    count("*").alias("event_count"),
    spark_round(avg("uhi_index"), 4).alias("avg_uhi_during_hotspot"),
    spark_round(avg("urban_temp_c"), 2).alias("avg_urban_temp")
).orderBy("city", "severity")

hotspot_summary.show(33, truncate=False)

# ── 4. Monthly Hotspot Frequency ──────────────────────────────────────────
print("\n── 4. Monthly Hotspot Frequency ──")

monthly_hotspots = hotspots.groupBy("city", "month").agg(
    count("*").alias("hotspot_count"),
    spark_round(avg("uhi_index"), 4).alias("avg_uhi")
).orderBy("city", "month")

monthly_hotspots.show(50, truncate=False)
monthly_hotspots.toPandas().to_csv("data/output/monthly_hotspot_freq.csv", index=False)
print("✅ Saved: data/output/monthly_hotspot_freq.csv")

# ── 5. Extreme Urban Heat Days (Urban LST > 45°C) ─────────────────────────
print("\n── 5. Extreme Urban Heat Days (LST > 45°C) ──")

extreme_heat = df.filter(col("LST_Day_Urban") > 45) \
    .select(
        "city", "date", "year", "month",
        spark_round("LST_Day_Urban", 2).alias("urban_temp_c"),
        spark_round("UHI_Index", 4).alias("uhi_index")
    ) \
    .orderBy(desc("urban_temp_c"))

extreme_count = extreme_heat.count()
print(f"Total extreme heat events (>45°C): {extreme_count}")
extreme_heat.show(20, truncate=False)
extreme_heat.toPandas().to_csv("data/output/extreme_heat_events.csv", index=False)
print("✅ Saved: data/output/extreme_heat_events.csv")

# ── 6. Top 10 Worst UHI Events per City ───────────────────────────────────
print("\n── 6. Top 10 Worst UHI Events per City ──")

window = Window.partitionBy("city").orderBy(desc("UHI_Index"))
top_events = df.withColumn("rank", row_number().over(window)) \
    .filter(col("rank") <= 10) \
    .select(
        "city", "date", "year", "month",
        spark_round("LST_Day_Urban", 2).alias("urban_temp_c"),
        spark_round("UHI_Index", 4).alias("uhi_index"),
        "rank"
    ) \
    .orderBy("city", "rank")

top_events.show(50, truncate=False)
top_events.toPandas().to_csv("data/output/top_uhi_events.csv", index=False)
print("✅ Saved: data/output/top_uhi_events.csv")

spark.stop()
print("\n🏁 Phase 3b Hotspot Detection complete!")
