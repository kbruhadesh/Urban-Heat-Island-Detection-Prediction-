"""
Phase 3c: Temporal Analysis
- Year-over-year UHI change
- Rate of change per decade (slope per year) → uhi_rate_of_change.csv
- Seasonal UHI evolution by decade
- Peak UHI month per city
- 5-year moving average
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, round as spark_round, when, lit, lag, count
)
from pyspark.sql.window import Window
import os
import pandas as pd
import numpy as np

spark = SparkSession.builder \
    .master("spark://localhost:7077") \
    .appName("UHI_Temporal_Analysis") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("PHASE 3c: Temporal Analysis")
print("=" * 60)

df = spark.read.parquet("data/parquet")
print(f"\nLoaded {df.count()} records")

os.makedirs("data/output", exist_ok=True)

# ── 1. Yearly aggregation ──────────────────────────────────────────────────
yearly = df.groupBy("city", "year").agg(
    spark_round(avg("UHI_Index"), 4).alias("avg_uhi"),
    spark_round(avg("LST_Day_Urban"), 2).alias("avg_urban_lst"),
    count("*").alias("obs")
)

# ── 2. Year-over-year change ───────────────────────────────────────────────
print("\n── 1. Year-over-Year UHI Change ──")

window_city = Window.partitionBy("city").orderBy("year")

yoy = yearly \
    .withColumn("prev_year_uhi", lag("avg_uhi", 1).over(window_city)) \
    .withColumn("yoy_change",
        spark_round(col("avg_uhi") - col("prev_year_uhi"), 4)
    ) \
    .withColumn("trend",
        when(col("yoy_change") > 0.1, lit("↑ Increasing"))
        .when(col("yoy_change") < -0.1, lit("↓ Decreasing"))
        .otherwise(lit("→ Stable"))
    ) \
    .orderBy("city", "year")

yoy.show(30, truncate=False)
yoy.toPandas().to_csv("data/output/uhi_yoy_trends.csv", index=False)
print(f"✅ Saved: data/output/uhi_yoy_trends.csv ({yoy.count()} rows)")

# ── 3. Rate of change (slope per year) using pandas linear regression ──────
print("\n── 2. Rate of Change (°C per decade) ──")

yearly_pdf = yearly.toPandas()
results = []

for city, group in yearly_pdf.groupby("city"):
    group = group.sort_values("year").dropna(subset=["avg_uhi"])
    if len(group) < 5:
        continue

    x = group["year"].values
    y = group["avg_uhi"].values

    # Linear regression: y = slope * x + intercept
    slope, intercept = np.polyfit(x, y, 1)
    y_pred = slope * x + intercept
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0

    results.append({
        "city":               city,
        "slope_per_year":     round(float(slope), 6),
        "rate_per_decade_c":  round(float(slope) * 10, 4),
        "r_squared":          round(float(r_squared), 4),
        "first_year_uhi":     round(float(y[0]), 4),
        "last_year_uhi":      round(float(y[-1]), 4),
        "overall_change":     round(float(y[-1]) - float(y[0]), 4),
        "trend":              "Warming" if slope > 0 else "Cooling"
    })

rate_df = pd.DataFrame(results).sort_values("slope_per_year", ascending=False)
print(rate_df.to_string(index=False))
rate_df.to_csv("data/output/uhi_rate_of_change.csv", index=False)
print("✅ Saved: data/output/uhi_rate_of_change.csv")

# ── 4. Seasonal UHI evolution by decade ───────────────────────────────────
print("\n── 3. Seasonal UHI Evolution by Decade ──")

seasonal_decade = df \
    .withColumn("season",
        when(col("month").isin(3,4,5), lit("Pre-Monsoon"))
        .when(col("month").isin(6,7,8,9), lit("Monsoon"))
        .when(col("month").isin(10,11), lit("Post-Monsoon"))
        .otherwise(lit("Winter"))
    ) \
    .withColumn("decade",
        when(col("year").between(2000,2009), lit("2000-2009"))
        .when(col("year").between(2010,2019), lit("2010-2019"))
        .otherwise(lit("2020-2024"))
    ) \
    .groupBy("city", "season", "decade") \
    .agg(
        spark_round(avg("UHI_Index"), 4).alias("avg_uhi"),
        spark_round(avg("LST_Day_Urban"), 2).alias("avg_urban_lst"),
        count("*").alias("obs")
    ) \
    .orderBy("city", "season", "decade")

seasonal_decade.show(50, truncate=False)
seasonal_decade.toPandas().to_csv("data/output/seasonal_evolution.csv", index=False)
print("✅ Saved: data/output/seasonal_evolution.csv")

# ── 5. Peak UHI month per city ─────────────────────────────────────────────
print("\n── 4. Peak UHI Month per City ──")

peak_months = df.groupBy("city", "month").agg(
    spark_round(avg("UHI_Index"), 4).alias("avg_uhi")
)

window_peak = Window.partitionBy("city").orderBy(col("avg_uhi").desc())
from pyspark.sql.functions import row_number
peak_months = peak_months \
    .withColumn("rn", row_number().over(window_peak)) \
    .filter(col("rn") == 1) \
    .drop("rn") \
    .withColumnRenamed("month", "peak_month") \
    .withColumnRenamed("avg_uhi", "peak_uhi") \
    .orderBy(col("peak_uhi").desc())

peak_months.show()
peak_months.toPandas().to_csv("data/output/peak_months.csv", index=False)
print("✅ Saved: data/output/peak_months.csv")

# ── 6. 5-year moving average ───────────────────────────────────────────────
print("\n── 5. 5-Year Moving Average UHI ──")

window_5yr = Window.partitionBy("city").orderBy("year").rowsBetween(-4, 0)

moving_avg = yearly \
    .withColumn("moving_avg_5yr",
        spark_round(avg("avg_uhi").over(window_5yr), 4)
    ) \
    .select("city", "year", "avg_uhi", "moving_avg_5yr") \
    .orderBy("city", "year")

moving_avg.show(30, truncate=False)
moving_avg.toPandas().to_csv("data/output/uhi_moving_avg.csv", index=False)
print("✅ Saved: data/output/uhi_moving_avg.csv")

spark.stop()
print("\n🏁 Phase 3c Temporal Analysis complete!")