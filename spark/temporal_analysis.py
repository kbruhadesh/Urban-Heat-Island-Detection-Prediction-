"""
Phase 3c: Temporal Analysis
- Year-over-year UHI trend analysis
- Linear regression for rate of change (°C per decade)
- Seasonal pattern evolution
- Output: data/output/temporal_trends.csv
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, round as spark_round, when, lit,
    lag, first, last
)
from pyspark.sql.window import Window
import os
import pandas as pd
import numpy as np

# Start Spark
spark = SparkSession.builder \
    .appName("UHI_Temporal_Analysis") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("PHASE 3c: Temporal Analysis")
print("=" * 60)

# ── Load cleaned Parquet data ──────────────────────────────────────────────
df = spark.read.parquet("data/parquet")
print(f"\nLoaded {df.count()} records")

os.makedirs("data/output", exist_ok=True)

# ── 1. Year-over-Year Change ──────────────────────────────────────────────
print("\n── 1. Year-over-Year UHI Change ──")

yearly = df.groupBy("city", "year").agg(
    spark_round(avg("UHI_Index"), 4).alias("avg_uhi"),
    spark_round(avg("LST_Day_Urban"), 2).alias("avg_urban_lst"),
    count("*").alias("obs")
).orderBy("city", "year")

# Calculate YoY change
window = Window.partitionBy("city").orderBy("year")
yoy = yearly.withColumn(
    "prev_year_uhi", lag("avg_uhi", 1).over(window)
).withColumn(
    "yoy_change", spark_round(col("avg_uhi") - col("prev_year_uhi"), 4)
).withColumn(
    "trend",
    when(col("yoy_change") > 0.1, lit("↑ Increasing"))
    .when(col("yoy_change") < -0.1, lit("↓ Decreasing"))
    .otherwise(lit("→ Stable"))
)

yoy.show(30, truncate=False)
yoy_pdf = yoy.toPandas()
yoy_pdf.to_csv("data/output/uhi_yoy_trends.csv", index=False)
print(f"✅ Saved: data/output/uhi_yoy_trends.csv ({len(yoy_pdf)} rows)")

# ── 2. Linear Regression: Rate of Change per Decade ───────────────────────
print("\n── 2. Rate of Change (°C per decade) ──")

# Use pandas + numpy for linear regression per city
yearly_pdf = yearly.toPandas()
trend_results = []

for city in yearly_pdf["city"].unique():
    city_data = yearly_pdf[yearly_pdf["city"] == city].sort_values("year")
    x = city_data["year"].values.astype(float)
    y = city_data["avg_uhi"].values.astype(float)

    # Remove NaN values
    mask = ~np.isnan(y)
    x, y = x[mask], y[mask]

    if len(x) >= 5:
        # Linear regression: y = mx + b
        coeffs = np.polyfit(x, y, 1)
        slope = coeffs[0]
        intercept = coeffs[1]

        # Rate per decade
        rate_per_decade = slope * 10

        # R² calculation
        y_pred = np.polyval(coeffs, x)
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0

        trend_results.append({
            "city": city,
            "slope_per_year": round(slope, 6),
            "rate_per_decade_c": round(rate_per_decade, 4),
            "r_squared": round(r_squared, 4),
            "first_year_uhi": round(y[0], 4),
            "last_year_uhi": round(y[-1], 4),
            "overall_change": round(y[-1] - y[0], 4),
            "trend": "Warming" if rate_per_decade > 0.01 else ("Cooling" if rate_per_decade < -0.01 else "Stable")
        })

trend_df = pd.DataFrame(trend_results).sort_values("rate_per_decade_c", ascending=False)
print(trend_df.to_string(index=False))
trend_df.to_csv("data/output/uhi_rate_of_change.csv", index=False)
print(f"\n✅ Saved: data/output/uhi_rate_of_change.csv")

# ── 3. Seasonal Evolution (by decade) ─────────────────────────────────────
print("\n── 3. Seasonal UHI Evolution by Decade ──")

seasonal_df = df.withColumn("season",
    when(col("month").isin(3, 4, 5), lit("Pre-Monsoon"))
    .when(col("month").isin(6, 7, 8, 9), lit("Monsoon"))
    .when(col("month").isin(10, 11), lit("Post-Monsoon"))
    .otherwise(lit("Winter"))
).withColumn("decade",
    when(col("year").between(2000, 2009), lit("2000-2009"))
    .when(col("year").between(2010, 2019), lit("2010-2019"))
    .otherwise(lit("2020-2024"))
)

seasonal_evolution = seasonal_df.groupBy("city", "season", "decade").agg(
    spark_round(avg("UHI_Index"), 4).alias("avg_uhi"),
    spark_round(avg("LST_Day_Urban"), 2).alias("avg_urban_lst"),
    count("*").alias("obs")
).orderBy("city", "season", "decade")

seasonal_evolution.show(50, truncate=False)
seasonal_evo_pdf = seasonal_evolution.toPandas()
seasonal_evo_pdf.to_csv("data/output/seasonal_evolution.csv", index=False)
print(f"✅ Saved: data/output/seasonal_evolution.csv ({len(seasonal_evo_pdf)} rows)")

# ── 4. Peak Month Analysis ────────────────────────────────────────────────
print("\n── 4. Peak UHI Month per City ──")

df.createOrReplaceTempView("uhi_data")

peak_month_sql = spark.sql("""
    WITH monthly AS (
        SELECT city, month, ROUND(AVG(UHI_Index), 4) as avg_uhi,
               ROW_NUMBER() OVER (PARTITION BY city ORDER BY AVG(UHI_Index) DESC) as rn
        FROM uhi_data
        GROUP BY city, month
    )
    SELECT city, month as peak_month, avg_uhi as peak_uhi
    FROM monthly WHERE rn = 1
    ORDER BY peak_uhi DESC
""")

peak_month_sql.show(11, truncate=False)
peak_month_sql.toPandas().to_csv("data/output/peak_months.csv", index=False)
print("✅ Saved: data/output/peak_months.csv")

# ── 5. 5-Year Moving Average ─────────────────────────────────────────────
print("\n── 5. 5-Year Moving Average UHI ──")

yearly_spark = spark.createDataFrame(yearly_pdf)
yearly_spark.createOrReplaceTempView("yearly_uhi")

moving_avg = spark.sql("""
    SELECT
        city, year, avg_uhi,
        ROUND(AVG(avg_uhi) OVER (
            PARTITION BY city ORDER BY year
            ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
        ), 4) as moving_avg_5yr
    FROM yearly_uhi
    ORDER BY city, year
""")

moving_avg.show(30, truncate=False)
moving_avg.toPandas().to_csv("data/output/uhi_moving_avg.csv", index=False)
print("✅ Saved: data/output/uhi_moving_avg.csv")

spark.stop()
print("\n🏁 Phase 3c Temporal Analysis complete!")
