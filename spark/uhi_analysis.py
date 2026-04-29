"""
Phase 3a: Core UHI Analysis using Spark SQL
- Monthly UHI aggregation per city
- Seasonal analysis (pre-monsoon, monsoon, post-monsoon, winter)
- Yearly trends per city
- Decade comparisons (2000-2009, 2010-2019, 2020-2024)
- Outputs: CSV files in data/output/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min as spark_min, max as spark_max, count,
    round as spark_round, when, lit, stddev
)
import os

# Start Spark
spark = SparkSession.builder \
    .appName("UHI_Analysis") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("PHASE 3a: UHI Spark SQL Analysis")
print("=" * 60)

# ── Load cleaned Parquet data ──────────────────────────────────────────────
df = spark.read.parquet("data/parquet")
print(f"\nLoaded {df.count()} records from Parquet")
df.printSchema()

# Create temp view for SQL queries
df.createOrReplaceTempView("uhi_data")

# Create output directory
os.makedirs("data/output", exist_ok=True)

# ── 1. Monthly UHI Aggregation ────────────────────────────────────────────
print("\n── 1. Monthly UHI Aggregation ──")

monthly_uhi = spark.sql("""
    SELECT
        city,
        year,
        month,
        ROUND(AVG(UHI_Index), 4) as avg_uhi,
        ROUND(AVG(LST_Day_Urban), 2) as avg_urban_lst,
        ROUND(AVG(LST_Day_Rural), 2) as avg_rural_lst,
        ROUND(AVG(LST_Night_Urban), 2) as avg_night_lst,
        ROUND(MIN(UHI_Index), 4) as min_uhi,
        ROUND(MAX(UHI_Index), 4) as max_uhi,
        COUNT(*) as observations
    FROM uhi_data
    GROUP BY city, year, month
    ORDER BY city, year, month
""")

monthly_uhi.show(20, truncate=False)
monthly_pdf = monthly_uhi.toPandas()
monthly_pdf.to_csv("data/output/uhi_monthly.csv", index=False)
print(f"✅ Saved: data/output/uhi_monthly.csv ({len(monthly_pdf)} rows)")

# ── 2. Yearly UHI Trends ──────────────────────────────────────────────────
print("\n── 2. Yearly UHI Trends ──")

yearly_uhi = spark.sql("""
    SELECT
        city,
        year,
        ROUND(AVG(UHI_Index), 4) as avg_uhi,
        ROUND(AVG(LST_Day_Urban), 2) as avg_urban_lst,
        ROUND(AVG(LST_Day_Rural), 2) as avg_rural_lst,
        ROUND(AVG(LST_Night_Urban), 2) as avg_night_lst,
        ROUND(STDDEV(UHI_Index), 4) as std_uhi,
        ROUND(MIN(UHI_Index), 4) as min_uhi,
        ROUND(MAX(UHI_Index), 4) as max_uhi,
        COUNT(*) as observations
    FROM uhi_data
    GROUP BY city, year
    ORDER BY city, year
""")

yearly_uhi.show(20, truncate=False)
yearly_pdf = yearly_uhi.toPandas()
yearly_pdf.to_csv("data/output/uhi_yearly.csv", index=False)
print(f"✅ Saved: data/output/uhi_yearly.csv ({len(yearly_pdf)} rows)")

# ── 3. Seasonal Analysis ──────────────────────────────────────────────────
print("\n── 3. Seasonal Analysis ──")

# Define Indian seasons based on month
seasonal_df = df.withColumn("season",
    when(col("month").isin(3, 4, 5), lit("Pre-Monsoon"))
    .when(col("month").isin(6, 7, 8, 9), lit("Monsoon"))
    .when(col("month").isin(10, 11), lit("Post-Monsoon"))
    .otherwise(lit("Winter"))
)

seasonal_df.createOrReplaceTempView("seasonal_data")

seasonal_uhi = spark.sql("""
    SELECT
        city,
        season,
        ROUND(AVG(UHI_Index), 4) as avg_uhi,
        ROUND(AVG(LST_Day_Urban), 2) as avg_urban_lst,
        ROUND(AVG(LST_Day_Rural), 2) as avg_rural_lst,
        ROUND(STDDEV(UHI_Index), 4) as std_uhi,
        COUNT(*) as observations
    FROM seasonal_data
    GROUP BY city, season
    ORDER BY city, season
""")

seasonal_uhi.show(50, truncate=False)
seasonal_pdf = seasonal_uhi.toPandas()
seasonal_pdf.to_csv("data/output/uhi_seasonal.csv", index=False)
print(f"✅ Saved: data/output/uhi_seasonal.csv ({len(seasonal_pdf)} rows)")

# ── 4. Decade Comparison ──────────────────────────────────────────────────
print("\n── 4. Decade Comparison ──")

decade_df = df.withColumn("decade",
    when(col("year").between(2000, 2009), lit("2000-2009"))
    .when(col("year").between(2010, 2019), lit("2010-2019"))
    .otherwise(lit("2020-2024"))
)

decade_df.createOrReplaceTempView("decade_data")

decade_uhi = spark.sql("""
    SELECT
        city,
        decade,
        ROUND(AVG(UHI_Index), 4) as avg_uhi,
        ROUND(AVG(LST_Day_Urban), 2) as avg_urban_lst,
        ROUND(AVG(LST_Day_Rural), 2) as avg_rural_lst,
        ROUND(AVG(LST_Night_Urban), 2) as avg_night_lst,
        ROUND(STDDEV(UHI_Index), 4) as std_uhi,
        COUNT(*) as observations
    FROM decade_data
    GROUP BY city, decade
    ORDER BY city, decade
""")

decade_uhi.show(33, truncate=False)
decade_pdf = decade_uhi.toPandas()
decade_pdf.to_csv("data/output/uhi_decade.csv", index=False)
print(f"✅ Saved: data/output/uhi_decade.csv ({len(decade_pdf)} rows)")

# ── 5. City Rankings ──────────────────────────────────────────────────────
print("\n── 5. Overall City UHI Rankings ──")

city_rankings = spark.sql("""
    SELECT
        city,
        ROUND(AVG(UHI_Index), 4) as avg_uhi,
        ROUND(MAX(UHI_Index), 4) as peak_uhi,
        ROUND(AVG(LST_Day_Urban), 2) as avg_urban_temp,
        ROUND(AVG(LST_Night_Urban), 2) as avg_night_temp,
        COUNT(*) as total_observations
    FROM uhi_data
    GROUP BY city
    ORDER BY avg_uhi DESC
""")

city_rankings.show(11, truncate=False)
rankings_pdf = city_rankings.toPandas()
rankings_pdf.to_csv("data/output/city_rankings.csv", index=False)
print(f"✅ Saved: data/output/city_rankings.csv ({len(rankings_pdf)} rows)")

spark.stop()
print("\n🏁 Phase 3a Analysis complete!")
