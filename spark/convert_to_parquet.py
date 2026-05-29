from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year as yr, month as mn, to_date, dayofyear
from pyspark.sql.types import DoubleType
import os

# =========================
# 🔥 SPARK CLUSTER CONFIG
# =========================
spark = SparkSession.builder \
    .master("spark://localhost:7077") \
    .appName("UHI_CSV_to_Parquet") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("PHASE 1: CSV → Parquet Conversion (Distributed)")
print("=" * 60)

# =========================
# LOAD CSV (OPTIMIZED)
# =========================
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("data/modis/*.csv")

print(f"\nRaw records loaded: {df.count()}")
print(f"Initial partitions: {df.rdd.getNumPartitions()}")

# =========================
# CLEAN & TRANSFORM
# =========================

df = df.drop("system:index", ".geo")

df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
df = df.withColumn("year", yr(col("date")).cast("int"))
df = df.withColumn("month", mn(col("date")).cast("int"))
df = df.withColumn("day_of_year", dayofyear(col("date")).cast("int"))

for c in ["LST_Day_Urban", "LST_Day_Rural", "LST_Night_Urban", "UHI_Index"]:
    df = df.withColumn(c, col(c).cast(DoubleType()))

df = df.filter(
    col("LST_Day_Urban").isNotNull() &
    col("LST_Day_Rural").isNotNull() &
    col("UHI_Index").isNotNull()
)

df = df.select(
    "city", "date", "year", "month", "day_of_year",
    "LST_Day_Urban", "LST_Day_Rural", "LST_Night_Urban", "UHI_Index"
)

print(f"\nCleaned records: {df.count()}")
print(f"Partitions after cleaning: {df.rdd.getNumPartitions()}")

# =========================
# DISTRIBUTED SUMMARY
# =========================
print("\n── Records per City ──")
df.groupBy("city").count().orderBy("city").show()

# =========================
# SAVE PARQUET (DISTRIBUTED)
# =========================
output_path = "data/parquet"

df.write \
    .partitionBy("city", "year") \
    .mode("overwrite") \
    .parquet(output_path)

print(f"\n✅ Parquet saved → {output_path}")

# =========================
# FINAL EXPORT
# =========================
os.makedirs("data/output", exist_ok=True)

df.limit(500000).toPandas().to_csv(
    "data/output/uhi_all_clean.csv", index=False
)

print("✅ Sample CSV saved (limited rows to avoid memory issues)")

print("\n🏁 Phase 1 complete (Distributed Pipeline)")

input("Press Enter to stop Spark...")

spark.stop()