"""
Phase 1: Convert MODIS CSV data to Parquet format
- Handles new schema: parse 'date' column → extract year, month
- Drops unnecessary columns (.geo, system:index)
- Filters null/empty LST values
- Outputs partitioned Parquet: data/parquet/city=*/year=*/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year as yr, month as mn, to_date, dayofyear
from pyspark.sql.types import DoubleType

# Start Spark
spark = SparkSession.builder \
    .appName("UHI_CSV_to_Parquet") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Load all CSVs ──────────────────────────────────────────────────────────
df = spark.read.csv("data/modis/*.csv", header=True, inferSchema=True)

print("=" * 60)
print("PHASE 1: CSV → Parquet Conversion")
print("=" * 60)
print(f"\nRaw records loaded: {df.count()}")
print("\nSchema:")
df.printSchema()
print("Sample raw data:")
df.show(5, truncate=False)

# ── Clean & Transform ──────────────────────────────────────────────────────

# Drop unnecessary columns
df = df.drop("system:index", ".geo")

# Parse date column → extract year and month
df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
df = df.withColumn("year", yr(col("date")).cast("int"))
df = df.withColumn("month", mn(col("date")).cast("int"))
df = df.withColumn("day_of_year", dayofyear(col("date")).cast("int"))

# Cast LST columns to double (handles empty strings → null)
for c in ["LST_Day_Urban", "LST_Day_Rural", "LST_Night_Urban", "UHI_Index"]:
    df = df.withColumn(c, col(c).cast(DoubleType()))

# Filter out rows with null temperature values
df = df.filter(
    col("LST_Day_Urban").isNotNull() &
    col("LST_Day_Rural").isNotNull() &
    col("UHI_Index").isNotNull()
)

# Reorder columns
df = df.select(
    "city", "date", "year", "month", "day_of_year",
    "LST_Day_Urban", "LST_Day_Rural", "LST_Night_Urban", "UHI_Index"
)

print(f"\nCleaned records: {df.count()}")
print("\nCleaned schema:")
df.printSchema()
print("Sample cleaned data:")
df.show(10, truncate=False)

# ── Summary Statistics ─────────────────────────────────────────────────────
print("\n── Summary Statistics ──")
df.describe("LST_Day_Urban", "LST_Day_Rural", "LST_Night_Urban", "UHI_Index").show()

print("\n── Records per City ──")
df.groupBy("city").count().orderBy("city").show(11)

# ── Save as Parquet (partitioned by city and year) ─────────────────────────
output_path = "data/parquet"
df.write \
    .partitionBy("city", "year") \
    .mode("overwrite") \
    .parquet(output_path)

print(f"\n✅ Parquet files saved to: {output_path}")
print("   Partitioned by: city / year")

# ── Also save a single combined CSV for dashboard quick-load ───────────────
df.toPandas().to_csv("data/output/uhi_all_clean.csv", index=False)
print("✅ Combined CSV saved to: data/output/uhi_all_clean.csv")

spark.stop()
print("\n🏁 Phase 1 complete!")