"""
Phase 4: ML Forecasting using Spark MLlib
- Feature engineering from temporal UHI data
- Random Forest Regressor model training
- Train: 2000-2022, Test: 2023-2024
- Forecast: 2025-2030
- Metrics: MAE, RMSE, R²
- Output: data/output/predictions.csv, model saved to data/models/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, when, lit, round as spark_round, lag, abs as spark_abs
)
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import os
import pandas as pd
import numpy as np

# Start Spark
spark = SparkSession.builder \
    .master("spark://localhost:7077") \
    .appName("UHI_ML_Forecast") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("PHASE 4: ML Forecasting (Spark MLlib)")
print("=" * 60)

# ── Load cleaned data ─────────────────────────────────────────────────────
df = spark.read.parquet("data/parquet")
print(f"\nLoaded {df.count()} records")

os.makedirs("data/output", exist_ok=True)
os.makedirs("data/models", exist_ok=True)

# ── Feature Engineering ────────────────────────────────────────────────────
print("\n── Feature Engineering ──")

# 1. Monthly aggregation (model works on monthly level)
monthly = df.groupBy("city", "year", "month").agg(
    spark_round(avg("UHI_Index"), 4).alias("avg_uhi"),
    spark_round(avg("LST_Day_Urban"), 2).alias("avg_urban_lst"),
    spark_round(avg("LST_Day_Rural"), 2).alias("avg_rural_lst"),
    spark_round(avg("LST_Night_Urban"), 2).alias("avg_night_lst"),
)

# 2. Add derived features
monthly = monthly.withColumn("season",
    when(col("month").isin(3, 4, 5), lit(0))      # Pre-Monsoon
    .when(col("month").isin(6, 7, 8, 9), lit(1))  # Monsoon
    .when(col("month").isin(10, 11), lit(2))       # Post-Monsoon
    .otherwise(lit(3))                              # Winter
)

monthly = monthly.withColumn("decade",
    when(col("year").between(2000, 2009), lit(0))
    .when(col("year").between(2010, 2019), lit(1))
    .otherwise(lit(2))
)

# 3. Lag features (previous month & year UHI)
window_city = Window.partitionBy("city").orderBy("year", "month")
monthly = monthly.withColumn("prev_month_uhi", lag("avg_uhi", 1).over(window_city))
monthly = monthly.withColumn("prev_year_uhi", lag("avg_uhi", 12).over(window_city))

# 4. Rolling average (3-month)
monthly = monthly.withColumn("rolling_3m_uhi",
    spark_round(avg("avg_uhi").over(
        window_city.rowsBetween(-2, 0)
    ), 4)
)

# Fill nulls from lag features
monthly = monthly.fillna(0, subset=["prev_month_uhi", "prev_year_uhi", "rolling_3m_uhi"])

print(f"Feature-engineered records: {monthly.count()}")
monthly.show(10, truncate=False)

# ── Encode city as numeric ─────────────────────────────────────────────────
city_indexer = StringIndexer(inputCol="city", outputCol="city_index")

# ── Assemble feature vector ───────────────────────────────────────────────
feature_cols = [
    "city_index", "year", "month", "season", "decade",
    "avg_urban_lst", "avg_rural_lst", "avg_night_lst",
    "prev_month_uhi", "prev_year_uhi", "rolling_3m_uhi"
]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features",
    handleInvalid="skip"
)

# ── Train/Test Split ───────────────────────────────────────────────────────
print("\n── Train/Test Split ──")

train_data = monthly.filter(col("year") <= 2022)
test_data = monthly.filter(col("year") >= 2023)


# ── Model 1: Random Forest Regressor ────────────────────────────────────────────────
print("\n── Training Random Forest Regressor ──")

rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="avg_uhi",
    numTrees=100,
    maxDepth=5,
    seed=42
)

pipeline = Pipeline(stages=[city_indexer, assembler, rf])
gbt_model = pipeline.fit(train_data)  # keep variable name same so rest of code works

# ── Evaluate on Test Set ───────────────────────────────────────────────────
print("\n── Model Evaluation ──")

test_predictions = gbt_model.transform(test_data)

evaluators = {
    "MAE": RegressionEvaluator(labelCol="avg_uhi", predictionCol="prediction", metricName="mae"),
    "RMSE": RegressionEvaluator(labelCol="avg_uhi", predictionCol="prediction", metricName="rmse"),
    "R²": RegressionEvaluator(labelCol="avg_uhi", predictionCol="prediction", metricName="r2"),
}

print("\n  GBT Regressor Performance:")
for name, evaluator in evaluators.items():
    value = evaluator.evaluate(test_predictions)
    print(f"    {name}: {value:.4f}")

# Show sample predictions vs actuals
print("\n  Sample Predictions vs Actuals:")
test_predictions.select(
    "city", "year", "month",
    spark_round("avg_uhi", 4).alias("actual"),
    spark_round("prediction", 4).alias("predicted"),
    spark_round(spark_abs(col("avg_uhi") - col("prediction")), 4).alias("error")
).orderBy("city", "year", "month").show(20, truncate=False)

# Save test predictions
test_pred_pdf = test_predictions.select(
    "city", "year", "month", "avg_uhi", "prediction"
).toPandas()
test_pred_pdf.columns = ["city", "year", "month", "actual_uhi", "predicted_uhi"]
test_pred_pdf["predicted_uhi"] = test_pred_pdf["predicted_uhi"].round(4)
test_pred_pdf.to_csv("data/output/test_predictions.csv", index=False)
print("✅ Saved: data/output/test_predictions.csv")

# ── Generate Future Forecasts (2025-2030) ──────────────────────────────────
print("\n── Generating Forecasts (2025-2030) ──")

# Load rate-of-change CSV to get per-city warming/cooling slope
import os
rate_df = pd.read_csv("data/output/uhi_rate_of_change.csv")
rate_map = dict(zip(rate_df["city"], rate_df["slope_per_year"]))

cities = [row.city for row in monthly.select("city").distinct().collect()]
future_rows = []

for city in cities:
    city_data = monthly.filter(col("city") == city).orderBy("year", "month").toPandas()

    # Base LST from last 2 years of data
    recent = city_data.tail(24)
    base_urban = recent["avg_urban_lst"].mean()
    base_rural = recent["avg_rural_lst"].mean()
    base_night = recent["avg_night_lst"].mean()

    # Per-city UHI trend slope (°C per year)
    city_slope = rate_map.get(city, 0.0)

    for year in range(2025, 2031):
        years_ahead = year - 2024  # 1 for 2025, 2 for 2026, etc.
        season_map = {**{m: 0 for m in [3,4,5]}, **{m: 1 for m in [6,7,8,9]},
                      **{m: 2 for m in [10,11]}, **{m: 3 for m in [1,2,12]}}

        for month in range(1, 13):
            season = season_map[month]
            decade = 2  # 2020+

            # Urban warms slightly faster than rural (UHI intensification)
            proj_urban = base_urban + years_ahead * 0.20
            proj_rural = base_rural + years_ahead * 0.15
            proj_night = base_night + years_ahead * 0.10

            # Same-month historical average for lag features
            same_month = city_data[city_data["month"] == month]
            base_uhi = same_month["avg_uhi"].mean() if len(same_month) > 0 else 0.0

            # ── Lag features also evolve with trend ───────────────────────
            prev_month_uhi = round(base_uhi + city_slope * years_ahead, 4)
            prev_year_uhi  = round(base_uhi + city_slope * (years_ahead - 1), 4)
            rolling        = round(base_uhi + city_slope * years_ahead, 4)

            future_rows.append({
                "city":            city,
                "year":            year,
                "month":           month,
                "season":          season,
                "decade":          decade,
                "avg_urban_lst":   round(proj_urban, 2),
                "avg_rural_lst":   round(proj_rural, 2),
                "avg_night_lst":   round(proj_night, 2),
                "prev_month_uhi":  prev_month_uhi,
                "prev_year_uhi":   prev_year_uhi,
                "rolling_3m_uhi":  rolling,
                "avg_uhi":         0.0  # placeholder label
            })

future_df = spark.createDataFrame(pd.DataFrame(future_rows))
future_predictions = gbt_model.transform(future_df)

forecast_pdf = future_predictions.select(
    "city", "year", "month",
    spark_round("prediction", 4).alias("predicted_uhi")
).toPandas()

print(f"Generated {len(forecast_pdf)} forecasts")
forecast_pdf.to_csv("data/output/uhi_forecast_2025_2030.csv", index=False)
print("✅ Saved: data/output/uhi_forecast_2025_2030.csv")

# Show forecast summary
print("\n  Forecast Summary (Annual Average UHI per City):")
forecast_summary = forecast_pdf.groupby(["city", "year"])["predicted_uhi"].mean().round(4)
print(forecast_summary.unstack().to_string())

# ── Save Model ─────────────────────────────────────────────────────────────
model_path = "data/models/gbt_uhi_model"
gbt_model.write().overwrite().save(model_path)
print(f"\n✅ Model saved to: {model_path}")

# ── Feature Importance ─────────────────────────────────────────────────────
print("\n── Feature Importance ──")
gbt_stage = gbt_model.stages[-1]
importances = gbt_stage.featureImportances.toArray()
for feat, imp in sorted(zip(feature_cols, importances), key=lambda x: -x[1]):
    print(f"  {feat:25s}: {imp:.4f}")

spark.stop()
print("\n🏁 Phase 4 ML Forecasting complete!")