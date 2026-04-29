"""
Phase 4: ML Forecasting using Spark MLlib
- Feature engineering from temporal UHI data
- GBTRegressor model training
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
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import os
import pandas as pd
import numpy as np

# Start Spark
spark = SparkSession.builder \
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

print(f"Training records (2000-2022): {train_data.count()}")
print(f"Test records (2023-2024):     {test_data.count()}")

# ── Model 1: GBT Regressor ────────────────────────────────────────────────
print("\n── Training GBT Regressor ──")

gbt = GBTRegressor(
    featuresCol="features",
    labelCol="avg_uhi",
    maxIter=100,
    maxDepth=5,
    stepSize=0.1,
    seed=42
)

pipeline = Pipeline(stages=[city_indexer, assembler, gbt])
gbt_model = pipeline.fit(train_data)

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

cities = [row.city for row in monthly.select("city").distinct().collect()]
future_rows = []

for city in cities:
    # Get latest city data for lag features
    city_data = monthly.filter(col("city") == city).orderBy("year", "month").toPandas()

    for year in range(2025, 2031):
        for month in range(1, 13):
            season = 0 if month in [3,4,5] else (1 if month in [6,7,8,9] else (2 if month in [10,11] else 3))
            decade = 2  # 2020+

            # Use recent averages for lag features
            recent = city_data.tail(24)  # last 2 years
            avg_urban = recent["avg_urban_lst"].mean()
            avg_rural = recent["avg_rural_lst"].mean()
            avg_night = recent["avg_night_lst"].mean()

            # Same-month historical average for lag
            same_month = city_data[city_data["month"] == month]
            prev_month_uhi = same_month["avg_uhi"].mean() if len(same_month) > 0 else 0
            prev_year_uhi = prev_month_uhi
            rolling = same_month["avg_uhi"].tail(3).mean() if len(same_month) >= 3 else prev_month_uhi

            future_rows.append({
                "city": city,
                "year": year,
                "month": month,
                "season": season,
                "decade": decade,
                "avg_urban_lst": round(avg_urban, 2),
                "avg_rural_lst": round(avg_rural, 2),
                "avg_night_lst": round(avg_night, 2),
                "prev_month_uhi": round(prev_month_uhi, 4),
                "prev_year_uhi": round(prev_year_uhi, 4),
                "rolling_3m_uhi": round(rolling, 4),
                "avg_uhi": 0.0  # placeholder
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
