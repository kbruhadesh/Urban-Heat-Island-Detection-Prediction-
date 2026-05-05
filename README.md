# 🌆 Urban Heat Island Detection & Prediction

A Big Data + Machine Learning project that analyzes and predicts **Urban Heat Island (UHI)** effects across **11 Indian cities** using 25 years of NASA MODIS satellite data (2000–2024).

---

## 🔥 Key Highlights

- 📡 **Satellite Data**: MODIS MOD11A2 (Land Surface Temperature)
- ⚡ **Big Data Processing**: Apache Spark (PySpark)
- 🤖 **Machine Learning**: Spark MLlib (UHI forecasting for 2025–2030)
- 📊 **Visualization**: Streamlit dashboard (Plotly + Folium)
- 💾 **Storage**: Parquet (partitioned by city/year)

---

## 🧠 Problem Statement

Urban areas are significantly hotter than surrounding rural regions due to:

- Dense infrastructure (concrete, asphalt)
- Reduced vegetation
- Heat retention and absorption

This project:

✔ Detects Urban Heat Island intensity  
✔ Analyzes long-term trends (2000–2024)  
✔ Predicts future UHI (2025–2030)  
✔ Supports data-driven urban planning  

---

## 🏙️ Cities Covered

Delhi, Mumbai, Bangalore, Chennai, Hyderabad, Kochi, Pune, Ahmedabad, Kolkata, Jaipur, Surat

---

## ⚙️ Tech Stack

| Layer | Technology |
|------|------------|
| Data Source | Google Earth Engine (MODIS) |
| Processing | Apache Spark (PySpark) |
| ML | Spark MLlib |
| Storage | Parquet |
| Dashboard | Streamlit + Plotly + Folium |

---

## 📂 Project Structure
UrbanHeat_Island_Detection/
├── scripts/ # Google Earth Engine scripts
├── spark/ # Spark processing + ML
├── dashboard/ # Streamlit app
├── data/
│ ├── modis/ # Raw CSV (GEE export)
│ ├── parquet/ # Processed data
│ └── output/ # Final CSV outputs
├── requirements.txt
└── README.md
---

## 🔄 Data Pipeline
MODIS CSV → Spark ETL → Parquet → Spark Analysis → ML Forecast → Dashboard
---

## 🚀 Setup & Installation

### 1️⃣ Clone Repository

git clone <your-repo-link>
cd Urban-Heat-Island-Detection-Prediction

### 2️⃣ Install Dependencies
pip install -r requirements.txt


### 3️⃣ Configure PySpark (IMPORTANT)

Ensure same Python version for driver and workers:

export PYSPARK_PYTHON=python3.11
export PYSPARK_DRIVER_PYTHON=python3.11


### ▶️ Running the Project

#### Step 1: Convert CSV → Parquet
python spark/convert_to_parquet.py
#### Step 2: Run Analysis
python spark/uhi_analysis.py
python spark/hotspot_detection.py
python spark/temporal_analysis.py

#### Step 3: Generate Forecast
python spark/ml_forecast.pyOutput:
data/output/uhi_forecast_2025_2030.csv

#### Step 4: Launch Dashboard
streamlit run dashboard/app.py

Open:

http://localhost:8501