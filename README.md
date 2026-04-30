# Urban Heat Island Detection & Prediction

Analyzing Urban Heat Island (UHI) effects across **11 Indian cities** using 25 years of NASA MODIS satellite data (2000–2024), powered by Apache Spark, Kafka, and Spark MLlib.

## Features

- **Data Source**: MODIS MOD11A2 (Land Surface Temperature) via Google Earth Engine
- **Big Data Processing**: Apache Spark (PySpark)
- **Machine Learning**: Spark MLlib GBTRegressor for UHI forecasting (2025–2030)
- **Streaming**: Apache Kafka producer/consumer for real-time data ingestion
- **Dashboard**: Streamlit with Plotly charts and Folium maps
- **Storage**: Apache Parquet (partitioned by city/year)

## Folder Structure

```
UrbanHeat_Island_Detection/
├── scripts/                        # Google Earth Engine scripts
│   ├── 1.Data.js                   # Data visualization check
│   ├── 2.DataCollection.js         # Full data export (11 cities)
│   └── 3.DataCollectionTest.js     # Delhi test script
├── spark/                          # Spark processing scripts
│   ├── convert_to_parquet.py       # CSV → cleaned Parquet conversion
│   ├── uhi_analysis.py             # Monthly/yearly/seasonal/decade analysis
│   ├── hotspot_detection.py        # 90th percentile hotspot detection
│   ├── temporal_analysis.py        # YoY trends, rate of change, moving avg
│   └── ml_forecast.py              # GBTRegressor ML forecasting
├── kafka/                          # Kafka streaming pipeline
│   ├── setup_kafka.sh              # Download & configure Kafka (KRaft mode)
│   ├── producer.py                 # Streams CSV data → Kafka topic
│   └── consumer.py                 # PySpark Structured Streaming consumer
├── dashboard/                      # Streamlit dashboard
│   ├── app.py                      # Main dashboard (7 interactive tabs)
│   ├── styles.css                  # Premium dark theme CSS
│   └── requirements.txt            # Dashboard-specific dependencies
├── data/
│   ├── modis/                      # Raw CSV exports from GEE (11 cities)
│   ├── output/                     # Analysis results (18 CSV files)
│   └── parquet/                    # Spark-partitioned Parquet (Git ignored)
├── .gitignore
├── requirements.txt                # All Python dependencies
└── README.md
```

## Cities Analyzed

Delhi, Mumbai, Bangalore, Chennai, Hyderabad, Kochi, Pune, Ahmedabad, Kolkata, Jaipur, Surat

## Prerequisites

- **Python 3.11** (required — PySpark needs matching driver/worker versions)
- **Java 11+** (required for Spark)
- **pip** (Python package manager)

### Verify Prerequisites

```bash
python3.11 --version    # Should show Python 3.11.x
java -version           # Should show java 11+ or 21+
```

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/kbruhadesh/Urban-Heat-Island-Detection-Prediction-.git
cd Urban-Heat-Island-Detection-Prediction-
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Set environment variables

**Critical** — PySpark worker and driver must use the same Python version:

```bash
export PYSPARK_PYTHON=python3.11
export PYSPARK_DRIVER_PYTHON=python3.11
```

> Add these to your `~/.bashrc` to make them permanent.

## Usage

### Step 1: Convert CSV data to Parquet

```bash
python3.11 spark/convert_to_parquet.py
```

Loads 11 city CSVs from `data/modis/`, cleans them, and saves partitioned Parquet to `data/parquet/`.

### Step 2: Run Spark analysis scripts

```bash
python3.11 spark/uhi_analysis.py
python3.11 spark/hotspot_detection.py
python3.11 spark/temporal_analysis.py
```

Generates 18 analysis CSVs in `data/output/` (monthly trends, city rankings, hotspot alerts, etc.)

### Step 3: Train ML model & generate forecasts

```bash
python3.11 spark/ml_forecast.py
```

Trains a GBTRegressor on 2000–2022 data, evaluates on 2023–2024, and forecasts UHI for 2025–2030.

### Step 4: Launch Streamlit dashboard

```bash
python3.11 -m streamlit run dashboard/app.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

### Step 5 (Optional): Kafka streaming demo

```bash
# Terminal 1: Setup & start Kafka
cd kafka && chmod +x setup_kafka.sh && ./setup_kafka.sh && cd ..

# Terminal 2: Start producer
python3.11 kafka/producer.py

# Terminal 3: Start consumer
python3.11 kafka/consumer.py
```

## Dashboard Tabs

| Tab | Description |
|-----|-------------|
| 🏠 Overview | KPI cards, city rankings, rate of change charts |
| 📈 Trends | Yearly UHI lines, 5-year moving average, decade comparison |
| 🗺️ Heatmap | Interactive Folium map + monthly UHI intensity heatmap |
| 🏙️ City Comparison | Seasonal patterns, temperature bars, peak months |
| 🔮 Forecast | Historical vs ML-predicted UHI (2025–2030) |
| ⚠️ Alerts | Critical/High/Moderate hotspot events table |
| 📋 Policy | Data-driven recommendations per city |

## Data Flow

```
MODIS CSVs → convert_to_parquet.py → Parquet → Analysis Scripts → Output CSVs → Dashboard
     ↓
Kafka Producer → Kafka Topic → PySpark Consumer → Parquet
```

## Troubleshooting

**Python version mismatch error:**
```
PySparkRuntimeError: [PYTHON_VERSION_MISMATCH]
```
Fix: Set both environment variables:
```bash
export PYSPARK_PYTHON=python3.11
export PYSPARK_DRIVER_PYTHON=python3.11
```

**Streamlit port already in use:**
```bash
python3.11 -m streamlit run dashboard/app.py --server.port 8502
```

## Acknowledgments

- NASA MODIS MOD11A2 Land Surface Temperature data
- Google Earth Engine for satellite data extraction
- Apache Spark & Kafka communities