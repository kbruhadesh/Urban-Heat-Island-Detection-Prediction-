# Urban Heat Island Detection using Apache Spark & Streamlit

This project detects Urban Heat Islands (UHI) in Indian cities using satellite thermal data and analyzes trends using Apache Spark and Streamlit.

## Features

- **Data Source**: MODIS MOD11A2 (Land Surface Temperature)
- **Big Data Processing**: Apache Spark 3.x
- **Machine Learning**: Spark MLlib GBTRegressor for UHI prediction
- **Frontend**: Streamlit dashboard with 7 interactive tabs
- **Real-time Streaming**: Kafka integration for live monitoring

## Folder Structure

```
UrbanHeat_Island_Detection/
├── spark_code/                    # Spark ETL & ML pipelines
│   ├── spark_etl.py               # Main Spark job
│   ├── spark_ml_gbt.py            # GBT model training
│   ├── spark_streaming.py         # Kafka streaming
│   ├── requirements.txt           # Spark dependencies
│   └── spark-warehouse/           # Spark metastore (Git ignored)
├── dashboard/                     # Streamlit app
│   ├── app.py                     # Main dashboard
│   ├── styles.css                 # Premium styling
│   └── requirements.txt           # Streamlit dependencies
├── data/
│   ├── input/                     # MODIS HDF5 files
│   ├── output/                    # Processed data (Parquet)
│   ├── parquet/                   # Processed data (Git ignored)
│   └── hotspot_alerts/            # Fire alerts
├── .env                           # Environment variables (Git ignored)
├── README.md                      # This file
└── requirements.txt               # Combined dependencies
```

## Prerequisites

- Java 8+
- Apache Spark 3.x installed
- Python 3.8+
- Kafka (optional, for streaming)

## Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd UrbanHeat_Island_Detection
   ```

2. **Create virtual environment**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### 1. Run Spark ETL Job

This job processes MODIS data and creates aggregated files:

```bash
cd spark_code
python3 spark_etl.py --input-dir /path/to/MODIS_data
```

**Output**: Creates parquet files in `data/parquet/` and CSVs in `data/output/`

### 2. Train ML Model (Optional)

Trains GBTRegressor on historical data:

```bash
cd spark_code
python3 spark_ml_gbt.py
```

### 3. Start Streaming (Optional)

Monitors Kafka for fire alerts:

```bash
cd spark_code
python3 spark_streaming.py
```

### 4. Run Streamlit Dashboard

Start the interactive dashboard:

```bash
cd dashboard
streamlit run app.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser

## Data Flow

```
HDF5 MODIS Data → Spark ETL Job → Parquet Files → Spark ML → Streamlit Dashboard
                                     ↓
                                 Kafka Topic → Real-time Alerts
```

## Advanced Configuration

**Environment Variables** (in `.env`):
```
SPARK_HOME=/opt/spark
DATA_DIR=data/input
OUTPUT_DIR=data/output
```

**Spark Configuration**: Edit `spark_code/requirements.txt` for custom Spark settings.

## File Descriptions

- **`spark_code/spark_etl.py`**: Main ETL pipeline that converts HDF5 to Parquet and aggregates data by city/year.
- **`spark_code/spark_ml_gbt.py`**: Trains GBTRegressor model for UHI prediction.
- **`spark_code/spark_streaming.py`**: Kafka streaming client for real-time fire detection.
- **`dashboard/app.py`**: Streamlit dashboard with 7 interactive tabs.
- **`dashboard/styles.css`**: Premium dark theme for the dashboard.

## License

[MIT License](LICENSE)

## Acknowledgments

- NASA Earthdata MODIS Program
- Apache Spark Community
- Streamlit Community