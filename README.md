# Weather_Analysis_Pyspark

## A Proof of Concept for Real-time Weather Data Processing

This project demonstrates real-time weather data processing and analysis using Apache Spark's streaming capabilities. It showcases the application of distributed computing techniques to analyze weather patterns, detect extreme conditions, and generate insights from continuous data streams.

## 🌟 Project Overview

This proof of concept (POC) demonstrates the following capabilities:

<ul>
<li>
Real-time processing of weather data streams with PySpark Structured Streaming
</li>
<li>
Complex data transformations and analytics with Spark SQL using windowed aggregations
</li>
<li>
Extreme weather detection and alerting
</li>
<li>
Cloud integration with AWS S3 for data storage
</li>
<li>
Modular software design with configuration management
</li>
</ul>

## 🔧 Technologies Used

This proof of concept (POC) demonstrates the following capabilities:

<ul>
<li>
<strong>PySpark <strong> - For distributed data processing
</li>
<li>
<strong>AWS S3 <strong> - For distributed data processing
</li>
<li>
<strong>streamlit <strong> - For dashboarding UI
</li>
</ul>

## 📊 Data Processing Pipeline

<ol>
<li>
<strong>Fetching Data <strong> - Retrieves data from weatherapi.com api and stores them to AWS S3 bucket
</li>
<li>
<strong>Data Ingestion <strong> - For distributed data processing
<ul>
<li>
Extracts location, timestamp, and weather metrics
</li>
<li>
Explodes nested arrays for daily and hourly forecasts
</li>
<li>
Flattens the data structure for analysis
</li>
</ul>
</li>
<li>
<strong>Analysis Streams <strong> - For dashboard UI
<ul>
<li>
<strong>Trend Analysis: <strong> - Aggregates data by time windows and location
</li>
<li>
<strong>Extreme Weather Detection: <strong> - Identifies temperatures and wind speeds outside normal ranges
</li>
<li>
Flattens the data structure for analysis
</li>
</ul>
</li>
<li>
<strong>Output <strong>
<ul>
<li>
Displays results in console
<li>
<li>
Optionally uploads processed data to AWS S3
<li>
</ul>
</li>
</ol>

## 🚀 Setup and Execution

### Prerequisites

<ul>
<li>
Python 3.7+
</li>
<li>
Apache Spark 3.x
</li>
<li>
AWS account (optional)
</li>
</ul>

### Installation

```bash
# Clone the repository
git clone https://github.com/edoardolai/Weather_Analysis_Pyspark.git
cd Weather_Analysis_Pyspark

# Install required packages
pip install -r requirements.txt
```

### Configuration

Edit the config.py file to set: <br>
WINDOW_DURATION: Time window for trend analysis
EXTREME_WEATHER: Thresholds for extreme condition detection
AWS_CONFIG: S3 configuration

### Execution

```bash
# Run the analysis
python main.py

# Start the streamlit dashboard
streamlit run weather_dashboard.py
```

## 📂 Project Structure

```
Weather_Analysis_Pyspark/
├── README.md
├── config.py
├── main.py
├── requirements.txt
├── s3_storage.py
├── weather_collector.py
├── weather_dashboard.py
└── weather_stream.py
```

## License

This project was created as an educational proof of concept for showcasing PySpark skills.
