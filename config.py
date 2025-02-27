import os
from dotenv import load_dotenv
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    ArrayType,
    IntegerType,
    DoubleType,
)

# Load environment variables from .env file
load_dotenv()

# API Configuration
API_KEY = os.getenv("WEATHER_API_KEY")

# Cities to monitor
CITIES = [
    {"name": "Brussels"},
    {"name": "Ghent"},
    {"name": "Antwerp"},
    {"name": "Liege"},
    {"name": "Namur"},
    {"name": "Leuven"},
    {"name": "Charleroi"},
]

# Data collection settings
FETCH_INTERVAL = 300  # seconds (5 minutes)
# Analysis settings
WINDOW_DURATION = "10 minutes"  # Duration for trend analysis windows

# Extreme weather thresholds
EXTREME_WEATHER = {
    "high_temp": 30.0,
    "low_temp": 0.0,  # °C
    "high_wind": 25.0,  # Km/s
}

# AWS S3 configuration
AWS_CONFIG = {
    "enabled": os.getenv("AWS_ENABLED", "False").lower() == "true",
    "bucket_name": os.getenv("AWS_BUCKET_NAME"),
    "region": os.getenv("AWS_REGION", "us-east-1"),
}

WEATHER_SCHEMA = StructType(
    [
        StructField(
            "location",
            StructType(
                [
                    StructField("name", StringType(), True),
                    # Removed: region, country, lat, lon, localtime
                ]
            ),
            True,
        ),
        StructField(
            "current",
            StructType(
                [
                    StructField("temp_c", FloatType(), True),
                    StructField(
                        "condition",
                        StructType(
                            [
                                StructField("text", StringType(), True),
                                # Removed: icon
                            ]
                        ),
                        True,
                    ),
                    StructField("wind_kph", FloatType(), True),
                    StructField("humidity", IntegerType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "forecast",
            StructType(
                [
                    StructField(
                        "forecastday",
                        ArrayType(
                            StructType(
                                [
                                    StructField("date", StringType(), True),
                                    StructField(
                                        "hour",
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField(
                                                        "time", StringType(), True
                                                    ),
                                                    StructField(
                                                        "temp_c", FloatType(), True
                                                    ),
                                                    StructField(
                                                        "condition",
                                                        StructType(
                                                            [
                                                                StructField(
                                                                    "text",
                                                                    StringType(),
                                                                    True,
                                                                ),
                                                            ]
                                                        ),
                                                        True,
                                                    ),
                                                    StructField(
                                                        "wind_kph", FloatType(), True
                                                    ),
                                                    StructField(
                                                        "humidity", IntegerType(), True
                                                    ),
                                                ]
                                            )
                                        ),
                                        True,
                                    ),
                                ]
                            )
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
        StructField("fetch_timestamp", StringType(), True),
    ]
)
