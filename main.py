"""
Weather Analysis POC - Main Script
---------------------------------
This script starts both the weather data collector and the PySpark analysis.
"""

import time
import os

# Import the collector and processor modules
from weather_collector import start_collector
from weather_stream import run_spark_analysis
from config import API_KEY


def main():
    """Main function to run the entire POC"""
    print("=" * 60)
    print("=" * 60)

    # Check if API key is available
    if not API_KEY:
        print("ERROR: WEATHER_API_KEY not found in .env file")
        print("Please create a .env file with your API key")
        return

    # Start the weather collector in a background thread
    collector_thread = start_collector()
    print("Weather collector started successfully.")

    # Give collector a moment to fetch initial data
    print("Waiting for initial data collection...")
    time.sleep(5)

    # Run the PySpark analysis in the main thread
    try:
        print("Starting PySpark analysis...")
        spark, queries = run_spark_analysis()

        # Wait for the streaming queries to terminate
        for query in queries:
            query.awaitTermination()

    except KeyboardInterrupt:
        print("\nStopping Weather Analysis POC...")
        if "spark" in locals():
            spark.stop()
        print("Analysis stopped.")


if __name__ == "__main__":
    main()
