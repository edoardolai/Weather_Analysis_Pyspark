import time
import requests
from datetime import datetime
import threading

# Import configuration
from config import API_KEY, CITIES, FETCH_INTERVAL, AWS_CONFIG

# Import S3 functionality if enabled
if AWS_CONFIG["enabled"]:
    try:
        from s3_storage import upload_to_s3

        print("AWS S3 integration enabled")
    except ImportError:
        print("Error importing S3 module. Make sure boto3 is installed:")
        print("pip install boto3")
        AWS_CONFIG["enabled"] = False


def fetch_weather_for_city(city_name):
    """Fetch weather data for a specific city using WeatherAPI.com"""
    url = f"https://api.weatherapi.com/v1/forecast.json?key={API_KEY}&q={city_name}&days=7&aqi=no&alerts=no"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Add timestamp for when we fetched this data
        data["fetch_timestamp"] = datetime.now().isoformat()

        print(f"Successfully fetched weather data for {city_name}")
        return data

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {city_name}: {e}")
        return None


def fetch_all_cities():
    """Fetch weather for all configured cities"""
    results = []

    for city in CITIES:
        print(f"Fetching weather for {city['name']}...")
        data = fetch_weather_for_city(city["name"])

        if data:
            results.append(data)
            # Print some basic info from the current weather
            print(f"  Temperature: {data['current']['temp_c']}°C")
            print(f"  Conditions: {data['current']['condition']['text']}")
            print(f"  Local time: {data['location']['localtime']}")
        else:
            print(f"  No data available for {city['name']}")

        # Add a small delay to avoid hitting API rate limits
        time.sleep(1)

    return results


def save_weather_data(data):
    """Save weather data directly to S3"""
    if not data:
        print("No data to save.")
        return None

    # Upload to S3
    if AWS_CONFIG["enabled"]:
        try:
            success = upload_to_s3(
                data, AWS_CONFIG["bucket_name"], AWS_CONFIG["region"]
            )
            if success:
                return True
            else:
                print("Failed to upload data to S3")
                return False
        except Exception as e:
            print(f"Error uploading to S3: {e}")
            return False
    else:
        print("AWS S3 is not enabled. Cannot save data.")
        return False


def weather_collector_thread():
    """Background thread function to collect weather data"""
    print(f"Starting weather data collection every {FETCH_INTERVAL} seconds.")

    while True:
        try:
            weather_data = fetch_all_cities()
            save_weather_data(weather_data)
            print(f"\nWaiting {FETCH_INTERVAL} seconds until next update...")
            time.sleep(FETCH_INTERVAL)
        except Exception as e:
            print(f"Error in weather collection: {e}")
            time.sleep(30)


def start_collector():
    """Start the weather collector in a background thread"""
    collector_thread = threading.Thread(target=weather_collector_thread, daemon=True)
    collector_thread.start()
    return collector_thread


if __name__ == "__main__":
    if not API_KEY:
        print("ERROR: WEATHER_API_KEY not found in .env file")
        print("Please create a .env file with your API key")
        exit(1)

    # Start the collector
    collector_thread = start_collector()

    # Keep the main thread running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Weather collection stopped by user.")
