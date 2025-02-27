import streamlit as st
import pandas as pd
import boto3
import json
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
WEATHER_EMOJI_MAP = {
    # Sunny conditions
    "Sunny": "☀️",
    "Clear": "☀️",
    # Partly cloudy
    "Partly cloudy": "⛅",
    "Partly Cloudy": "⛅",
    # Cloudy
    "Cloudy": "☁️",
    "Overcast": "☁️",
    "Mist": "🌫️",
    "Fog": "🌫️",
    "Freezing fog": "🌫️❄️",
    # Rain
    "Patchy rain possible": "🌦️",
    "Light rain": "🌧️",
    "Light rain shower": "🌧️",
    "Moderate rain": "🌧️",
    "Heavy rain": "🌧️",
    "Heavy rain at times": "🌧️",
    "Moderate or heavy rain shower": "🌧️",
    "Torrential rain shower": "🌧️",
    "Rain": "🌧️",
    "Drizzle": "🌧️",
    # Thunderstorms
    "Thundery outbreaks possible": "⛈️",
    "Patchy light rain with thunder": "⛈️",
    "Moderate or heavy rain with thunder": "⛈️",
    "Thunderstorm": "⛈️",
    # Snow
    "Patchy snow possible": "🌨️",
    "Patchy light snow": "🌨️",
    "Light snow": "🌨️",
    "Patchy moderate snow": "🌨️",
    "Moderate snow": "🌨️",
    "Patchy heavy snow": "❄️",
    "Heavy snow": "❄️",
    "Blizzard": "❄️",
    "Blowing snow": "🌬️❄️",
    # Mixed
    "Patchy light rain with snow": "🌨️🌧️",
    "Moderate or heavy rain with snow": "🌨️🌧️",
    "Light sleet": "🌧️❄️",
    "Moderate or heavy sleet": "🌧️❄️",
    "Ice pellets": "🧊",
    "Light sleet showers": "🌧️❄️",
    "Moderate or heavy sleet showers": "🌧️❄️",
}

# AWS Configuration
AWS_ENABLED = os.getenv("AWS_ENABLED", "False").lower() == "true"
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Set page config
st.set_page_config(
    page_title="Weather Forecast Dashboard",
    layout="wide",
)

# Dashboard title
st.title("Weather Forecast Dashboard")
st.write("Displaying forecasts processed by PySpark")


def get_weather_emoji(condition_text):
    """Get emoji for a weather condition"""
    if not condition_text:
        return "❓"  # Unknown/missing condition

    # Try exact match
    if condition_text in WEATHER_EMOJI_MAP:
        return WEATHER_EMOJI_MAP[condition_text]

    # Try partial match
    for condition, emoji in WEATHER_EMOJI_MAP.items():
        if condition.lower() in condition_text.lower():
            return emoji

    # Default emoji for unknown conditions
    return "🌡️"  # Generic weather emoji


# Function to fetch data from S3
def get_latest_data_from_s3(data_type):
    """Fetch the latest processed data from S3"""
    if not AWS_ENABLED or not AWS_BUCKET_NAME:
        st.error("AWS S3 is not properly configured")
        return None

    try:
        # Initialize S3 client
        s3_client = boto3.client("s3", region_name=AWS_REGION)

        # Define the key for latest data
        s3_key = f"processed_data/latest/{data_type}.json"

        # Get object from S3
        response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=s3_key)
        content = response["Body"].read().decode("utf-8")

        # Parse JSON data
        data = json.loads(content)
        return data

    except Exception as e:
        st.error(f"Error fetching {data_type} data from S3: {e}")
        return None


# Function to prepare trend data
def prepare_trend_data(data):
    if not data:
        return None

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Process window column - handling the array format from S3
    if "window" in df.columns:
        # Check if window is a list
        if isinstance(df["window"].iloc[0], list):
            df["start"] = pd.to_datetime([w[0] for w in df["window"]])
            df["end"] = pd.to_datetime([w[1] for w in df["window"]])
        else:
            # Try other formats
            try:
                df["start"] = pd.to_datetime(df["window"])
            except:
                st.warning("Window format not as expected, using raw data")
                return df

        # Add time info columns
        df["date"] = df["start"].dt.date
        df["hour"] = df["start"].dt.hour
        df["time_str"] = df["start"].dt.strftime("%H:%M")
        df["date_str"] = df["start"].dt.strftime("%Y-%m-%d")
        df["display_time"] = df["start"].dt.strftime("%m-%d %H:%M")

        # Sort by time
        df = df.sort_values(["name", "start"])

    return df


# Function to check for extreme weather alerts and get a summary
def get_alert_summary(city_name, extreme_data):
    if extreme_data is None or len(extreme_data) == 0:
        return None

    alerts_df = pd.DataFrame(extreme_data)

    city_alerts = alerts_df[alerts_df["name"] == city_name]

    if city_alerts.empty:
        return None

    city_alerts["timestamp"] = pd.to_datetime(city_alerts["timestamp"])

    alert = city_alerts.iloc[0]
    if "alert_type" in alert and "temperature" in alert:
        return {
            "type": alert["alert_type"],
            "value": (
                alert["temperature"]
                if alert["alert_type"] != "HIGH WIND"
                else alert["wind_speed"]
            ),
        }


# Function to display forecast for a city
def display_city_forecast(city_name, trend_data, extreme_data=None):
    """Display forecast for a specific city"""
    if trend_data is None:
        st.error(f"No trend data available for {city_name}")
        return

    # Filter data for this city
    city_data = trend_data[trend_data["name"] == city_name]

    if city_data.empty:
        st.error(f"No data available for {city_name}")
        return

    # Get the latest data point for current conditions
    latest = city_data.iloc[-1]

    # Check for alerts
    alert_summary = get_alert_summary(city_name, extreme_data)

    # Get current weather emoji
    current_emoji = "🌡️"  # Default
    if "common_condition" in latest:
        current_emoji = get_weather_emoji(latest["common_condition"])
    elif "last_condition" in latest:
        current_emoji = get_weather_emoji(latest["last_condition"])

    # Create the city header with current conditions, emoji, and alert if needed
    header_col1, header_col2 = st.columns([0.95, 0.05])

    with header_col1:
        st.subheader(
            f"{city_name} {current_emoji} - Current: {latest['avg_temp']:.1f}°C, Humidity: {latest['avg_humidity']:.0f}%"
        )

    # Show alert icon if there are alerts
    with header_col2:
        if alert_summary:
            metric = "°C" if alert_summary["type"] != "HIGH WIND" else "Km/h"
            st.markdown(
                f"⚠️",
                help=f"**Alert:** {alert_summary['type']} - {alert_summary['value']:.1f}{metric} right now",
            )

    # Create two columns for charts
    col1, col2 = st.columns(2, gap="medium")

    # COLUMN 1: Hourly forecast
    with col1:
        st.write("**Hourly Temperature Forecast**")

        # Prepare hourly data - last 24 hours and upcoming hours
        now = datetime.now()
        past_cutoff = now - timedelta(hours=12)
        future_cutoff = now + timedelta(hours=36)

        # Filter recent data
        hourly_data = city_data[
            (city_data["start"] >= past_cutoff) & (city_data["start"] <= future_cutoff)
        ]

        if not hourly_data.empty:
            # Create chart for hourly temps
            chart_data = hourly_data[["display_time", "avg_temp"]].copy()
            chart_data.loc[:, "avg_temp"] = chart_data["avg_temp"].round(2)
            chart_data = chart_data.rename(columns={"avg_temp": "Temperature (°C)"})

            # Plot the chart
            st.line_chart(chart_data.set_index("display_time"))
        else:
            st.write("No hourly forecast data available")

    # COLUMN 2: Daily forecast
    with col2:
        st.write("**Daily Temperature Range**")

        # Group by date to get daily min/max
        if "date_str" in city_data.columns:
            daily_data = (
                city_data.groupby("date_str")
                .agg(
                    {
                        "max_temp": "max",
                        "min_temp": "min",
                        "date": "first",
                        "avg_wind_speed": "mean",
                        "avg_humidity": "mean",
                    }
                )
                .reset_index()
            )

            # Add condition information if available
            if "common_condition" in city_data.columns:
                # Take the most common condition for each day
                condition_by_day = (
                    city_data.groupby("date_str")
                    .agg(
                        {
                            "common_condition": lambda x: (
                                x.mode()[0]
                                if not x.empty and not x.mode().empty
                                else None
                            )
                        }
                    )
                    .reset_index()
                )
                daily_data = daily_data.merge(
                    condition_by_day, on="date_str", how="left"
                )
            elif "last_condition" in city_data.columns:
                # Take the last condition for each day
                condition_by_day = (
                    city_data.groupby("date_str")
                    .agg({"last_condition": "last"})
                    .reset_index()
                )
                daily_data = daily_data.merge(
                    condition_by_day, on="date_str", how="left"
                )

            daily_data["date_str"] = pd.to_datetime(daily_data["date_str"]).dt.strftime(
                "%a %d"
            )

            # Now we have condition info in daily_data, we can display it
            header_cols = st.columns(6)  # Added one more column for condition
            with header_cols[0]:
                st.markdown("**Date**")
            with header_cols[1]:
                st.markdown("**Condition**")  # New column
            with header_cols[2]:
                st.markdown("**Max Temp**")
            with header_cols[3]:
                st.markdown("**Min Temp**")
            with header_cols[4]:
                st.markdown("**Wind**")
            with header_cols[5]:
                st.markdown("**Humidity**")

            # Add a separator line
            st.markdown(
                "<hr style='margin: 0.5em 0; border: 0; border-top: 1px solid rgba(0,0,0,0.1);'>",
                unsafe_allow_html=True,
            )

            for _, row in daily_data.iterrows():
                _col1, _col2, _col3, _col4, _col5, _col6 = st.columns(6)
                with _col1:
                    st.write(f"{row['date_str']}")
                with _col2:
                    # Display emoji for the condition
                    condition_text = None
                    if "common_condition" in row and not pd.isna(
                        row["common_condition"]
                    ):
                        condition_text = row["common_condition"]
                    elif "last_condition" in row and not pd.isna(row["last_condition"]):
                        condition_text = row["last_condition"]

                    emoji = get_weather_emoji(condition_text)
                    weather_text = condition_text or "Unknown"
                    st.write(f"{emoji}")
                with _col3:
                    st.write(f"{row['max_temp']:.1f}°C")
                with _col4:
                    st.write(f"{row['min_temp']:.1f}°C")
                with _col5:
                    st.write(f"{row['avg_wind_speed']:.1f}Km/h")
                with _col6:
                    st.write(f"{row['avg_humidity']:.1f}%")
        else:
            st.write("Date information not available in the data")


# Main dashboard
# Get weather trends data
trends_data = get_latest_data_from_s3("trends")
extreme_data = get_latest_data_from_s3("extreme")

if trends_data:
    # Prepare the data
    processed_data = prepare_trend_data(trends_data)

    if processed_data is not None:
        # Get all cities
        cities = processed_data["name"].unique().tolist()

        # Display each city's forecast
        for city in cities:
            st.markdown("---")
            display_city_forecast(city, processed_data, extreme_data)
    else:
        st.error("Could not process the trend data properly")
else:
    st.error("No trend data available from PySpark processing")

# Sidebar controls
st.sidebar.title("Dashboard Settings")
refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 10, 60, 30)

# Auto-refresh
if st.sidebar.checkbox("Auto-refresh dashboard", value=True):
    time.sleep(refresh_interval)
