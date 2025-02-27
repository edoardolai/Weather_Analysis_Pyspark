from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import findspark
from pyspark.sql.functions import (
    col,
    avg,
    max,
    min,
    window,
    to_timestamp,
    when,
    explode,
    collect_list,
)
import os

# Import configuration
from config import (
    WEATHER_SCHEMA,
    WINDOW_DURATION,
    EXTREME_WEATHER,
    AWS_CONFIG,
)

# Import S3 functionality if enabled
if AWS_CONFIG["enabled"]:
    try:
        from s3_storage import upload_processed_data

        print("AWS S3 integration enabled for processing")
    except ImportError:
        print("Error importing S3 module. Make sure boto3 is installed")
        AWS_CONFIG["enabled"] = False


def configure_aws_for_spark(spark):
    """Configure Spark session with AWS S3 credentials"""
    if not AWS_CONFIG["enabled"]:
        return spark

    # Set AWS credentials directly in Spark context
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID"))
    hadoop_conf.set("fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY"))
    hadoop_conf.set("fs.s3a.endpoint", f"s3.{AWS_CONFIG['region']}.amazonaws.com")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )

    print(f"Configured Spark for S3 access in region {AWS_CONFIG['region']}")
    return spark


def create_spark_session():
    """Create and configure Spark session with S3 support"""
    # Import findspark if available to help with JAR management
    try:
        findspark.init()  # automatically find the spark path and environment variables
        print("Initialized Spark environment with findspark")
    except ImportError:
        print(
            "Findspark not available, continuing with standard PySpark initialization"
        )

    # Create the Spark session with S3 configurations
    spark = (
        SparkSession.builder.appName("Weather Analysis")
        .master("local[2]")
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        # Add S3 specific configurations - this downloads the JARs automatically
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    # Configure AWS if enabled
    if AWS_CONFIG["enabled"]:
        spark = configure_aws_for_spark(spark)

    return spark


def mode_udf(conditions):
    if not conditions:
        return None
    # Count occurrences of each condition
    condition_counts = {}
    for condition in conditions:
        if condition in condition_counts:
            condition_counts[condition] += 1
        else:
            condition_counts[condition] = 1
    # Find the condition with the highest count
    max_count = 0
    mode_condition = None
    for condition, count in condition_counts.items():
        if count > max_count:
            max_count = count
            mode_condition = condition
    return mode_condition


# Register the UDF
mode_udf_spark = F.udf(mode_udf)


def process_batch(df, batch_id, data_type):
    """Process a batch and save to S3 if enabled"""
    # First show in console
    print(f"\n===== {data_type.upper()} BATCH {batch_id} =====")
    df.show(truncate=False)

    # Save to S3 if enabled
    if AWS_CONFIG["enabled"]:
        try:
            print(f"Saving {df.count()} records of {data_type} data to S3...")
            success = upload_processed_data(
                df, data_type, AWS_CONFIG["bucket_name"], AWS_CONFIG["region"]
            )
            if success:
                print(f"✅ Successfully saved {data_type} analysis to S3")
            else:
                print(f"❌ Failed to save {data_type} analysis to S3")
        except Exception as e:
            print(f"❌ Error saving {data_type} data to S3: {str(e)}")


def run_spark_analysis():
    """Run the PySpark streaming analysis with the new API format"""
    spark = create_spark_session()

    # Configure data source (S3 or local)
    if AWS_CONFIG["enabled"]:
        # S3 path to read from
        s3_path = f"s3a://{AWS_CONFIG['bucket_name']}/weather_data"

        # Read from S3
        stream_df = (
            spark.readStream.format("json")
            .option("multiLine", True)
            .schema(WEATHER_SCHEMA)
            .load(s3_path)
        )
    else:
        # Fallback to local file reading if S3 is not enabled
        print(f"Could not read from S3 bucket, aborting analysis")

    # Extract city name and timestamp
    base_df = stream_df.select(
        col("location.name").alias("name"),
        col("fetch_timestamp"),
        col("current.temp_c").alias("current_temp"),
        col("current.humidity").alias("current_humidity"),
        col("current.wind_kph").alias("current_wind"),
        col("current.condition.text").alias("current_condition"),
        col("forecast.forecastday").alias("forecast_days"),
    )

    # Explode the forecast days array to get individual days
    days_df = base_df.select(
        col("name"),
        to_timestamp(col("fetch_timestamp")).alias("fetch_time"),
        explode(col("forecast_days")).alias("day_forecast"),
    )

    # Extract hourly forecasts
    hours_df = days_df.select(
        col("name"),
        col("fetch_time"),
        col("day_forecast.date").alias("forecast_date"),
        explode(col("day_forecast.hour")).alias("hour_forecast"),
    )

    # Extract relevant fields from each hour forecast
    forecast_df = hours_df.select(
        col("name"),
        col("fetch_time"),
        col("forecast_date"),
        to_timestamp(col("hour_forecast.time")).alias("forecast_time"),
        col("hour_forecast.temp_c").alias("temp"),
        col("hour_forecast.humidity").alias("humidity"),
        col("hour_forecast.wind_kph").alias("wind_speed"),
        col("hour_forecast.condition.text").alias("condition"),
    )

    # ANALYSIS STREAM 1: Trend analysis with windows
    trend_analysis = (
        forecast_df.groupBy(window("forecast_time", WINDOW_DURATION), col("name"))
        .agg(
            avg(col("temp")).alias("avg_temp"),
            max(col("temp")).alias("max_temp"),
            min(col("temp")).alias("min_temp"),
            avg(col("humidity")).alias("avg_humidity"),
            avg(col("wind_speed")).alias("avg_wind_speed"),
            mode_udf_spark(collect_list("condition")).alias("common_condition"),
        )
        .orderBy("window", "name")
    )

    # ANALYSIS STREAM 2: Extreme weather detection
    extreme_weather = forecast_df.filter(
        (col("temp") > EXTREME_WEATHER["high_temp"])
        | (col("temp") < EXTREME_WEATHER["low_temp"])
        | (col("wind_speed") > EXTREME_WEATHER["high_wind"])
    ).select(
        col("name"),
        col("forecast_time").alias("timestamp"),
        col("temp").alias("temperature"),
        col("wind_speed"),
        when(col("temp") > EXTREME_WEATHER["high_temp"], "HIGH TEMPERATURE")
        .when(col("temp") < EXTREME_WEATHER["low_temp"], "LOW TEMPERATURE")
        .when(col("wind_speed") > EXTREME_WEATHER["high_wind"], "HIGH WIND")
        .alias("alert_type"),
    )

    # Start the queries with foreachBatch to process and save results
    trend_query = (
        trend_analysis.writeStream.foreachBatch(
            lambda df, batch_id: process_batch(df, batch_id, "trends")
        )
        .outputMode("complete")
        .start()
    )

    extreme_query = (
        extreme_weather.writeStream.foreachBatch(
            lambda df, batch_id: process_batch(df, batch_id, "extreme")
        )
        .outputMode("append")
        .start()
    )

    # Return query handles for termination handling
    return spark, [trend_query, extreme_query]


if __name__ == "__main__":
    print("Starting Weather Analysis with PySpark...")

    # Run the PySpark analysis
    try:
        spark, queries = run_spark_analysis()

        # Wait for the streaming queries to terminate
        for query in queries:
            query.awaitTermination()

    except KeyboardInterrupt:
        print("Stopping Weather Analysis...")
        if "spark" in locals():
            spark.stop()
        print("Analysis stopped.")
