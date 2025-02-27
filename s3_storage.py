"""
S3 Storage Module
----------------
Functions for storing data in AWS S3.
"""

import boto3
import os
import json
from datetime import datetime


class TimestampEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle timestamp objects"""

    def default(self, obj):
        # Handle various timestamp types
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        # Handle PySpark window objects which have start and end times
        elif hasattr(obj, "start") and hasattr(obj, "end"):
            return [
                (
                    obj.start.isoformat()
                    if hasattr(obj.start, "isoformat")
                    else str(obj.start)
                ),
                obj.end.isoformat() if hasattr(obj.end, "isoformat") else str(obj.end),
            ]
        return super(TimestampEncoder, self).default(obj)


def upload_processed_data(df, data_type, bucket_name, region="eu-north-1"):
    """Upload processed PySpark DataFrame to S3 bucket"""
    try:
        # Convert PySpark DataFrame to pandas
        pandas_df = df.toPandas()

        # Convert to records format for JSON
        records = pandas_df.to_dict(orient="records")

        # Initialize S3 client
        s3_client = boto3.client("s3", region_name=region)

        # Save with timestamp (for history)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        history_key = f"processed_data/{data_type}/{timestamp}.json"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=history_key,
            Body=json.dumps(records, cls=TimestampEncoder),
            ContentType="application/json",
        )

        # Also save as 'latest' for the dashboard
        latest_key = f"processed_data/latest/{data_type}.json"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=latest_key,
            Body=json.dumps(records, cls=TimestampEncoder),
            ContentType="application/json",
        )

        print(f"✅ Uploaded {len(records)} {data_type} records to S3")
        return True

    except Exception as e:
        print(f"❌ Error uploading processed data to S3: {e}")
        import traceback

        traceback.print_exc()
        return False


def upload_to_s3(data, bucket_name, region="eu-north-1"):
    """Upload JSON data to S3 bucket"""
    try:
        # Initialize S3 client
        s3_client = boto3.client("s3", region_name=region)

        # Generate key with timestamp for organization
        timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
        s3_key = f"weather_data/{timestamp}.json"

        # Upload data to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(data, cls=TimestampEncoder),
            ContentType="application/json",
        )

        print(f"Uploaded data to s3://{bucket_name}/{s3_key}")
        return True

    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return False


# You can test this directly
if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    # Get configuration from environment
    bucket_name = os.getenv("AWS_BUCKET_NAME")
    region = os.getenv("AWS_REGION", "us-east-1")

    if not bucket_name:
        print("Error: AWS_BUCKET_NAME not found in .env file")
        exit(1)

    # Test data
    test_data = {"city": "Test", "temp": 20, "timestamp": datetime.now().isoformat()}

    # Upload test data
    upload_to_s3(test_data, bucket_name, region)
