import boto3
from configparser import ConfigParser
import csv
from io import StringIO

config = ConfigParser()
config.read("config.ini")

access_key = config["aws"]["access_key"]
secret_key = config["aws"]["secret_key"]

s3 = boto3.client(
    "s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

def get_csv_files_from_s3(bucket_name, prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".csv")
    ]

def get_product_map(bucket_name, key):
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    content = obj["Body"].read().decode("utf-8")
    reader = csv.DictReader(StringIO(content))
    return {row["product_id"]: int(row["price"]) for row in reader}

def upload_to_s3(bucket_name, key, body_bytes):
    s3.put_object(Bucket=bucket_name, Key=key, Body=body_bytes)
