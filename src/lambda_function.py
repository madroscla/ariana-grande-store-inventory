import boto3
import os
from botocore.config import Config
from io import StringIO

import pandas as pd

import shopify_scrape

def lambda_handler(event, context):
    """Appends new data to CSV file in S3 bucket."""
    json_url = os.environ['json_url']
    base_url = os.environ['base_url']
    access_key = os.environ['access_key']
    access_secret_key = os.environ['access_secret_key']
    bucket_name = os.environ['bucket_name']
    s3_file_name = os.environ['s3_file_name']
    
    new_df = shopify_scrape.get_product_info(json_url, base_url)

    # Connect to S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=access_secret_key,
        config=Config(signature_version='s3v4')
    )

    old_csv = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
    old_csv_data = old_csv['Body'].read().decode('utf-8')
    
    old_df = pd.read_csv(StringIO(old_csv_data))
    
    combined_df = pd.concat([old_df, new_df], ignore_index=True)
    
    new_csv = combined_df.to_csv(index=False)
    s3_client.put_object(Bucket=bucket_name, Key=s3_file_name, Body= new_csv)