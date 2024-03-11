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
    
    size_check = old_csv['ResponseMetadata']['HTTPHeaders']['content-length']
    
    # Checks if file is over 50 MB
    if int(size_check) >= 52428800:
        bucket = s3_client.list_objects(Bucket=bucket_name)
        names = [bucket['Contents'][index]['Key'] for index, _ in enumerate(bucket['Contents'])]
        names_list = [name[:-4] for name in names]
        base_file_name = s3_file_name[:-4]
        
        new_file_name = None
        index = 1
        while base_file_name + str(index) in names_list:
            index += 1
        else:
            new_file_name = base_file_name + str(index) + '.csv'
        
        archived_csv = old_df.to_csv(index=False)
        s3_client.put_object(Bucket=bucket_name, Key=new_file_name, Body=archived_csv)
        
        new_csv = new_df.to_csv(index=False)
        s3_client.put_object(Bucket=bucket_name, Key=s3_file_name, Body= new_csv)
    else:
        combined_df = pd.concat([old_df, new_df], ignore_index=True)
        new_csv = combined_df.to_csv(index=False)
        s3_client.put_object(Bucket=bucket_name, Key=s3_file_name, Body= new_csv)
    