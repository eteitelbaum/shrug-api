import json
import os
import boto3

def get_data_path(filename, api_name='pop-census'):
    # Determine the path to data-config.json
    # This will look for the config file in the specific API directory
    config_path = os.path.join(os.path.dirname(__file__), '..', api_name, 'data-config.json')
    
    # Load the data config
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Find the file configuration
    file_config = next((file for file in config['data_files'] if file['filename'] == filename), None)
    
    if not file_config:
        raise ValueError(f"Configuration for file {filename} not found")
    
    # Check if we're running in an AWS Lambda environment
    if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ:
        return file_config['s3_path']
    else:
        # Calculate the absolute path based on the location of the config file
        config_dir = os.path.dirname(config_path)
        return os.path.abspath(os.path.join(config_dir, file_config['local_path']))

def is_s3_path(path):
    return path.startswith('s3://')

def read_parquet_file(file_path):
    if is_s3_path(file_path):
        # Extract bucket and key from S3 path
        s3_path = file_path[5:]  # Remove "s3://"
        bucket, key = s3_path.split('/', 1)
        
        # Use boto3 to read from S3
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj['Body'].read()
    else:
        # Read local file
        with open(file_path, 'rb') as f:
            return f.read()