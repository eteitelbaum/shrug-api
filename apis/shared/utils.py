import json
import os
import boto3

def get_data_path(data_type, year, aggregation_level, api_name='pop-census'):
    config_path = os.path.join(os.path.dirname(__file__), '..', api_name, 'data-config.json')
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    file_config = next((file for file in config['data_files'] if file['type'] == data_type), None)
    
    if not file_config:
        raise ValueError(f"Configuration for data type {data_type} not found")
    
    if year not in file_config['years']:
        raise ValueError(f"Year {year} not available for data type {data_type}")
    
    available_levels = file_config['available_levels'][str(year)]
    if aggregation_level not in available_levels:
        raise ValueError(f"Aggregation level {aggregation_level} not available for data type {data_type} in year {year}")
    
    year_short = str(year)[-2:]
    filename = file_config['aggregation_levels'][aggregation_level].format(year_short=year_short)
    path = file_config['path_template'].format(year=year, year_short=year_short, filename=filename)
    
    if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ:
        s3_path = f"s3://your-bucket/census/{os.path.basename(path)}"
        return s3_path
    else:
        config_dir = os.path.dirname(config_path)
        return os.path.abspath(os.path.join(config_dir, path))

def get_table_name(data_type, year, aggregation_level, api_name='pop-census'):
    config_path = os.path.join(os.path.dirname(__file__), '..', api_name, 'data-config.json')
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    file_config = next((file for file in config['data_files'] if file['type'] == data_type), None)
    
    if not file_config:
        raise ValueError(f"Configuration for data type {data_type} not found")
    
    year_short = str(year)[-2:]
    filename = file_config['aggregation_levels'][aggregation_level].format(year_short=year_short)
    return os.path.splitext(filename)[0]

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