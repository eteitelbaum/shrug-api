# This file will convert all the csv files in the data/raw directory to parquet files in the data/processed directory

import duckdb
import os

def csv_to_parquet_duckdb(input_dir, output_dir):
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Connect to DuckDB (in-memory database)
    con = duckdb.connect(':memory:')

    # Iterate through all files in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            print(f"Converting {filename}...")
            
            # Construct full file paths
            csv_path = os.path.join(input_dir, filename)
            parquet_path = os.path.join(output_dir, filename.replace('.csv', '.parquet'))
            
            # Convert CSV to Parquet using DuckDB
            con.execute(f"""
                COPY (SELECT * FROM read_csv_auto('{csv_path}'))
                TO '{parquet_path}' (FORMAT PARQUET)
            """)
            
            print(f"Converted {filename} to Parquet")

    print(f"Conversion complete for {input_dir}!")
    con.close()

def process_all_csv_folders(base_dir):
    raw_dir = os.path.join(base_dir, 'raw')
    processed_dir = os.path.join(base_dir, 'processed')
    
    # Find all directories in the raw folder
    csv_folders = [d for d in os.listdir(raw_dir) if os.path.isdir(os.path.join(raw_dir, d))]
    
    for csv_folder in csv_folders:
        input_dir = os.path.join(raw_dir, csv_folder)
        output_dir = os.path.join(processed_dir, csv_folder)
        
        print(f"Processing folder: {csv_folder}")
        csv_to_parquet_duckdb(input_dir, output_dir)

# Usage
base_directory = os.path.join(os.path.dirname(__file__), '..')  # Parent of the 'data' directory
data_directory = os.path.join(base_directory, 'data')
process_all_csv_folders(data_directory)