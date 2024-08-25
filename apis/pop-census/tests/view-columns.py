import pandas as pd
#import os

# Get the project root directory
#project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

# Construct the path to the Parquet file
#file_path = os.path.join(project_root, 'data', 'processed', 'shrug-vd91-parquet', 'pc91_vd_clean_shrid.parquet')

file_path = 'data/processed/shrug-pca01-parquet/pc01_pca_clean_con08.parquet'

# Read the Parquet file
df = pd.read_parquet(file_path)

# Print the column names
#print(df.columns.tolist())

print(df[['pc01_pca_tot_p', 'pc01_pca_tot_m']])