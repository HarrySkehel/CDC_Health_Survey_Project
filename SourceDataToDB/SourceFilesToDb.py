import zipfile
import pandas as pd
from sqlalchemy import create_engine
import os
import glob
import polars as pl
import psycopg2
'''
# Path to your zip file
zip_path = '/Users/harryskehel/Python/CDC_SourceData/Archive.zip'
extract_path = '/Users/harryskehel/Python/CDC_SourceData/extracted/'

# Unzip
with zipfile.ZipFile(zip_path, 'r') as z:
    z.extractall(extract_path)
    print('Files unzipped to :', extract_path)


df = pl.read_csv('/Users/harryskehel/Python/CDC_SourceData/extracted/2011.csv', n_rows=5)
print(df.columns)
'''

import psycopg2



# Connect to Postgres
conn = psycopg2.connect(
    dbname='cdc_health_survey',
    user='harryskehel',
    host='localhost',
    port='5432'
)
conn.autocommit = True
cur = conn.cursor()

extract_path = '/Users/harryskehel/Python/CDC_SourceData/extracted/'
'''
for file in glob.glob(extract_path + '*.csv'):
    year = os.path.basename(file).replace('.csv', '')
    table = f'survey_{year}'
    print(f'Loading {year}...')

    # Get columns and dtypes from first 5 rows
    df = pl.read_csv(file, n_rows=5, infer_schema_length=5)
    
    # Build CREATE TABLE statement
    col_defs = []
    for col, dtype in zip(df.columns, df.dtypes):
        if 'Float' in str(dtype):
            col_defs.append(f'"{col}" FLOAT')
        else:
            col_defs.append(f'"{col}" TEXT')
    
    cur.execute(f'DROP TABLE IF EXISTS {table}')
    cur.execute(f'CREATE TABLE {table} ({", ".join(col_defs)})')
    
    # Use COPY for fast loading
    with open(file, 'r') as f:
        next(f)  # skip header
        cur.copy_expert(f"COPY {table} FROM STDIN WITH CSV", f)
    
    print(f'Done - {year}')

cur.close()
conn.close()
print('All done!')

'''


conn = psycopg2.connect(
    dbname='cdc_health_survey',
    user='harryskehel',
    host='localhost',
    port='5432'
)
cur = conn.cursor()

for year in range(2011, 2016):
    cur.execute(f'SELECT COUNT(*) FROM survey_{year}')
    count = cur.fetchone()[0]
    print(f'survey_{year}: {count} rows')

cur.close()
conn.close()