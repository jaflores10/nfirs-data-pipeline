# The below script will define where NFIRS DuckDB database will exist, create and connect to the database
# create a schema called nfirs and process the NFIRS text files into their own database tables.

import os
import duckdb
import pandas as pd

# Get current script directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Define database and data folder paths
db_path = os.path.join(current_dir, '..', 'nfirs_database', 'nfirs.duckdb')
data_folder = os.path.join(current_dir, '..', 'nfirs_data')

# Connect to DuckDB and create the database
conn = duckdb.connect(database=db_path, read_only=False)

# Create the 'nfirs_raw' schema in the 'nfirs' database
conn.execute("CREATE SCHEMA IF NOT EXISTS nfirs_raw")

# Get list of text files in the data folder
text_files = [f for f in os.listdir(data_folder) if f.endswith('.txt')]

# Iterate over the text files in the data folder
for file in text_files:
    file_path = os.path.join(data_folder, file)
    table_name = os.path.splitext(file)[0] # Retrieve the file name without the extension

    # Read the text file into a pandas Dataframe
    df = pd.read_csv(file_path, delimiter='^', encoding='iso-8859-1', dtype=str)

    # Write the Dataframe to DuckDB, with all columns as VARCHAR
    conn.execute(f"""
        CREATE TABLE nfirs_raw.{table_name} AS 
        SELECT {', '.join([f'CAST({col} AS VARCHAR) AS {col}' for col in df.columns])}
        FROM df
    """)
    print(f" Table '{table_name} created and data inserted.")

# Close the connection
conn.close()

print("All text files have been processed and imported into DuckDB")