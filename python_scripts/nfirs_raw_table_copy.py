import duckdb
# This script will copy all tables from the 'nfirs_raw' schema to the 'nfirs_transformed' schema
# For each table in 'nfirs_transformed,' dbt will be used to transform columns to the correct data type
# Connect to your DuckDB database
con = duckdb.connect(f'C:\\Users\\javier.flores\\Desktop\\Portfolio\\NFIRS Data Pipeline and Analysis\\nfirs_database\\nfirs.duckdb')

# List all tables in the 'nfirs_raw' schema
tables_query = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'nfirs_raw'
"""

# fetchall() will store the table names
tables = con.execute(tables_query).fetchall()

# Execute the copy query to copy tables from 'nfirs_raw' to 'nfirs_transformed'
for table in tables:
    table_name = table[0]
    copy_query = f"""
    CREATE TABLE nfirs_transformed.{table_name}_transformed AS 
    SELECT * FROM nfirs_raw.{table_name};
    """
    con.execute(copy_query)

# Close the connection
con.close()