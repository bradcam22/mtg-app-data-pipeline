import duckdb
import os

# Path to the directory containing Parquet files
parquet_dir = "/workspaces/mtg_app/data/parquet_files/AllPrintingsParquetFiles/"

# Connect to DuckDB
con = duckdb.connect("/workspaces/mtg_app/db/mtg.duckdb")

# Loop through each file in the parquet directory
for file_name in os.listdir(parquet_dir):
    if file_name.endswith(".parquet"):
        # Get the file path
        parquet_file_path = os.path.join(parquet_dir, file_name)
        
        # Use the file name (without extension) as the table name
        table_name = os.path.splitext(file_name)[0]

        # Create or replace the table with the new data
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_parquet('{parquet_file_path}');
        """)
        
        print(f"Table '{table_name}' updated with data from {file_name}")

# Example query: List all tables
tables = con.execute("SHOW TABLES").fetchdf()
print("Tables in DuckDB:")
print(tables)
