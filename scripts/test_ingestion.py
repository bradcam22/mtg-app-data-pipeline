import duckdb
import os
from dotenv import load_dotenv
from urllib.parse import urlparse
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

load_dotenv()
db_url = os.getenv("POSTGRES_URL")

# Keep only the essential connection parameters
parsed = urlparse(db_url)
clean_url = f"postgresql://{parsed.username}:{parsed.password}@{parsed.hostname}:{parsed.port}{parsed.path}?sslmode=require"

# Connect to DuckDB (in-memory)
duck = duckdb.connect(':memory:')

# Create tables in DuckDB from the parquet files
duck.sql("CREATE TABLE sets AS SELECT * FROM read_parquet('https://mtgjson.com/api/v5/parquet/sets.parquet')")
duck.sql("CREATE TABLE cards AS SELECT * FROM read_parquet('https://mtgjson.com/api/v5/parquet/cards.parquet')")

# Export schema and data to PostgreSQL
duck.sql(f"""
    INSTALL postgres;
    LOAD postgres;
    ATTACH '{clean_url}' AS postgres_db (TYPE postgres);
    
    DROP TABLE IF EXISTS postgres_db.sets CASCADE;
    DROP TABLE IF EXISTS postgres_db.cards CASCADE;
    
    CREATE TABLE postgres_db.sets AS SELECT * FROM sets;
    CREATE TABLE postgres_db.cards AS SELECT * FROM cards;
""")

# Now connect directly to Postgres for the view creation
conn = psycopg2.connect(clean_url)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()

# Read and execute the views SQL
with open('scripts/test_recreate_views.sql', 'r') as file:
    views_sql = file.read()
    cur.execute(views_sql)

cur.close()
conn.close()

print("Data transferred and views recreated successfully")