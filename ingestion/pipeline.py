import duckdb
import os
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from urllib.parse import urlparse
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def setup_logs():
    """Configure logging with both console and file handlers."""
    os.makedirs('logs', exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            RotatingFileHandler(
                filename='logs/ingestion.log',
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5,
                encoding='utf-8'
            )
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logs()

def setup_db_connections():
    """Setup database connections and return necessary connection objects."""
    load_dotenv()
    db_url = os.getenv("POSTGRES_URL")
    if not db_url:
        raise ValueError("POSTGRES_URL environment variable not set")
        
    parsed = urlparse(db_url)
    clean_url = f"postgresql://{parsed.username}:{parsed.password}@{parsed.hostname}:{parsed.port}{parsed.path}?sslmode=require"
    
    duck = duckdb.connect(':memory:')
    return duck, clean_url

def fetch_parquet_data(duck):
    """Fetch parquet data into DuckDB tables."""
    try:
        logger.info("Fetching parquet data...")
        duck.sql("CREATE TABLE sets AS SELECT * FROM read_parquet('https://mtgjson.com/api/v5/parquet/sets.parquet')")
        duck.sql("CREATE TABLE cards AS SELECT * FROM read_parquet('https://mtgjson.com/api/v5/parquet/cards.parquet')")
        logger.info("Parquet data fetched successfully")
    except Exception as e:
        logger.error(f"Error fetching parquet data: {e}")
        raise

def transfer_to_postgres(duck, clean_url):
    """Transfer data from DuckDB to PostgreSQL."""
    try:
        logger.info("Transferring data to Postgres...")
        duck.sql(f"""
            INSTALL postgres;
            LOAD postgres;
            ATTACH '{clean_url}' AS postgres_db (TYPE postgres);
            
            DROP TABLE IF EXISTS postgres_db.sets CASCADE;
            DROP TABLE IF EXISTS postgres_db.cards CASCADE;
            
            CREATE TABLE postgres_db.sets AS SELECT * FROM sets;
            CREATE TABLE postgres_db.cards AS SELECT * FROM cards;
        """)
        logger.info("Data transferred to Postgres successfully")
    except Exception as e:
        logger.error(f"Error transferring data to Postgres: {e}")
        raise

def create_views(clean_url):
    """Create database views in PostgreSQL."""
    try:
        logger.info("Creating views...")
        conn = psycopg2.connect(clean_url)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        with open('ingestion/views.sql', 'r') as file:
            views_sql = file.read()
            cur.execute(views_sql)
            
        logger.info("Views created successfully")
    except Exception as e:
        logger.error(f"Error creating views: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def main():
    """Main execution function."""
    try:
        duck, clean_url = setup_db_connections()
        fetch_parquet_data(duck)
        transfer_to_postgres(duck, clean_url)
        create_views(clean_url)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        if duck:
            duck.close()

if __name__ == "__main__":
    main()
