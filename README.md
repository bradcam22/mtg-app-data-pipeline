# MTG Data Pipeline

This project maintains a data pipeline that processes Magic: The Gathering card data.

## Data Flow
- Source: [mtgjson.com](https://mtgjson.com/)
- Storage: PostgreSQL database [Supabase](https://supabase.com/)
- Optimization: Materialized views for efficient frontend queries
- Processing: [DuckDB](https://duckdb.org/) for Parquet file handling

## Technical Details
- **Scheduling**: Daily updates via GitHub Actions
- **Data Processing**: DuckDB is utilized for:
  - Fast, in-memory processing
  - Automatic PostgreSQL schema handling
  - Efficient Parquet file operations

## Setup & Usage
This project leverages dev containers for development. You will need to install [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) in VSCode or any editor that supports it.

### Environment Configuration
This project requires the following environment variables:
- `POSTGRES_URL`: Connection string for the database
  > **Note**: When using Supabase, remove the `sslmode=require` parameter from the connection URL. Logic is implemented to handle this in the pipeline. 
