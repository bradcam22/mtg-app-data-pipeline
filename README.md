# MTG Data Pipeline

This project maintains a data pipeline that processes Magic: The Gathering card data.

## Data Flow
- Source: [mtgjson.com](https://mtgjson.com/)
- Processing: [DuckDB](https://duckdb.org/) for Parquet file handling
- Storage: PostgreSQL [Supabase](https://supabase.com/)
- Optimization: Materialized views for efficient frontend queries

**Scheduling**: Daily updates via GitHub Actions.

## Setup & Usage
This project leverages dev containers for development. You will need to install [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) in VSCode or any editor that supports it. 

### Environment Configuration
This project requires the following environment variables:
- `POSTGRES_URL`: Connection string for the database
  > **Note**: When using Supabase, remove the `supa=base-pooler.x` parameter from the connection URL. Logic is implemented to handle this in the pipeline. 
- `DISCORD_WEBHOOK_URL`: URL for the Discord webhook (optional)
