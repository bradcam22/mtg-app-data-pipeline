import duckdb

# Connect to DuckDB
con = duckdb.connect("/workspaces/mtg_app/db/mtg.duckdb")

# Load JSON directly into DuckDB from the file
con.execute("""
    CREATE TABLE cards AS
    SELECT * FROM read_json_auto('/workspaces/mtg_app/data/src_cards.json', maximum_object_size=1000000000);
""")

# Query the DuckDB database
result = con.execute("SELECT * FROM cards LIMIT 10").fetchdf()

# Print the result
print(result)
