import requests
import lzma
import tarfile
import os

# Define file paths and URL
url = "https://mtgjson.com/api/v5/AllPrintingsParquetFiles.tar.xz"
xz_file_path = "/workspaces/mtg_app/data/AllPrintingsParquetFiles.tar.xz"
extract_dir = "/workspaces/mtg_app/data/parquet_files"

# Download and save the file
response = requests.get(url)
with open(xz_file_path, "wb") as xz_file:
    xz_file.write(response.content)

# Unzip the .xz file
with lzma.open(xz_file_path) as xz_file:
    # Extract the tar contents to the specified directory
    with tarfile.open(fileobj=xz_file) as tar:
        # Ensure the directory exists
        os.makedirs(extract_dir, exist_ok=True)
        tar.extractall(path=extract_dir)

print(f"Parquet files extracted to {extract_dir}")
