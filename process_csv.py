import pandas as pd
import gzip
import requests
import sqlite3
import logging
from io import BytesIO

# Configure logging
logging.basicConfig(
    filename="data_processing.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Constants
CSV_URL = "https://tyroo-engineering-assesments.s3.us-west-2.amazonaws.com/Tyroo-dummy-data.csv.gz"
DB_NAME = "data.db"
TABLE_NAME = "transformed_data"
CHUNK_SIZE = 10000

def download_csv_gz(url):
    try:
        logging.info(f"Downloading CSV file from {url}")
        response = requests.get(url)
        response.raise_for_status()
        return BytesIO(response.content)
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download file: {e}")
        raise

def create_table(cursor):
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            age INTEGER,
            email TEXT,
            country TEXT
        );
    ''')

def transform_data(chunk):
    logging.info("Raw columns: %s", list(chunk.columns))
    chunk.columns = chunk.columns.str.strip().str.lower().str.replace(" ", "_")
    logging.info("Normalized columns: %s", list(chunk.columns))

    # Try to infer real column names
    column_map = {}
    for col in chunk.columns:
        if 'name' in col:
            column_map['name'] = col
        if 'age' in col:
            column_map['age'] = col
        if 'email' in col:
            column_map['email'] = col
        if 'country' in col:
            column_map['country'] = col

    logging.info("Mapped columns: %s", column_map)

    if len(column_map) < 4:
        logging.warning("One or more required fields are missing, skipping chunk.")
        return pd.DataFrame(columns=['name', 'age', 'email', 'country'])

    # Apply transformation
    chunk[column_map['email']] = chunk[column_map['email']].str.lower().str.strip()
    chunk[column_map['age']] = pd.to_numeric(chunk[column_map['age']], errors='coerce')

    chunk = chunk.dropna(subset=[column_map['email'], column_map['age']])

    transformed = chunk.rename(columns={
        column_map['name']: 'name',
        column_map['age']: 'age',
        column_map['email']: 'email',
        column_map['country']: 'country'
    })[['name', 'age', 'email', 'country']]

    logging.info("Transformed chunk size: %d", len(transformed))
    return transformed

def process_csv(file_obj):
    try:
        with gzip.open(file_obj, 'rt') as f:
            reader = pd.read_csv(f, chunksize=CHUNK_SIZE)
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()
            create_table(cursor)

            total_inserted = 0
            for i, chunk in enumerate(reader):
                logging.info(f"Processing chunk {i + 1}")
                transformed = transform_data(chunk)
                if not transformed.empty:
                    transformed.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
                    total_inserted += len(transformed)
                else:
                    logging.warning(f"Chunk {i + 1} had no valid data.")

            conn.commit()
            conn.close()
            logging.info(f"✅ Done. Inserted total rows: {total_inserted}")
            print(f"✅ Done. Inserted total rows: {total_inserted}")
    except Exception as e:
        logging.error(f"Error during CSV processing: {e}")
        raise

if __name__ == "__main__":
    try:
        csv_file = download_csv_gz(CSV_URL)
        process_csv(csv_file)
    except Exception as e:
        logging.critical(f"Pipeline failed: {e}")
