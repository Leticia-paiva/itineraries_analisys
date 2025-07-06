import duckdb
import os
import time 
from google.cloud import bigquery

bigquery_dw = "dw_itineraries"
storage_bucket = "flight_itineraries_project"

def convert_itineraries_from_csv_to_parquet_duck_db():
    duck_time = time.time() 
    key_id = os.environ.get('GCS_ACCESS_KEY_ID')
    private_key = os.environ.get('GCS_SECRET_ACCESS_KEY')

    if not key_id or not private_key:
        raise ValueError("unable to acess ACESS_KEY and SECRET")

    csv_path = f"gs://{storage_bucket}/itineraries.csv"
    parquet_path = f"gs://{storage_bucket}/bronze/itineraries_raw.parquet"

    conn = duckdb.connect(database=':memory:')

    try:
        conn.install_extension("httpfs")
        conn.load_extension("httpfs")

        conn.execute(f"""
            CREATE SECRET (
                TYPE gcs,
                KEY_ID '{key_id}',
                SECRET '{private_key}'
            );
        """)

        print(f"Starting conversion from CSV: {csv_path} to parquet: {parquet_path}")
        conn.execute(f"SET memory_limit = '4GB';")
        conn.execute(f"SET enable_progress_bar = true;")
        conn.execute(f"""
            COPY (
                SELECT * FROM read_csv_auto('{csv_path}', HEADER=TRUE)
            ) TO '{parquet_path}' (FORMAT PARQUET);
        """)
        print("Successfully wrote parquet table!")
        duck_time = int(time.time() - duck_time)/60

        print(f"Process time: {duck_time} minutes")
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.close()

    conn.close()
    print("\nDuckDB connection closed.")

def create_external_table_bigquery():
    client = bigquery.Client()
    project = os.environ.get('BIG_QUERY_PROJECT')
    table_id = f"{project}.{bigquery_dw}.itineraries_raw"

    external_source_format = "PARQUET"
    source_uris = [
        f"gs://{storage_bucket}/bronze/itineraries_raw.parquet",
    ]
    print(f'Creating external table {table_id} on big query')
    external_config = bigquery.ExternalConfig(external_source_format)
    external_config.source_uris = source_uris

    table = bigquery.Table(table_id)
    table.external_data_configuration = external_config
    client.create_dataset(bigquery_dw, exists_ok=True)
    table = client.create_table(table,exists_ok=True)

    print(
        f"Created table with external source format {table.external_data_configuration.source_format}"
    )

def main():
    convert_itineraries_from_csv_to_parquet_duck_db()
    create_external_table_bigquery()

if __name__ == "__main__":
    main()
