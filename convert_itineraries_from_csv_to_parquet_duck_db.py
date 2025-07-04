import duckdb
import os
import time 

def main():
    duck_time = time.time() 
    key_id = os.environ.get('GCS_ACCESS_KEY_ID')
    private_key = os.environ.get('GCS_SECRET_ACCESS_KEY')

    if not key_id or not private_key:
        raise ValueError("unable to acess ACESS_KEY and SECRET")

    csv_path = "gs://itineraries_airflow/itineraries.csv"
    parquet_path = "gs://itineraries_airflow/bronze/itineraries_duckdb.parquet"

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
        duck_time = time.time() - duck_time

        print(f"Duckdb time: {duck_time:.2f} seconds")
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.close()

    conn.close()
    print("\nDuckDB connection closed.")

if __name__ == "__main__":
    main()
