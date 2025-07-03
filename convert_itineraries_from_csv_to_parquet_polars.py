import polars as pl
import os
import time 

def main():
    start_time = time.time() 
    gcs_access_key_id = os.environ.get('GCS_ACCESS_KEY_ID')
    gcs_secret_access_key = os.environ.get('GCS_SECRET_ACCESS_KEY')

    if not gcs_access_key_id or not gcs_secret_access_key:
        raise ValueError(
            "GCS_ACCESS_KEY_ID and GCS_SECRET_ACCESS_KEY environment "
            "variables must be set for GCS authentication."
        )

    # Define your GCS paths
    csv_path = "gs://itineraries_airflow/itineraries.csv"
    parquet_path = "gs://itineraries_airflow/bronze/itineraries_polars.parquet"

    print(f"Starting conversion from CSV: {csv_path} to parquet: {parquet_path}")

    try:
        # Read CSV from GCS using Polars with lazy evaluation (scan_csv).
        # This is crucial for large files (like 34GB) and strict memory limits (like 4GB),
        # as it avoids loading the entire dataset into memory at once.
        # Polars will optimize the reading and writing process internally.
        # The GCS_ACCESS_KEY_ID and GCS_SECRET_ACCESS_KEY environment variables
        # are picked up by gcsfs for authentication.
        
        # Use scan_csv for lazy loading.
        # infer_schema_length can be adjusted to read more rows for schema inference,
        # but for very large files, it's often better to explicitly define dtypes
        # if schema inference is too slow or memory intensive.
        lf = pl.scan_csv(csv_path, infer_schema_length=1000) 

        print("\nSchema of the CSV data (inferred from a sample):")
        # To show the schema, we can collect a small sample or rely on the lazy frame's schema
        # print(lf.schema)

        # Write DataFrame to Parquet in GCS.
        # The .collect() method triggers the computation and writes the data.
        # Polars will handle this efficiently for large files when using scan_csv.
        lf.sink_parquet(parquet_path,        compression="snappy",
        row_group_size=100_000)
        print("\nSuccessfully wrote parquet table!")
        polar_time = time.time() - start_time

        print(f"Polar time: {polar_time:.2f} seconds")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("\nPolars operation completed.")

if __name__ == "__main__":
    main()
