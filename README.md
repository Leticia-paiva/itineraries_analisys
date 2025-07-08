# itineraries_ingestion_test

# Processing Large CSV Files in Memory-Constrained Environments

-----

This project addresses the challenge of processing a **35GB CSV file** (`itineraries.csv`) within a **4GB memory limit** for deployment in an ECS environment.

## The Challenge and Approach

Initially, I considered using **Apache Spark** due to my familiarity with the framework. While Spark performed adequately in local testing, optimizing it to fit within the strict 4GB memory constraint for ECS proved difficult. Even after extensive configuration adjustments (e.g., `driver.memory`, `executor.memory`, `shuffle.partitions`), Spark's baseline memory footprint was too high to meet the requirements.

Given that the task involved a one-time ingestion of a large CSV, I explored alternative solutions. **Pandas** was quickly ruled out as it's designed for in-memory data manipulation, making it unsuitable for datasets exceeding available RAM.

Ultimately, I found **DuckDB** to be the most effective solution for the initial phase of the project.

## Solution Overview

The solution involves the following steps:

1.  **Cloud Storage for Large File:** The 35GB `itineraries.csv` file is too large to be included directly in a Docker image. Therefore, it's stored in **Google Cloud Storage (GCS)**. I chose GCS due to existing free trial credits and its capacity for large files, exceeding the limits of Amazon S3's free tier.

2.  **CSV to Parquet Conversion with DuckDB:**

      * DuckDB is used to convert the large CSV file into a more memory-efficient **Parquet format**. This conversion significantly reduces the file size from 35GB to **8.4GB**.
      * To ensure compliance with the 4GB memory limit, DuckDB's `memory_limit` configuration is set, and the script runs within a **Docker container with a strict memory limit**.
      * A **bash script** is provided to monitor the Docker container's memory usage, verifying that the process stays within the defined limits.

3.  **BigQuery Integration:** Once the data is converted and stored in GCS as Parquet, an **external table** is created in **Google BigQuery** on top of the Parquet file. This allows for efficient analysis of the data directly in BigQuery without needing to load it into memory.

## How to Run

To run this project, follow these steps:

1.  **Clone this repository:**

2.  **GCP Permissions:** Ensure your Google Cloud Platform (GCP) service principal has all the necessary permissions to access Google Cloud Storage and BigQuery.

3.  **Create `.env` file:** Create a file named `my_env.env` in the root directory of the repository with your GCP credentials:

    ```
    export GCS_ACCESS_KEY_ID=YOUR_GCS_ACCESS_KEY_ID
    export GCS_SECRET_ACCESS_KEY=YOUR_GCS_SECRET_ACCESS_KEY
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/google_credentials.json # Path to your service account key file
    export BIG_QUERY_PROJECT=YOUR_BIG_QUERY_PROJECT_ID
    ```

    **Note:** Replace the placeholder values with your actual GCP credentials and project ID.

4.  **Run with Docker Compose:** Execute the following command to build and run the Docker containers:

    ```bash
    sudo docker compose up --build
    ```

5.  **Monitor Memory Usage (Optional):** To check the memory consumption of the Docker container, run the following commands in a separate terminal:

    ```bash
    chmod +x docker_monitor.sh
    ./docker_monitor.sh
    ```

## Big query Data Flow and Transformations
![image](https://github.com/user-attachments/assets/1b049106-6cd5-460f-aaee-769d54907b5a)


The data flows through several stages, starting from a raw input table and progressing to cleaned tables, a fact table, a dimension table, and finally, analytical views.

1. **`itineraries_raw`**: The initial source table containing raw, unvalidated flight itinerary data.

2. **`itinerary_cleaned_table`**: Cleans and de-duplicates the raw data, ensuring data quality and proper data types.

3. **`itineraries_fact_table`**: Serves as the central fact table for itineraries, introducing a surrogate key and a flag for current records.

4. **`itinerary_dimension_segments`**: A dimension table derived from `itinerary_cleaned_table`, specifically designed to handle and unnest the array-like segment data.

5. **`price_analysis` (View)**: An analytical view for understanding price changes over time for specific itineraries.

6. **`flights_type_analysis` (View)**: An analytical view for summarizing different types of flights (non-stop, basic economy) by route and date.

## Database Schema Details

### Source Table: `dbt-project-459000.dw_itineraries.itineraries_raw`

* **Purpose:** This table holds the raw, untransformed data directly ingested into the BigQuery dataset. It is the starting point for all subsequent data processing.

* **Key Characteristics:** Contains all original columns as they appear in the source.

### Cleaned Table: `dbt-project-459000.dw_itineraries.itinerary_cleaned_table`

* **Purpose:** This table is the first step in cleaning the raw data. It selects distinct records and filters out rows where critical identifying or date fields are NULL. It also parses `searchDate` and `flightDate` into proper `DATE` types.

* **Transformation:**

  * `DISTINCT`: Ensures uniqueness of records based on all selected columns.

  * `SAFE.PARSE_DATE('%Y-%m-%d', ...)`: Converts string date fields (`searchDate`, `flightDate`) into `DATE` data types, handling potential parsing errors gracefully by returning `NULL` for invalid dates.

  * `WHERE` clauses: Filters out records with `NULL` values in `legId`, parsed `searchDate`, parsed `flightDate`, `startingAirport`, `destinationAirport`, and segment-related raw fields, ensuring data completeness for core analysis.

* **New or Transformed Columns:**

  * `searchDate` (DATE): Transformed from STRING to DATE.

  * `flightDate` (DATE): Transformed from STRING to DATE.

### Fact Table: `dbt-project-459000.dw_itineraries.itineraries_fact_table`

* **Purpose:** This table serves as the central fact table for flight itineraries. It contains the core measures and key identifiers for each itinerary. It also implements a form of Slowly Changing Dimension (SCD) Type 2 by tracking the "current" state of an itinerary based on the latest `searchDate`.

* **Transformation:**

  * `is_current` (BOOLEAN): A flag generated using `ROW_NUMBER()` partitioned by `flightDate`, `startingAirport`, `destinationAirport`, and `legId`, ordered by `searchDate DESC`. This identifies the most recent search record for a given flight itinerary, useful for analyzing current prices.

  * `itinerary_sk` (STRING): A surrogate key generated by concatenating `legId`, formatted `searchDate`, formatted `flightDate`, `startingAirport`, and `destinationAirport`. This provides a unique, stable identifier for each specific itinerary search instance.

* **New or Transformed Columns:**

  * `is_current` (BOOLEAN): Indicates if this is the most recent record for the specific itinerary.

  * `itinerary_sk` (STRING): Surrogate key for the itinerary.

### Dimension Table: `dbt-project-459000.dw_itineraries.itinerary_dimension_segments`

* **Purpose:** This dimension table stores detailed information about individual flight segments within each itinerary. It addresses the challenge of `segments` data being stored as pipe-separated strings in the raw and cleaned tables by unnesting them into separate rows.

* **Transformation:**

  * `SPLIT(..., '||')`: Converts the pipe-separated string fields (e.g., `segmentsDepartureTimeEpochSeconds`) into arrays of strings.

  * `UNNEST(...) WITH OFFSET AS idx`: Flattens these arrays, creating a new row for each segment within an itinerary. `idx` provides the segment's order.

  * `segment_sk` (STRING): A surrogate key for each individual segment, combining the `itinerary_sk` with the `segment_index`.

  * Individual `segment...` columns: Extracts specific segment attributes (e.g., `segmentDepartureTimeRaw`, `segmentAirlineName`) from the unnested arrays.

* **New or Transformed Columns:**

  * `itinerary_sk` (STRING): Foreign key linking to `itineraries_fact_table`.

  * `segment_sk` (STRING): Unique surrogate key for each flight segment.

  * `segment_index` (INT64): The 0-based index of the segment within the itinerary.

  * `segmentDepartureTimeEpochSeconds` (STRING), `segmentDepartureTimeRaw` (STRING), `segmentArrivalTimeEpochSeconds` (STRING), `segmentArrivalTimeRaw` (STRING), `segmentArrivalAirportCode` (STRING), `segmentDepartureAirportCode` (STRING), `segmentAirlineName` (STRING), `segmentAirlineCode` (STRING), `segmentEquipmentDescription` (STRING), `segmentDurationInSeconds` (STRING), `segmentDistance` (STRING), `segmentCabinCode` (STRING): These are the unnested and extracted details for each individual segment.

### Analytical View: `dbt-project-459000.dw_itineraries.price_analysis`

* **Purpose:** This view provides insights into how flight prices change over time for specific itineraries. It calculates price changes relative to the oldest recorded price and the immediately previous recorded price.

* **Logic:**

  * **`ranked_and_compared_data` CTE:**

    * Uses `FIRST_VALUE()` to get the `totalFare` from the earliest `searchDate` for a given `legId`, `flightDate`, `startingAirport`, `destinationAirport` (i.e., the "oldest" price).

    * Uses `LAG()` to get the `totalFare` from the previous `searchDate` for the same itinerary.

  * **`joined_and_aggregated_data` CTE:**

    * Joins with `itinerary_dimension_segments` on `itinerary_sk` to include detailed flight segment information.

    * Uses `ARRAY_AGG()` to group the segment details back into an array for each itinerary, ordered by `segment_index`.

  * **Final SELECT:**

    * Calculates `price_went_up_vs_oldest`, `price_went_down_vs_oldest` based on comparison with `oldest_total_fare`.

    * Calculates `price_change_vs_previous` (HIGHER, LOWER, SAME, N/A) based on comparison with `previous_total_fare`.

    * Identifies `price_down_and_low_seats_vs_oldest` for potential deals.

* **New or Transformed Columns:**

  * `oldest_total_fare` (FLOAT64): The `totalFare` from the earliest `searchDate` for a given itinerary.

  * `previous_total_fare` (FLOAT64): The `totalFare` from the immediately preceding `searchDate` for a given itinerary.

  * `flight_segments_details` (ARRAY of STRUCT): An array containing structured details for each flight segment within an itinerary.

  * `price_went_up_vs_oldest` (BOOLEAN): Indicates if the current `totalFare` is higher than the `oldest_total_fare`.

  * `price_went_down_vs_oldest` (BOOLEAN): Indicates if the current `totalFare` is lower than the `oldest_total_fare`.

  * `price_change_vs_previous` (STRING): Categorizes the price change compared to the `previous_total_fare` as 'HIGHER', 'LOWER', 'SAME', or 'N/A'.

  * `price_down_and_low_seats_vs_oldest` (BOOLEAN): Indicates if the price has gone down compared to the oldest price AND there are less than 10 seats remaining.

### Analytical View: `dbt-project-459000.dw_itineraries.flights_type_analysis`

* **Purpose:** This view aggregates and summarizes the counts of different flight types (non-stop, basic economy) for each unique flight route and date. It focuses only on the `is_current` itineraries.

* **Logic:**

  * Groups data by `flightDate`, `startingAirport`, and `destinationAirport`.

  * Uses `COUNT(CASE WHEN ... THEN 1 END)` to count occurrences of specific flight characteristics (e.g., `isNonStop`, `isBasicEconomy`).

  * Filters for `is_current is true` to ensure analysis is based on the most recent itinerary data.

* **New or Transformed Columns:**

  * `non_stop_flights` (INT64): Count of non-stop flights within the group.

  * `not_non_stop_flights` (INT64): Count of flights with stops within the group.

  * `basic_economy_flights` (INT64): Count of basic economy flights within the group.

  * `not_basic_economy_flights` (INT64): Count of non-basic economy flights within the group.

  * `non_stop_economic_flights` (INT64): Count of non-stop and basic economy flights within the group.

  * `stop_economic_flights` (INT64): Count of flights with stops and basic economy within the group.

  * `non_stop_not_economic_flights` (INT64): Count of non-stop and non-basic economy flights within the group.

  * `stop_not_economic_flights` (INT64): Count of flights with stops and non-basic economy within the group.

  * `total_flights_in_group` (INT64): Total number of flights considered in the current grouping.

## Relationships and Data Model Design

The data model follows a star-like schema approach:

* **Fact Table (`itineraries_fact_table`):** Contains the core numerical data (fares, seats remaining, duration) and foreign keys to dimensions.

* **Dimension Table (`itinerary_dimension_segments`):** Provides descriptive attributes for each flight segment, which can be joined with the fact table via `itinerary_sk`.

**Key Relationships:**

* **One-to-Many (`itineraries_fact_table` to `itinerary_dimension_segments`):** One record in the `itineraries_fact_table` can correspond to multiple segment records in `itinerary_dimension_segments` (one for each segment in the itinerary). This relationship is established through the `itinerary_sk` column.

## Design Decisions and Rationale

* **Data Cleaning and Validation:** The `itinerary_cleaned_table` step is crucial for ensuring data quality, handling missing values, and standardizing data types before further processing.

* **Surrogate Keys (`itinerary_sk`, `segment_sk`):** These are used to provide stable, unique identifiers for records, independent of the source system's natural keys. This is particularly important for tracking changes over time and for efficient joining.

* **Slowly Changing Dimension (SCD) Type 2 (`is_current`):** The `is_current` flag in `itineraries_fact_table` allows for historical tracking of itinerary attributes. When an itinerary's details (like price) change, a new record is inserted, and the `is_current` flag helps identify the most recent state.

* **Unnesting Array-like Data:** The `itinerary_dimension_segments` table effectively handles the denormalized, pipe-separated segment data by unnesting it. This makes individual segment attributes easily queryable and analyzable, adhering to dimensional modeling principles where detailed attributes are stored in dimension tables.

* **Analytical Views:** `price_analysis` and `flights_type_analysis` are created as views to provide pre-calculated, aggregated, or transformed data for specific analytical purposes without materializing the data into new tables. This saves storage and ensures the analysis is always based on the latest underlying data.
