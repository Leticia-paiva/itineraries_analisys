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
