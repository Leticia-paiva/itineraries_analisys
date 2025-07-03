# Use an official Python runtime as a parent image.
# We choose a slim-buster image to keep the image size smaller.
FROM python:3.9-slim-buster

# Set the working directory inside the container.
# All subsequent commands will be executed relative to this directory.
WORKDIR /app

# Copy the requirements.txt file into the container's working directory.
# This is done before copying the rest of the application code to leverage Docker's build cache.
COPY requirements.txt .

# Install any Python packages specified in requirements.txt.
# --no-cache-dir prevents pip from storing cached downloads, reducing image size.
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Python script (now named convert_itineraries_from_csv_to_parquet_duck_db.py) into the container.
COPY convert_itineraries_from_csv_to_parquet_duck_db.py .

# Set the entrypoint for the container.
# This command will be executed when the container starts.
# It tells Docker to run your script using the python interpreter.
ENTRYPOINT ["python", "convert_itineraries_from_csv_to_parquet_duck_db.py"]