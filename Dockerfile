FROM python:3.9-slim-buster

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY convert_itineraries_from_csv_to_parquet_duck_db.py .

ENTRYPOINT ["python", "-u", "convert_itineraries_from_csv_to_parquet_duck_db.py"]