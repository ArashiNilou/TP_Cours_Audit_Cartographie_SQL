FROM python:3.11-slim
WORKDIR /app
RUN pip install prometheus_client pymongo psycopg[binary]
COPY raw_clean_exporter.py .
CMD ["python", "raw_clean_exporter.py"]
