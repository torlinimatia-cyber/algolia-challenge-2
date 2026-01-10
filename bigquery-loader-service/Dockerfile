FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py .
COPY bigquery_loader.py .

CMD ["python", "-u", "bigquery_loader.py"]