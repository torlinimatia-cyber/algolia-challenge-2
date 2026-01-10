FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py .
COPY cdc_consumer.py .

CMD ["python", "-u", "cdc_consumer.py"]