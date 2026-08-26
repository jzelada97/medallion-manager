FROM python:3.12-slim

WORKDIR /app

# System deps for confluent-kafka (librdkafka) and psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc librdkafka-dev && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "hr_etl"]
