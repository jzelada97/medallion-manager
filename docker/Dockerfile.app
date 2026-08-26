# ---- build stage: install deps into a venv (keeps build tools out of runtime) ----
FROM python:3.12-slim AS build

WORKDIR /app

# Build-time system deps for confluent-kafka (librdkafka) and psycopg2.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc librdkafka-dev && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy only dependency metadata first so this layer is cached across code changes.
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

# ---- runtime stage: slim image, no compilers, non-root user ----
FROM python:3.12-slim AS runtime

WORKDIR /app

# Runtime-only system dep for confluent-kafka.
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 appuser

COPY --from=build /opt/venv /opt/venv
COPY src ./src

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

USER appuser
CMD ["python", "-m", "hr_etl"]
