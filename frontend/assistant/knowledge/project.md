# HR Insights ETL — Qué es este proyecto

HR Insights ETL es un sistema de ingeniería de datos en tiempo real construido como proyecto del Bootcamp de Ingeniería de Datos de Factoría F5 Madrid.

## Objetivo
Consumir mensajes fragmentados desde Apache Kafka, almacenarlos crudos en MongoDB (Data Lake), agrupar y normalizar los fragmentos de cada persona, y persistir el registro consolidado en PostgreSQL (Data Warehouse).

## Niveles implementados
- **Esencial**: pipeline Kafka → MongoDB → PostgreSQL funcional
- **Avanzado**: Redis buffer, matching sin ID global, upsert idempotente
- **Experto**: API REST (FastAPI), frontend (Streamlit), métricas Prometheus
- **Experto+**: Gold Layer con agregados pre-computados, Airflow DAGs, reconciliación batch
- **Experto++**: Chatbot amurallado con Groq + MCP (este asistente)

## Equipo
- **Karina Romero Vásquez** (PM): processing (detector, normalizer, matcher, consolidator), consumer Kafka, cache Redis, chatbot
- **José Zelada** (Scrum Master): lake MongoDB, warehouse PostgreSQL, API FastAPI, frontend Streamlit, Docker, Prometheus, logs, docs

## Stack principal
Python 3.12, confluent-kafka, MongoDB 7, PostgreSQL 16, Redis 7, SQLAlchemy 2, Pydantic 2, FastAPI, Streamlit, Prometheus, Docker Compose
