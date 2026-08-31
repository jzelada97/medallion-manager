# HR Insights ETL — Arquitectura

## Flujo principal
Kafka → Consumer → Detector de tipo → MongoDB (Data Lake, dato crudo) → Matcher → Redis Buffer → Consolidator → PostgreSQL (Data Warehouse) → API FastAPI → Streamlit

## Capas
- **Bronze / Data Lake**: MongoDB. Guarda cada mensaje crudo sin modificar. Sin TTL, sirve para auditoría y reprocesamiento.
- **Silver / Data Warehouse**: PostgreSQL. Registros consolidados por persona. Upsert con ON CONFLICT + COALESCE (idempotente).
- **Gold Layer**: Tablas pre-computadas en PostgreSQL (gold_top_cities, gold_top_companies, gold_completeness, gold_stats). Refrescadas por Airflow cada hora.

## Componentes
- **Consumer**: confluent-kafka, commit manual tras procesar (at-least-once)
- **Redis Buffer**: acumula fragmentos por match_key con TTL de 5 minutos
- **API**: FastAPI con endpoints /health, /stats, /persons, /gold/*, /candidates
- **Frontend**: Streamlit con dashboard y este asistente
- **Prometheus**: métricas del pipeline (mensajes consumidos, consolidaciones, latencia)
- **Airflow**: DAG de refresco Gold cada hora + DAG de mantenimiento (reconciliación batch)

## Decisiones clave
- Commit manual en Kafka: si crashea, se reenvían mensajes, no se pierden
- Lake sin TTL: los datos crudos nunca se borran (auditoría)
- Upsert con COALESCE: nunca se sobreescribe un dato bueno con null
- Import diferido de confluent-kafka: permite testear sin la librería nativa
- PIIMaskingFilter en logs: enmascara passport, IBAN y email automáticamente
