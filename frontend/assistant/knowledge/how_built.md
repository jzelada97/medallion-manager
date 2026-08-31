# HR Insights ETL — Cómo se construyó

## Decisiones técnicas importantes

1. **confluent-kafka vs kafka-python**: Se eligió confluent-kafka por ser el binding nativo de librdkafka, 10x más rápido para alto throughput.
2. **Commit manual en Kafka**: Se hace DESPUÉS de guardar en el lake. Garantía at-least-once: si crashea, Kafka reenvía y no se pierde nada.
3. **MongoDB para el lake**: Documental, flexible, acepta cualquier schema sin migraciones. Ideal para datos crudos heterogéneos.
4. **PostgreSQL para el warehouse**: Relacional, ACID, upsert nativo, índices. Ideal para consultas analíticas.
5. **Redis como buffer**: In-memory, TTL nativo. Acumula fragmentos temporales sin bloquear la ingesta.
6. **Upsert con COALESCE**: `INSERT ... ON CONFLICT DO UPDATE SET x = COALESCE(EXCLUDED.x, persons.x)`. Nunca pierde un dato bueno por un fragmento vacío.
7. **Import diferido de confluent-kafka**: Permite importar y testear el módulo sin la librería nativa instalada.
8. **PIIMaskingFilter**: Filtra automáticamente passport, IBAN y email en cualquier log.
9. **Gold Layer**: Tablas pre-computadas para que la API sea rápida sin hacer agregaciones en cada request.
10. **Airflow deferrable sensor**: El sensor que espera N personas usa el modo deferrable para no bloquear un worker.

## Tests y calidad
- 112 tests con pytest
- 91% de cobertura de líneas
- Sin infraestructura real en tests unitarios: mongomock, fakeredis, SQLite
- Tests de integración marcados con @pytest.mark.integration
- CI con GitHub Actions: ruff + black + pytest en cada PR

## Estructura del repositorio
- `src/hr_etl/` — código fuente principal
- `frontend/` — Streamlit dashboard + asistente
- `tests/` — pytest unit + integración
- `docker/` — Dockerfiles
- `airflow/dags/` — DAGs de Airflow
- `monitoring/` — prometheus.yml + Grafana dashboards
- `specs/` — especificaciones de features
