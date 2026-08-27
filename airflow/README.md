# Apache Airflow — HR ETL Scheduler

## Qué es

Airflow programa y ejecuta automáticamente los jobs batch del pipeline:
- **Refresh Gold** (cada 5 min): recalcula las tablas Gold (estadísticas, tops, completitud)
- **Reconciliation** (cada 30 min): detecta duplicados probables entre personas

## Cómo levantar Airflow

Añadir al `docker-compose.yml`:

```yaml
  airflow:
    image: apache/airflow:2.9-python3.12
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://hr_user:changeme@postgres:5432/hr_warehouse
      AIRFLOW__CORE__FERNET_KEY: ''
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'false'
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      # Pass ETL config so the DAGs can connect to the same infra
      POSTGRES_HOST: postgres
      POSTGRES_PORT: "5432"
      POSTGRES_DB: hr_warehouse
      POSTGRES_USER: hr_user
      POSTGRES_PASSWORD: changeme
      MONGO_URI: mongodb://hr_user:changeme@mongo:27017/
      REDIS_HOST: redis
      REDIS_PASSWORD: changeme
    volumes:
      - ./airflow/dags:/opt/airflow/dags:ro
      - ./src:/opt/airflow/src:ro
    ports:
      - "8080:8080"
    depends_on:
      postgres:
        condition: service_healthy
    command: >
      bash -c "
        pip install -e /opt/airflow/src/.. --quiet &&
        airflow db init &&
        airflow users create --username admin --password admin --firstname Admin --lastname ETL --role Admin --email admin@hr.local &&
        airflow webserver & airflow scheduler
      "
    restart: unless-stopped
    networks: [hr_net]
```

## Acceso

- UI: http://localhost:8080
- User: admin / admin
- DAGs visibles: `hr_etl_refresh_gold`, `hr_etl_reconciliation`

## Notas

- Airflow usa la misma BD Postgres que el warehouse (schema separado para sus metadatos internos).
- Los DAGs invocan los mismos comandos CLI que puedes ejecutar manualmente:
  - `python -m hr_etl.warehouse.gold_layer`
  - `python -m hr_etl.processing.reconcile`
- Para producción se recomienda CeleryExecutor o KubernetesExecutor; LocalExecutor basta para demo.
