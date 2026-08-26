# HR Insights ETL

Sistema de ingeniería de datos para "HR Insights": consume mensajes en tiempo real desde Apache
Kafka, los almacena crudos en **MongoDB** (Data Lake), une los fragmentos de cada persona
(Personal, Location, Professional, Bank, Net) y persiste el registro consolidado en **PostgreSQL**
(Data Warehouse). Incluye caché **Redis**, métricas **Prometheus**, **API** de consulta (FastAPI) y
**frontend** (Streamlit). Todo dockerizado.

> ⚠️ **Regla del reto**: el código del generador de datos NO se lee ni se inspecciona. Solo se
> ejecuta con su propio `docker-compose up --build`. Este repositorio nunca accede a esa lógica.

## Arquitectura (resumen)

```
Kafka (externo) ─▶ Consumer ─▶ Lake (MongoDB, crudo)
                       │
                       ▼
                 Detector de tipo ─▶ Normalizer ─▶ Matcher (clave persona)
                       │
                       ▼
                 Redis (buffer por persona) ─▶ Consolidator ─▶ Warehouse (PostgreSQL)
                                                                     │
                                                          API (FastAPI) ─▶ Frontend (Streamlit)
                                                          Prometheus (/metrics)
```

Detalle completo en `.kiro/pipeline/architecture.md`.

## Requisitos
- Docker + Docker Compose
- (Desarrollo local) Python 3.12

## Puesta en marcha

1. Levanta el servidor Kafka del reto (repositorio del generador, caja negra):
   ```
   docker compose up --build   # en el repo del generador
   ```
2. Copia la configuración y ajusta `KAFKA_BOOTSTRAP_SERVERS` y `KAFKA_TOPIC`:
   ```
   cp .env.example .env
   ```
   En Docker Desktop, para alcanzar el Kafka del host usa `host.docker.internal:9092`.
3. Levanta el stack de la aplicación:
   ```
   docker compose up --build
   ```
   Servicios: `app` (ETL), `api` (http://localhost:8000), `frontend` (http://localhost:8501),
   `mongo`, `postgres`, `redis`, `prometheus` (http://localhost:9090).

   El arranque es ordenado: `mongo`, `postgres` y `redis` tienen healthchecks y los servicios
   `app`/`api` esperan (`depends_on: condition: service_healthy`) a que las bases de datos estén
   listas antes de arrancar, evitando fallos de conexión en el primer `up`. Todos los servicios
   usan `restart: unless-stopped`.

## Desarrollo local (sin Docker)

```
python -m venv .venv
.venv\Scripts\activate            # Windows
pip install -e ".[dev,frontend]"  # requiere toolchain para confluent-kafka/psycopg2
pytest                            # tests + cobertura
ruff check src frontend           # lint
```

Nota: `confluent-kafka` y `psycopg2-binary` necesitan librerías nativas; en Docker ya están
resueltas. Los tests no requieren infra real (usan mongomock, fakeredis y SQLite).

## API

- `GET /health` — estado.
- `GET /metrics` — métricas Prometheus.
- `GET /persons?limit=&offset=&q=&city=&company=&job=` — listado con búsqueda libre, filtros y paginación (`total`, `count`, `limit`, `offset`).
- `GET /persons/{id}` — detalle (404 si no existe).
- `GET /stats` — resumen: total de personas, top ciudades/empresas, cuántas tienen banco.

## Monitorización (Prometheus)

Prometheus (http://localhost:9090) scrapea:
- La app ETL en `app:9100` (métricas del pipeline).
- La API en `api:8000/metrics`.

Métricas expuestas: mensajes consumidos por tipo (`hr_etl_messages_consumed_total`), fallidos
(`hr_etl_messages_failed_total`), personas persistidas (`hr_etl_persons_persisted_total`),
consolidaciones (`hr_etl_consolidations_total`), fragmentos en buffer (`hr_etl_pending_fragments`),
y tiempos de procesado y persistencia (`hr_etl_processing_seconds`, `hr_etl_persist_seconds`).
La velocidad (msg/s) se obtiene con `rate()` sobre los contadores.

## Estrategia de matching

Como los mensajes no tienen un ID único, cada fragmento se mapea a una clave de persona por prioridad:

1. **passport** — clave más fiable (`passport:h85111106`)
2. **nombre normalizado** — desde `fullname` o `name`+`last_name` (`name:wayne griffiths`)
3. **dirección** — puente para fragmentos Net sin nombre ni pasaporte (`addr:471 samantha cliff`)

Los fragmentos sin ninguna de estas claves se tratan como huérfanos y se descartan sin romper el pipeline. Los fragmentos de la misma clave se acumulan en Redis y se consolidan en un único registro `Person` cuando se alcanzan los fragmentos mínimos configurados (`CONSOLIDATION_MIN_FRAGMENTS`).

## Tests y calidad
- `pytest` — 58 tests, cobertura de líneas 96% / ramas ~93%.
- `sonar-project.properties` listo para SonarQube (usa `coverage.xml`).

## Estructura del repositorio
Ver `.kiro/steering/20-conventions.md`. El directorio `.kiro/` contiene el **Agentic Harness**
compartido del equipo (reglas, contexto, agentes y artefactos del pipeline).

## Gestión del proyecto
GitHub Issues + GitHub Projects (tablero Kanban). Ramas: `main` (estable), `dev` (integración),
`feature/*` (trabajo).

## Equipo
Ver reparto de trabajo en `.kiro/steering/30-team-roles.md`.
