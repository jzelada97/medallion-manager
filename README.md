# HR Insights ETL

Sistema de ingenieria de datos en tiempo real para **HR Insights**: consume mensajes fragmentados
desde Apache Kafka, los almacena crudos en **MongoDB** (Data Lake), agrupa y normaliza los
fragmentos de cada persona, y persiste el registro consolidado en **PostgreSQL** (Data Warehouse).
Incluye cache **Redis** para buffering intermedio, metricas **Prometheus**, **API** REST de
consulta (FastAPI) y **frontend** interactivo (Streamlit). Todo dockerizado y orquestado con
Docker Compose.

> **Nota sobre el generador**: la lógica de limpieza/matching se diseñó de forma
> independiente del generador de datos (no se basa en inspeccionar cómo genera los datos).
> Cerrada esa fase, el generador puede incluirse/desplegarse si hace falta para la demo.

---

## Tabla de contenidos

1. [Arquitectura](#arquitectura)
2. [Flujo de datos detallado](#flujo-de-datos-detallado)
3. [Estructura del repositorio](#estructura-del-repositorio)
4. [Stack tecnologico y decisiones](#stack-tecnologico-y-decisiones)
5. [Estrategia de matching y normalizacion](#estrategia-de-matching-y-normalizacion)
6. [Puesta en marcha](#puesta-en-marcha)
7. [Desarrollo local](#desarrollo-local)
8. [API REST](#api-rest)
9. [Frontend (Streamlit)](#frontend-streamlit)
10. [Monitorizacion (Prometheus)](#monitorizacion-prometheus)
11. [Tests y calidad](#tests-y-calidad)
12. [Configuracion](#configuracion)
13. [Limitaciones conocidas y mejoras futuras](#limitaciones-conocidas-y-mejoras-futuras)
14. [Equipo y gestion](#equipo-y-gestion)

---

## Arquitectura

```
                         +-------------------+
                         |  Kafka (externo)  |  <-- Generador caja negra
                         |  topic: probando  |
                         +--------+----------+
                                  |
                                  v
                    +-------------+-------------+
                    |     Consumer (confluent)   |
                    |  alto throughput, commit   |
                    |  manual tras procesar      |
                    +-------------+-------------+
                                  |
                    +-------------v--------------+
                    |      Detector de tipo       |
                    |  (keys del mensaje -> enum) |
                    +-------------+--------------+
                                  |
              +-------------------+-------------------+
              |                                       |
              v                                       v
   +----------+----------+              +-------------+-------------+
   |    Lake (MongoDB)    |              |     Matcher (match_key)    |
   |  Guarda el mensaje   |              |  passport > name > addr   |
   |  CRUDO sin tocar     |              +-------------+-------------+
   +----------------------+                            |
                                                       v
                                         +-------------+-------------+
                                         |     Redis Buffer           |
                                         |  Agrupa fragmentos por     |
                                         |  match_key con TTL         |
                                         +-------------+-------------+
                                                       |
                                                       | >= min_fragments?
                                                       v
                                         +-------------+-------------+
                                         |      Consolidator          |
                                         |  Merge de fragmentos en    |
                                         |  un unico Person record    |
                                         +-------------+-------------+
                                                       |
                                                       v
                                         +-------------+-------------+
                                         |   Warehouse (PostgreSQL)   |
                                         |  Upsert ON CONFLICT        |
                                         |  (idempotente, COALESCE)   |
                                         +-------------+-------------+
                                                       |
                                    +------------------+------------------+
                                    |                                     |
                                    v                                     v
                          +---------+--------+                  +---------+--------+
                          |   API (FastAPI)   |                  |   Frontend       |
                          |   /persons        |                  |   (Streamlit)    |
                          |   /stats          |                  |   Dashboard      |
                          +------------------+                  +------------------+
                                    |
                                    v
                          +---------+--------+
                          |   Prometheus      |
                          |   metricas del    |
                          |   pipeline + API  |
                          +------------------+
```

### Por que esta arquitectura

- **Lambda-lite**: guardamos el dato crudo en MongoDB (immutable, auditoria, reprocesamiento)
  y el dato procesado en PostgreSQL (consultas, JOIN, indices, ACID).
- **Desacoplamiento via Redis**: el buffer permite que la ingesta sea rapida sin esperar a la
  consolidacion. Si un fragmento llega antes que otro, se acumula hasta tener los minimos.
- **Idempotencia**: el upsert en Postgres usa ON CONFLICT con COALESCE para no perder datos
  existentes. Si el pipeline se reinicia y reprocessa, no duplica ni sobreescribe.
- **Resiliencia**: el commit de offset en Kafka se hace DESPUES de guardar en el lake. Si crashea,
  Kafka reenvia y el pipeline reprocesa (at-least-once).

---

## Flujo de datos detallado

### 1. Consumer (`src/hr_etl/consumer/kafka_consumer.py`)

- Usa `confluent-kafka` (binding nativo de librdkafka) por rendimiento.
- Import diferido para poder testear sin la libreria nativa instalada.
- `enable.auto.commit: False` — commit manual tras procesar cada mensaje.
- Soporta `max_messages` para modo demo/pruebas.

### 2. Detector de tipo (`src/hr_etl/processing/detector.py`)

El generador envia 5 tipos de fragmento. Cada uno tiene un conjunto de keys distinto:

| Tipo | Keys esperadas |
|------|---------------|
| Personal | name, lastname, sex, telfnumber, passport, email |
| Location | fullname, city, address |
| Professional | fullname, company, companyaddress, companytelfnumber, companyemail, job |
| Bank | passport, iban, salary |
| Net | address, ipv4 |

El detector normaliza las keys del mensaje (lowercase, sin espacios/guiones) y busca la mayor
cobertura (>= 50%) contra los schemas conocidos.

### 3. Normalizer (`src/hr_etl/processing/normalizer.py`)

- **Keys**: `"E-Mail"` -> `"email"`, `"Company Address"` -> `"companyaddress"`, `"Company Adress"` (typo del generador) -> `"companyaddress"` (via alias).
- **Texto**: lowercase, strip accents (NFKD), collapse whitespace.
- **Salary**: parsea formatos con moneda, puntos/comas, separadores de miles.

### 4. Lake (`src/hr_etl/lake/mongo_lake.py`)

- Guarda el mensaje CRUDO (payload original + metadata) en MongoDB.
- Batch mode: flush por tamano (500 docs) O por tiempo (1 segundo) para alto throughput.
- Indice en `ingested_at` para consultas temporales.

### 5. Matcher (`src/hr_etl/processing/matcher.py`)

Deriva una **match_key** estable para identificar a la persona:

1. `passport:H85111106` — clave mas fiable (Personal y Bank comparten passport).
2. `name:wayne griffiths` — desde fullname o name+lastname normalizado.
3. `addr:471 samantha cliff` — puente para Net que no tiene nombre ni passport.

Fragmentos sin ninguna key utilizable se tratan como huerfanos y se descartan del consolidation
(pero se conservan en el lake para auditoria).

### 6. Redis Buffer (`src/hr_etl/cache/redis_buffer.py`)

- Almacena fragmentos agrupados por match_key como JSON serializado.
- TTL configurable (default: 300s) para no acumular huerfanos indefinidamente.
- Cuando un key alcanza `CONSOLIDATION_MIN_FRAGMENTS` (default: 2), se dispara la consolidacion.

### 7. Consolidator (`src/hr_etl/processing/consolidator.py`)

- Recibe la lista de fragmentos de una persona.
- Convierte cada fragmento en un `Person` parcial.
- Los merge secuencialmente: campos no-null del fragmento nuevo rellenan huecos del existente (nunca sobreescribe un valor con null).
- Resultado: un unico `Person` con los campos disponibles de todos los fragmentos.

### 8. Warehouse (`src/hr_etl/warehouse/`)

- **SQLAlchemy** ORM con modelo `PersonRow`.
- Upsert nativo con `INSERT ... ON CONFLICT (match_key) DO UPDATE SET ... = COALESCE(EXCLUDED.x, persons.x)`.
- Preserva datos existentes, solo rellena huecos.
- Batch upsert para alto volumen.

### 9. Pipeline (`src/hr_etl/pipeline.py`)

Orquesta todo el flujo por mensaje. Instrumentado con metricas Prometheus.
Envuelto en try/except para que un mensaje corrupto nunca mate al pipeline.

---

## Estructura del repositorio

```
Proyecto1_modulo3_DE2/
├── src/hr_etl/                 # Codigo fuente principal
│   ├── __main__.py             # Entrypoint: wires infra + consume loop
│   ├── config.py               # Configuracion 12-factor (pydantic-settings)
│   ├── logging_conf.py         # Logging estructurado + PIIMaskingFilter
│   ├── pipeline.py             # Orquestacion del flujo
│   ├── consumer/               # Kafka consumer (confluent-kafka)
│   ├── lake/                   # MongoDB writer (batch + individual)
│   ├── processing/             # Detector, normalizer, matcher, consolidator
│   ├── warehouse/              # PostgreSQL writer (upsert ON CONFLICT)
│   ├── cache/                  # Redis buffer (fragmentos por persona)
│   ├── metrics/                # Prometheus counters/histograms/gauges
│   ├── models/                 # Pydantic (Person) + SQLAlchemy (PersonRow)
│   └── api/                    # FastAPI endpoints
├── frontend/                   # Streamlit dashboard
├── tests/                      # pytest (unit + integracion)
├── docker/                     # Dockerfiles (app, api, frontend)
├── monitoring/                 # prometheus.yml
├── data-generator/             # Submodule: generador Kafka (caja negra)
├── docker-compose.yml          # Stack completo
├── docker-compose.override.yml # Puertos de BD expuestos (solo local)
├── pyproject.toml              # Build + dependencias
├── .env.example                # Variables de entorno (template)
└── sonar-project.properties    # Config SonarQube
```

---

## Stack tecnologico y decisiones

| Tecnologia | Uso | Por que |
|---|---|---|
| Python 3.12 | Lenguaje principal | Requerido por el bootcamp |
| confluent-kafka | Consumer Kafka | Binding nativo de librdkafka, 10x mas rapido que kafka-python para alto throughput |
| MongoDB 7 | Data Lake (crudo) | Documental, flexible, acepta cualquier schema sin migraciones |
| PostgreSQL 16 | Data Warehouse | Relacional, ACID, upsert nativo, indices, ideal para consultas analiticas |
| Redis 7 | Buffer intermedio | In-memory, TTL nativo, ideal para acumular fragmentos temporales |
| SQLAlchemy 2 | ORM | Standard de facto en Python, soporta upsert nativo con `on_conflict_do_update` |
| Pydantic 2 | Modelos y config | Validacion en runtime, `pydantic-settings` para config 12-factor |
| FastAPI | API REST | Async, autodocumentacion Swagger, rapida |
| Streamlit | Frontend | Dashboard interactivo en Python sin necesidad de JS |
| Prometheus | Metricas | Standard para observabilidad, facil de instrumentar |
| Docker + Compose | Infraestructura | Entorno reproducible, healthchecks, redes aisladas |
| pytest | Tests | Framework standard, fixtures, coverage integrada |
| ruff + black | Lint + formato | Rapidos, configuracion minima, consistencia |

### Decisiones tecnicas importantes

1. **Commit manual en Kafka** — se hace DESPUES de guardar en el lake. Si crashea, se reenvian mensajes (at-least-once, no at-most-once).
2. **Lake SIN TTL** — no borramos datos crudos. Sirven para auditoria y reprocesamiento.
3. **Upsert con COALESCE** — nunca perdemos un dato bueno por un fragmento que llega vacio.
4. **Import diferido de confluent-kafka** — permite importar y testear el modulo sin la libreria nativa instalada.
5. **PIIMaskingFilter en logging** — enmascara automaticamente passport, IBAN y email en cualquier log para cumplir con proteccion de datos.
6. **Red externa para Kafka** — la app se conecta a la red Docker del generador (`data-generator_default`) para resolver `kafka:9092` directamente.

---

## Estrategia de matching y normalizacion

### El problema

Los mensajes del generador NO tienen un ID unico global. Cada fragmento llega por separado y hay
que descubrir a que persona pertenece.

### Claves de union (por prioridad)

1. **Passport** — Personal y Bank comparten este campo. Es la clave mas fiable.
2. **Fullname normalizado** — Location y Professional tienen `Fullname`. Personal tiene `Name` + `Lastname` que se concatenan.
3. **Address** — Location y Net comparten este campo. Puente para fragmentos sin nombre ni passport.

### Normalizacion de texto

Antes de comparar, todo pasa por:
- Lowercase
- Strip accents (NFKD decomposition)
- Collapse whitespace
- Strip bordes
- **Strip titles/honorifics** — prefijos: Mr, Mrs, Dr, Dr(a)., Ing., Lic., Mtro., Sr(a)., Dott., Sig., Prof.
- **Strip suffixes** — sufijos profesionales/generacionales: MD, PhD, Jr., Sr., II, III, Pi

Ejemplo: `"Dr(a). José  García MD"` -> `"jose garcia"`

### Normalizacion de keys

Las keys JSON del generador son inconsistentes (`"E-Mail"`, `"Company Address"`, `"Company Adress"`).
El normalizer las convierte a formato canonico sin espacios ni guiones:
- `"E-Mail"` -> `"email"`
- `"Company Address"` -> `"companyaddress"`
- `"Company Adress"` (typo) -> `"companyaddress"` (via alias)

### Limitacion conocida del matching

El cross-linking entre fragmentos por passport (Personal+Bank) y fragmentos por nombre
(Location+Professional) funciona por **match exacto del nombre normalizado**. Cuando el
Personal tiene `name+lastname` = `"octavio ponce"` y el Location tiene `fullname` = `"Octavio Ponce"`,
se cruzan correctamente (la normalizacion elimina mayusculas y acentos).

Sin embargo, si el generador produce un `fullname` distinto al `name+lastname` (ej: con un tercer
apellido como `"Octavio Ponce Gimenez"`), NO se cruzan — y es correcto no hacerlo, porque podrian
ser personas distintas. Estos registros quedan como entradas separadas en el warehouse.

Detalle menor: algunos nombres del generador contienen dobles espacios (ej: `"Uma  Gimenez Suarez"`).
Se guardan tal cual en el warehouse; la normalizacion de keys los colapsa para el matching pero el
valor persistido conserva el original.

### Batch Reconciliation (match_candidates)

Para abordar los casos ambiguos donde el matching en streaming no puede decidir, implementamos un
**job batch de reconciliacion** que analiza el warehouse y detecta pares de registros que
*probablemente* son la misma persona:

- **Tabla `match_candidates`**: almacena pares (person_id_a, person_id_b) con un score de confianza
  (0.0 a 1.0) y la razon del match hipotetico.
- **Estrategias de deteccion**:
  1. Registros con passport cuyo nombre es prefijo de un registro por nombre (ej: "octavio ponce" -> "octavio ponce gimenez")
  2. Registros por nombre que son prefijos entre si
- **Confianza** = longitud_prefijo / longitud_total. A mayor cobertura, mas probable que sea la misma persona.
- **Nunca se auto-mergean** — quedan como candidatos para revision humana o un threshold de confianza.

Ejecucion: `python -m hr_etl.processing.reconcile`
API: `GET /candidates?min_confidence=0.8`

Ejemplo de resultado:
```
crystal cunningham (passport:445725093) <-> Crystal Cunningham MD  | confianza: 0.86
octavio ponce (passport:140749868)      <-> Octavio Ponce Gimenez  | confianza: 0.74
```

### Analisis de los datos del generador (hallazgos)

Tras analizar ~500k mensajes del generador, documentamos estos patrones:

| Aspecto | Hallazgo |
|---------|----------|
| Distribucion de tipos | ~20% cada uno (Personal, Location, Professional, Bank, Net) balanceado |
| Fragmentos unknown | 0% — el detector identifica el 100% |
| Location.fullname vs Professional.fullname | Siempre identicos para la misma persona |
| Personal.email vs Professional.companyemail | Nunca coinciden (generados independientemente) |
| Personal.telfnumber vs Professional.companytelfnumber | Nunca coinciden |
| Location.address vs Net.address | Siempre identicos (100% match exacto) |
| Titulos en fullname | ~10% tienen prefijos (Dr, Mr, Ing, etc.) o sufijos (MD, Pi) |
| Apellidos extra | ~30% de Location tiene mas apellidos que Personal (tercer apellido) |
| Timing entre fragmentos | <1 segundo entre fragmentos de la misma persona |
| Salarios | Rango 30k-200k, sin negativos ni absurdos |
| Emails/IBAN/IPv4 | Todos con formato valido, sin basura |

---

## Puesta en marcha

### Requisitos

- Docker + Docker Compose
- Git (para clonar submodulos)

### Pasos

```bash
# 1. Clonar el repositorio (incluye el generador como submodule)
git clone --recurse-submodules https://github.com/Bootcamp-IA-MAD-P7/Proyecto1_modulo3_DE2.git
cd Proyecto1_modulo3_DE2

# 2. Arrancar el generador Kafka (caja negra)
cd data-generator
docker-compose up --build -d
cd ..

# 3. Copiar y configurar variables de entorno
cp .env.example .env
# Ajustar KAFKA_BOOTSTRAP_SERVERS segun tu setup:
#   - Desde otro contenedor en la misma red: kafka:9092
#   - Desde el host: localhost:29092
# El topic por defecto es "probando"

# 4. Arrancar el stack de la aplicacion
docker compose up --build -d

# 5. Verificar que todo esta arriba
docker ps
```

### Servicios disponibles

| Servicio | URL | Descripcion |
|----------|-----|-------------|
| API | http://localhost:8000 | FastAPI (Swagger en /docs) |
| Frontend | http://localhost:8501 | Dashboard Streamlit |
| Prometheus | http://localhost:9090 | Metricas y graficos |
| MongoDB | localhost:27017 | Data Lake (via override) |
| PostgreSQL | localhost:5432 | Data Warehouse (via override) |
| Redis | localhost:6379 | Buffer (via override) |

### Verificar el flujo

```bash
# Mensajes crudos en MongoDB
docker exec proyecto1_modulo3_de2-mongo-1 mongosh -u hr_user -p changeme --quiet \
  --eval "db = db.getSiblingDB('hr_lake'); print(db.raw_messages.countDocuments())"

# Personas consolidadas en PostgreSQL
docker exec proyecto1_modulo3_de2-postgres-1 psql -U hr_user -d hr_warehouse \
  -t -c "SELECT count(*) FROM persons;"

# Stats via API
curl http://localhost:8000/stats
```

---

## Desarrollo local

```bash
python -m venv .venv
.venv\Scripts\activate            # Windows
pip install -e ".[dev,frontend]"

# Tests (no requieren infra real)
pytest                            # unit + coverage

# Con infra local (docker compose up -d postgres mongo redis)
$env:TEST_POSTGRES_DSN="postgresql+psycopg2://hr_user:changeme@localhost:5432/hr_warehouse"
$env:TEST_MONGO_URI="mongodb://hr_user:changeme@localhost:27017/"
pytest                            # incluye tests de integracion

# Lint
ruff check src frontend
```

---

## API REST

Base URL: `http://localhost:8000`

| Endpoint | Metodo | Descripcion |
|----------|--------|-------------|
| `/health` | GET | Healthcheck |
| `/metrics` | GET | Metricas Prometheus (texto) |
| `/persons` | GET | Listado con paginacion y busqueda |
| `/persons/{id}` | GET | Detalle de una persona (404 si no existe) |
| `/stats` | GET | Estadisticas agregadas |

### Parametros de `/persons`

| Param | Tipo | Descripcion |
|-------|------|-------------|
| `limit` | int | Tamano de pagina (default: 20) |
| `offset` | int | Posicion de inicio (default: 0) |
| `q` | str | Busqueda libre (nombre, passport, email, ciudad) |
| `city` | str | Filtro por ciudad |
| `company` | str | Filtro por empresa |
| `job` | str | Filtro por puesto |

### Respuesta de `/persons`

```json
{
  "total": 14047,
  "count": 20,
  "limit": 20,
  "offset": 0,
  "items": [...]
}
```

### Respuesta de `/stats`

```json
{
  "total_persons": 14047,
  "top_cities": [{"value": "Santa Rosa", "count": 7}, ...],
  "top_companies": [{"value": "Teixeira", "count": 4}, ...],
  "with_bank": 7035
}
```

---

## Frontend (Streamlit)

Dashboard interactivo en http://localhost:8501 con:

- Tarjetas de metricas (total personas, con banco, top ciudad)
- Graficos de barras (top ciudades, top empresas) usando los datos de `/stats`
- Buscador con filtros (nombre, ciudad, empresa)
- Tabla de resultados con paginacion
- Ficha de detalle al seleccionar una persona

---

## Monitorizacion (Prometheus)

Prometheus (http://localhost:9090) scrapea:
- **App ETL** en `app:9100` (metricas del pipeline)
- **API** en `api:8000/metrics`

### Metricas expuestas

| Metrica | Tipo | Descripcion |
|---------|------|-------------|
| `hr_etl_messages_consumed_total` | Counter | Mensajes consumidos (por tipo) |
| `hr_etl_messages_failed_total` | Counter | Mensajes fallidos |
| `hr_etl_persons_persisted_total` | Counter | Personas guardadas en warehouse |
| `hr_etl_consolidations_total` | Counter | Consolidaciones ejecutadas |
| `hr_etl_pending_fragments` | Gauge | Fragmentos pendientes en buffer |
| `hr_etl_processing_seconds` | Histogram | Tiempo de procesamiento por mensaje |
| `hr_etl_persist_seconds` | Histogram | Tiempo de persistencia en warehouse |

### Queries utiles en Prometheus

```promql
# Velocidad de ingesta (msg/s)
rate(hr_etl_messages_consumed_total[1m])

# Consolidaciones por segundo
rate(hr_etl_consolidations_total[1m])

# Percentil 99 de tiempo de procesamiento
histogram_quantile(0.99, rate(hr_etl_processing_seconds_bucket[5m]))
```

---

## Tests y calidad

- **Framework**: pytest con pytest-cov
- **Cobertura**: 79 tests, 91% lineas
- **Umbrales**: lineas >= 80%, ramas >= 75%, funciones >= 85%
- **Sin infra real**: tests unitarios usan mongomock, fakeredis y SQLite
- **Con infra**: tests de integracion marcados `@pytest.mark.integration` (se saltan si no hay BD disponible)

```bash
# Solo unit (rapido, sin infra)
pytest --no-cov -q

# Completo con cobertura
pytest

# Lint
ruff check src frontend
```

### SonarQube

`sonar-project.properties` listo. Genera `coverage.xml` con pytest-cov y ejecuta:
```bash
sonar-scanner
```

---

## Configuracion

Toda la configuracion es via variables de entorno (12-factor). Ver `.env.example` para el listado
completo. Las principales:

| Variable | Default | Descripcion |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | - | Direccion del broker Kafka |
| `KAFKA_TOPIC` | probando | Topic a consumir |
| `KAFKA_GROUP_ID` | hr-etl-consumer | Consumer group |
| `KAFKA_AUTO_OFFSET_RESET` | earliest | Donde empezar si no hay offset |
| `MONGO_URI` | - | Connection string MongoDB |
| `MONGO_DB` | hr_lake | Base de datos del lake |
| `POSTGRES_HOST/PORT/DB/USER/PASSWORD` | - | Conexion PostgreSQL |
| `REDIS_HOST/PORT/PASSWORD` | - | Conexion Redis |
| `REDIS_BUFFER_TTL` | 300 | TTL de fragmentos en buffer (segundos) |
| `CONSOLIDATION_MIN_FRAGMENTS` | 2 | Minimo de fragmentos para consolidar |
| `MAX_RECORDS` | None | Limite de mensajes (None = infinito) |
| `LOG_LEVEL` | INFO | Nivel de logging |

---

## Limitaciones conocidas y mejoras futuras

### Limitaciones

1. **Nombres no coincidentes**: Cuando el generador produce un `fullname` con mas palabras que
   `name+lastname` (ej: tercer apellido), no se cruzan — quedan como registros separados.
   Es la decision correcta para evitar falsos positivos.

2. **Dobles espacios**: Algunos valores del generador contienen dobles espacios en nombres.
   Se conservan tal cual en el warehouse (la key de matching si los normaliza).

3. **Dependencia del orden**: El cross-linking solo funciona si el fragmento Personal llega ANTES
   que el Location/Professional. Si el orden es inverso, no se resuelve el alias (el alias aun
   no existe). Inherente al procesamiento streaming.

4. **Fragmentos Net**: El fragmento Net (address + IPv4) raramente se une a otros porque depende
   de un match exacto de address, que pocas veces coincide.

### Mejoras futuras

1. **Reconciliacion batch**: Segundo paso periodico que cruce registros por passport con registros
   por nombre usando similitud de texto (no en streaming, sino como job separado).

2. **Normalizacion de valores persistidos**: Colapsar dobles espacios tambien en los valores
   que se guardan en Postgres, no solo en las keys de matching.

3. **Metricas de calidad**: Dashboard con ratio de campos rellenos, fragmentos huerfanos, etc.

4. **Backpressure**: Si Redis se llena, reducir velocidad de consumo (consumer pause/resume).

---

## Equipo y gestion

| Rol | GitHub | Modulos |
|-----|--------|---------|
| Scrum Master | **jzelada97** | lake, warehouse, api, frontend, docker, prometheus, logs, docs |
| PM | **karinaromerovasquez** | processing (join/normalizacion), consumer (Kafka), cache (Redis) |

### Gestion

- **GitHub Issues + Projects** (tablero Kanban)
- **Ramas**: `main` (estable) -> `dev` (integracion) -> `feature/*` (trabajo)
- **Commits**: formato `tipo(area): descripcion`
- **PR**: siempre hacia `dev`, revisado por al menos otro miembro

---

## Licencia

Proyecto educativo del Bootcamp de Ingenieria de Datos (Factoria F5 Madrid).

---

## CI/CD y despliegue

### Integración continua (GitHub Actions)

En cada push/PR a `main` o `dev` se ejecuta `.github/workflows/ci.yml`:

- **Job `unit`**: levanta Postgres/Mongo/Redis como *service containers*, instala el
  paquete con extras `dev,frontend`, y corre `ruff`, `black --check` y `pytest` con
  cobertura (los 112 tests). Sube `coverage.xml` como artefacto.
- **Job `airflow`**: levanta el stack de Airflow y ejecuta los tests del sensor
  deferrable dentro del contenedor (`scripts/run-airflow-tests.sh`).

### Despliegue

Pensado para una **VM del Always Free de Oracle Cloud**: overlay de producción
`docker-compose.prod.yml` + `deploy/Caddyfile` (HTTPS automático con Caddy, bases de
datos no expuestas al exterior) y script de arranque `deploy/setup-oracle-vm.sh`.
Para practicar el flujo de OCI antes del stack completo hay un ensayo mínimo en
[`deploy/oracle-hello/`](deploy/oracle-hello/README.md).
