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

- **Medallion (Bronze / Silver / Gold)**: los datos fluyen por capas de calidad creciente.
  - **Bronze**: mensajes crudos en MongoDB (inmutable, auditoria, reprocesamiento).
  - **Silver**: registros de persona consolidados y limpios en PostgreSQL (tabla `persons`).
  - **Gold**: agregados precomputados en PostgreSQL (`gold_stats`, `gold_top_cities`,
    `gold_top_companies`, `gold_completeness`) refrescados por un DAG de Airflow.
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
│   ├── warehouse/              # PostgreSQL writer (upsert) + gold_layer (agregados)
│   ├── cache/                  # Redis buffer (fragmentos por persona)
│   ├── metrics/                # Prometheus counters/histograms/gauges
│   ├── models/                 # Pydantic (Person) + SQLAlchemy (PersonRow, MatchCandidate)
│   ├── processing/reconcile.py # Reconciliacion batch (candidatos a duplicado)
│   ├── airflow_ext/            # Sensor deferrable para el DAG event-driven
│   └── api/                    # FastAPI endpoints
├── frontend/                   # Streamlit dashboard (Arquitectura, Personas, Duplicados)
├── airflow/dags/               # DAGs: refresh Gold (event-driven) + reconciliacion
├── tests/                      # pytest (unit + integracion)
├── docker/                     # Dockerfiles (app, api, frontend)
├── deploy/                     # setup-oracle-vm.sh, deploy.sh, Caddyfile
├── monitoring/                 # prometheus.yml + dashboards de Grafana
├── data-generator/             # Submodule: generador Kafka (caja negra)
├── docker-compose.yml          # Stack completo
├── docker-compose.prod.yml     # Overlay de produccion (Caddy, sin puertos de BD)
├── docker-compose.override.yml # Puertos de BD expuestos (solo local)
├── docker-compose.airflow.yml  # Airflow 3 (opt-in)
├── .github/workflows/CICD.yml  # CI/CD (lint + tests + deploy por SSH)
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

### Batch Reconciliation (historia — ver la seccion final para la regla VIGENTE)

> ⚠️ Esta seccion describe una version PREVIA del reconciliador (dos fases, exacto+difuso,
> B1/B2, corroboracion). La **regla vigente** esta en "Subsistema Duplicados / Consolidacion
> / Gold" mas abajo: reconcile agrupa en Silver SOLO por nombre (identico/typo/contencion),
> **sin corroboracion**, con no-contradiccion de passport como unica guarda; Gold excluye a
> los nombres ambiguos. Se conserva esta narrativa por su valor de "como llegamos aqui".

Para abordar los casos ambiguos donde el matching en streaming no puede decidir, implementamos un
**job batch de reconciliacion** que analiza el warehouse y **agrupa** registros que
*probablemente* son la misma persona. Decisiones de diseño relevantes:

- **Todo en SQL (Postgres), no en Python.** La version inicial cargaba toda la tabla
  `persons` en memoria de Python y agrupaba con diccionarios; con millones de filas eso
  agota la RAM (llego a tumbar la VM de demo). Ahora la deteccion (normalizacion,
  similitud, agrupacion) corre dentro de Postgres y solo vuelven las filas de pertenencia.
- **Dos fases: exacto primero, difuso solo sobre nombres DISTINTOS.** El coste esta en la
  comparacion difusa, pero la mayoria de duplicados son el mismo nombre repetido (no
  necesita difuso). Por eso: (1) se colapsan las personas a un catalogo de nombres
  normalizados distintos (un `GROUP BY`, reduce millones de filas a muchos menos nombres);
  (2) el difuso corre SOLO entre esos nombres distintos; (3) cada persona hereda el grupo
  de su nombre. Asi el paso caro nunca ve nombres repetidos y no explota.
- **Sin limites de resultado.** No hay maximo de miembros por grupo ni `LIMIT` de filas
  escritas: capar resultados descartaria duplicados reales al crecer el dataset. El diseño
  es barato de por si; queda un `statement_timeout` SOLO como salvavidas anti-cuelgue (si
  saltara, cancela toda la pasada y no escribe nada — nunca un resultado parcial).
- **Normalizacion: quita titulos en AMBOS lados.** Honorificos y sufijos (Mr, Dr, MD,
  PhD, Jr...) pueden venir antes o despues del nombre; se quitan de los dos extremos (misma
  lista que el `normalizer` del streaming) para que "Dr Juan Perez", "Juan Perez MD" y
  "Juan Perez" normalicen igual.
- **Dos reglas de match** (sin bajar el umbral para todos):
  - *Typo/letra cambiada* — similitud `pg_trgm` >= 0.85 (indice GIN). Detecta "leclerc"
    vs "leclercq", "martinez" vs "martenez".
  - *Contencion de palabras* — todas las palabras de un nombre estan en el otro. Detecta
    "octavio ponce" ⊆ "octavio ponce gimenez" (apellido extra), caso que la similitud
    difusa por si sola dejaria por debajo de 0.85. Cada regla tiene su propio *blocking*
    eficiente (trigramas para typo; nombre+primer apellido para contencion), asi no se
    comparan todos contra todos.
- **Grupos, no pares** (tabla `duplicate_groups`): varios registros de la misma persona
  comparten `group_id` (el id minimo del grupo, ancla canonica). Una fila por miembro.
- **Agrupacion por ancla, NO clustering de componentes conexos.** Convertir pares en
  grupos transitivos (A~B, B~C ⇒ {A,B,C}) requeriria Union-Find (algoritmo imperativo,
  mal encaje con SQL y obligaria a cargar el grafo en memoria) o una CTE recursiva
  (costosa en tiempo/memoria sobre datos densos, arriesgado en la VM pequeña). Se
  descarto a proposito: el ancla es correcto para la gran mayoria de nombres de personas
  y evita ese coste. Documentado en `processing/reconcile.py`.
- **Nunca se auto-mergean** — son candidatos para revision humana.

Ejecucion: `python -m hr_etl.processing.reconcile`
API: `GET /groups?min_confidence=0.85`
Frontend: pestaña **Duplicados** (muestra los grupos y el detalle de cada miembro).

> Nota: `match_candidates` (pares) queda como tabla legacy; el modelo activo es
> `duplicate_groups` (grupos). En una BD existente, hacer `DROP TABLE` de la vieja es
> seguro porque la reconciliacion reconstruye todo en cada pasada.

### Subsistema Duplicados / Consolidacion / Gold (rediseño para 2.2M filas)

Validado con datos reales de la VM (2.211.131 filas). Tres jobs SQL-first encadenados,
en orden estricto por dependencia de datos:

```
consolidate_merge  >>  reconcile  >>  refresh_gold
```

**`norm_name` materializado (columna en `persons`).** El nombre normalizado (minusculas,
sin acentos, titulos quitados en ambos extremos, espacios colapsados) se persiste como
columna indexada. Se calcula UNA vez y se consulta N veces, en vez de recomputar el regex
sobre 2.2M filas en cada pasada. Fuente unica de verdad:
`normalizer.compute_norm_name()` (streaming) espejado carácter a carácter por la expresion
SQL `sql_norm.norm_sql()` (batch + backfill). Un test de paridad Python↔SQL evita que
diverjan. Indices: btree (`ix_persons_norm_name`) para el JOIN/GROUP BY y GIN trigram
(`ix_persons_norm_name_trgm`) para `similarity()`/`%`. Los crea la migracion idempotente
`warehouse/migrations/001_reconcile.sql` (ademas de `CREATE EXTENSION pg_trgm` y el
backfill de las filas historicas). La migracion la ejecuta `init_schema` de forma
idempotente (se salta en backends no-Postgres).

**Contexto (por que el nombre es el problema).** Los datos vienen en dos "islas" que no
comparten identificador: (A) Personal+Bank unidas por `passport`; (B)
Location+Professional+Net unidas por `fullname`/`address`. Entre A y B solo esta el
NOMBRE, y difiere: Personal lo trae corto (`name`+`last_name`, 2 palabras), Location/
Professional lo traen completo (`fullname`, con apellido extra). Medido en los datos
reales: una "Maite Rodriguez Sanchez" (isla B, sin passport) puede corresponder a varias
"Maite Rodriguez" (isla A, con passports distintos) → la union Personal↔Location es
**intrinsecamente ambigua** (~68% de los nombres cortos mapean a >1 candidato). Por eso el
sistema **consolida solo lo inequivoco** y **propone el resto a revision humana**.

**1. Fix de consolidacion (`consolidate_merge.py`, Silver).** Fusiona en Postgres la misma
persona partida en varias filas, por dos vias:

- **VIA 1 — mismo `passport` Y `norm_name` muy parecido** (`similarity >= 0.85`). Passport
  igual + nombres claramente distintos = colision del generador → NO se fusiona.
- **VIA 2 — `norm_name` IDENTICO en un bucket de tamaño 2**, con perfil complementario (un
  lado Personal con passport, otro Location con address) y **sin contradiccion de
  passport**. Son personas partidas que el streaming no unio (mismo nombre exacto, fuentes
  distintas). Medido en la VM: fusiono **202.769** filas (persons 2.21M → 1.95M). Buckets
  de 3+ o con passports distintos NO se tocan (riesgo de mezclar personas distintas).
- **Survivorship**: superviviente = `min(id)` (cierre transitivo por si hay cadenas);
  primer valor no nulo por campo (nunca pisa un dato bueno con NULL); `full_name` = el mas
  largo; `created_at` = el mas antiguo; `updated_at` = el mas reciente; `norm_name`
  recalculado del `full_name` ganador. Todo set-based, en una transaccion, idempotente.

**2. Reconciliacion (`reconcile.py`, solo SUGIERE).** Busca en **Silver** candidatos a
duplicado para **revision humana** (nunca auto-merge). El criterio es **SOLO el nombre**,
**sin exigir ningun campo compartido**:

- **Match por nombre**: identico repetido, typo (`0.85 <= sim < 1.0`, trigrama GIN) o
  contencion (apellido extra, "octavio ponce" ⊆ "octavio ponce gimenez").
- **Sin corroboracion por campo, a proposito**: las personas partidas reales tienen datos
  DISJUNTOS entre islas (no comparten email/phone/company), asi que exigir un campo
  compartido descartaria justo a los duplicados verdaderos. Y si dos comparten nombre Y un
  campo fuerte, es que son la misma → eso lo arregla la consolidacion, no la revision.
- **Unica guarda negativa**: no-contradiccion de `passport` (passports distintos = personas
  distintas; separa los homonimos tipo "jose luis"). Nombres de 1 palabra excluidos.
- Deteccion 100% SQL sobre un catalogo de nombres DISTINTOS (escala), ancla `min(id)`,
  `INSERT...SELECT`, sin caps, `statement_timeout` como salvavidas. `reason` = etiquetas
  fijas sin PII (`exact_name` / `fuzzy_name` / `name_containment`).
- Es deliberadamente permisiva: nombres comunes generan grupos de revision (p. ej. varios
  "juan perez"). Es aceptable porque solo propone y porque Gold ya excluye los ambiguos.

**3. Gold de personas (`gold_layer.py`) — sin duplicados por construccion.** `gold_persons`
= subconjunto "completo" de Silver: **≥80% de los 8 campos Y los 5 obligatorios**
(`full_name`, `passport`, `email`, `city`, `company`), **Y** cuyo nombre **NO este marcado
en `duplicate_groups`** (no es identico/typo/contenido con otro en Silver). Esa segunda
condicion es la clave: una persona con nombre ambiguo NO llega a Gold, porque no hay certeza
de que sus 5 campos sean de una sola persona real. Asi Gold queda **libre de duplicados por
construccion**. Exige el orden `reconcile >> refresh_gold` (garantizado por el DAG). Las
stats `gold_*` se calculan sobre `gold_persons`. Rebuild completo idempotente.

Ejecucion (o via el DAG `hr_etl_maintenance`):
```
python -m hr_etl.processing.consolidate_merge
python -m hr_etl.processing.reconcile
python -m hr_etl.warehouse.gold_layer
```

Metricas Prometheus añadidas (solo numericas, sin PII): `hr_etl_consolidation_merged_rows_total`,
`hr_etl_reconcile_duration_seconds`, `hr_etl_reconcile_groups`, `hr_etl_reconcile_memberships`,
`hr_etl_gold_persons`.

#### Ideas RECHAZADAS y por que (DEC-9)

- **Exigir corroboracion por campo en la reconciliacion** → las personas partidas reales
  tienen datos DISJUNTOS entre islas (Personal con passport/email vs Location con
  address/company), asi que exigir un campo compartido descarta justo a los duplicados
  verdaderos. Y un par que comparte nombre + campo fuerte no es "posible duplicado": es la
  misma persona que debio consolidarse. Por eso reconcile agrupa SOLO por nombre.
- **Agrupar por nombre identico solo (regla laxa B1)** → con 2.2M filas, el nombre identico
  es coincidencia frecuentisima, no evidencia: dio 506k grupos de ruido. Ahora los identicos
  seguros los fusiona la consolidacion (VIA 2, bucket de 2 + fuentes complementarias) y el
  resto se propone a revision, nunca se auto-mergea.
- **Unir Personal↔Location por prefijo de 2 palabras** → el fullname completo (que
  desambiguaria) vive solo en el lado sin passport; el 68% de los prefijos mapean a varios
  candidatos con passport distinto (una "Maite Rodriguez Sanchez" vs cinco "Maite
  Rodriguez"). Imposible elegir sin inventar; no se auto-mergea por prefijo.
- **Dos columnas de nombre (`full_name` Personal + `professional_name` Location)** → ordena
  mejor el almacenamiento pero NO crea puente de union: la mitad Personal no tiene el
  fullname completo, asi que las dos mitades siguen sin un valor comun. No resuelve el caso.
- **passport/email como eje de la pestaña Duplicados** → passport repetido es un fix de
  consolidacion (misma persona partida), no un "posible duplicado"; email repetido es RUIDO
  del generador (p. ej. `cgonzalez@yahoo.com` en 45 personas distintas). Ni email ni phone
  ni iban se usan para agrupar/consolidar (iban ademas es unico → identificador, no señal
  de duplicado). Fallo conocido del generador.
- **`similarity()` en el `JOIN ON` sobre columna calculada** → no usa el GIN → self-join
  O(n²) → timeout. Se usa blocking por igualdad + trigrama solo dentro de micro-bloques.
- **Blocking por primera palabra del nombre** → bloques contaminados por nombres de pila
  frecuentes ("juan" ~20k) → cartesiano. Se usa `keyt` (nombre + 3 letras del apellido).
- **LATERAL nearest-neighbor con `<-> LIMIT k` por fila** → una busqueda GIN por cada una
  de 2.2M filas → 312s. No hay busqueda por fila: un unico hash-join sobre el catalogo.
- **Recomputar el regex de normalizacion en cada corrida** (lo hacia la version previa) →
  sustituido por `norm_name` materializado + backfill.
- **Escritura de memberships via ORM `add_all` (cientos de miles de objetos)** →
  `INSERT...SELECT`, las filas nunca viajan a Python.
- **Clustering transitivo perfecto (Union-Find / CTE recursiva)** → coste/riesgo en la VM;
  el ancla `min(id)` es correcto para la gran mayoria de nombres. Fuera de alcance.
- **Incremental por bloques con `reconcile_state`** → para no perder conexiones entre lotes
  habria que recomputar los bloques `keyt`/`key2` completos afectados, que con nombres muy
  frecuentes abarca casi todo el catalogo (degenera en rebuild pero con deuda de estado).
  Se eligio **rebuild completo optimizado**, que cabe en presupuesto y es idempotente. El
  incremental queda como plan B documentado solo si la medicion en la VM lo exige.

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
| `/stats` | GET | Estadisticas agregadas (Silver, en vivo) |
| `/candidates` | GET | Pares candidatos (reconciliacion legacy por pares) |
| `/groups` | GET | Grupos de duplicados por similitud difusa (`min_confidence`, `limit`) |
| `/gold/stats` | GET | Estadisticas precomputadas (capa Gold) |
| `/gold/completeness` | GET | Distribucion de completitud de campos (Gold) |
| `/medallion` | GET | Conteos de las 3 capas Bronze/Silver/Gold para el dashboard |

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

Dashboard interactivo en http://localhost:8501 con tres pestañas:

- **🏅 Arquitectura**: vista Medallion en vivo (Bronze → Silver → Gold) con los conteos
  de cada capa y el ratio de mensajes crudos por persona consolidada.
- **👤 Personas**: buscador con filtros (nombre, ciudad, empresa, puesto), selector de
  tamaño de página (25–500), paginación con botones Anterior/Siguiente, tabla de
  resultados y ficha de detalle al seleccionar una persona. Arriba, tarjetas de métricas
  y gráficos de barras (top ciudades, top empresas) desde `/stats`.
- **🔗 Duplicados**: candidatos a duplicado de la reconciliación (`/candidates`) con la
  confianza como barra de progreso, filtro por confianza mínima y comparación lado a
  lado de las dos personas de cada par.

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

### Ya implementado (nivel Experto)

- **Reconciliacion batch**: job periodico que agrupa candidatos a duplicado por nombre
  difuso en `duplicate_groups`, leyendo `norm_name` materializado (ver seccion de matching).
- **Fix de consolidacion (Silver)**: `consolidate_merge` fusiona la misma persona partida en
  varias filas (mismo passport + nombre muy parecido) con reglas de survivorship.
- **Capa Gold + Medallion**: `gold_persons` (subconjunto completo de Silver) + agregados
  `gold_*` recalculados sobre ese subconjunto, expuestos por la API (`/gold/*`, `/medallion`)
  y visualizados en la pestaña Arquitectura del frontend.
- **Orquestacion con Airflow 3**: DAG secuencial `hr_etl_maintenance`
  (`consolidate_merge >> reconcile >> refresh_gold`, cada 30 min, `max_active_runs=1`) por
  dependencia de datos; y DAG event-driven con sensor deferrable que refresca Gold cuando
  entran suficientes personas nuevas (o cada 15 min como fallback).
- **CI/CD**: workflow unico que corre lint + tests y, si pasan en `main`, despliega por SSH
  a la VM de Oracle.

### Mejoras futuras

1. **Normalizacion de valores persistidos**: Colapsar dobles espacios tambien en los valores
   que se guardan en Postgres, no solo en las keys de matching.

2. **Backpressure**: Si Redis se llena, reducir velocidad de consumo (consumer pause/resume).

3. **Reconciliacion incremental**: hoy es un rebuild completo cada pasada (siempre
   correcto, y barato gracias al colapso a nombres distintos). Con volumenes mucho mayores
   se podria procesar solo las personas nuevas desde la ultima corrida.

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

### CI/CD (GitHub Actions)

Un unico workflow (`.github/workflows/CICD.yml`, nombre visible **CI/CD**) cubre
integracion y despliegue:

- **Job `unit`**: levanta Postgres/Mongo/Redis como *service containers*, instala el
  paquete con extras `dev,frontend`, y corre `ruff`, `black --check` y `pytest` con
  cobertura. Sube `coverage.xml` como artefacto.
- **Job `airflow`**: levanta el stack de Airflow y ejecuta los tests del sensor
  deferrable dentro del contenedor (`scripts/run-airflow-tests.sh`).
- **Job `deploy`**: solo en **push a `main`** y solo si `unit` pasa. Entra por SSH a la
  VM de Oracle y ejecuta `deploy/deploy.sh` (pull de `main` + rebuild/restart del stack
  principal). Requiere los secrets `VM_HOST`, `VM_USER`, `VM_SSH_KEY`.

### Despliegue

Pensado para una **VM del Always Free de Oracle Cloud**:

- Overlay de producción `docker-compose.prod.yml` + `deploy/Caddyfile`: Caddy termina
  HTTPS automáticamente (Let's Encrypt) y hace de reverse proxy; las bases de datos no se
  exponen al exterior (solo Caddy publica 80/443).
- Script de arranque inicial `deploy/setup-oracle-vm.sh` (instala Docker, abre el
  firewall, clona y levanta el stack).
- Script de redeploy idempotente `deploy/deploy.sh` (usado por el CD y también a mano).
- **Airflow** se despliega aparte con `docker-compose.airflow.yml` (opt-in). Sus
  credenciales y el nombre de red se leen del `.env` (sin secretos en el repo). Los DAGs
  van montados por volumen, así que un `git pull` los actualiza sin reiniciar Airflow.
