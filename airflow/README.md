# Apache Airflow 3.3.1 — HR ETL Scheduler

## Qué es

Airflow programa y ejecuta automáticamente los jobs batch del pipeline (capa de
mantenimiento de la arquitectura Medallion):

- **Refresh Gold** (`hr_etl_refresh_gold`, cada 5 min): recalcula las tablas Gold
  (`gold_stats`, `gold_top_cities`, `gold_top_companies`, `gold_completeness`).
- **Reconciliation** (`hr_etl_reconciliation`, cada 30 min): detecta duplicados
  probables entre personas y los guarda en `match_candidates`.
- **Gold event-driven** (`hr_etl_gold_eventdriven`): refresca Gold **cuando hay
  suficientes personas nuevas**, no a ciegas por reloj, usando un *sensor deferrable*.
  Ver la sección [Sensor deferrable](#sensor-deferrable-gold-event-driven) más abajo.

Estos DAGs usan `BashOperator` que invocan los mismos CLI que puedes ejecutar a mano:

```
python -m hr_etl.warehouse.gold_layer
python -m hr_etl.processing.reconcile
```

## Diseño

- Airflow vive en un compose **separado** (`docker-compose.airflow.yml`) para que la
  imagen (pesada) sea *opt-in* y no ralentice el stack principal.
- Ejecutor: **LocalExecutor** (suficiente para demo; en producción se recomienda
  Celery/Kubernetes). Servicios de Airflow 3:
  - `airflow-api-server` — API + UI en el puerto 8080 (sustituye al `webserver` de v2).
  - `airflow-scheduler` — planifica y dispara los tasks.
  - `airflow-dag-processor` — parsea los ficheros de DAG (proceso separado en v3).
  - `airflow-triggerer` — corre el event loop asíncrono de las tareas *deferrable*.
  - `airflow-init` — one-shot: crea la BD de metadatos, migra y siembra el usuario admin.
- **Metadatos aislados**: Airflow usa una base de datos `airflow` propia dentro del
  mismo servidor Postgres, separada de `hr_warehouse` (los DAGs sí leen/escriben en
  `hr_warehouse`, que es donde vive la capa Silver/Gold).
- **Sin entorno aislado**: la imagen `apache/airflow:3.3.1` ya trae SQLAlchemy 2.x,
  pydantic 2.11+, pydantic-settings y psycopg2 — exactamente lo que `hr_etl` necesita.
  El paquete se hace importable montando `./src` y con `PYTHONPATH=/opt/airflow/src`.
  No se fuerzan versiones con `_PIP_ADDITIONAL_REQUIREMENTS` (hacerlo degradaría las
  dependencias del propio Airflow y lo rompería).

## Cómo levantarlo

El stack principal debe estar arriba primero, porque Airflow se engancha a su red
(`hr_net`) y a su Postgres:

```powershell
# 1) Infra del stack principal (crea la red hr_net y Postgres)
docker compose up -d postgres mongo redis

# 2) Airflow (init corre solo la primera vez; luego api-server + scheduler + dag-processor)
docker compose -f docker-compose.airflow.yml up -d
```

La primera arrancada tarda un poco (descarga la imagen de Airflow 3.3.1 y migra la BD).

## Acceso

- UI: http://localhost:8080
- Usuario: **admin** / **admin**
- DAGs visibles: `hr_etl_refresh_gold`, `hr_etl_reconciliation`
- Los DAGs se crean **en pausa** (`DAGS_ARE_PAUSED_AT_CREATION=true`); actívalos con el
  toggle de la UI para que corran por su schedule.

### Autenticación (Airflow 3)

Airflow 3 usa por defecto el *Simple Auth Manager*. Ya no existe `airflow users create`.
El usuario se define con `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin` y la
contraseña se fija sembrando el fichero
`AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE` con `{"admin": "admin"}` (lo hace
`airflow-init`). Es una comodidad para desarrollo/demo, no para producción.

## Probar los jobs sin esperar al schedule

```powershell
# Listar DAGs y comprobar que no hay errores de parseo
docker exec proyecto1_modulo3_de2-airflow-scheduler-1 airflow dags list
docker exec proyecto1_modulo3_de2-airflow-scheduler-1 airflow dags list-import-errors

# Ejecutar un task directamente (no necesita despausar el DAG)
docker exec proyecto1_modulo3_de2-airflow-scheduler-1 airflow tasks test hr_etl_refresh_gold refresh_gold_layer
docker exec proyecto1_modulo3_de2-airflow-scheduler-1 airflow tasks test hr_etl_reconciliation batch_reconciliation
```

## Apagar / limpiar

```powershell
# Parar Airflow (conserva volúmenes: logs, auth, metadatos en Postgres)
docker compose -f docker-compose.airflow.yml down

# Limpieza total de Airflow (borra sus volúmenes; NO toca hr_warehouse ni pg_data)
docker compose -f docker-compose.airflow.yml down --volumes
```

> Nota: evita `--remove-orphans` aquí, ya que comparte nombre de proyecto con el stack
> principal y pararía también postgres/mongo/redis/app/api.

## Notas

- Los CLI (`gold_layer`, `reconcile`) se conectan a la misma infra que el stack
  principal usando las variables de entorno definidas en el compose de Airflow.
- Para producción se recomienda CeleryExecutor o KubernetesExecutor; LocalExecutor
  basta para la demo.

---

## Sensor deferrable (Gold event-driven)

Esta sección documenta, paso a paso, el DAG `hr_etl_gold_eventdriven`, que refresca la
capa Gold **cuando de verdad hay datos nuevos** en lugar de hacerlo a ciegas cada X
minutos. Es una pieza de nivel Experto+ y demuestra el patrón *deferrable* de Airflow.

### 1) ¿Qué es una tarea deferrable y por qué?

Un sensor "clásico" que espera una condición (p.ej. "que lleguen N registros") se queda
en un bucle *poke* ocupando un slot de worker todo el rato que espera. Si tienes muchas
esperas simultáneas, gastas muchos workers sin hacer nada útil.

Una tarea **deferrable** parte la espera en dos:

1. La tarea comprueba una vez si la condición ya se cumple. Si no, en vez de bloquear el
   worker, lanza un pequeño objeto **trigger** (una corrutina `async`) y se **aparca**
   (estado `deferred`), **liberando el worker**.
2. El proceso **triggerer** corre un event loop `asyncio` y vigila muchos triggers a la
   vez con muy pocos recursos.
3. Cuando el trigger detecta que la condición se cumple, la tarea se **reanuda** en un
   worker para terminar.

En resumen: espera barata y centralizada, en lugar de un worker bloqueado por espera.

### 2) La señal y el umbral elegidos

- **Señal**: `COUNT(*) FROM persons WHERE created_at > <watermark>`.
- **Watermark**: `gold_stats.updated_at`, es decir, el momento del **último refresh de
  Gold**. Así medimos exactamente lo que Gold va a consumir (filas nuevas en `persons`),
  no el volumen bruto de Kafka (que es ruidoso: ~5 fragmentos crudos por persona, más los
  que aún están en el buffer de Redis sin consolidar).
- **Se dispara** cuando `personas_nuevas >= GOLD_TRIGGER_MIN_NEW_PERSONS` **o** cuando
  vence el `timeout` del sensor (fallback temporal para que Gold no se quede obsoleto en
  baja carga).

Parámetros configurables por variable de entorno (definidos en
`docker-compose.airflow.yml`):

| Variable | Default | Significado |
|----------|---------|-------------|
| `GOLD_TRIGGER_MIN_NEW_PERSONS` | `150` | Personas nuevas necesarias para disparar |
| `GOLD_TRIGGER_POLL_SECONDS` | `30` | Cada cuánto sondea el trigger en el triggerer |

### 3) Dónde vive el código

- `src/hr_etl/airflow_ext/persons_threshold.py`:
  - `NewPersonsTrigger(BaseTrigger)` — trigger **async** que consulta Postgres con
    `asyncpg` (no bloquea el event loop del triggerer). Su `serialize()` devuelve el
    *dotted path* para que el triggerer lo reconstruya.
  - `NewPersonsSensor(BaseSensorOperator)` — hace un pre-check síncrono barato; si el
    umbral ya se cumple, termina sin diferir; si no, llama a `self.defer(...)`.
- `airflow/dags/hr_etl_gold_eventdriven.py`: el DAG `wait_for_new_persons >> refresh_gold`.

El código está **dentro del paquete `hr_etl`** (no en la carpeta `dags/`) a propósito:
así es importable por *dotted path* desde todos los procesos de Airflow, incluido el
triggerer que deserializa el trigger, y además es testeable.

### 4) Requisito de infraestructura

Para que las tareas deferrable funcionen hace falta el servicio **`airflow-triggerer`**
(ya incluido en el compose). Además, en este despliegue multi-contenedor los tasks de
Airflow 3 llaman de vuelta a la Task Execution API del `api-server`; por eso el compose
fija:

```
AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-api-server:8080/execution/
```

Sin esa variable, los workers intentarían `localhost:8080` y fallarían con
`Connection refused`.

### 5) Cómo probarlo en vivo

```powershell
# Activar y disparar el DAG
docker exec proyecto1_modulo3_de2-airflow-scheduler-1 airflow dags unpause hr_etl_gold_eventdriven
docker exec proyecto1_modulo3_de2-airflow-scheduler-1 airflow dags trigger hr_etl_gold_eventdriven

# Ver el estado de las tareas (sustituye <run_id> por el que devuelve el trigger)
docker exec proyecto1_modulo3_de2-airflow-scheduler-1 `
  airflow tasks states-for-dag-run hr_etl_gold_eventdriven <run_id>
# -> wait_for_new_persons debería estar en estado 'deferred' si aún no hay 150 nuevas
```

Para forzar que se cumpla el umbral y ver el ciclo `deferred -> success` (inserta 150
personas de prueba con `created_at = NOW()`):

```powershell
docker exec proyecto1_modulo3_de2-postgres-1 psql -U hr_user -d hr_warehouse -c `
 "INSERT INTO persons (match_key, full_name, created_at, updated_at)
  SELECT 'test:trigger-'||g, 'Test '||g, NOW(), NOW() FROM generate_series(1,150) AS g;"
```

En el siguiente sondeo del triggerer (~30s) el sensor pasa a `success` y se ejecuta
`refresh_gold_layer`. Limpieza de las filas de prueba:

```powershell
docker exec proyecto1_modulo3_de2-postgres-1 psql -U hr_user -d hr_warehouse -c `
 "DELETE FROM persons WHERE match_key LIKE 'test:trigger-%';"
```

> Nota: `airflow tasks test ...` ejecuta la tarea *inline* y no muestra el ciclo de
> defer; para ver el estado `deferred` hay que hacer un `dags trigger` real como arriba.

### 5b) Tests del sensor (se ejecutan dentro del contenedor)

Los tests unitarios del sensor/trigger están en `tests/test_persons_threshold.py`. Como
necesitan el paquete real `apache-airflow` (que **no** está en el venv de dev, por
pesado y frágil de versiones), en local se **saltan solos** (`pytest.importorskip`).
Para ejecutarlos de verdad, hay un script que los corre **dentro del contenedor** de
Airflow (donde Airflow sí está instalado):

```powershell
# Requiere el stack de Airflow arriba: docker compose -f docker-compose.airflow.yml up -d
.\scripts\run-airflow-tests.ps1        # Windows / PowerShell
```

```bash
./scripts/run-airflow-tests.sh          # Linux / macOS / CI
```

El script copia el test al contenedor `scheduler`, ejecuta `pytest` con
`PYTHONPATH=/opt/airflow/src` y devuelve el código de salida de pytest (0 = OK), así que
sirve tal cual como paso de CI. Si el sensor se rompe, el script falla y te enteras (un
poco más tarde que en la suite local, pero te enteras).

### 6) ¿Y el DAG `hr_etl_refresh_gold` de cada 5 min?

`hr_etl_gold_eventdriven` es la versión **event-driven** y sustituye conceptualmente al
refresh "a reloj". Para la demo, activa el event-driven y deja pausado el de 5 minutos
(o al revés, según lo que quieras enseñar). Ambos hacen el mismo refresh; la diferencia
es *cuándo* deciden ejecutarlo.
