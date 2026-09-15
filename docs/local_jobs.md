# Operaciones y jobs locales

La #54 anade operaciones a FastAPI sobre casos de uso existentes. No ejecuta
ordenes de inversion. Streamlit y los CLI siguen disponibles, pero no deben
escribir al mismo tiempo que la API sobre el mismo entorno.

## Activacion explicita

El comando original conserva el modo de solo lectura. Para habilitar operaciones
hay que elegir el entorno al arrancar, nunca mediante una peticion HTTP:

```powershell
.\.venv\Scripts\python.exe scripts\run_api.py --operations real
.\.venv\Scripts\python.exe scripts\run_api.py --env-file demo/synthetic_config/.env.demo --operations demo
```

POSIX, con el entorno virtual activado:

```bash
python scripts/run_api.py --operations real
python scripts/run_api.py --env-file demo/synthetic_config/.env.demo --operations demo
```

Ambos siguen escuchando solo en `127.0.0.1`. Usar **una sola instancia operativa**
por entorno; el bloqueo de sistema operativo rechaza una segunda. No usar
workers multiples de Uvicorn ni ejecutar importaciones/refresh de Streamlit o
CLI simultaneamente. El bloqueo de la API no coordina esos programas legacy.

- Real: solo escribe en `src/data/local/` y `src/degiro_exports/local/` dentro
  del repositorio configurado. Rutas personalizadas fuera de estos limites se
  rechazan, aunque esten en `.env`.
- Demo: exige `DATA_DIR=demo/local_data` y `PRICE_PROVIDER=synthetic`. Al activar
  operaciones, copia exports/config sinteticos a ese directorio **solo si no
  existen**. Uploads van a `demo/local_data/degiro_exports/incoming`; brief,
  targets y seleccion de benchmark tienen copias editables en `demo/local_data`.
  Las fixtures versionadas nunca son destinos de escritura.
- En ambos, las subrutas configuradas y los enlaces simbolicos/junctions se
  validan para evitar destinos fuera del entorno. Es una proteccion local,
  no un sandbox contra otro proceso malicioso del mismo usuario.

El arranque operativo crea `jobs.duckdb` y `api-worker.lock` en el directorio
de datos. No cambia `portfolio.duckdb` hasta ejecutar una operacion que lo
necesite. El modo de lectura no crea esos archivos.

## Enviar una operacion

Las operaciones aceptan JSON, no paths, YAML libre, credenciales, snapshots de
cartera ni opciones arbitrarias de providers. Cada envio requiere:

- `Content-Type: application/json`.
- `X-ML-Finance-Confirm: local-write`.
- `Idempotency-Key`: identificador de 8–128 caracteres alfanumericos, `_` o `-`.
- Body con `workspace_mode` igual al entorno del servidor y `confirm: true`.

Estos controles evitan cambios accidentales y peticiones simples de paginas
ajenas. No son autenticacion: otros procesos locales pueden llamar a la API.
Host/Origin y la allowlist CORS de Vite siguen activos; no publicar ni tunelizar
el servidor. Usar solo origen propio o Vite en `localhost:5173`/`127.0.0.1:5173`.

Ejemplo PowerShell para generar un informe en la demo:

```powershell
$jobHeaders = @{ "X-ML-Finance-Confirm" = "local-write"; "Idempotency-Key" = [guid]::NewGuid().ToString() }
$jobBody = @{ workspace_mode = "demo"; confirm = $true } | ConvertTo-Json
$job = Invoke-RestMethod http://127.0.0.1:8000/api/v1/reports/monthly -Method Post -ContentType application/json -Headers $jobHeaders -Body $jobBody
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/jobs/$($job.job_id)"
```

Equivalente POSIX; conservar la misma clave si se pierde la respuesta y se
reenvia **la misma solicitud**, y elegir otra para una nueva ejecucion:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/reports/monthly \
  -H 'Content-Type: application/json' \
  -H 'X-ML-Finance-Confirm: local-write' \
  -H 'Idempotency-Key: demo-report-001' \
  -d '{"workspace_mode":"demo","confirm":true}'
```

## Rutas

Prefijo `/api/v1`. Las operaciones devuelven **202 con un job**, no el resultado
financiero final. Consultar `/jobs/{job_id}` hasta un estado terminal.

| Metodo y ruta | Campos adicionales del body |
| --- | --- |
| POST `/degiro/uploads` | `uploaded_at` ISO y `uploads`: lista de `filename` + `content_base64` |
| POST `/degiro/import` | Ninguno; importa el incoming configurado |
| POST `/market-data/refresh` | `fx_provider` y `price_provider` obligatorios; `start_date`/`end_date` opcionales |
| POST `/benchmarks/refresh` | Solo real; `provider: "yfinance_ecb"`, `start_date` y `end_date` obligatorios, fechas anteriores a hoy |
| POST `/reports/monthly` | `as_of_date` opcional |
| POST `/portfolio/contributions/simulate` | `contribution_amount`, `allow_fractional_units`, `minimum_order_value`, `max_orders`, `as_of_date` opcionales |
| POST `/agents/monthly-runs` | `llm_provider=static`, `search_provider=null`, presupuesto `monthly_budget` e interes `user_satellite_interest` opcionales |
| PUT `/settings/investment-brief` | `content` y `expected_previous_hash` obligatorios |
| PUT `/settings/portfolio-targets` | `portfolio_targets` como objeto y `expected_previous_hash` obligatorios |
| GET `/settings/investment-brief` | Texto, existencia y hash; sin path |
| GET `/settings/portfolio-targets` | Contrato, existencia y hash; sin path |
| GET `/jobs` | Query `limit`, default 20, entre 1 y 100 |
| GET `/jobs/{job_id}` | Estado de un job, o 404 |
| POST `/jobs/{job_id}/retry` | Confirmacion/entorno; clave nueva; solo simulaciones fallidas |

El frontend puede convertir archivos CSV a base64; no hay multipart ni una
dependencia nueva para uploads. Maximo 5 archivos, 5 MiB por archivo, 10 MiB
decodificados en total y 14 MiB de body HTTP (incluido con transferencia por
fragmentos). Se mantienen los nombres canonicos DEGIRO. Se rechazan rutas en
nombres, tipos desconocidos, colisiones en un lote y reemplazos de un archivo
existente con contenido distinto. Un CSV guardado todavia necesita importacion;
los parsers validan su contenido al importar. Limite del brief: 65.536 caracteres;
parametros persistidos de otras operaciones: 128 KiB.

Actualizar brief/targets exige primero leer su hash. El control se comprueba
**al ejecutar el job**, bajo el bloqueo de escritura, no solo al recibirlo. Si
otro job ya cambio el contenido, el nuevo termina `failed/content_conflict` y
no sobrescribe nada. Recargar y revisar antes de volver a enviar.

Benchmarks se actualizan por separado, con confirmacion e Idempotency-Key. No
forman parte del refresh general; no admiten retry de jobs fallidos. La CLI
`refresh_benchmarks.py` comparte el bloqueo del worker. Ver [fuentes y cache](benchmarks.md).

Providers de refresh: `synthetic` solo para demo, `yfinance` solo con seleccion
explicita en real. Agentes: defaults `static/null`; `openai`, `tavily` o
`duckduckgo` solo por seleccion explicita en real. La demo rechaza proveedores
externos. Ejecutar agentes sigue entrando por `RunMonthlyAgentsUseCase`, con
preflight, construccion de analitica y auditoria existentes; no se salta ninguna
validacion ni se acepta contexto financiero fabricado por HTTP.

## Persistencia, concurrencia y recuperacion

Un worker en el mismo proceso atiende la cola secuencialmente. `jobs.duckdb` es
una base separada del almacen financiero; los accesos a su conexion se serializan.
No hay Redis, Celery, Postgres ni servidor adicional.

Los estados son `pending`, `running`, `succeeded`, `partial` y `failed`. Cada job
conserva fechas, fase, progreso, warnings, codigo de error y resultado/artefactos.
El progreso es por etapas (inicio, FX/precios, fin), no una estimacion de tiempo.
El resultado `skipped` de un caso de uso se representa como job `partial`,
conservando el estado original dentro de `result`. No hay mas de 20 jobs activos.

Durante la ejecucion, las lecturas de datos devuelven 409 `workspace_busy`;
health y polling de jobs siguen disponibles. Esto evita abrir DuckDB de lectura
y escritura con configuraciones incompatibles o leer artefactos a medio generar.
Los GET no escriben; el worker puede estar cambiando estado en paralelo.

Una clave de idempotencia repetida con los mismos parametros devuelve el mismo
job, incluso tras reiniciar. Reutilizarla con otros parametros da 409
`idempotency_conflict`. Esto evita dobles envios; **no promete ejecucion
exactamente una vez frente a un fallo de proceso entre un efecto y su registro**.

Al reiniciar, jobs `pending`/`running` abandonados pasan a
`failed/worker_interrupted`. No se reejecutan automaticamente. Revisar posibles
efectos ya realizados antes de solicitar otra ejecucion, especialmente llamadas
a proveedores, informes o importaciones. Ctrl+C espera la operacion activa y
marca pendientes como `worker_stopped`; no cancela una escritura a medias.

Solo `/retry` de una simulacion fallida crea otro job: no modifica cartera ni
llama a proveedores, aunque usa el estado actual. Reintentar el envio con la
misma clave nueva devuelve ese mismo job. Otros tipos reciben `retry_not_safe`.

Errores HTTP incluyen 403 confirmacion/origen, 409 conflicto/ocupado,
413 tamano, 415 formato, 422 parametros/entorno, 429 cola llena y 503 worker no
disponible. Un fallo tras el 202 aparece en el job, no cambia retroactivamente la
respuesta HTTP. Las excepciones inesperadas usan `operation_failed` sin texto
privado; los resultados conservan warnings y referencias de artefactos saneadas.

La base de jobs guarda parametros privados (incluidos uploads, brief y targets)
para trazabilidad, pero los GET no los exponen. No hay purga automatica todavia:
el historial consume disco. Jobs y resultados siguen siendo privados; ver
[privacidad](privacy.md). El modo operativo no es apto para un servicio publico.

## Verificacion

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_api_jobs.py tests/test_api.py tests/test_interface_boundaries.py
```

Tests con temporales/fixtures sinteticas: estados, fallos, reinicio, deduplicacion,
bloqueo de segundo worker, control optimista, limites, defaults offline y flujo
importacion → refresh → informe → simulacion → agentes. Se comprueba que las
fixtures versionadas no cambian. El archivo de bloqueo de Windows se excluye
del hash durante su uso porque el sistema impide leer el byte bloqueado.

Referencias: [concurrencia DuckDB](https://duckdb.org/docs/stable/connect/concurrency.html)
y [ciclo de vida FastAPI](https://fastapi.tiangolo.com/advanced/events/).
