# API local: lectura y operaciones opcionales

La issue #53 incorpora FastAPI como adaptador de `src/application/`.
React es la interfaz mantenida. El modo por defecto sigue siendo de solo lectura.
La #54 incorpora [operaciones y jobs](local_jobs.md), que requieren activacion
explicita con `--operations demo` o `--operations real`.
La [UI React](../frontend/README.md) (#55/#56) consume analitica de lectura y
operaciones opt-in; utiliza API:8000 y Vite:5173 en loopback.

## Arranque

Instalar las dependencias actualizadas con `python -m pip install -r requirements.txt`.
Desde la raiz del repositorio, en Windows:

```powershell
.\.venv\Scripts\python.exe scripts\run_api.py
```

En POSIX, con el entorno virtual activado:

```bash
python scripts/run_api.py
```

El servidor queda en `http://127.0.0.1:8000`. `--port` permite cambiar el puerto;
no se admite un host publico. Se detiene con Ctrl+C. No arrancarlo como check
automatizado sin controlar y finalizar su proceso.

La configuracion se resuelve una sola vez en el servidor. Para la demo ya
preparada, usar `--env-file demo/synthetic_config/.env.demo`. Si aun no existe,
prepararla antes en un proceso separado:

```powershell
$env:ML_FINANCE_ENV_FILE = "demo/synthetic_config/.env.demo"
.\.venv\Scripts\python.exe scripts\bootstrap_demo.py
.\.venv\Scripts\python.exe scripts\run_api.py --env-file demo/synthetic_config/.env.demo
```

Equivalente POSIX:

```bash
ML_FINANCE_ENV_FILE=demo/synthetic_config/.env.demo python scripts/bootstrap_demo.py
python scripts/run_api.py --env-file demo/synthetic_config/.env.demo
```

`bootstrap_demo.py` escribe exclusivamente el entorno sintetico configurado;
la API en modo de lectura no prepara datos ni crea la base DuckDB al arrancar
o consultar. El modo operativo inicializa su almacen de jobs y copias demo.
Sin datos, health y el catalogo funcionan y la analitica informa que no esta
disponible. La salud del proceso no certifica calidad financiera.

## Endpoints disponibles

Todas las rutas siguientes llevan el prefijo `/api/v1` y usan GET:

| Ruta | Respuesta |
| --- | --- |
| `/health` | Salud, version API, modo `read_only`/`operations` y entorno `demo`/`real` |
| `/portfolio/state` | Resumen, posiciones, historico opcional y avisos |
| `/analytics/summary` | Resumen analitico completo |
| `/analytics/performance` | Rendimiento, TWR/MWR y flujos |
| `/analytics/risk` | Riesgo, concentracion y correlaciones |
| `/analytics/benchmarks` | Seleccion, catalogo y comparacion disponibles |
| `/analytics/metric-definitions` | Catalogo educativo completo |
| `/analytics/metrics/{metric_id}` | Una definicion, o 404 si no existe |
| `/reports` | Lista de IDs de informes mensuales, `limit` entre 1 y 100 |
| `/reports/{report_id}` | ID y contenido Markdown como texto JSON |
| `/agents/runs` | Runs persistidos, `limit` entre 1 y 100 |
| `/agents/runs/{run_id}` | Auditoria compatible con legacy, sin metadata de rutas |

Los IDs de informe son los nombres sin `.md` de archivos legacy `monthly_*.md`
o actuales `YYYY-MM-DD-monthly-YYYYMMDDTHHMMSSffffff.md` en
`Settings.reports_dir`; no se siguen rutas externas guardadas en el historial.
Los IDs de run son los directorios de auditoria configurados. No se aceptan
rutas arbitrarias ni saltos de directorio; enlaces fuera del directorio
permitido se rechazan antes de leer contenido.

Los parametros y payloads de cartera/analitica estan en
[contratos API](api_contracts.md). La cartera siempre usa `persist=False` y
DuckDB de solo lectura; `persist` no es un parametro HTTP admitido. Los
endpoints reutilizan calculos existentes y no construyen proveedores externos.
En datos reales se lee la cache local de benchmarks validada, si existe. Sin
cache se conserva `benchmark_provider_unavailable`: GET no descarga ni sustituye
por datos sinteticos. Fuentes, proxies, hashes y actualizacion explicita se
documentan en [benchmarks](benchmarks.md).

`/portfolio/state?include_history=true` incluye tambien `asset_history`:
series por ID con `series_kind=valuation_price_proxy` y puntos
`valuation_date`/`price_change` (fraccion decimal o null). La base es la primera
valoracion valida desde la primera compra, no el inicio del filtro de UI.
Sin historico solicitado devuelve una lista vacia. No son retornos totales;
ver [migracion y limites](react_migration.md).

OpenAPI con esquemas Pydantic: `GET /openapi.json`. Swagger/ReDoc no se sirven
por defecto para evitar cargar recursos de un CDN. Referencias del framework:
[modelos de query](https://fastapi.tiangolo.com/tutorial/query-param-models/) y
[servidor ASGI](https://fastapi.tiangolo.com/deployment/manually/).

## Errores y privacidad

Error estable: `{"error":{"code":"invalid_request","message":"Invalid request parameters."}}`.

- 422 `invalid_request`: fecha/periodo/benchmark/tasa/ID invalidos o parametros
  adicionales en consultas de cartera, analitica y listados.
- 404 `portfolio_data_unavailable`: falta cartera; 404 `not_found`: recurso ausente.
- 400 `invalid_host` y 403 `origin_not_allowed`: proteccion de acceso local.
- 405 `method_not_allowed`: metodo no disponible.
- 500 `internal_error`: fallo inesperado o datos malformados; nunca se envia
  el texto de la excepcion, SQL, credenciales o rutas locales en el error.
- Analitica sin datos: 200 con `status=unavailable`, `reason_code` y warnings.
  Las metricas parciales conservan su propia cobertura y motivo; no son cero.

Las respuestas llevan `Cache-Control: no-store`. Solo se permiten hosts
`127.0.0.1` y `localhost`; las peticiones de navegador deben proceder del mismo
origen o de Vite local en puerto 5173. CORS no permite credenciales ni origen
comodin. El launcher desactiva access logs y confianza en headers de proxy.

Esto no es autenticacion: otros procesos/usuarios locales pueden acceder a la
API. No exponerla a Internet, tuneles o proxies. Una publicacion requiere otro
modelo de seguridad. La API no renderiza HTML de informes ni ejecuta Markdown.
La futura UI debe desactivar HTML inseguro y recursos remotos al representarlo.

Informes y auditorias siguen siendo privados. La proyeccion omite campos de
ruta y redacta rutas configuradas y patrones comunes de credenciales, pero no
garantiza anonimizar texto libre ni detectar secretos arbitrarios. No publicar
estas respuestas. Los artefactos originales no se modifican y sus hashes se
refieren al original, no a la proyeccion redactada.

## Verificacion

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_api.py tests/test_interface_boundaries.py
```

Los tests usan temporales y exportaciones sinteticas; comprueban rutas, errores,
OpenAPI, aislamiento de entornos y hashes de archivos antes/despues de lecturas.
Bloquean conexiones externas (el loopback interno de asyncio se permite).
La prueba de symlinks se omite en Windows si el sistema no permite crearlos;
la misma prueba corre en CI POSIX.
