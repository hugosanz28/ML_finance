# Dashboard Streamlit

## Ejecucion

Desde la raiz del repo:

```powershell
.\.venv\Scripts\python.exe -m streamlit run src\portfolio\dashboard.py
```

Por defecto usa `.env` y las rutas reales locales. Para abrir la demo
sintetica, prepara primero `scripts\bootstrap_demo.py` y ejecuta:

```powershell
$env:ML_FINANCE_ENV_FILE="demo/synthetic_config/.env.demo"
.\.venv\Scripts\python.exe -m streamlit run src\portfolio\dashboard.py
```

Ese modo apunta a `demo/local_data/` y no a `src/data/local/`.
Ademas usa `PRICE_PROVIDER=synthetic`: los refresh de FX y precios no acceden a
la red y conservan las series precargadas por el bootstrap.

Despues abre:

```text
http://localhost:8501
```

Alternativa equivalente:

```powershell
.\.venv\Scripts\streamlit.exe run src\portfolio\dashboard.py
```

Si el puerto `8501` ya esta ocupado, Streamlit elegira otro puerto y lo mostrara
en la terminal.

## Vistas incluidas

- `Vista general`: asignacion actual y valor total de la ultima fecha valorada. El ultimo snapshot DEGIRO se muestra como ancla, pero la fecha principal puede avanzar con market data posterior. Incluye `Actualizar a hoy` para refrescar FX y precios sin importar nuevos CSVs.
- `Evolucion`: valor historico calculado por activo. DEGIRO fija el precio local de referencia en cada snapshot y market data aporta la variacion relativa diaria, sin aplicar un segundo anclaje global.
- `Informes`: lectura de informes mensuales generados en `reports_history` o en la carpeta de informes.
- `Actualizar datos`: subida de CSVs DEGIRO, importacion, carga DuckDB, refresh FX, refresh precios e informe mensual.
- `Agentes`: revision de inputs, edicion del `investment_brief`, presupuesto mensual editable (`monthly_budget`), control de envio de `target_weights`, ejecucion de la red mensual de agentes y exploracion de auditoria de runs guardados. Antes de ejecutar muestra la fecha valorada actual, la fecha del informe mensual y la fecha del snapshot que recibiran los agentes.

## Organizacion del codigo

`src/portfolio/dashboard.py` es solo el entrypoint de Streamlit: configura la
pagina, pinta el sidebar y conecta las pestanas. La logica visual vive en
modulos separados:

- `dashboard_overview.py`: `Vista general`, `Evolucion` y refresh a hoy.
- `dashboard_reports.py`: lectura y generacion de informes.
- `dashboard_data_update.py`: subida de CSVs e import/refresh de datos.
- `dashboard_agents.py`: inputs, ejecucion y visualizacion de agentes.
- `dashboard_common.py`: tema, cache, formateo y helpers compartidos.

La logica de transformacion de datos para graficas se mantiene en
`dashboard_transforms.py`, y la gestion de uploads DEGIRO en
`dashboard_uploads.py`.

## Actualizar desde Vista general

El boton `Actualizar a hoy` de `Vista general` ejecuta solo la parte de mercado:

- refresca `fx_rates` hasta la fecha actual,
- refresca `prices_daily` hasta la fecha actual,
- limpia la cache de Streamlit para recalcular la vista,
- y conserva el ultimo snapshot DEGIRO como ancla de posiciones/precios locales.

Tras refrescar, la `Fecha de referencia` de `Vista general` sale de la ultima
fecha valorada por `portfolio_daily_metrics`, no de la fecha del ultimo snapshot.
Si hay precios posteriores al snapshot, las posiciones se proyectan hasta esa
fecha con cantidades ancladas a DEGIRO y precios relativos de mercado.

No importa nuevos CSVs ni cambia el snapshot de broker disponible. Si quieres que
la cartera incluya nuevas compras, ventas, movimientos de efectivo o cantidades
oficiales de DEGIRO, primero debes subir/importar las exportaciones en
`Actualizar datos`.

## Flujo desde la UI

1. Entra en `Actualizar datos`.
2. Sube los CSV de DEGIRO o deja los existentes en `src/degiro_exports/local/incoming/`.
3. Pulsa `Guardar CSVs en incoming` si has subido archivos desde la UI.
4. Pulsa `1. Importar DEGIRO`.
5. Pulsa `2. Refrescar FX`.
6. Pulsa `3. Refrescar precios`.
7. Pulsa `4. Generar informe`.
8. Entra en `Agentes`, revisa los inputs y activa o desactiva `Enviar
   target_weights al pipeline`. Usa `static/null` como baseline offline o
   `static/static` para una demo completa con contexto sintetico. Para una
   ejecucion real usa `openai/tavily` si tienes `TAVILY_API_KEY`;
   `openai/duckduckgo` queda como fallback best-effort sin API key.

La pestaña `Agentes` envia los inputs a `RunMonthlyAgentsUseCase`, que ejecuta el
mismo preflight usado por CLI. Si el informe, las metricas calculadas y el
`portfolio_metrics_snapshot` no comparten fecha, o faltan precios/FX, la UI
muestra los codigos de calidad y no se construyen providers. Si aparece el
bloqueo, vuelve a `Actualizar datos` y corrige los datos o genera un informe
nuevo antes de ejecutar la red.

`BuildAgentDashboardSnapshotUseCase` prepara el
`portfolio_metrics_snapshot` editable y aplica `asset_overrides.csv`. Cuando
DEGIRO trae nombres truncados, la UI conserva el valor original como
`broker_asset_name` y muestra en `asset_name` el nombre normalizado que usaran
los agentes.

### Auditoria de agentes

La pestana `Agentes` incluye un bloque `Auditoria de agentes` para revisar runs
persistidos en `src/data/local/agents/monthly_pipeline/<run_id>/` o en el
directorio demo equivalente. Esta vista permite inspeccionar:

- estado del run, preflight de calidad, fechas, inputs y versiones de prompts;
- plan interno, acciones permitidas, acciones usadas, acciones descartadas y
  base de decision de cada agente;
- warnings y errores;
- findings, artifacts y metadata completa;
- fuentes citadas por agente y por finding;
- prompts renderizados;
- inputs efectivos recibidos por cada agente;
- request y raw response guardada.

La `raw_response` puede aparecer como `not_captured` porque los proveedores
actuales devuelven objetos de dominio ya parseados. Si mas adelante interesa
auditar la respuesta bruta del LLM, habra que ampliar el contrato de providers.
Los intentos bloqueados persistidos tambien aparecen en la lista con estado
`blocked`. Incluyen metadata, inputs y preflight, pero no `pipeline_result.json`,
outputs ni tabs de agentes vacios.

Al guardar desde la UI, el dashboard detecta el tipo de exportacion por el
nombre del archivo y lo copia a `incoming` con el nombre canonico que exige el
importador:

- cartera, portfolio, posiciones o snapshot -> `portfolio_YYYY-MM-DD.csv`
- transacciones, transactions, operaciones u orders -> `transactions_YYYY-MM-DD_YYYY-MM-DD.csv`
- cuenta, account, cash, efectivo, movimientos o activity -> `account_YYYY-MM-DD_YYYY-MM-DD.csv`

Si el nombre contiene fechas, se usan esas fechas. Se aceptan formatos como
`YYYY-MM-DD`, `DD-MM-YYYY`, `YYYYMMDD` y `DDMMYYYY`, tambien con `_` o `.` como
separador. Para transacciones y cuenta se usa el rango minimo-maximo detectado.
Para portfolio se usa la fecha mas reciente detectada. Si no hay fecha en el
nombre, se usa el dia de subida.

Los archivos cuyo tipo no pueda detectarse por nombre no se guardan y aparecen
como `omitido` en la tabla de resultados.

Flujo rapido equivalente:

1. Entra en `Actualizar datos`.
2. Pulsa `1. Importar DEGIRO`.
3. Pulsa `Ejecutar flujo mensual basico`.

El boton `Ejecutar flujo mensual basico` encadena importacion, carga DuckDB, FX,
precios e informe. No ejecuta automaticamente los agentes; eso se hace en la
pestana `Agentes` para poder revisar antes los inputs.

Para actualizar una cartera ya importada hasta hoy sin generar informe ni tocar
CSV, usa `Vista general` -> `Actualizar a hoy`.

## Parar el dashboard

Si lo arrancaste en una terminal, usa `Ctrl+C`.

Si quedo en segundo plano, localiza el proceso Python/Streamlit:

```powershell
Get-Process | Where-Object { $_.ProcessName -like "*python*" }
```

## Contratos usados

El dashboard no calcula cartera por su cuenta. Tanto las acciones operativas
como los read models entran por `src/application/`; los modulos
`dashboard_*` solo mantienen composicion visual y transformaciones para
graficas.

Casos de uso operativos:

- `SaveDegiroUploadsUseCase`
- `ImportDegiroUseCase`
- `RefreshFxUseCase`
- `RefreshMarketDataUseCase`
- `GenerateMonthlyReportUseCase`
- `RunMonthlyAgentsUseCase`
- `UpdateInvestmentBriefUseCase`

Read models usados por la UI:

- `LoadPortfolioMetricsUseCase`
- `LoadPortfolioSnapshotsUseCase`
- `LoadPortfolioTransactionsUseCase`
- `GetNetExternalContributionsUseCase`
- `GetPendingDegiroImportStatusUseCase`
- `GetWarehouseCountsUseCase`
- `ListDashboardReportsUseCase`
- `ReadInvestmentBriefUseCase`
- `ReadTargetWeightsUseCase`
- `BuildAgentDashboardSnapshotUseCase`
- `ListAgentRunsUseCase`
- `GetAgentRunAuditUseCase`

`GetPortfolioStateUseCase` ofrece, ademas, un read model general serializable
para otros adaptadores. La UI actual usa los casos de uso granulares anteriores
para no cargar historico o posiciones cuando una vista no los necesita.

El `investment_brief` editable vive por defecto en:

```text
src/data/local/investment_brief.md
```

La UI lo guarda mediante `UpdateInvestmentBriefUseCase`, con escritura atomica
y un hash de la version cargada para no sobrescribir cambios hechos desde otra
sesion.

Los objetivos estructurados de cartera viven por defecto en:

```text
src/data/local/portfolio_targets.yaml
```

El dashboard usa ese archivo para pre-rellenar `target_weights` en la pestaña
`Agentes`. Si el archivo no existe o no es valido, mantiene un valor JSON de
fallback editable. El contrato de ejemplo esta en
`src/data/sample/portfolio_targets.example.yaml`.
