# Flujo mensual

## Orden recomendado

```powershell
.\.venv\Scripts\python.exe scripts\import_degiro.py
.\.venv\Scripts\python.exe scripts\refresh_fx_rates.py --only-missing-base
.\.venv\Scripts\python.exe scripts\refresh_market_data.py
.\.venv\Scripts\python.exe scripts\generate_monthly_report.py
.\.venv\Scripts\python.exe scripts\run_monthly_agents.py --llm-provider static --search-provider null
```

El mismo flujo puede ejecutarse desde Streamlit:

```powershell
.\.venv\Scripts\python.exe -m streamlit run src\portfolio\dashboard.py
```

Abre `http://localhost:8501` cuando Streamlit termine de arrancar.

La pestaña `Actualizar datos` permite subir CSVs, importar, refrescar FX,
refrescar precios y generar informes. La pestaña `Agentes` permite revisar los
inputs y ejecutar la red mensual.

Las interfaces comparten los mismos casos de uso:

- uploads/importacion: `SaveDegiroUploadsUseCase` e `ImportDegiroUseCase`;
- FX/precios: `InferFxRequirementsUseCase`, `RefreshFxUseCase` y
  `RefreshMarketDataUseCase`;
- informes: `GenerateMonthlyReportUseCase`;
- cartera: `GetPortfolioStateUseCase`;
- agentes: `RunMonthlyAgentsUseCase` y, para ejecucion aislada,
  `RunMonitorTematicoUseCase`.

`RunMonthlyAgentsUseCase` ejecuta un preflight determinista obligatorio. Errores
de valoracion, precios, FX o fechas bloquean antes de llamar a LLM/busqueda. Los
warnings quedan auditados y permiten continuar con estado `partial`.

Para una actualizacion rapida sin importar nuevos CSVs ni generar informe, usa
`Vista general` -> `Actualizar a hoy`. Ese boton refresca FX y precios hasta la
fecha actual, limpia la cache y mantiene el ultimo snapshot DEGIRO como ancla.
La fecha visible de la vista pasa a ser la ultima fecha valorada disponible, no
necesariamente la fecha del snapshot.

## Entradas estables

- CSV canonicos de DEGIRO: `src/degiro_exports/local/incoming/`
- Investment brief editable: `src/data/local/investment_brief.md`
- Overrides de market data: `src/data/local/market_data/asset_overrides.csv`

## Salidas principales

- Normalizados DEGIRO: `src/data/local/normalized/degiro/`
- Bodega DuckDB: `src/data/local/portfolio.duckdb`
- Informes mensuales: `src/data/local/reports/`
- Resultados de agentes: `src/data/local/agents/monthly_pipeline/<run_id>/pipeline_result.json`
- Preflight de calidad: `src/data/local/agents/monthly_pipeline/<run_id>/preflight.json`
- Audit trail de agentes: `src/data/local/agents/monthly_pipeline/<run_id>/run_metadata.json`,
  `input_payload.json` y `agents/<agent_name>/...`
- Overrides temporales de informes para agentes: `src/data/local/agents/input_overrides/latest_monthly_report_override_YYYY-MM-DD.md`

### Auditoria reproducible

El schema v2 persiste la peticion y el contexto efectivos de cada agente. El
`request.json` conserva `scope`, `parameters`, `constraints`, `metadata` e
`input_refs`; estas referencias se resuelven contra el contexto real, por lo que
analista y asistente incluyen tambien resultados de agentes anteriores.

Cada directorio de agente añade `provider.json` y `audit_metadata.json`.
`provider.json` contiene solo provider, modelo y opciones allowlisted; no
incluye credenciales, cabeceras, variables de entorno ni clientes SDK.
`raw_response.json` usa `captured`, `partial` o `not_captured` y un
`reason_code` estable cuando la captura no es completa.

Los hashes SHA-256 se calculan sobre JSON canonico y contenido semantico. Los
hashes de entrada excluyen ids de run y timestamps volatiles; los de salida
representan la salida parseada. Sirven para detectar cambios reproducibles, no
para anonimizar datos ni validar una recomendacion.

Los runs sin `schema_version` o sin los artefactos nuevos son legacy v1.
`GetAgentRunAuditUseCase` y Streamlit los leen sin reescribirlos y muestran como
no disponible la metadata que no existia entonces.

## Notas operativas

- `import_degiro.py` carga por defecto los parquets normalizados a DuckDB.
- `refresh_fx_rates.py` no reescribe los parquets; alimenta `fx_rates`.
- `refresh_market_data.py` usa `asset_overrides.csv` para tickers manuales y exclusiones.
- En entorno normal FX/precios usan `yfinance`. La demo usa
  `PRICE_PROVIDER=synthetic`: conserva datos sembrados, no accede a red y no
  fabrica nuevas series.
- La valoracion diaria usa `broker_snapshot_anchored`: el precio local absoluto
  viene de snapshots DEGIRO y market data aporta la variacion relativa.
- `run_monthly_agents.py` usa `static/null` como baseline offline y default
  seguro. El monitor no recibe resultados de busqueda y puede devolver
  cobertura `partial`.
- La construccion directa de los agentes tambien usa providers offline. OpenAI
  y cualquier busqueda web deben seleccionarse de forma explicita.
- Para una demo offline completa, `static/static` añade resultados de busqueda
  sinteticos y deterministas. No representan hechos de mercado reales.
- Para una ejecucion IA real, usa `--llm-provider openai`. Para busqueda externa
  prioriza `--search-provider tavily` si tienes `TAVILY_API_KEY`; usa
  `--search-provider duckduckgo` como fallback best-effort sin API key.
- La red de agentes valida fechas antes de ejecutarse: el `as_of_date` del
  informe mensual y el `as_of_date` de `portfolio_metrics_snapshot` deben
  coincidir. Si se pasa un informe Markdown manual, la fecha se extrae de
  `as_of_date:` en el frontmatter o del titulo `Informe mensual ... YYYY-MM-DD`.
- Si el preflight bloquea, el comando termina con codigo `1` y, salvo que se use
  `--no-persist`, guarda el intento sin crear resultados ni prompts de agentes.
  Con solo warnings ejecuta la red y termina con codigo `0`.
- Streamlit envia sus metricas y el snapshot editable al mismo
  `RunMonthlyAgentsUseCase` que usa CLI, y muestra el preflight comun. Si las
  fechas no coinciden, hay que generar un informe nuevo antes de ejecutar.
- Antes de llamar a los agentes, `portfolio_metrics_snapshot` se enriquece con
  `asset_overrides.csv`: nombre normalizado, ticker, mercado y divisa de trading.
  Si DEGIRO entrega un nombre truncado, se conserva como `broker_asset_name` y
  `asset_name` pasa a ser el nombre normalizado.
- El informe que reciben los agentes incorpora una seccion `Referencia de activos
  para agentes` con nombre normalizado, nombre broker, ISIN, ticker, divisa y
  tipo. Esto evita que el LLM tenga que inferir el producto desde nombres
  truncados en tablas Markdown.
- `pipeline_result.json` persiste resultados finales, fuentes y findings, pero
  no repite cuerpos completos de inputs dentro de cada `source.metadata`. Las
  claves voluminosas como `content`, `positions`, `daily` o `findings` se
  reflejan en `omitted_metadata_keys`.

## Proveedores de agentes

Combinaciones recomendadas:

- `static/null`: baseline y tests offline.
- `static/static`: demo publica offline con contexto sintetico.
- `openai/tavily`: ejecucion externa con busqueda API.
- `openai/duckduckgo`: ejecucion externa con busqueda best-effort.

## Busqueda externa para agentes

El monitor tematico soporta cuatro modos:

- `null`: no busca en la web; util como baseline o para pruebas sin red.
- `static`: genera fixtures sinteticos locales; util solo para demo y tests.
- `duckduckgo`: scraping HTML best-effort sin coste ni API key. Puede devolver
  cero resultados si cambia el HTML, hay bloqueo o la query no encaja.
- `tavily`: proveedor API mas estable para agentes. Requiere `TAVILY_API_KEY`
  en `.env` o en variables de entorno.

Ejemplo con Tavily:

```powershell
.\.venv\Scripts\python.exe scripts\run_monthly_agents.py --llm-provider openai --search-provider tavily
```
