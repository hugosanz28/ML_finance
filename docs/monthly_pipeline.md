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
- Audit trail de agentes: `src/data/local/agents/monthly_pipeline/<run_id>/run_metadata.json`,
  `input_payload.json` y `agents/<agent_name>/...`
- Overrides temporales de informes para agentes: `src/data/local/agents/input_overrides/latest_monthly_report_override_YYYY-MM-DD.md`

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
- Desde Streamlit la validacion es mas estricta: el informe seleccionado y el
  snapshot enviado a agentes deben coincidir tambien con la fecha valorada
  actual de la cartera. Si no coinciden, hay que generar un informe nuevo antes
  de ejecutar agentes.
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
