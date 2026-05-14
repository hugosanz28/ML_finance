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

## Notas operativas

- `import_degiro.py` carga por defecto los parquets normalizados a DuckDB.
- `refresh_fx_rates.py` no reescribe los parquets; alimenta `fx_rates`.
- `refresh_market_data.py` usa `asset_overrides.csv` para tickers manuales y exclusiones.
- La valoracion diaria usa `broker_snapshot_anchored`: el precio local absoluto
  viene de snapshots DEGIRO y market data aporta la variacion relativa.
- `run_monthly_agents.py` puede ejecutarse en modo demo sin red/API con `--llm-provider static --search-provider null`.
- Para una ejecucion IA real, usa `--llm-provider openai` y, si quieres busqueda externa, `--search-provider duckduckgo`.
