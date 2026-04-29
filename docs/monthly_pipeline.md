# Flujo mensual

## Orden recomendado

```powershell
.\.venv\Scripts\python.exe scripts\import_degiro.py
.\.venv\Scripts\python.exe scripts\refresh_fx_rates.py --only-missing-base
.\.venv\Scripts\python.exe scripts\refresh_market_data.py
.\.venv\Scripts\python.exe scripts\generate_monthly_report.py
.\.venv\Scripts\python.exe scripts\run_monthly_agents.py --llm-provider static --search-provider null
```

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
- `run_monthly_agents.py` puede ejecutarse en modo demo sin red/API con `--llm-provider static --search-provider null`.
- Para una ejecucion IA real, usa `--llm-provider openai` y, si quieres busqueda externa, `--search-provider duckduckgo`.
