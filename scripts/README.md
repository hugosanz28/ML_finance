# Scripts

Este directorio se reserva para comandos manuales o automatizables del proyecto.

Scripts disponibles:

- `import_degiro.py`
- `refresh_market_data.py`
- `refresh_fx_rates.py`
- `generate_monthly_report.py`
- `run_monitor_tematico.py`
- `run_monthly_agents.py`
- `bootstrap_demo.py`
- `test.ps1`
- `run_dashboard.ps1`
- `run_demo.ps1`
- `refresh_market_data.ps1`

La idea es que cada script haga una sola cosa y pueda ejecutarse de forma reproducible en local.

Notas de uso:

- `import_degiro.py` lee los CSV canonicos de `src/degiro_exports/local/incoming/` y genera parquets normalizados bajo `src/data/local/normalized/degiro/`.
- `refresh_market_data.py` sincroniza primero los activos desde `src/data/local/normalized/degiro/`.
- `refresh_fx_rates.py` infiere pares de divisas desde los normalizados DEGIRO y alimenta `fx_rates`.
- Ambos aceptan `--end-date YYYY-MM-DD`; Streamlit usa la misma idea en `Vista general` -> `Actualizar a hoy`.
- Si un activo necesita ticker manual o debe excluirse del refresh, usa `src/data/local/market_data/asset_overrides.csv`.
- Los precios externos se usan como variacion relativa en la politica `broker_snapshot_anchored`, no como precio absoluto final.
- `generate_monthly_report.py` genera el informe mensual en Markdown; el flujo esta documentado en `docs/monthly_report.md`.
- `run_monitor_tematico.py` prepara y ejecuta `monitor_tematico`; con `--dry-run` permite inspeccionar inputs y temas observados sin llamadas reales.
- `run_monthly_agents.py` ejecuta la red mensual completa mediante `RunMonthlyAgentsUseCase` en `src/application/`: `monitor_tematico`, `analista_activos` y `asistente_aportacion_mensual`.
- `bootstrap_demo.py` prepara la demo publica sintetica con `demo/synthetic_config/.env.demo`: importa CSV ficticios, carga DuckDB demo, inserta precios sinteticos offline y genera un informe mensual inicial.
- `test.ps1` ejecuta la suite con el Python de `.venv`.
- `run_dashboard.ps1` abre Streamlit para la cartera configurada por `.env` o por `-EnvFile`.
- `run_demo.ps1` prepara la demo sintetica y abre Streamlit con `demo/synthetic_config/.env.demo`.
- `refresh_market_data.ps1` ejecuta FX y precios en orden, con `-EndDate YYYY-MM-DD` opcional.
- Para busqueda externa, `run_monitor_tematico.py` y `run_monthly_agents.py`
  aceptan `--search-provider tavily` si tienes `TAVILY_API_KEY`. `duckduckgo`
  sigue disponible como fallback best-effort sin API key, `static` genera
  resultados sinteticos locales para demo y `null` desactiva la busqueda web.

## Comandos rapidos en Windows

Ejecutar tests:

```powershell
.\scripts\test.ps1
```

Abrir dashboard real:

```powershell
.\scripts\run_dashboard.ps1
```

Preparar y abrir demo publica:

```powershell
.\scripts\run_demo.ps1
```

Refrescar FX y precios hasta una fecha:

```powershell
.\scripts\refresh_market_data.ps1 -EndDate 2026-05-14
```

Ejemplo de inicio de flujo mensual:

```powershell
.\.venv\Scripts\python.exe scripts\import_degiro.py
.\.venv\Scripts\python.exe scripts\refresh_fx_rates.py
.\.venv\Scripts\python.exe scripts\refresh_market_data.py
.\.venv\Scripts\python.exe scripts\generate_monthly_report.py
.\.venv\Scripts\python.exe scripts\run_monthly_agents.py --llm-provider static --search-provider null
```

Ejemplo de ejecucion real con OpenAI y Tavily:

```powershell
.\.venv\Scripts\python.exe scripts\run_monthly_agents.py --llm-provider openai --search-provider tavily
```

Ejemplo para extender datos hasta hoy o una fecha concreta:

```powershell
.\.venv\Scripts\python.exe scripts\refresh_fx_rates.py --end-date 2026-05-14
.\.venv\Scripts\python.exe scripts\refresh_market_data.py --end-date 2026-05-14
```
