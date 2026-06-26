# Scripts

Entradas manuales para la **v1 local con Streamlit**. En Windows se recomienda
usar los wrappers PowerShell.

## Comandos principales

```powershell
.\scripts\test.ps1
.\scripts\run_dashboard.ps1
.\scripts\run_demo.ps1
.\scripts\refresh_market_data.ps1 -EndDate 2026-05-14
```

## Flujos

- `run_dashboard.ps1`: abre Streamlit con `.env` o con `-EnvFile`.
- `run_demo.ps1`: prepara la demo sintetica y abre Streamlit con
  `demo/synthetic_config/.env.demo`.
- `refresh_market_data.ps1`: refresca FX y precios hasta `-EndDate`.
- `test.ps1`: ejecuta la suite pytest con `.venv`.

## Scripts Python

- `import_degiro.py`: importa CSVs canonicos de DEGIRO.
- `refresh_fx_rates.py`: refresca pares FX.
- `refresh_market_data.py`: sincroniza activos y refresca precios.
- `generate_monthly_report.py`: genera informe mensual Markdown.
- `run_monitor_tematico.py`: ejecuta el monitor tematico.
- `run_monthly_agents.py`: ejecuta la red mensual de agentes.
- `bootstrap_demo.py`: prepara datos sinteticos de demo sin abrir Streamlit.

Ejemplo de pipeline mensual local:

```powershell
.\.venv\Scripts\python.exe scripts\import_degiro.py
.\.venv\Scripts\python.exe scripts\refresh_fx_rates.py
.\.venv\Scripts\python.exe scripts\refresh_market_data.py
.\.venv\Scripts\python.exe scripts\generate_monthly_report.py
.\.venv\Scripts\python.exe scripts\run_monthly_agents.py --llm-provider static --search-provider null
```

Para ejecuciones reales con proveedores externos:

```powershell
.\.venv\Scripts\python.exe scripts\run_monthly_agents.py --llm-provider openai --search-provider tavily
```

Notas:

- La demo escribe en `demo/local_data/`, ignorado por Git.
- El uso privado lee `.env`, `src/degiro_exports/local/` y `src/data/local/`.
- Para instrucciones operativas de agentes de programacion, consulta `AGENTS.md`.
