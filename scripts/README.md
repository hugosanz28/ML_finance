# Scripts

Entradas manuales para la **v2 local FastAPI/React y la v1 Streamlit**. En Windows se recomienda
usar los wrappers PowerShell.

Requieren Python 3.11 o posterior y la instalación de ejecución:

```powershell
python -m pip install -r requirements.txt
```

Para desarrollar o ejecutar la suite usa `requirements-dev.txt`; consulta
`CONTRIBUTING.md` para el resto de puertas de calidad.

## Comandos principales

```powershell
.\scripts\test.ps1
.\scripts\run_dashboard.ps1
.\scripts\run_demo.ps1
.\scripts\refresh_market_data.ps1 -EndDate 2026-05-14
```

## Flujos

- `run_frontend_e2e_api.py`: servidor exclusivo de tests Playwright, con copia
  sintetica temporal y red externa bloqueada. No acepta configuracion real.
  Se lanza mediante `npm run test:e2e` o `npm run showcase`, no como servidor
  habitual del usuario. Ver [capturas reproducibles](../docs/showcase.md).
- `refresh_benchmarks.py`: descarga explicita de proxies ETF y €STR oficiales
  a cache local real. Requiere fechas, `--provider yfinance_ecb` y `--confirm`;
  comparte el bloqueo del worker. Ver [benchmarks](../docs/benchmarks.md).
- `export_frontend_demo_contract.py`: exporta 16 respuestas sinteticas de
  FastAPI a `.test_tmp/frontend-api.json` para verificar los contratos del
  frontend. Requiere el bootstrap demo previo y dependencias de desarrollo.
  No admite paths ni entornos reales. Ver [UI React](../frontend/README.md).
- `run_api.py`: inicia FastAPI de solo lectura en `127.0.0.1:8000`.
  Windows: `.\.venv\Scripts\python.exe scripts\run_api.py`;
  POSIX con entorno activo: `python scripts/run_api.py`.
  Admite `--env-file` y `--port`; ver [API local](../docs/local_api.md).
  `--operations demo|real` habilita escrituras y worker persistente; consultar
  [jobs locales](../docs/local_jobs.md) antes de activarlo.
- `run_dashboard.ps1`: abre Streamlit con `.env` o con `-EnvFile`.
- `run_demo.ps1`: prepara la demo sintetica y abre Streamlit con
  `demo/synthetic_config/.env.demo`.
- `refresh_market_data.ps1`: refresca FX y precios hasta `-EndDate`.
- `test.ps1`: ejecuta la suite pytest con `.venv`.

`run_dashboard.ps1` y `run_demo.ps1` dejan Streamlit activo hasta que el usuario
lo cierre. Para una validacion automatizada de la demo ejecuta solo:

```powershell
$env:ML_FINANCE_ENV_FILE = "demo/synthetic_config/.env.demo"
.\.venv\Scripts\python.exe scripts\bootstrap_demo.py
```

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

Cuando los inputs pueden prepararse, el runner imprime el preflight de calidad.
Devuelve codigo `1` y no llama a providers si detecta errores bloqueantes; los
warnings permiten continuar y mantienen codigo `0`. `--no-persist` desactiva
tanto el resultado del pipeline como su audit trail.

`static/null` es el baseline offline: el monitor no recibe resultados de
busqueda. Para una demo offline mas completa usa `static/static`; la busqueda
devuelve exclusivamente fixtures sinteticos:

```powershell
.\.venv\Scripts\python.exe scripts\run_monthly_agents.py --llm-provider static --search-provider static
```

Para ejecuciones reales con proveedores externos:

```powershell
.\.venv\Scripts\python.exe scripts\run_monthly_agents.py --llm-provider openai --search-provider tavily
```

En Ubuntu, Linux o macOS se ejecutan los scripts Python directamente. Por
ejemplo:

```bash
python scripts/refresh_market_data.py --end-date 2026-05-14
ML_FINANCE_ENV_FILE=demo/synthetic_config/.env.demo python scripts/bootstrap_demo.py
python -m streamlit run src/portfolio/dashboard.py
```

Notas:

- La demo escribe en `demo/local_data/`, ignorado por Git.
- El uso privado lee `.env`, `src/degiro_exports/local/` y `src/data/local/`.
- `static/null` y `static/static` no usan red. `openai`, `tavily` y
  `duckduckgo` deben seleccionarse de forma explicita y sí pueden enviar
  consultas a servicios externos.
- Para instrucciones operativas de agentes de programacion, consulta `AGENTS.md`.
