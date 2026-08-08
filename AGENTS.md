# AGENTS.md

Instrucciones para agentes de programacion que trabajen en este repo. Si necesitas detalle humano o narrativo, consulta los README y `docs/`.

## Resumen del proyecto

`ML_finance` es una aplicacion local de analitica de cartera para exportaciones oficiales de DEGIRO. La version actual es `v0.1.0`: **v1 local con Streamlit**.

El sistema importa CSVs de DEGIRO, normaliza datos, guarda estado local en DuckDB/Parquet, refresca FX/precios, reconstruye cartera historica, genera informes Markdown y ejecuta agentes mensuales con auditoria. La demo publica usa datos sinteticos en `demo/` y no debe tocar datos reales.

Separacion publico/privado:

- Versionable: codigo, docs, tests, `demo/synthetic_*`, `src/data/sample/`.
- Privado e ignorado por Git: `src/data/local/`, `src/degiro_exports/local/`, `.env`, `demo/local_data/`.
- Para market data de demo usa `PRICE_PROVIDER=synthetic`. Para agentes en
  demo/tests usa `llm_provider=static` y `search_provider=null`; usa
  `search_provider=static` solo cuando necesites fixtures de busqueda
  sinteticos. No uses proveedores externos salvo que la tarea lo pida.

La UI actual es Streamlit. Una futura API/FastAPI debe entrar casi siempre por `src/application/`, no por modulos internos de dominio.

## Directrices de trabajo

- Antes de tocar codigo: revisa `git status --short`, lee el README/doc del modulo afectado, localiza tests existentes y confirma si hay datos privados implicados.
- Lee primero este archivo; despues abre docs especificas segun la tarea.
- Usa `rg` / `rg --files` para buscar codigo y referencias.
- Manten cambios acotados. No refactorices fuera del area necesaria.
- No versionar, imprimir ni abrir artefactos privados innecesarios de `src/data/local/`, `src/degiro_exports/local/` o `.env`.
- No introduzcas red/proveedores externos en tests, demo o ejemplos. Market
  data offline usa `synthetic`; agentes usan por defecto
  `llm_provider=static`, `search_provider=null`.
- `src/application/` es la frontera para scripts, Streamlit y futura API. Si una interfaz necesita dominio, crea o usa un `*UseCase`.
- Casos de uso actuales que no deben puentearse desde interfaces:
  `GetPortfolioStateUseCase`, `SaveDegiroUploadsUseCase`,
  `InferFxRequirementsUseCase`, `RunMonitorTematicoUseCase`,
  `UpdateInvestmentBriefUseCase`, `ReadPortfolioTargetsUseCase`,
  `UpdatePortfolioTargetsUseCase`, `SimulateContributionUseCase` y los casos
  operativos de importacion, refresh, informes y agentes.
- Manten entradas como dataclasses `*Request` y salidas estructuradas. Para acciones operativas usa `ApplicationResult`.
- No dupliques calculos financieros en `src/application/`; coordina servicios de dominio existentes.
- `src/portfolio/dashboard.py` debe seguir siendo entrypoint fino de Streamlit. Las vistas viven en `dashboard_overview.py`, `dashboard_contribution_lab.py`, `dashboard_reports.py`, `dashboard_data_update.py` y `dashboard_agents.py`.
- No metas logica de UI en dominio ni queries/repositorios directos en Streamlit si existe caso de uso equivalente.
- Los datos de cartera salen de exportaciones oficiales DEGIRO y artefactos derivados validados. Los agentes no deben inventar estado de cartera.
- Preserva auditoria de agentes: plan interno, acciones permitidas/usadas/descartadas, fuentes, prompts, warnings, inputs y outputs.
- Si cambias la auditoria, conserva lectura legacy, JSON estricto, hashes
  semanticos y provider metadata allowlisted; nunca persistas credenciales,
  cabeceras de autorizacion, variables de entorno ni clientes SDK.
- Si cambia comportamiento de agentes, actualiza tests y, cuando aplique, prompts versionados o documentacion de agentes.
- Si cambias comandos documentados, valida que siguen existiendo.
- Si tocas docs, evita duplicar manuales largos: enlaza a `docs/` o README de dominio.

Invariantes y errores tipicos:

- No asumas que `src/data/local/` existe, esta poblado o puede abrirse sin necesidad.
- No mezcles un informe mensual y un `portfolio_metrics_snapshot` con fechas distintas; el pipeline debe bloquearlo.
- Para rendimiento, solo `DEPOSIT` y `WITHDRAWAL` son flujos externos. No uses
  `daily_return_pct` como TWR ni reclasifiques dividendos, comisiones, FX o
  transferencias internas como aportaciones.
- Las interfaces y ejecuciones de usuario deben entrar por
  `RunMonthlyAgentsUseCase`: el preflight bloquea errores antes de construir
  providers y audita el intento cuando la persistencia esta activa.
- No uses `.\scripts\run_demo.ps1` en automatizacion: abre Streamlit y queda vivo.
- No llames desde Streamlit a `src.agents`, `src.reports`, `src.market_data` o importadores si ya hay caso de uso en `src/application/`.
- No aceptes YAML libre de targets desde interfaces: usa un mapping JSON
  estructurado y `UpdatePortfolioTargetsUseCase`.
- El laboratorio de aportacion es siempre `contributions_only`: solo puede
  proponer compras de posiciones actuales valoradas y mapeadas. No vende, no
  ejecuta ordenes y muestra la caja residual separada de los pesos posteriores.
- No infieras buckets por nombres: `asset_bucket_mapping` debe resolver de forma
  exacta cada `asset_id` o ISIN activo contra un bucket de
  `target_allocation`.
- No cambies prompts/agentes sin mantener la auditoria: plan, acciones, fuentes, prompts, warnings, inputs y outputs.

## Como ejecutar tests

Suite completa:

```powershell
.\.venv\Scripts\python.exe -m pytest
```

Wrapper equivalente en Windows:

```powershell
.\scripts\test.ps1
```

Tests focalizados frecuentes:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_application_layer.py
.\.venv\Scripts\python.exe -m pytest tests\test_agent_audit_trail.py
.\.venv\Scripts\python.exe -m pytest tests\test_streamlit_dashboard_uploads.py
.\.venv\Scripts\python.exe -m pytest tests\test_dashboard_transforms.py
.\.venv\Scripts\python.exe -m pytest tests\test_streamlit_dashboard_smoke.py
.\.venv\Scripts\python.exe -m pytest tests\test_public_documentation.py
.\.venv\Scripts\python.exe -m pytest tests\test_contribution_planner.py tests\test_contribution_application.py
```

Mapa rapido de tests por area:

| Area | Tests recomendados |
| --- | --- |
| `src/application/` | `tests\test_application_layer.py`, `tests\test_interface_boundaries.py` |
| Agentes | `tests\test_agent_*.py`, `tests\test_*agente*.py`, `tests\test_monitor_tematico.py`, `tests\test_analista_activos.py`, `tests\test_asistente_aportacion_mensual.py` |
| Auditoria de agentes | `tests\test_agent_audit_trail.py`, `tests\test_application_layer.py` |
| Preflight de agentes | `tests\test_data_quality.py`, `tests\test_application_layer.py`, `tests\test_agent_audit_trail.py`, `tests\test_run_monthly_agents_cli.py` |
| Dashboard | `tests\test_dashboard_transforms.py`, `tests\test_streamlit_dashboard_uploads.py` |
| DEGIRO/importacion | `tests\test_degiro_*.py` |
| Portfolio/metricas y laboratorio | `tests\test_portfolio_metrics.py`, `tests\test_portfolio_performance.py`, `tests\test_positions.py`, `tests\test_portfolio_state_projection.py`, `tests\test_portfolio_contributions.py`, `tests\test_contribution_planner.py`, `tests\test_contribution_application.py` |
| Market data/FX | `tests\test_market_data.py`, `tests\test_fx_refresh.py` |
| Defaults offline | `tests\test_agent_safe_defaults.py`, `tests\test_demo_workspace.py` |
| Documentacion/publicacion | `tests\test_public_documentation.py`, `tests\test_dev_commands.py` |
| Smoke Streamlit | `tests\test_streamlit_dashboard_smoke.py` |

Validar demo sin abrir Streamlit:

```powershell
$env:ML_FINANCE_ENV_FILE="demo/synthetic_config/.env.demo"
.\.venv\Scripts\python.exe scripts\bootstrap_demo.py
```

No uses `.\scripts\run_demo.ps1` como verificacion automatica: prepara la demo y abre Streamlit, por lo que queda vivo hasta que el usuario lo cierre.

Checks utiles antes de publicar:

```powershell
git diff --check
.\.venv\Scripts\python.exe -m ruff check src scripts tests
.\.venv\Scripts\python.exe -m mypy
.\.venv\Scripts\python.exe -m pytest --cov=src --cov-report=term-missing
$trackedFiles = git ls-files
.\.venv\Scripts\detect-secrets-hook.exe --baseline .secrets.baseline $trackedFiles
rg -n "C:\\\\Users|OPENAI_API_KEY\s*=|TAVILY_API_KEY\s*=|BEGIN .*PRIVATE KEY|password\s*=|token\s*=|secret\s*=" README.md AGENTS.md docs scripts src tests demo
```

La cobertura minima es 70% y se configura en `pyproject.toml`. Para build,
instalacion del wheel, `pip check` y `pip-audit`, sigue
[CONTRIBUTING.md](CONTRIBUTING.md) y [scripts/README.md](scripts/README.md);
no dupliques aqui el pipeline completo de CI.

## Documentacion de rutas

| Ruta | Uso | Notas |
| --- | --- | --- |
| `README.md` | Entrada humana del proyecto | Vision, quickstart y enlaces. No meter instrucciones largas para agentes. |
| `AGENTS.md` | Entrada para agentes de programacion | Mantener por debajo de 32 KB y sin paja. |
| `src/application/` | Casos de uso reutilizables | Frontera para scripts, Streamlit y futura API. |
| `src/portfolio/` | Metricas, reconstruccion y dashboard Streamlit | `dashboard.py` fino; vistas en `dashboard_*.py`. |
| `src/agents/` | Pipeline mensual, modelos, agentes y prompts | Leer `src/agents/README.md` antes de cambiar agentes. |
| `src/agents/prompts/` | Prompts versionados | Cambios de comportamiento LLM deben quedar trazables. |
| `src/degiro_exports/` | Parsers/importacion de CSVs DEGIRO | `local/` es privado e ignorado. |
| `src/market_data/` | FX, precios, proveedores y overrides | Precios externos aportan variacion relativa; no sustituyen precio broker absoluto. |
| `src/reports/` | Informes mensuales e historial | Markdown para revision manual y agentes. |
| `src/data/sample/` | Datos de ejemplo versionables | Sinteticos o anonimizados. |
| `src/data/local/` | Datos reales derivados privados | No versionar. Incluye DuckDB, reports, agents audit, caches. |
| `demo/` | Demo publica sintetica | `demo/local_data/` es generado/ignorado. |
| `scripts/` | Entradas CLI/PowerShell | Ver `scripts/README.md` para comandos humanos. |
| `tests/` | Suite pytest | Configurada en `pyproject.toml`. |
| `docs/architecture.md` | Arquitectura v1 | Componentes y flujo de datos. |
| `docs/api_contracts.md` | Contratos futuros API local | No implementa FastAPI; define contratos. |
| `docs/streamlit_dashboard.md` | Uso del dashboard | Flujos UI, auditoria y uploads. |
| `docs/privacy.md` | Privacidad y secretos | Leer antes de publicar, demo o capturas. |
| `docs/monthly_pipeline.md` | Flujo mensual completo | Datos, informes y agentes. |
| `docs/performance.md` | Rendimiento de cartera | Flujos externos, retornos diarios, TWR, MWR/XIRR y limites. |

Comandos principales:

- Dashboard local: `.\scripts\run_dashboard.ps1`
- Demo publica: `.\scripts\run_demo.ps1`
- Refresh FX/precios: `.\scripts\refresh_market_data.ps1 -EndDate YYYY-MM-DD`
- Agentes mensuales CLI: `.\.venv\Scripts\python.exe scripts\run_monthly_agents.py --llm-provider static --search-provider null`
