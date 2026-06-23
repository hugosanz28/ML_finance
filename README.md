# ML_finance

`ML_finance` evoluciona desde un repositorio de exploración en Jupyter hacia un sistema local para:

- importar exportaciones oficiales de DEGIRO,
- reconstruir y seguir el histórico de la cartera,
- enriquecer posiciones con datos de mercado,
- generar informes periódicos,
- exponer una interfaz simple en Streamlit,
- y servir como proyecto público enseñable sin exponer datos personales.

## Estado actual

El proyecto esta en **v1 local con Streamlit**. Esta version esta pensada para
ejecutarse en el ordenador del usuario, con datos privados en rutas locales
ignoradas por Git y una demo sintetica publicable.

La v1 local con Streamlit incluye:

- estructura de carpetas reorganizada,
- documentación de roadmap y arquitectura,
- zona `legacy` para notebooks antiguos,
- separación entre datos públicos de ejemplo y datos privados locales,
- esquema inicial en DuckDB,
- parsers para transacciones, movimientos de efectivo y snapshot de cartera DEGIRO,
- reconstruccion diaria de posiciones con reconciliacion opcional contra snapshots del broker,
- metricas agregadas de cartera con valor, pesos, rentabilidad basica y drawdown,
- informe mensual en Markdown para revision manual y agentes,
- historico de informes y metadatos persistidos en DuckDB,
- refresh de market data diario con `yfinance`,
- politica de valoracion `broker_snapshot_anchored`: DEGIRO fija el precio local observado y `yfinance` solo aporta variacion relativa,
- contratos de datos normalizados validados antes de persistir y cargar en DuckDB,
- dashboard Streamlit como interfaz principal de la v1,
- boton para actualizar FX/precios hasta hoy desde la vista general,
- workflow de CI con pytest en GitHub Actions,
- agentes mensuales con prompts versionados, trazabilidad y modo demo sintetico,
- y dashboard Streamlit reutilizando la capa de casos de uso.

La reconstruccion diaria de posiciones, las metricas agregadas, los informes,
los agentes y la demo local de Streamlit ya estan disponibles. Una futura
interfaz web esta documentada como direccion tecnica, pero no forma parte de la
v1 actual.

## Estructura del repositorio

La estructura separa codigo, datos privados, datos de ejemplo, scripts operativos, documentacion y tests.

```text
ML_finance/
|- .github/
|  |- ISSUE_TEMPLATE/
|  `- pull_request_template.md
|- docs/
|  |- architecture.md
|  |- decisions.md
|  `- roadmap.md
|- notebooks/
|  |- old/
|  `- README.md
|- scripts/
|  `- README.md
|- src/
|  |- application/
|  |- agents/
|  |- analytics/
|  |- data/
|  |- degiro_exports/
|  |- market_data/
|  |- reports/
|  `- portfolio/
|- tests/
|  `- README.md
|- .env.example
|- .gitattributes
|- .gitignore
|- pyproject.toml
|- README.md
`- requirements.txt
```

## Convención público / privado

El repositorio sigue una separación simple:

- `src/degiro_exports/example/`: reserva para ejemplos saneados de formato DEGIRO.
- `src/degiro_exports/local/`: exportaciones reales del broker, ignoradas por Git.
- `demo/`: demo publica sintetica ejecutable sin datos reales.
- `src/data/sample/`: datasets sinteticos o anonimizados para tests y ejemplos pequenos.
- `src/data/local/`: base local, cachés, informes y artefactos privados, ignorados por Git.

Esto permite tener un repositorio público útil y, a la vez, trabajar con tu cartera real sin subir datos sensibles.

## Puesta en marcha

Python 3.10+ recomendado.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

El proyecto define metadatos y configuración de tests en `pyproject.toml`.
Para validar el entorno local usa siempre el Python del virtualenv:

```powershell
.\.venv\Scripts\python.exe -m pytest
```

Tambien hay comandos PowerShell versionados para los flujos habituales:

```powershell
.\scripts\test.ps1
.\scripts\run_dashboard.ps1
.\scripts\run_demo.ps1
.\scripts\refresh_market_data.ps1 -EndDate 2026-05-14
```

Los archivos del repositorio se mantienen en UTF-8. En Windows, si una consola
muestra caracteres raros, valida el contenido desde Python o usa una terminal
configurada con UTF-8; no debería afectar a los archivos versionados.

Despues, elige uno de estos dos flujos:

### Flujo demo publico

Usa datos sinteticos y no toca tu cartera real:

```powershell
.\scripts\run_demo.ps1
```

### Flujo local privado

Usa tus exportaciones reales y rutas ignoradas por Git:

1. Coloca exportaciones reales en `src/degiro_exports/local/incoming/`.
2. Ejecuta `.\scripts\run_dashboard.ps1` para abrir la v1 local con Streamlit.
3. Si quieres refrescar FX y precios de mercado por consola, ejecuta `.\scripts\refresh_market_data.ps1 -EndDate YYYY-MM-DD`.
4. Consulta `docs/streamlit_dashboard.md` para el uso completo del dashboard.

## Objetivos de cartera

`investment_brief.md` sigue siendo el mandato narrativo de la cuenta: objetivo,
horizonte, filosofia de inversion, preferencias y restricciones cualitativas.
`portfolio_targets.yaml` anade la parte estructurada: aportacion mensual, pesos
objetivo, perfil de riesgo y limites cuantitativos.

Por defecto, el brief privado se carga desde:

```text
src/data/local/investment_brief.md
```

Y los objetivos estructurados privados se cargan desde:

```text
src/data/local/portfolio_targets.yaml
```

Ejemplos versionados sin datos reales:

- `src/data/sample/investment_brief.example.md`
- `src/data/sample/portfolio_targets.example.yaml`

El pipeline envia ambos a los agentes: el brief como contexto cualitativo y los
targets como `target_weights` para que `asistente_aportacion_mensual` compare
cartera actual frente a objetivo.

## Dashboard

La interfaz principal de la v1 es un dashboard local de Streamlit. Permite
revisar cartera, evolucion, informes, actualizar datos DEGIRO y ejecutar
agentes. En `Vista general`, el boton `Actualizar a hoy` refresca FX y precios
hasta la fecha actual; la fecha principal de la vista avanza hasta la ultima
fecha valorada disponible y el ultimo snapshot DEGIRO queda como ancla.

```powershell
.\.venv\Scripts\python.exe -m streamlit run src\portfolio\dashboard.py
```

Comando equivalente:

```powershell
.\scripts\run_dashboard.ps1
```

Abre `http://localhost:8501` cuando Streamlit termine de arrancar. Guia completa: `docs/streamlit_dashboard.md`.

## Demo publica

La demo sintetica vive en `demo/` y sirve para ensenar la v1 local con Streamlit
sin exponer datos reales. No usa `src/data/local/` ni
`src/degiro_exports/local/`. Para prepararla y abrirla:

```powershell
.\scripts\run_demo.ps1
```

Si quieres ejecutar los pasos manualmente:

```powershell
$env:ML_FINANCE_ENV_FILE="demo/synthetic_config/.env.demo"
.\.venv\Scripts\python.exe scripts\bootstrap_demo.py
```

Despues abre el dashboard con la misma variable de entorno:

```powershell
$env:ML_FINANCE_ENV_FILE="demo/synthetic_config/.env.demo"
.\.venv\Scripts\python.exe -m streamlit run src\portfolio\dashboard.py
```

Guia completa: `demo/README.md`.

## Documentación clave

- `docs/roadmap.md`: fases, backlog y traducción del plan a tareas de GitHub.
- `docs/architecture.md`: flujo de datos, componentes y límites del sistema.
- `docs/architecture_v2.md`: direccion futura FastAPI + Angular sin iniciar la migracion.
- `docs/api_contracts.md`: contratos futuros de API local antes de implementar FastAPI.
- `docs/decisions.md`: decisiones ya cerradas y su justificación.
- `docs/data_model.md`: esquema inicial de DuckDB, claves y relaciones entre tablas.
- `docs/market_data_refresh.md`: flujo real de refresh de precios y overrides manuales.
- `docs/position_history.md`: reconstruccion diaria de cantidades y contraste con snapshots del broker.
- `docs/portfolio_metrics.md`: valoracion diaria, pesos, rentabilidad basica y drawdown.
- `docs/monthly_report.md`: generacion manual del informe mensual en Markdown.
- `docs/monthly_pipeline.md`: flujo mensual completo, incluyendo FX, market data y agentes.
- `docs/streamlit_dashboard.md`: uso del dashboard local para cartera, informes, actualizacion de datos y agentes.
- `docs/privacy.md`: politica de privacidad local, rutas sensibles y checklist de secret scanning.
- `src/application/README.md`: capa de casos de uso reutilizables para scripts, Streamlit y futuras interfaces.

## Direccion futura

La v1 se mantiene como aplicacion local con Streamlit. Antes de migrar a otra
interfaz web, el objetivo es consolidar esta version: demo publica, privacidad,
comandos reproducibles, capa `src/application/` y separacion progresiva del
dashboard. La direccion FastAPI + Angular esta documentada en
`docs/architecture_v2.md`, pero no es prioridad inmediata.

## Legacy

Los notebooks anteriores siguen disponibles en `notebooks/old/`. Se conservan como referencia histórica, pero ya no definen la arquitectura principal del proyecto.
