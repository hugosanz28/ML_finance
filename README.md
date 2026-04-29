# ML_finance

`ML_finance` evoluciona desde un repositorio de exploración en Jupyter hacia un sistema local para:

- importar exportaciones oficiales de DEGIRO,
- reconstruir y seguir el histórico de la cartera,
- enriquecer posiciones con datos de mercado,
- generar informes periódicos,
- exponer una interfaz simple en Streamlit,
- y servir como proyecto público enseñable sin exponer datos personales.

## Estado actual

El proyecto ya tiene una primera base funcional:

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
- y primer esqueleto para agentes y dashboard.

El siguiente bloque importante es reconstruir posiciones por fecha y calcular métricas agregadas de cartera.

## Estructura del repositorio

La reconstruccion diaria de posiciones ya esta disponible; el siguiente bloque funcional real es explotar ese historico para metricas agregadas.

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
|  |- agents/
|  |- analytics/
|  |- data/
|  |- degiro_exports/
|  |- market_data/
|  `- portfolio/
|- tests/
|  `- README.md
|- .env.example
|- .gitattributes
|- .gitignore
|- README.md
`- requirements.txt
```

## Convención público / privado

El repositorio sigue una separación simple:

- `src/degiro_exports/example/`: ejemplos saneados y compartibles.
- `src/degiro_exports/local/`: exportaciones reales del broker, ignoradas por Git.
- `src/data/sample/`: datasets sintéticos o anonimizados para demo pública.
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

Después:

1. Coloca exportaciones reales en `src/degiro_exports/local/incoming/`.
2. Usa `src/degiro_exports/example/` y `src/data/sample/` para demos públicas.
3. Si quieres refrescar precios de mercado, ejecuta `.\.venv\Scripts\python.exe scripts\refresh_market_data.py`.
4. Consulta el plan en `docs/roadmap.md`.
5. Si quieres ver el flujo del historico de posiciones, consulta `docs/position_history.md`.
6. Si quieres ver la capa de valoracion agregada, consulta `docs/portfolio_metrics.md`.
7. Si quieres generar el informe mensual para revision y agentes, consulta `docs/monthly_report.md`.

## Dashboard

La primera interfaz será una app de Streamlit. El punto de entrada previsto es:

```powershell
streamlit run src/portfolio/dashboard.py
```

Ahora mismo solo hay un esqueleto mínimo; la funcionalidad llegará después del modelo de datos y del importador.

## Documentación clave

- `docs/roadmap.md`: fases, backlog y traducción del plan a tareas de GitHub.
- `docs/architecture.md`: flujo de datos, componentes y límites del sistema.
- `docs/decisions.md`: decisiones ya cerradas y su justificación.
- `docs/data_model.md`: esquema inicial de DuckDB, claves y relaciones entre tablas.
- `docs/market_data_refresh.md`: flujo real de refresh de precios y overrides manuales.
- `docs/position_history.md`: reconstruccion diaria de cantidades y contraste con snapshots del broker.
- `docs/portfolio_metrics.md`: valoracion diaria, pesos, rentabilidad basica y drawdown.
- `docs/monthly_report.md`: generacion manual del informe mensual en Markdown.
- `docs/monthly_pipeline.md`: flujo mensual completo, incluyendo FX, market data y agentes.

## Legacy

Los notebooks anteriores siguen disponibles en `notebooks/old/`. Se conservan como referencia histórica, pero ya no definen la arquitectura principal del proyecto.
