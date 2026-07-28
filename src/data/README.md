# Data

Este directorio guarda datos derivados y datasets de soporte.

- `sample/`: datasets sinteticos o anonimizados para tests y ejemplos pequenos.
- `local/`: base local, caché de mercado, snapshots e informes privados. Esta carpeta está ignorada por Git.

La demo publica ejecutable vive en `demo/`, no bajo `src/data/sample/`.

El runtime crea bajo `local/` las rutas que necesita:

- `raw/`: aterrizaje reservado para datos fuente;
- `normalized/`: Parquet normalizado, incluido `normalized/degiro/`;
- `curated/`: posiciones y metricas derivadas;
- `market_data/`: overrides y cache de proveedores;
- `reports/`: informes Markdown persistidos;
- `agents/`: runs, auditoria, caches e inputs editados desde la UI.

Artefactos relevantes ya en uso:

- `portfolio.duckdb`: base local principal del proyecto.
- `market_data/asset_overrides.csv`: overrides manuales para tickers, proxies o exclusiones del refresh.
- `normalized/degiro/`: salida normalizada de los parsers DEGIRO, usada tambien para bootstrap de `assets_master`.
- `portfolio_targets.yaml`: objetivos privados de cartera, pesos objetivo,
  aportacion mensual y limites de concentracion. Hay un ejemplo versionado en
  `sample/portfolio_targets.example.yaml`.
- `investment_brief.md`: mandato narrativo privado de la cuenta. Hay un ejemplo
  versionado en `sample/investment_brief.example.md`.

Las ubicaciones efectivas son configurables mediante `.env`. La demo usa
`demo/local_data/` y no crea ni lee estos artefactos privados.

DDL versionado:

- `sql/001_initial_schema.sql`: esquema inicial de DuckDB para el proyecto.
