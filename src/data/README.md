# Data

Este directorio guarda datos derivados y datasets de soporte.

- `sample/`: datasets sinteticos o anonimizados para tests y ejemplos pequenos.
- `local/`: base local, caché de mercado, snapshots e informes privados. Esta carpeta está ignorada por Git.

La demo publica ejecutable vive en `demo/`, no bajo `src/data/sample/`.

Estructura prevista dentro de `local/`:

- `raw/`
- `normalized/`
- `curated/`
- `market_data/`
- `reports/`
- `legacy/`

Artefactos relevantes ya en uso:

- `portfolio.duckdb`: base local principal del proyecto.
- `market_data/asset_overrides.csv`: overrides manuales para tickers, proxies o exclusiones del refresh.
- `normalized/degiro/`: salida normalizada de los parsers DEGIRO, usada tambien para bootstrap de `assets_master`.
- `portfolio_targets.yaml`: objetivos privados de cartera, pesos objetivo,
  aportacion mensual y limites de concentracion. Hay un ejemplo versionado en
  `sample/portfolio_targets.example.yaml`.
- `investment_brief.md`: mandato narrativo privado de la cuenta. Hay un ejemplo
  versionado en `sample/investment_brief.example.md`.

DDL versionado:

- `sql/001_initial_schema.sql`: esquema inicial de DuckDB para el proyecto.
