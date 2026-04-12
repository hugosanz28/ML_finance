# Data

Este directorio guarda datos derivados y datasets de soporte.

- `sample/`: datasets sintéticos o anonimizados para tests, ejemplos y demo pública.
- `local/`: base local, caché de mercado, snapshots e informes privados. Esta carpeta está ignorada por Git.

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

DDL versionado:

- `sql/001_initial_schema.sql`: esquema inicial de DuckDB para el proyecto.
