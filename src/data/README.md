# Data

Este directorio guarda datos derivados y datasets de soporte.

- `sample/`: datasets sintéticos o anonimizados para tests, ejemplos y demo pública.
- `local/`: base local, caché de mercado, snapshots e informes privados. Esta carpeta está ignorada por Git.

Estructura prevista dentro de `local/`:

- `raw/`
- `normalized/`
- `curated/`
- `reports/`
- `legacy/`

DDL versionado:

- `sql/001_initial_schema.sql`: esquema inicial de DuckDB para el proyecto.
