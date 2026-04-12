# Scripts

Este directorio se reserva para comandos manuales o automatizables del proyecto.

Scripts previstos:

- `import_degiro.py`
- `refresh_market_data.py`
- `rebuild_history.py`
- `generate_report.py`

La idea es que cada script haga una sola cosa y pueda ejecutarse de forma reproducible en local.

Notas de uso:

- `refresh_market_data.py` sincroniza primero los activos desde `src/data/local/normalized/degiro/`.
- Si un activo necesita ticker manual o debe excluirse del refresh, usa `src/data/local/market_data/asset_overrides.csv`.
