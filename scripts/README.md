# Scripts

Este directorio se reserva para comandos manuales o automatizables del proyecto.

Scripts previstos:

- `import_degiro.py`
- `refresh_market_data.py`
- `rebuild_history.py`
- `generate_monthly_report.py`
- `run_monitor_tematico.py`

La idea es que cada script haga una sola cosa y pueda ejecutarse de forma reproducible en local.

Notas de uso:

- `refresh_market_data.py` sincroniza primero los activos desde `src/data/local/normalized/degiro/`.
- Si un activo necesita ticker manual o debe excluirse del refresh, usa `src/data/local/market_data/asset_overrides.csv`.
- `generate_monthly_report.py` genera el informe mensual en Markdown; el flujo esta documentado en `docs/monthly_report.md`.
- `run_monitor_tematico.py` prepara y ejecuta `monitor_tematico`; con `--dry-run` permite inspeccionar inputs y temas observados sin llamadas reales.
