# Refresh de Market Data

## Resumen

El refresh de precios diarios parte de los activos normalizados en:

- `src/data/local/normalized/degiro/assets/`
- `src/data/local/normalized/degiro/portfolio_snapshots/`

Esos activos se sincronizan en `assets_master` y despues se consulta el proveedor configurado, actualmente `yfinance`.

El refresh de tipos de cambio se documenta aparte en `docs/fx_refresh.md`.
Alimenta `fx_rates` con pares inferidos desde los normalizados de DEGIRO, por
ejemplo `EUR/USD` o `EUR/CAD`.

## Ejecucion

```powershell
.\.venv\Scripts\python.exe scripts\refresh_market_data.py
```

Opciones utiles:

- `--start-date YYYY-MM-DD`
- `--end-date YYYY-MM-DD`
- `--asset-id <asset_id>`
- `--include-inactive`

## Overrides manuales

Si un activo no resuelve bien en `yfinance` o debe excluirse del refresh, se ajusta en:

- `src/data/local/market_data/asset_overrides.csv`

Campos mas utiles:

- `ticker`: ticker manual para Yahoo Finance.
- `asset_similar`: proxy opcional si interesa valorar el activo con otro parecido.
- `is_active`: permite sacar un activo del refresh.
- `notes`: aclaracion corta del motivo.

## Estado actual

- `cash` no se refresca con market data externa.
- los derechos no negociables se excluyen del refresh.
- `BITCOIN` usa `BTC-EUR`.
- `AMUNDI PRIME EURO GOVERNMENT BOND 0-1Y UCITS ACC ETF` usa `PRAB.DE`.
