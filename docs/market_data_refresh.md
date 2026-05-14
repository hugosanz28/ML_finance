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

Para actualizar hasta una fecha concreta:

```powershell
.\.venv\Scripts\python.exe scripts\refresh_market_data.py --end-date 2026-05-14
```

Desde Streamlit, `Vista general` -> `Actualizar a hoy` ejecuta el refresh de FX
y precios hasta la fecha actual y limpia la cache del dashboard.

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

En la politica de valoracion actual, estos precios no se usan como verdad
absoluta del broker. Se usan para calcular variaciones relativas desde el
precio local observado en snapshots DEGIRO.

Los overrides vacios o `NaN` se tratan como ausentes. No deben convertirse en el
simbolo literal `NAN`, porque eso contaminaria varias series con el mismo ticker.

## Estado actual

- `cash` no se refresca con market data externa.
- los derechos no negociables se excluyen del refresh.
- `BITCOIN` usa `BTC-EUR`.
- `AMUNDI PRIME EURO GOVERNMENT BOND 0-1Y UCITS ACC ETF` usa `PRAB.DE`.
- los ETC/ETF o acciones con simbolo ambiguo se resuelven mediante
  `asset_overrides.csv`; el precio resultante solo aporta variacion relativa
  cuando la valoracion esta anclada al broker.
