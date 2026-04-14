# Metricas de Cartera

## Resumen

La capa de metricas agregadas vive en `src/portfolio/metrics.py`.

Parte de:

- historico de posiciones por `asset_id` y fecha,
- precios diarios en `prices_daily`,
- transacciones normalizadas para estimar coste base,
- y `fx_rates` cuando existen conversiones necesarias.

## Salidas

El modulo devuelve dos datasets reutilizables:

- `position_metrics`: valoracion diaria por activo.
- `portfolio_daily_metrics`: agregados diarios de cartera.

### `position_metrics`

Campos principales:

- `quantity`
- `close_price`
- `market_value_local`
- `market_value_base`
- `cost_basis_base`
- `unrealized_pnl_base`
- `unrealized_return_pct`
- `weight`
- `valuation_status`

### `portfolio_daily_metrics`

Campos principales:

- `total_market_value_base`
- `total_cost_basis_base`
- `total_unrealized_pnl_base`
- `portfolio_return_pct`
- `daily_return_pct`
- `drawdown_pct`
- `valuation_coverage_ratio`
- `return_coverage_ratio`

## Comportamiento actual

- usa precio disponible mas reciente en o antes de cada fecha de valoracion,
- soporta coste base con media ponderada movil para `BUY` y `SELL`,
- marca `missing_price` o `missing_fx` cuando no puede valorar una posicion,
- y calcula drawdown sobre el valor agregado efectivamente valorado.

## Persistencia

Si se llama con `persist=True`, guarda parquet por defecto en:

- `src/data/local/curated/portfolio/metrics/`

Ficheros generados:

- `position_metrics_YYYY-MM-DD_YYYY-MM-DD.parquet`
- `portfolio_daily_metrics_YYYY-MM-DD_YYYY-MM-DD.parquet`

## Alcance y limites

- la rentabilidad actual es basica y se apoya en coste base e inventario restante,
- no calcula aun rentabilidad money-weighted ni time-weighted,
- y la cobertura de divisa depende de que existan `fx_rates` o de que el activo ya cotice en la moneda base.
