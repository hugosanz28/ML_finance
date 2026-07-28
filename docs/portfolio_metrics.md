# Metricas de Cartera

## Resumen

La capa de calculo de metricas agregadas vive en `src/portfolio/metrics.py`.
Los contratos compartidos de salida (`PortfolioMetricsResult`,
`POSITION_METRICS_COLUMNS` y `PORTFOLIO_DAILY_METRICS_COLUMNS`) viven en
`src/portfolio/metrics_models.py`.

La proyeccion reutilizable de snapshots broker, coste y PnL vive en
`src/portfolio/state_projection.py`. Informes, dashboard y snapshot de agentes
consumen esa implementacion comun para no duplicar reglas financieras.

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

- por defecto usa `broker_snapshot_anchored`: DEGIRO fija el precio local de
  referencia por activo en cada snapshot y `yfinance` solo aporta la variacion
  relativa entre fechas,
- la formula de precio local es:
  `precio_DEGIRO_ancla * precio_proveedor_fecha / precio_proveedor_ancla`,
- el valor local se ancla preferentemente al `market_value` del snapshot, no a
  `quantity * market_price`, porque algunos brokers redondean la cantidad
  visible en el CSV y conservan mas precision internamente,
- para activos no EUR, el precio local anclado se convierte a moneda base con
  el FX diario disponible en `fx_rates`; por eso el total EUR puede no coincidir
  exactamente con el snapshot si DEGIRO uso otro cambio,
- para fechas anteriores al primer snapshot disponible usa ese primer snapshot
  como ancla hacia atras, de forma que la serie historica no queda sin valorar,
- si `calculate_portfolio_metrics_from_normalized_degiro` se llama sin
  `end_date`, la fecha final se extiende hasta el ultimo `price_date`
  disponible en `prices_daily` para los activos de la cartera,
- mantiene `external_absolute` como politica alternativa para comparar contra
  precios absolutos del proveedor,
- usa precio disponible mas reciente en o antes de cada fecha de valoracion,
- recalibra cantidades cuando aparece un snapshot posterior del broker,
- soporta coste base con media ponderada movil para `BUY` y `SELL`,
- marca `missing_price`, `missing_anchor`, `missing_provider_anchor_price` o
  `missing_fx` cuando no puede valorar una posicion,
- y calcula drawdown sobre el valor agregado efectivamente valorado.

En la demo, `PRICE_PROVIDER=synthetic` conserva las series sinteticas sembradas
y no accede a red. No sustituye la politica de valoracion ni fabrica precios
nuevos.

Columnas de auditoria relevantes:

- `pricing_policy`
- `anchor_snapshot_date`
- `anchor_market_price`
- `provider_anchor_price`
- `provider_anchor_price_date`
- `provider_price_age_days`
- `provider_anchor_age_days`

Estados habituales de `valuation_status`:

- `valued_anchored`: posicion valorada con precio DEGIRO anclado y variacion del proveedor.
- `valued_cash`: efectivo valorado directamente.
- `missing_anchor`: no hay snapshot DEGIRO util para anclar.
- `missing_provider_anchor_price`: falta el precio del proveedor en la fecha de ancla.
- `missing_price`: falta precio diario del proveedor para la fecha de valoracion.
- `missing_fx`: falta tipo de cambio para convertir a moneda base.

## Persistencia

Si se llama con `persist=True`, guarda parquet por defecto en:

- `src/data/local/curated/portfolio/metrics/`

Ficheros generados:

- `position_metrics_YYYY-MM-DD_YYYY-MM-DD.parquet`
- `portfolio_daily_metrics_YYYY-MM-DD_YYYY-MM-DD.parquet`

## Frontera de aplicacion

- `LoadPortfolioMetricsUseCase` expone el resultado interno para consumidores
  Python existentes.
- `GetPortfolioStateUseCase` es el read model neutral para interfaces y futura
  API: convierte fechas y escalares a primitivas JSON y no devuelve
  `PortfolioMetricsResult`, `DataFrame` ni `Path`.
- Las aportaciones netas usadas en el resumen se consultan mediante
  `src/portfolio/contributions.py`.

## Alcance y limites

- la rentabilidad actual es basica y se apoya en coste base e inventario restante,
- no calcula aun rentabilidad money-weighted ni time-weighted,
- y la cobertura de divisa depende de que existan `fx_rates` o de que el activo ya cotice en la moneda base.
