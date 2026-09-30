# Metricas de Cartera

## Resumen

La capa de calculo de metricas agregadas vive en `src/portfolio/metrics.py`.
Los contratos compartidos de salida (`PortfolioMetricsResult`,
`POSITION_METRICS_COLUMNS` y `PORTFOLIO_DAILY_METRICS_COLUMNS`) viven en
`src/portfolio/metrics_models.py`.

La proyeccion reutilizable de snapshots broker, coste y PnL vive en
`src/portfolio/state_projection.py`. Informes, dashboard y snapshot de agentes
consumen esa implementacion comun para no duplicar reglas financieras.

El rendimiento ajustado por aportaciones y retiradas vive por separado en
`src/portfolio/performance.py`. Calcula retornos por intervalo, TWR y MWR/XIRR
con contratos explicitos; consulta `docs/performance.md`.

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
- en la fecha exacta del snapshot usa el valor oficial de DEGIRO en moneda base,
  incluso si existe cotizacion externa; los dias posteriores necesitan la serie
  de precios del proveedor,
- la formula de precio local es:
  `precio_DEGIRO_ancla * precio_proveedor_fecha / precio_proveedor_ancla`,
- el valor local se ancla preferentemente al `market_value` del snapshot, no a
  `quantity * market_price`, porque algunos brokers redondean la cantidad
  visible en el CSV y conservan mas precision internamente,
- fuera de la fecha del snapshot, el precio local anclado de activos no EUR se
  convierte a moneda base con el FX diario disponible en `fx_rates`,
- para fechas anteriores al primer snapshot disponible usa ese primer snapshot
  como ancla hacia atras, de forma que la serie historica no queda sin valorar,
- si `calculate_portfolio_metrics_from_normalized_degiro` se llama sin
  `end_date`, la fecha final se extiende hasta el ultimo `price_date`
  disponible en `prices_daily` para los activos de la cartera,
- mantiene `external_absolute` como politica alternativa para comparar contra
  precios absolutos del proveedor,
- usa precio disponible mas reciente en o antes de cada fecha de valoracion,
- recalibra cantidades cuando aparece un snapshot posterior del broker,
- concilia IDs de producto sin ISIN entre transacciones y snapshots solo cuando
  la coincidencia de nombre, tipo e identificador es unica,
- soporta coste base con media ponderada movil para `BUY` y `SELL`,
- marca `missing_price`, `missing_anchor`, `missing_provider_anchor_price` o
  `missing_fx` cuando no puede valorar una posicion,
- y calcula drawdown sobre el valor agregado efectivamente valorado.

`daily_return_pct` es la variacion bruta del valor agregado y no descuenta
aportaciones o retiradas. Para analizar rendimiento financiero debe usarse la
serie ajustada y el TWR de `src/portfolio/performance.py`.

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
- `valued_snapshot`: valor oficial de DEGIRO en la fecha exacta del snapshot.
- `valued_cash`: efectivo valorado directamente.
- `valued_trade_anchor`: precio de compra documentado como ancla histórica.
- `valued_dividend_receivable`: dividendo pendiente reconstruido desde su liquidación.
- `valued_reviewed_rights`: cierre revisado de derechos, con fuente local.
- `valued_estimated_rights`: hueco estimado con un cierre cercano por autorización explícita.
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
- `GetPortfolioStateUseCase` es el read model usado por la API y otras
  interfaces: convierte fechas y escalares a primitivas JSON y no devuelve
  `PortfolioMetricsResult`, `DataFrame` ni `Path`.
- Las aportaciones netas usadas en el resumen se consultan mediante
  `src/portfolio/contributions.py`.

## Alcance y limites

- la rentabilidad basada en coste sigue describiendo el inventario restante;
- TWR y MWR/XIRR están integrados en UI, API y el snapshot analítico de agentes;
- el TWR diario aplica la convencion documentada para flujos fechados sin
  valoracion intradia;
- y la cobertura de divisa depende de que existan `fx_rates` o de que el activo
  ya cotice en la moneda base.

La cobertura completa puede incluir estimaciones explícitas de derechos;
no significa que todos los precios sean observaciones de mercado. Véanse las
[políticas y límites de reconstrucción](risk_analytics.md#calidad-del-histórico-reconstruido).
