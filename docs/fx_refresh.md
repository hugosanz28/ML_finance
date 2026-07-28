# Refresh de FX

## Resumen

El refresh de FX alimenta la tabla `fx_rates` con tipos diarios para las
divisas que aparecen en los datos normalizados de DEGIRO.

El contrato de la tabla es:

- `base_currency`: moneda base del par.
- `quote_currency`: moneda cotizada.
- `rate`: unidades de `quote_currency` por 1 unidad de `base_currency`.

Ejemplo: `EUR/USD = 1.17` significa `1 EUR = 1.17 USD`. Para convertir un
importe en USD a EUR se divide entre ese tipo.

## Inferencia de pares

El script detecta pares necesarios desde:

- `src/data/local/normalized/degiro/transactions/`
- `src/data/local/normalized/degiro/cash_movements/`
- `src/data/local/normalized/degiro/portfolio_snapshots/`

Solo se refrescan pares donde la moneda del evento o posicion es distinta de
`base_currency`. Con los datos actuales aparecen pares como `EUR/USD` y
`EUR/CAD`.

La inferencia serializable para scripts y futuras interfaces la expone
`InferFxRequirementsUseCase`. El refresh se ejecuta mediante
`RefreshFxUseCase`; las interfaces no llaman directamente al repositorio ni a
`FxRefreshService`.

## Ejecucion

```powershell
.\.venv\Scripts\python.exe scripts\refresh_fx_rates.py
```

Opciones utiles:

- `--start-date YYYY-MM-DD`
- `--end-date YYYY-MM-DD`
- `--pair EUR/USD`
- `--pair EUR/CAD`
- `--only-missing-base`
- `--no-infer-from-normalized`
- `--provider yfinance`
- `--provider synthetic` (solo demo offline ya sembrada)

Ejemplos:

```powershell
.\.venv\Scripts\python.exe scripts\refresh_fx_rates.py --only-missing-base
.\.venv\Scripts\python.exe scripts\refresh_fx_rates.py --pair EUR/USD --start-date 2025-11-01 --end-date 2026-04-12
.\.venv\Scripts\python.exe scripts\refresh_fx_rates.py --end-date 2026-05-14
```

Desde Streamlit, `Vista general` -> `Actualizar a hoy` ejecuta este refresh
hasta la fecha actual antes de recalcular la cartera.

## Relacion con los normalizados

El refresh de FX no reescribe los parquets normalizados de DEGIRO. Los parquets
siguen representando lo importado y normalizado desde el broker.

La tabla `fx_rates` queda como fuente complementaria para:

- cubrir huecos de `amount_base`, `net_cash_amount_base` o `market_value_base`;
- recalcular metricas en moneda base cuando haga falta;
- dejar trazabilidad por proveedor y fecha de ingesta.

## Providers

El provider normal y valor por defecto es `yfinance`. Usa simbolos tipo:

- `EURUSD=X` para `EUR/USD`
- `EURCAD=X` para `EUR/CAD`

Si un par no devuelve datos, el resultado queda como `skipped` y no se inventa
ningun tipo de cambio.

La demo configura `PRICE_PROVIDER=synthetic`. Ese provider no descarga ni
inventa FX: mantiene las filas `synthetic` creadas por
`scripts/bootstrap_demo.py` y devuelve `skipped` si se intenta refrescar una
serie externa. Por tanto, los checks de demo deben usar el bootstrap y no
forzar red.
