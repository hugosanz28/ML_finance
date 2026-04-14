# Historico de Posiciones

## Resumen

La reconstruccion de posiciones vive en `src/portfolio/positions.py`.

El modulo:

- carga transacciones normalizadas y snapshots del broker en parquet,
- reconstruye cantidades diarias por `asset_id`,
- permite usar snapshots como ancla para el estado inicial de un rango,
- y genera una reconciliacion separada contra snapshots del broker cuando existen.

## API principal

Funciones mas utiles:

- `reconstruct_positions_by_date(transactions, snapshots=...)`
- `reconstruct_positions_from_normalized_degiro(...)`
- `persist_reconstructed_positions(...)`

La salida principal contiene:

- `positions`: una fila por `asset_id` y `position_date` con `quantity`,
- `transaction_delta` y `transaction_count` por dia,
- y metadatos del snapshot usado como ancla si aplica.

La salida de contraste contiene:

- `snapshot_date`,
- `snapshot_quantity`,
- `reconstructed_quantity`,
- `quantity_difference`,
- y `comparison_status` con valores `matched` o `mismatch`.

## Persistencia

Si se llama con `persist=True`, el resultado se guarda por defecto en:

- `src/data/local/curated/portfolio/positions_history/`

Ficheros generados:

- `positions_YYYY-MM-DD_YYYY-MM-DD.parquet`
- `snapshot_reconciliation_YYYY-MM-DD_YYYY-MM-DD.parquet`

## Alcance actual

- soporta `BUY` y `SELL` como deltas de posicion,
- detecta cantidades negativas reconstruidas y falla temprano,
- puede arrancar desde un snapshot del broker si ese snapshot cubre el inicio del rango,
- y no intenta aun valorar posiciones ni calcular coste medio o PnL.

Eso queda preparado para la siguiente capa de metricas agregadas de cartera.
