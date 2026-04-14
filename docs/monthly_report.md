# Informe Mensual

## Resumen

El informe mensual genera una revision en `Markdown` de la cartera para la fecha
del snapshot disponible o para una fecha `as_of` concreta.

Esta pensado para el flujo manual del proyecto:

1. exportar o actualizar datos de DEGIRO,
2. refrescar precios si hace falta,
3. generar el informe mensual,
4. usar ese informe como entrada para agentes o para la revision personal.

## Entradas

El generador vive en `src/reports/monthly.py` y se apoya en:

- `portfolio_daily_metrics` y `position_metrics` derivados de `src/portfolio/metrics.py`,
- transacciones normalizadas en `src/data/local/normalized/degiro/transactions/`,
- y movimientos de efectivo normalizados en `src/data/local/normalized/degiro/cash_movements/`.

La fecha de referencia real es la ultima fecha valorada disponible, salvo que se
pase `--as-of-date`.

## Salida

El script manual escribe por defecto en:

- `src/data/local/reports/`

Nombre del fichero:

- `YYYY-MM-DD-monthly-YYYYMMDDTHHMMSSffffff.md`

Ejemplo real:

- `2026-04-12-monthly-20260414T235637794724.md`

## Contenido actual

El informe incluye:

- resumen de cartera en la fecha de referencia,
- asignacion actual por activo,
- comparativas para `ultimo mes`, `ultimos 3 meses` y `ultimos 12 meses`,
- actividad del periodo: compras, ventas, dividendos y entradas o salidas de caja,
- variaciones relevantes por cambio de peso, valor y cantidad,
- y notas de cobertura cuando faltan precios, FX o historico suficiente.

Si no existe historico completo para una ventana, el informe no falla: usa el
primer dato disponible y lo deja indicado en las notas.

## Ejecucion manual

```powershell
.\.venv\Scripts\python.exe scripts\generate_monthly_report.py --as-of-date 2026-04-12
```

Opciones utiles:

- `--as-of-date YYYY-MM-DD`: fija la fecha de referencia.
- `--output-dir RUTA`: cambia la carpeta de salida.
- `--normalized-degiro-dir RUTA`: cambia la carpeta de parquets normalizados.
- `--stdout`: imprime tambien el Markdown en consola.
- `--latest`: muestra el ultimo informe mensual persistido y termina.

## Historico y metadatos

Cada ejecucion persistida:

- escribe un fichero nuevo sin sobreescribir informes anteriores,
- inserta una fila en `reports_history` dentro de `portfolio.duckdb`,
- guarda ruta, fecha de referencia, periodo cubierto, hash y parametros,
- y permite consultar el ultimo informe mensual disponible.

Consulta rapida del ultimo informe mensual:

```powershell
.\.venv\Scripts\python.exe scripts\generate_monthly_report.py --latest
```

## Notas de alcance

- No propone aun rebalanceos ni compras; prepara el contexto para ese paso.
- La cobertura del informe depende de los precios y tipos de cambio disponibles.
