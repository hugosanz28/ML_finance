# Dashboard Streamlit

## Ejecucion

Desde la raiz del repo:

```powershell
cd C:\Users\huugosz\Documents\GitHub\ML_finance
.\.venv\Scripts\python.exe -m streamlit run src\portfolio\dashboard.py
```

Despues abre:

```text
http://localhost:8501
```

Alternativa equivalente:

```powershell
.\.venv\Scripts\streamlit.exe run src\portfolio\dashboard.py
```

Si el puerto `8501` ya esta ocupado, Streamlit elegira otro puerto y lo mostrara
en la terminal.

## Vistas incluidas

- `Cartera`: asignacion actual y valor total anclado al ultimo snapshot DEGIRO, con PnL/rentabilidad sobre coste base conocido.
- `Evolucion`: valor historico anclado a DEGIRO (nivel broker + variacion diaria de market data), drawdown, cobertura y tabla diaria.
- `Informes`: lectura de informes mensuales generados en `reports_history` o en la carpeta de informes.
- `Actualizar datos`: subida de CSVs DEGIRO, importacion, carga DuckDB, refresh FX, refresh precios e informe mensual.
- `Agentes`: revision de inputs, edicion del `investment_brief`, presupuesto mensual editable (`monthly_budget`), control de envio de `target_weights` y ejecucion de la red mensual de agentes.

## Flujo desde la UI

1. Entra en `Actualizar datos`.
2. Sube los CSV de DEGIRO o deja los existentes en `src/degiro_exports/local/incoming/`.
3. Pulsa `Guardar CSVs en incoming` si has subido archivos desde la UI.
4. Pulsa `1. Importar DEGIRO`.
5. Pulsa `2. Refrescar FX`.
6. Pulsa `3. Refrescar precios`.
7. Pulsa `4. Generar informe`.
8. Entra en `Agentes`, revisa los inputs, activa o desactiva `Enviar target_weights al pipeline`, y ejecuta la red con `static/null` para demo local o `openai/duckduckgo` para una ejecucion real.

Al guardar desde la UI, el dashboard detecta el tipo de exportacion por el
nombre del archivo y lo copia a `incoming` con el nombre canonico que exige el
importador:

- cartera, portfolio, posiciones o snapshot -> `portfolio_YYYY-MM-DD.csv`
- transacciones, transactions, operaciones u orders -> `transactions_YYYY-MM-DD_YYYY-MM-DD.csv`
- cuenta, account, cash, efectivo, movimientos o activity -> `account_YYYY-MM-DD_YYYY-MM-DD.csv`

Si el nombre contiene fechas, se usan esas fechas. Se aceptan formatos como
`YYYY-MM-DD`, `DD-MM-YYYY`, `YYYYMMDD` y `DDMMYYYY`, tambien con `_` o `.` como
separador. Para transacciones y cuenta se usa el rango minimo-maximo detectado.
Para portfolio se usa la fecha mas reciente detectada. Si no hay fecha en el
nombre, se usa el dia de subida.

Los archivos cuyo tipo no pueda detectarse por nombre no se guardan y aparecen
como `omitido` en la tabla de resultados.

Flujo rapido equivalente:

1. Entra en `Actualizar datos`.
2. Pulsa `1. Importar DEGIRO`.
3. Pulsa `Ejecutar flujo mensual basico`.

El boton `Ejecutar flujo mensual basico` encadena importacion, carga DuckDB, FX,
precios e informe. No ejecuta automaticamente los agentes; eso se hace en la
pestana `Agentes` para poder revisar antes los inputs.

## Parar el dashboard

Si lo arrancaste en una terminal, usa `Ctrl+C`.

Si quedo en segundo plano, localiza el proceso Python/Streamlit:

```powershell
Get-Process | Where-Object { $_.ProcessName -like "*python*" }
```

## Contratos usados

El dashboard no calcula cartera por su cuenta. Consume:

- `calculate_portfolio_metrics_from_normalized_degiro`
- `generate_monthly_report` y `get_latest_monthly_report`
- `import_degiro_exports` y `load_normalized_degiro_to_duckdb`
- `FxRefreshService` y `PriceRefreshService`
- `run_monthly_agent_pipeline`

El `investment_brief` editable vive por defecto en:

```text
src/data/local/investment_brief.md
```
