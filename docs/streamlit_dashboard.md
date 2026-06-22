# Dashboard Streamlit

## Ejecucion

Desde la raiz del repo:

```powershell
cd C:\Users\huugosz\Documents\GitHub\ML_finance
.\.venv\Scripts\python.exe -m streamlit run src\portfolio\dashboard.py
```

Por defecto usa `.env` y las rutas reales locales. Para abrir la demo
sintetica, prepara primero `scripts\bootstrap_demo.py` y ejecuta:

```powershell
$env:ML_FINANCE_ENV_FILE="demo/synthetic_config/.env.demo"
.\.venv\Scripts\python.exe -m streamlit run src\portfolio\dashboard.py
```

Ese modo apunta a `demo/local_data/` y no a `src/data/local/`.

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

- `Vista general`: asignacion actual y valor total de la ultima fecha valorada. El ultimo snapshot DEGIRO se muestra como ancla, pero la fecha principal puede avanzar con market data posterior. Incluye `Actualizar a hoy` para refrescar FX y precios sin importar nuevos CSVs.
- `Evolucion`: valor historico calculado por activo. DEGIRO fija el precio local de referencia en cada snapshot y market data aporta la variacion relativa diaria, sin aplicar un segundo anclaje global.
- `Informes`: lectura de informes mensuales generados en `reports_history` o en la carpeta de informes.
- `Actualizar datos`: subida de CSVs DEGIRO, importacion, carga DuckDB, refresh FX, refresh precios e informe mensual.
- `Agentes`: revision de inputs, edicion del `investment_brief`, presupuesto mensual editable (`monthly_budget`), control de envio de `target_weights` y ejecucion de la red mensual de agentes. Antes de ejecutar muestra la fecha valorada actual, la fecha del informe mensual y la fecha del snapshot que recibiran los agentes.

## Actualizar desde Vista general

El boton `Actualizar a hoy` de `Vista general` ejecuta solo la parte de mercado:

- refresca `fx_rates` hasta la fecha actual,
- refresca `prices_daily` hasta la fecha actual,
- limpia la cache de Streamlit para recalcular la vista,
- y conserva el ultimo snapshot DEGIRO como ancla de posiciones/precios locales.

Tras refrescar, la `Fecha de referencia` de `Vista general` sale de la ultima
fecha valorada por `portfolio_daily_metrics`, no de la fecha del ultimo snapshot.
Si hay precios posteriores al snapshot, las posiciones se proyectan hasta esa
fecha con cantidades ancladas a DEGIRO y precios relativos de mercado.

No importa nuevos CSVs ni cambia el snapshot de broker disponible. Si quieres que
la cartera incluya nuevas compras, ventas, movimientos de efectivo o cantidades
oficiales de DEGIRO, primero debes subir/importar las exportaciones en
`Actualizar datos`.

## Flujo desde la UI

1. Entra en `Actualizar datos`.
2. Sube los CSV de DEGIRO o deja los existentes en `src/degiro_exports/local/incoming/`.
3. Pulsa `Guardar CSVs en incoming` si has subido archivos desde la UI.
4. Pulsa `1. Importar DEGIRO`.
5. Pulsa `2. Refrescar FX`.
6. Pulsa `3. Refrescar precios`.
7. Pulsa `4. Generar informe`.
8. Entra en `Agentes`, revisa los inputs, activa o desactiva `Enviar target_weights al pipeline`, y ejecuta la red con `static/null` para demo local. Para una ejecucion real usa `openai/tavily` si tienes `TAVILY_API_KEY`; `openai/duckduckgo` queda como fallback best-effort sin API key.

La pestaña `Agentes` bloquea la ejecucion si el informe mensual seleccionado no
corresponde a la fecha valorada actual de la cartera, o si el
`portfolio_metrics_snapshot` editable tiene otro `as_of_date`. Esto evita mezclar
un informe antiguo con metricas actuales. Si aparece el bloqueo, vuelve a
`Actualizar datos` y genera un informe nuevo para la fecha actual antes de
ejecutar la red.

El `portfolio_metrics_snapshot` editable se prepara con `asset_overrides.csv`.
Cuando DEGIRO trae nombres truncados, la UI conserva el valor original como
`broker_asset_name` y muestra en `asset_name` el nombre normalizado que usaran
los agentes.

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

Para actualizar una cartera ya importada hasta hoy sin generar informe ni tocar
CSV, usa `Vista general` -> `Actualizar a hoy`.

## Parar el dashboard

Si lo arrancaste en una terminal, usa `Ctrl+C`.

Si quedo en segundo plano, localiza el proceso Python/Streamlit:

```powershell
Get-Process | Where-Object { $_.ProcessName -like "*python*" }
```

## Contratos usados

El dashboard no calcula cartera por su cuenta. Para acciones operativas usa la
capa `src/application/` y, para lectura/visualizacion, consume contratos de
dominio ya existentes.

Casos de uso operativos:

- `ImportDegiroUseCase`
- `RefreshFxUseCase`
- `RefreshMarketDataUseCase`
- `GenerateMonthlyReportUseCase`
- `RunMonthlyAgentsUseCase`

Contratos de dominio consumidos directamente:

- `calculate_portfolio_metrics_from_normalized_degiro`
- `get_latest_monthly_report`

El `investment_brief` editable vive por defecto en:

```text
src/data/local/investment_brief.md
```

Los objetivos estructurados de cartera viven por defecto en:

```text
src/data/local/portfolio_targets.yaml
```

El dashboard usa ese archivo para pre-rellenar `target_weights` en la pestaña
`Agentes`. Si el archivo no existe o no es valido, mantiene un valor JSON de
fallback editable. El contrato de ejemplo esta en
`src/data/sample/portfolio_targets.example.yaml`.
