# Demo publica sintetica

Esta carpeta permite ensenar el proyecto sin usar datos personales. Todos los
CSV, objetivos y brief incluidos aqui son ficticios.

La demo permite recorrer la **v2 local React/FastAPI** con datos ficticios,
nunca la cartera real.

## Demo v2 recomendada

Sigue el [quickstart del README](../README.md) para preparar datos y levantar
FastAPI con `--operations demo` y React en otra terminal. El detalle de comandos
Windows/POSIX vive en [frontend](../frontend/README.md).

Para ensenar el proyecto: [recorrido, capturas y video](../docs/showcase.md).
Los assets publicables se regeneran con `npm run showcase` desde `frontend/`,
usando una copia temporal nueva y red bloqueada. No captures un workspace al
que hayas subido datos reales, aunque el banner diga demo.

## Separacion frente a la cartera real

La cartera real usa por defecto:

```text
.env
src/degiro_exports/local/
src/data/local/
```

La demo usa:

```text
demo/synthetic_config/.env.demo
demo/synthetic_degiro_exports/
demo/local_data/
```

`demo/local_data/` se genera localmente y esta ignorado por Git. No se mezcla
con `src/data/local/`.

## Preparar los datos sin abrir servidores

```powershell
$env:ML_FINANCE_ENV_FILE="demo/synthetic_config/.env.demo"
.\.venv\Scripts\python.exe scripts\bootstrap_demo.py
```

El bootstrap importa los CSV DEGIRO sinteticos, carga DuckDB en
`demo/local_data/portfolio.duckdb`, inserta precios sinteticos offline y genera
un informe mensual demo en `demo/local_data/reports/`.

La configuracion demo fija `PRICE_PROVIDER=synthetic`. Los botones de refresh de
FX y precios permanecen offline: conservan las series precargadas y marcan como
omitidos los rangos que no cubre la demo, en vez de consultar proveedores
externos.

La demo incluye tambien la seleccion MSCI World, S&P 500, cartera 60/40 y
efectivo €STR. `SyntheticBenchmarkProvider` genera esas referencias y el FX
necesario de forma determinista y sin red. React ya muestra estas referencias
con sus limites.

En Operaciones → Agentes, `static/null` es el baseline offline. Selecciona
`static/static` solo si quieres resultados de busqueda sinteticos; no son
hechos reales de mercado. No hacen falta API keys.

En la demo puedes mostrar:

- vista general de cartera ficticia;
- laboratorio determinista de aportacion sobre las posiciones sinteticas;
- evolucion y metricas;
- informe mensual demo;
- agentes con plan interno, acciones usadas, restricciones y trazabilidad.

El material visual versionado muestra React. Sigue [privacidad](../docs/privacy.md)
y el procedimiento aislado del showcase para regenerarlo, no el workspace de
demo editable utilizado en una sesion anterior.

## Ejecutar agentes demo por consola

Primero prepara la demo con `scripts\bootstrap_demo.py`. Despues:

```powershell
$env:ML_FINANCE_ENV_FILE="demo/synthetic_config/.env.demo"
.\.venv\Scripts\python.exe scripts\run_monthly_agents.py --llm-provider static --search-provider static
```

Los outputs quedan bajo `demo/local_data/agents/`.

## Volver a la cartera real

En la misma terminal, elimina la variable de entorno:

```powershell
Remove-Item Env:\ML_FINANCE_ENV_FILE
```

Deten primero la API demo. En una terminal nueva ejecuta la API real y usa
la misma UI React (puerto 5173):

```powershell
.\.venv\Scripts\python.exe scripts/run_api.py --operations real
```

## Datos incluidos

- `synthetic_config/investment_brief.md`: mandato ficticio.
- `synthetic_config/portfolio_targets.yaml`: objetivos ficticios con el mismo
  contrato estructurado que lee y guarda la UI, incluido el mapping exacto
  entre activos sinteticos y buckets.
- `synthetic_config/benchmark_selection.json`: benchmark principal,
  referencias secundarias y composicion 60/40.
- `synthetic_degiro_exports/incoming/transactions_2026-01-15_2026-04-30.csv`
- `synthetic_degiro_exports/incoming/account_2026-01-15_2026-04-30.csv`
- `synthetic_degiro_exports/incoming/portfolio_2026-04-30.csv`

Limitaciones:

- La demo no pretende modelar una cartera real.
- El laboratorio solo propone compras de posiciones actuales valoradas y
  mapeadas. No vende ni ejecuta ordenes; la caja residual se muestra aparte y
  no entra en los pesos posteriores.
- Los precios sinteticos solo cubren el periodo necesario para mostrar el flujo.
- Los agentes en modo `static/static` devuelven resultados deterministas de demo.
