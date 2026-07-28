# Demo publica sintetica

Esta carpeta permite ensenar el proyecto sin usar datos personales. Todos los
CSV, objetivos y brief incluidos aqui son ficticios.

La demo representa la **v1 local con Streamlit**: prepara un workspace sintetico
en `demo/local_data/` y abre el mismo dashboard que se usa con la cartera real,
pero apuntando a datos ficticios.

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

## Preparar demo

Desde la raiz del repo:

```powershell
.\scripts\run_demo.ps1
```

Ese comando prepara la demo y abre Streamlit. Si quieres separar los pasos:

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

Este es el comando recomendado para ensenar el proyecto a otra persona:

```powershell
.\scripts\run_demo.ps1
```

## Abrir dashboard demo

Si ya has ejecutado el bootstrap y no quieres regenerar datos:

```powershell
.\scripts\run_demo.ps1 -SkipBootstrap
```

Alternativa manual:

```powershell
$env:ML_FINANCE_ENV_FILE="demo/synthetic_config/.env.demo"
.\.venv\Scripts\python.exe -m streamlit run src\portfolio\dashboard.py
```

Abre `http://localhost:8501`. En la pestana `Agentes`, usa:

- `LLM provider`: `static`
- `Search provider`: `static`

Asi no se usan claves API ni red y el monitor recibe resultados sinteticos para
mostrar el flujo completo. `static/null` tambien es offline, pero se reserva
como baseline: no genera contexto de busqueda y el monitor puede quedar
`partial`.

En la demo puedes mostrar:

- vista general de cartera ficticia;
- evolucion y metricas;
- informe mensual demo;
- agentes con plan interno, acciones usadas, restricciones y trazabilidad.

El repositorio no incluye actualmente capturas del dashboard. Si se anade
material visual, debe generarse desde este workspace sintetico despues del
bootstrap y revisarse con el checklist de `docs/privacy.md`.

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

O abre una terminal nueva y ejecuta el dashboard normal:

```powershell
.\.venv\Scripts\python.exe -m streamlit run src\portfolio\dashboard.py
```

## Datos incluidos

- `synthetic_config/investment_brief.md`: mandato ficticio.
- `synthetic_config/portfolio_targets.yaml`: objetivos ficticios.
- `synthetic_degiro_exports/incoming/transactions_2026-01-15_2026-04-30.csv`
- `synthetic_degiro_exports/incoming/account_2026-01-15_2026-04-30.csv`
- `synthetic_degiro_exports/incoming/portfolio_2026-04-30.csv`

Limitaciones:

- La demo no pretende modelar una cartera real.
- Los precios sinteticos solo cubren el periodo necesario para mostrar el flujo.
- Los agentes en modo `static/static` devuelven resultados deterministas de demo.
