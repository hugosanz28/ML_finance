# Demo publica sintetica

Esta carpeta permite ensenar el proyecto sin usar datos personales. Todos los
CSV, objetivos y brief incluidos aqui son ficticios.

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

Asi no se usan claves API ni red.

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
