# ML_finance

`ML_finance` evoluciona desde un repositorio de exploración en Jupyter hacia un sistema local para:

- importar exportaciones oficiales de DEGIRO,
- reconstruir y seguir el histórico de la cartera,
- enriquecer posiciones con datos de mercado,
- generar informes periódicos,
- exponer una interfaz simple en Streamlit,
- y servir como proyecto público enseñable sin exponer datos personales.

## Estado actual

La base del proyecto ya está preparada:

- estructura de carpetas reorganizada,
- documentación de roadmap y arquitectura,
- zona `legacy` para notebooks antiguos,
- separación entre datos públicos de ejemplo y datos privados locales,
- y primer esqueleto para agentes y dashboard.

Las siguientes fases son implementar el modelo de datos local y el importador de exportaciones DEGIRO.

## Estructura del repositorio

```text
ML_finance/
|- .github/
|  |- ISSUE_TEMPLATE/
|  `- pull_request_template.md
|- docs/
|  |- architecture.md
|  |- decisions.md
|  `- roadmap.md
|- notebooks/
|  |- old/
|  `- README.md
|- scripts/
|  `- README.md
|- src/
|  |- agents/
|  |- analytics/
|  |- data/
|  |- degiro_exports/
|  |- market_data/
|  `- portfolio/
|- tests/
|  `- README.md
|- .env.example
|- .gitattributes
|- .gitignore
|- README.md
`- requirements.txt
```

## Convención público / privado

El repositorio sigue una separación simple:

- `src/degiro_exports/example/`: ejemplos saneados y compartibles.
- `src/degiro_exports/local/`: exportaciones reales del broker, ignoradas por Git.
- `src/data/sample/`: datasets sintéticos o anonimizados para demo pública.
- `src/data/local/`: base local, cachés, informes y artefactos privados, ignorados por Git.

Esto permite tener un repositorio público útil y, a la vez, trabajar con tu cartera real sin subir datos sensibles.

## Puesta en marcha

Python 3.10+ recomendado.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Después:

1. Coloca exportaciones reales en `src/degiro_exports/local/` cuando el importador exista.
2. Usa `src/degiro_exports/example/` y `src/data/sample/` para demos públicas.
3. Consulta el plan en `docs/roadmap.md`.

## Dashboard

La primera interfaz será una app de Streamlit. El punto de entrada previsto es:

```powershell
streamlit run src/portfolio/dashboard.py
```

Ahora mismo solo hay un esqueleto mínimo; la funcionalidad llegará después del modelo de datos y del importador.

## Documentación clave

- `docs/roadmap.md`: fases, backlog y traducción del plan a tareas de GitHub.
- `docs/architecture.md`: flujo de datos, componentes y límites del sistema.
- `docs/decisions.md`: decisiones ya cerradas y su justificación.

- `docs/data_model.md`: esquema inicial de DuckDB, claves y relaciones entre tablas.

## Legacy

Los notebooks anteriores siguen disponibles en `notebooks/old/`. Se conservan como referencia histórica, pero ya no definen la arquitectura principal del proyecto.
