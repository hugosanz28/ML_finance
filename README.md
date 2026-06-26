# ML_finance

`ML_finance` es una aplicacion local para analizar una cartera importada desde
exportaciones oficiales de DEGIRO. Permite reconstruir historico, enriquecer
posiciones con datos de mercado, generar informes mensuales y ejecutar agentes
de apoyo a la revision manual.

Estado actual: **v0.1.0 / v1 local con Streamlit**.

La v1 esta pensada para ejecutarse en el ordenador del usuario. Los datos reales
viven en rutas locales ignoradas por Git y la demo publica usa datos sinteticos.

## Que incluye

- Importacion y normalizacion de transacciones, efectivo y snapshots DEGIRO.
- Base local DuckDB/Parquet para datos derivados.
- Reconstruccion diaria de posiciones y metricas agregadas de cartera.
- Refresco de FX y precios con politica `broker_snapshot_anchored`.
- Informe mensual en Markdown.
- Dashboard Streamlit para cartera, evolucion, informes, actualizacion de datos y agentes.
- Agentes mensuales con prompts versionados y auditoria visual.
- Demo sintetica ejecutable sin exponer datos reales.

## Puesta en marcha

Python 3.10+ recomendado.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Validar el entorno:

```powershell
.\.venv\Scripts\python.exe -m pytest
```

## Demo publica

Usa datos sinteticos y no toca `src/data/local/` ni
`src/degiro_exports/local/`.

```powershell
.\scripts\run_demo.ps1
```

Guia completa: `demo/README.md`.

## Uso local privado

1. Coloca exportaciones reales en `src/degiro_exports/local/incoming/`.
2. Abre el dashboard:

```powershell
.\scripts\run_dashboard.ps1
```

3. Desde Streamlit puedes importar DEGIRO, refrescar FX/precios, generar informes
   y ejecutar agentes.

Guia completa: `docs/streamlit_dashboard.md`.

## Rutas clave

| Ruta | Uso |
| --- | --- |
| `src/application/` | Casos de uso para scripts, Streamlit y futura API. |
| `src/portfolio/` | Metricas de cartera y dashboard Streamlit. |
| `src/agents/` | Pipeline mensual, agentes, modelos y prompts. |
| `src/degiro_exports/` | Importadores y parsers DEGIRO. |
| `src/data/local/` | Datos privados locales, ignorados por Git. |
| `demo/` | Demo publica sintetica. |
| `docs/` | Arquitectura, contratos, privacidad y flujos. |

## Documentacion

- `docs/architecture.md`: arquitectura v1 y flujo de datos.
- `docs/architecture_v2.md`: direccion futura FastAPI + Angular, sin migracion iniciada.
- `docs/api_contracts.md`: contratos futuros de API local.
- `docs/privacy.md`: separacion publico/privado y checklist de secretos.
- `docs/streamlit_dashboard.md`: uso del dashboard local.
- `docs/monthly_pipeline.md`: flujo mensual completo con informes y agentes.
- `src/agents/README.md`: detalle funcional de agentes.
- `src/application/README.md`: capa de casos de uso.
- `AGENTS.md`: instrucciones compactas para agentes de programacion.

## Para agentes de programacion

Este README esta orientado a personas. Si vas a modificar el repo como agente,
lee primero `AGENTS.md`; contiene directrices de trabajo, comandos de test y el
mapa operativo de rutas.

## Release

La primera release local demostrable esta marcada con el tag `v0.1.0`.
