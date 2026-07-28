# ML_finance

`ML_finance` es una aplicacion local para analizar una cartera importada desde
exportaciones oficiales de DEGIRO. Permite reconstruir historico, enriquecer
posiciones con datos de mercado, generar informes mensuales y ejecutar agentes
de apoyo a la revision manual.

Estado actual: **v0.1.0 / v1 local con Streamlit**.

La v1 esta pensada para ejecutarse en el ordenador del usuario. Los datos reales
viven en rutas locales ignoradas por Git y la demo publica usa datos sinteticos.

> [!IMPORTANT]
> El proyecto ofrece apoyo analitico, no asesoramiento financiero. No ejecuta
> ordenes. Revisa manualmente exportaciones, precios, FX, supuestos y
> recomendaciones antes de tomar una decision.

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

Requisitos:

- Python 3.11 o posterior.
- Windows es la ruta principal y dispone de wrappers PowerShell.
- La matriz de CI esta configurada para Windows con Python 3.11–3.14 y Ubuntu
  con Python 3.12. macOS puede usar los comandos POSIX, pero no tiene runner
  dedicado.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
python -m pip check
.\scripts\run_dashboard.ps1
```

`requirements.txt` instala solo la aplicacion. Para contribuir o ejecutar tests,
instala en su lugar el entorno de desarrollo:

```powershell
python -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest
```

En Linux o macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
python -m pip check
python -m streamlit run src/portfolio/dashboard.py
```

Para desarrollo, sustituye la instalacion por
`python -m pip install -r requirements-dev.txt`. Los comandos de lint, tipos,
cobertura, secretos, build y auditoria estan en
[CONTRIBUTING.md](CONTRIBUTING.md).

## Demo publica

Usa datos sinteticos y no toca `src/data/local/` ni
`src/degiro_exports/local/`.

```powershell
.\scripts\run_demo.ps1
```

Guia completa: `demo/README.md`.

## Resultado sintetico reproducible

Tras preparar la demo, el dashboard muestra una cartera ficticia, su evolucion,
un informe mensual y la auditoria de los tres agentes. Los datos, el brief, los
precios y las respuestas `static` estan etiquetados como sinteticos y no
representan una cartera ni una recomendacion real.

La vista se genera desde `demo/synthetic_config/.env.demo` y
`demo/local_data/`; no contiene datos de una cartera real. Puedes reproducirla
con `.\scripts\run_demo.ps1`.

## Uso local privado

1. Coloca exportaciones reales en `src/degiro_exports/local/incoming/`.
2. Abre el dashboard:

```powershell
.\scripts\run_dashboard.ps1
```

3. Desde Streamlit puedes importar DEGIRO, refrescar FX/precios, generar informes
   y ejecutar agentes.

Guia completa: `docs/streamlit_dashboard.md`.

## Modos de agentes

| LLM / busqueda | Uso | Red |
| --- | --- | --- |
| `static` / `null` | Baseline determinista; el monitor queda sin contexto externo y puede ser `partial`. | No |
| `static` / `static` | Demo publica completa con resultados de busqueda sinteticos. | No |
| `openai` / `tavily` | Analisis con proveedores externos configurados. | Si |
| `openai` / `duckduckgo` | Fallback web best-effort sin clave de busqueda. | Si |

El pipeline, los runners y la construccion directa de agentes usan defaults
offline seguros (`static/null`). Los proveedores externos requieren seleccion
explicita. Para enseñar la demo usa `static/static`; ambos modos `static` son
offline y no envian datos fuera del equipo.

## Solucion de problemas

- **PowerShell bloquea scripts:** ejecuta los comandos con
  `.\.venv\Scripts\python.exe` o habilita scripts solo para la sesion con
  `Set-ExecutionPolicy -Scope Process Bypass`.
- **Falta `investment_brief.md`:** copia
  `src/data/sample/investment_brief.example.md` a
  `src/data/local/investment_brief.md` o al `DATA_DIR` privado configurado.
- **Streamlit no abre en `8501`:** usa la URL alternativa que imprime la
  terminal; el puerto puede estar ocupado.
- **Los agentes bloquean por fechas:** regenera el informe para que su
  `as_of_date` coincida con las metricas y el snapshot.
- **Fallan FX o precios:** revisa red, ticker y overrides. La demo evita
  proveedores externos y sirve para separar un fallo local de uno de red.

Consulta `docs/streamlit_dashboard.md`, `docs/monthly_pipeline.md` y
`docs/privacy.md` para diagnóstico detallado.

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
- [CONTRIBUTING.md](CONTRIBUTING.md): entorno y reglas para contribuir.
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md): normas de participacion.
- [SECURITY.md](SECURITY.md): comunicacion privada de vulnerabilidades.
- [CHANGELOG.md](CHANGELOG.md): historial de versiones.

## Para agentes de programacion

Este README esta orientado a personas. Si vas a modificar el repo como agente,
lee primero `AGENTS.md`; contiene directrices de trabajo, comandos de test y el
mapa operativo de rutas.

## Release

La primera release local demostrable esta marcada con el tag `v0.1.0`.

Licencia: MIT. Consulta `LICENSE`.
