# Tests

La suite usa `pytest` y está configurada en `pyproject.toml`. Requiere Python
3.11 o posterior y el entorno de desarrollo:

```powershell
python -m pip install -r requirements-dev.txt
```

La matriz de CI ejecuta la suite en Windows con Python 3.11–3.14 y en Ubuntu con
Python 3.12.

Ejecutar todo:

```powershell
.\.venv\Scripts\python.exe -m pytest
```

Wrapper equivalente:

```powershell
.\scripts\test.ps1
```

Ejemplo focalizado y validación con cobertura de ramas:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_application_layer.py
.\.venv\Scripts\python.exe -m pytest --cov=src --cov-report=term-missing --cov-report=xml
```

El umbral global configurado es del 70 %. Los fixtures públicos viven en
`tests/`, `src/data/sample/` o `demo/`, siempre anonimizados o sintéticos. Los
tests deben escribir en directorios temporales y no depender de
`src/data/local/`, `src/degiro_exports/local/` ni `.env`.

Las pruebas y demos automatizadas deben ser deterministas y sin red:
`llm_provider=static`, `search_provider=null` o, para la búsqueda sintética de
demo, `static/static`. Los proveedores externos se simulan con mocks cuando una
prueba necesita cubrir su integración.

Para comandos focalizados y reglas de trabajo para agentes de programacion,
consulta `AGENTS.md`. Las puertas completas de lint, tipos, secretos, build y
auditoría están en `CONTRIBUTING.md`.
