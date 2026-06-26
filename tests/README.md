# Tests

La suite usa `pytest` y esta configurada en `pyproject.toml`.

Ejecutar todo:

```powershell
.\.venv\Scripts\python.exe -m pytest
```

Wrapper equivalente:

```powershell
.\scripts\test.ps1
```

Los fixtures publicos viven en `tests/`, `src/data/sample/` o `demo/`, siempre
anonimizados o sinteticos.

Para comandos focalizados y reglas de trabajo para agentes de programacion,
consulta `AGENTS.md`.
