# Contribuir a ML_finance

Gracias por ayudar a mejorar el proyecto. Antes de empezar, abre un issue para
cambios amplios o que afecten a contratos, persistencia, cálculos financieros o
privacidad. Las correcciones pequeñas pueden ir directamente a un pull request.

## Entorno de desarrollo

Se necesita Python 3.11 o posterior. `requirements-dev.txt` instala el proyecto
en modo editable, sus dependencias de ejecución y las herramientas de calidad.
En Windows:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
```

En Ubuntu, Linux o macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
```

La matriz de CI cubre Windows con Python 3.11, 3.12, 3.13 y 3.14, además de
Ubuntu con Python 3.12. Windows sigue siendo la ruta principal de los wrappers
PowerShell.

## Validación local

Ejecuta primero el test focalizado del área modificada y después la suite:

```powershell
python -m pytest tests\test_application_layer.py
python -m pytest
```

Las puertas de calidad configuradas en CI son:

```powershell
python -m ruff check src scripts tests
python -m mypy
python -m pytest --cov=src --cov-report=term-missing --cov-report=xml
$trackedFiles = git ls-files
detect-secrets-hook --baseline .secrets.baseline $trackedFiles
```

La cobertura mide ramas y exige al menos el 70 % según `pyproject.toml`.
`mypy` comprueba por ahora una frontera gradual, también definida allí.

Para validar la demo sin abrir un proceso Streamlit:

```powershell
$env:ML_FINANCE_ENV_FILE = "demo/synthetic_config/.env.demo"
python scripts/bootstrap_demo.py
```

En un shell POSIX, el equivalente es:

```bash
ML_FINANCE_ENV_FILE=demo/synthetic_config/.env.demo python scripts/bootstrap_demo.py
git ls-files -z | xargs -0 detect-secrets-hook --baseline .secrets.baseline
```

Para comprobar empaquetado y dependencias:

```powershell
python -m build
python -m pip check
python -m pip_audit . --strict --progress-spinner off --cache-dir .test_tmp/pip-audit-cache
```

La auditoría consulta metadatos de vulnerabilidades y puede requerir red. CI
también instala la wheel construida y comprueba que incluye prompts y SQL. No
interpretes un escáner limpio como sustituto de la revisión manual.

Consulta `AGENTS.md` para el mapa de tests focalizados y las reglas de cada
módulo.

## Pull requests

- Mantén cada cambio acotado a un problema.
- Añade o actualiza tests cuando cambie el comportamiento.
- Ejecuta primero los tests focalizados y después la suite completa.
- Ejecuta las puertas de calidad aplicables; si cambias dependencias o
  empaquetado, incluye también build y auditoría.
- Actualiza `CHANGELOG.md` si el cambio es visible para usuarios.
- No introduzcas dependencias ni rompas contratos públicos sin justificarlo.
- Sigue `CODE_OF_CONDUCT.md`.

## Datos y secretos

No adjuntes exportaciones reales de DEGIRO, bases DuckDB, informes privados,
audit trails, capturas personales ni archivos `.env`. Para tests y ejemplos usa
solo `demo/`, `src/data/sample/` y proveedores deterministas. Los defaults
seguros son `llm_provider=static` y `search_provider=null`; la demo puede usar
`static/static`. No habilites proveedores externos en tests, fixtures o
ejemplos. Revisa `docs/privacy.md` antes de publicar un diff.

## Alcance financiero

Las salidas son apoyo analítico para revisión manual. Una contribución no debe
añadir ejecución automática de órdenes ni presentar resultados sintéticos como
asesoramiento financiero real.
