# Notebooks

Este directorio queda para trabajo exploratorio y material histórico. Requiere
Python 3.11 o posterior; las dependencias de notebooks están separadas de la
aplicación:

```powershell
python -m pip install -e ".[notebooks]"
```

Para combinar exploración y herramientas de desarrollo:

```powershell
python -m pip install -e ".[dev,notebooks]"
```

- `old/`: notebooks anteriores conservados como referencia.
- raíz de `notebooks/`: espacio para nuevas exploraciones puntuales, siempre que
  no sustituyan al código reutilizable en `src/`.

Los notebooks históricos no forman parte de la ruta de ejecución de la
aplicación ni de la suite de CI. Para nuevas exploraciones usa exclusivamente
datos sintéticos o anonimizados de `demo/` y `src/data/sample/`; no cargues,
muestres ni guardes exportaciones reales, `.env`, claves o artefactos de
`src/data/local/`.

Si una lógica deja de ser solo exploratoria y pasa a ser parte del producto,
debe migrarse a módulos Python en `src/` con tests.
