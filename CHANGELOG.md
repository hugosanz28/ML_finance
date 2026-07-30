# Changelog

Los cambios relevantes de cada versión se documentan en este archivo. El
formato sigue [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/) y el
proyecto usa [versionado semántico](https://semver.org/lang/es/).

## [Sin publicar]

### Añadido

- Documentación de contribución, seguridad, conducta y licencia MIT.
- Matriz de CI para Python 3.11–3.14 en Windows y Python 3.12 en Ubuntu.
- Puertas de calidad para Ruff, tipado gradual, cobertura de ramas, secretos,
  build, wheel instalada, demo sintética offline y auditoría de dependencias.
- Casos de uso para estado de cartera serializable, uploads DEGIRO, requisitos
  FX y ejecución aislada del monitor temático.
- Caso de uso para actualizar el `investment_brief` con escritura atómica y
  control optimista por hash.
- Proyección reutilizable de estado/PnL y cálculo centralizado de aportaciones
  externas para dashboard, informes y futuras interfaces.
- Providers `synthetic` de precios y FX que mantienen la demo completamente
  offline, además de búsqueda `static` determinista para agentes.
- Auditoría `preflight.json` para runs permitidos e intentos bloqueados.
- Auditoría reproducible de agentes con requests y contextos efectivos,
  providers allowlisted, respuestas raw trazables y hashes semánticos.

### Cambiado

- Python 3.11 pasa a ser la versión mínima; las dependencias de ejecución,
  desarrollo y notebooks quedan separadas en extras.
- Requisitos, soporte, troubleshooting y presentación pública del README.
- Roadmap reorganizado por horizonte temporal.
- Prompts de agentes desacoplados de un mandato personal fijo.
- Scripts y vistas Streamlit delegan operaciones y read models en
  `src/application/` en lugar de acceder directamente al dominio.
- Los defaults de agentes son `llm_provider=static` y
  `search_provider=null`; `static/static` queda como modo demo offline completo.
- La ejecución mensual aplica quality checks obligatorios antes de llamar a
  providers; los warnings continúan como `partial`.

### Corregido

- La demo ya acepta sus providers sintéticos también al usar los botones de
  refresh.
- La inferencia FX no se ejecuta cuando el usuario selecciona exclusivamente
  pares explícitos.
- Las fechas de requisitos FX y el estado de cartera se serializan de forma
  estable para adaptadores CLI/HTTP.
- Los read models comparten una serialización JSON estricta que convierte
  valores no finitos en `null`.
- La salida humana del runner temático conserva el estado del agente mientras
  `ApplicationResult` usa el vocabulario normalizado de la capa de aplicación.
- CLI y Streamlit comparten el bloqueo estructurado por calidad y no crean
  resultados ficticios cuando el preflight falla.

### Seguridad

- Actualizada la dependencia de datos de mercado `yfinance` a `1.5.2`.
- Añadidos escaneos de archivos versionados y dependencias a la CI.
- La auditoría de providers excluye credenciales y redacta claves sensibles
  antes de persistir o mostrar respuestas raw.

## [0.1.0] - 2026-06-23

### Añadido

- Aplicación local Streamlit para importar exportaciones DEGIRO.
- Persistencia local DuckDB/Parquet y reconstrucción histórica de cartera.
- Refresco de FX y precios, informes mensuales y agentes con auditoría.
- Demo pública reproducible basada en datos sintéticos.
