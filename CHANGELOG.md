# Changelog

Los cambios relevantes de cada versión se documentan en este archivo. El
formato sigue [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/) y el
proyecto usa [versionado semántico](https://semver.org/lang/es/).

## [Sin publicar]

### Añadido

- Showcase v2 (#57): README visual, capturas y video de la API/UI reales con
  datos sinteticos, portada editorial, caso de estudio y regeneracion offline en CI.
- Operaciones React (#56): importacion, refresh, aportaciones, informes, agentes,
  configuracion con hash, confirmacion e historial de jobs; prueba E2E offline.
  Health identifica demo/real y el lector admite IDs de informes actuales y legacy.
- Benchmarks reales locales (#60): proxies ETF via yfinance y €STR/FX del BCE,
  refresh explicito por CLI/job, cache atomica validada y fuentes/hashes en React.
  Los huecos limitan la comparacion al ultimo tramo continuo, sin relleno ficticio.
- UI React + TypeScript + Vite (#55), oscura y local de solo lectura, con
  resumen, rendimiento/benchmarks, riesgo, explicaciones y tablas de graficos.
- Contratos TypeScript validados en runtime, tests de componentes y CI con
  respuestas reales de FastAPI sobre la demo sintetica (16 filtros).
- Operaciones FastAPI opt-in (#54) con worker local, jobs persistidos en DuckDB,
  idempotencia, controles de entorno/confirmacion y actualizaciones por hash.
- Copias editables de la demo sin modificar fixtures, limites de uploads,
  recuperacion de jobs interrumpidos y retry solo para simulaciones fallidas.
- API FastAPI local de solo lectura (#53), esquemas OpenAPI, consultas de
  cartera/analitica/definiciones/informes/auditoria y launcher Windows/POSIX.
- Documentación de contribución, seguridad, conducta y licencia MIT.
- Matriz de CI para Python 3.11–3.14 en Windows y Python 3.12 en Ubuntu.
- Puertas de calidad para Ruff, tipado gradual, cobertura de ramas, secretos,
  build, wheel instalada, demo sintética offline y auditoría de dependencias.
- Casos de uso para estado de cartera serializable, uploads DEGIRO, requisitos
  FX y ejecución aislada del monitor temático.
- Caso de uso para actualizar el `investment_brief` con escritura atómica y
  control optimista por hash.
- Contratos completos para leer y actualizar `portfolio_targets` mediante JSON
  validado, escritura atómica y control optimista por hash.
- Laboratorio determinista de aportación con compras sobre posiciones
  existentes, límites configurables, caja residual y comparación de pesos antes
  y después, sin ventas ni ejecución de órdenes.
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

- Lecturas de cartera sin persistencia ya no inicializan DuckDB ni schemas.
- Valoracion sin precios externos conserva el schema de la tabla vacia.
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
