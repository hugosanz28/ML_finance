# Caso de estudio: de exportaciones DEGIRO a una revision mensual auditable

## Problema y alcance

ML Finance nacio como proyecto personal para reducir el trabajo de fin de mes:
unir exportaciones, distinguir aportaciones de resultado y preparar una
aportacion informada. No pretende predecir mercados ni operar por el usuario.
La [demo reproducible](showcase.md) muestra el flujo; no mide rentabilidad real
ni acredita adopcion, ahorro de tiempo o resultados comerciales.

## 1. Primero contratos y procedencia

Los parsers convierten transacciones, movimientos de efectivo y snapshots del
broker a contratos normalizados y validados. DuckDB permite consultar estado
local; Parquet conserva artefactos tabulares derivados. Markdown es un informe
generado desde esos datos, no una conversion ciega de todo el contenido de la base.

Ejemplo conceptual: compras y un snapshot permiten reconstruir cantidades;
valoracion, costes y flujos alimentan metricas; el informe resume esas metricas
con su fecha. Las interfaces entran por casos de uso, nunca por consultas
independientes que redefinan el significado de una cifra.

Evidencia: [arquitectura de dominio](architecture.md),
[capa application](../src/application/README.md),
[tests de fronteras](../tests/test_interface_boundaries.py).

## 2. Valoracion anclada al broker

Un precio externo puede tener otra escala o pertenecer a un proxy. La politica
`broker_snapshot_anchored` conserva el precio local del ultimo snapshot y usa
la variacion relativa de mercado para extenderlo. FX convierte divisas; los
overrides explicitos permiten resolver correspondencias, no inventarlas.

Ejemplo ilustrativo, no un resultado de demo: un precio broker de 100 EUR y un
proxy que pasa de 50 a 55 implican una extension de 110 EUR antes de cualquier
conversion necesaria, no sustituir directamente el precio por 55. Un nuevo
snapshot aporta una nueva referencia. El precio estimado no garantiza un precio
ejecutable ni transforma el proxy en una serie total return del activo.

Evidencia: [politica de valoracion](architecture.md),
[tests de metricas](../tests/test_portfolio_metrics.py).

## 3. Analitica con limites explicitos

Solo depositos y retiradas son flujos externos; dividendos, comisiones o FX no
se reclasifican como aportaciones. TWR encadena retornos ajustados por flujos
al cierre. MWR/XIRR pondera importes y fechas y devuelve una tasa anualizada.
No se comparan directamente ambas cifras sin atender al periodo y anualizacion.

MSCI World, S&P 500, 60/40 y €STR son referencias seleccionables, no sugerencias
de compra. Las fuentes reales son proxies ETF y BCE descargados explicitamente;
GET solo lee cache. En demo las referencias son sinteticas, sin respaldo
sintetico para una cartera real cuando falta cache. Moneda, fechas comunes,
muestra y cobertura limitan las comparaciones. Un valor ausente sigue siendo
`null` con motivo: no un cero ni una curva rellenada.

Formulas y evidencia: [rendimiento](performance.md), [benchmarks](benchmarks.md),
[riesgo](risk_analytics.md), [tests de TWR/MWR](../tests/test_portfolio_performance.py).

## 4. Agentes despues de las comprobaciones

El preflight bloquea errores de calidad antes de construir proveedores. Informe,
metricas y snapshot analitico deben tener fechas coherentes. Application crea y
valida el snapshot; el LLM no inventa el estado de cartera. Analista y asistente
reciben contexto financiero estructurado; el monitor no recibe ese snapshot.

La auditoria conserva plan, acciones, fuentes, referencias de prompts, requests,
outputs y metadata permitida del proveedor. SHA-256 identifica cambios en
artefactos y contenido semantico. Ayuda a explicar que se ejecuto, pero no cifra
datos, no anonimiza, no garantiza acierto ni asegura que un proveedor externo
repita exactamente su respuesta. Se mantiene lectura de auditorias legacy.

Evidencia: [analitica en agentes](agent_analytics.md),
[tests de auditoria](../tests/test_agent_audit_trail.py), [privacidad](privacy.md).

## 5. Separar dominio, API e interfaz

React presenta resultados calculados por Python a traves de FastAPI y valida
contratos HTTP con Zod. Las explicaciones vienen del catalogo del servidor.
Los graficos escalan valores y ofrecen tablas; no hay otro motor financiero
JavaScript que pueda discrepar del backend.

Las operaciones requieren opt-in, confirmacion y clave de idempotencia; un
worker evita escritores concurrentes. Brief y targets exigen el hash vigente.
Un envio incierto conserva la misma clave, y no se reejecutan agentes
automaticamente. Informes y auditorias se muestran como texto inerte.

Evidencia: [API local](local_api.md), [jobs](local_jobs.md),
[operaciones React](react_operations.md), [E2E sintetico](../frontend/e2e/monthly.spec.ts).

## Privacidad y siguientes limites

Local-first no significa que todo sea automaticamente privado: seleccionar
proveedores externos puede enviar contexto financiero. Bases, informes y
auditorias reales no se publican. El showcase usa un workspace sintetico nuevo,
red externa bloqueada y revision visual antes de versionar assets.

Streamlit se retira tras la [migracion documentada](react_migration.md). Un servicio publico necesita un
diseno adicional de autenticacion, aislamiento y operacion. El enfoque educativo
todavia requiere pruebas con usuarios; no se declara validacion comercial.
