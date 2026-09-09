# Application layer

`src/application/` contiene casos de uso reutilizables por scripts, Streamlit y
una futura API o aplicacion de escritorio.

Esta capa no implementa calculos financieros nuevos. Coordina modulos ya
existentes, normaliza entradas/salidas y devuelve resultados estructurados para
que la interfaz no dependa de detalles internos de `src/portfolio/`,
`src/market_data/`, `src/reports/` o `src/agents/`.

## Convenciones

- Cada caso de uso vive en un modulo pequeno y expone una clase `*UseCase`.
- Las entradas se agrupan en dataclasses `*Request`.
- Las acciones operativas incluyen `ApplicationResult` con estado, mensaje,
  avisos y artefactos relevantes; las lecturas devuelven read models tipados y
  serializables.
- Los wrappers deben delegar en servicios existentes y evitar reescrituras
  grandes de dominio.

## Casos de uso disponibles

- importar exportaciones DEGIRO;
- refrescar FX;
- refrescar precios de mercado;
- generar informe mensual;
- ejecutar agentes mensuales;
- ejecutar el monitor tematico aislado con proveedores offline por defecto;
- listar y leer auditoria persistida de agentes;
- ejecutar quality checks antes de agentes o recomendaciones;
- cargar read models de dashboard: estado de cartera JSON, metricas, snapshots,
  transacciones, reports, brief, targets, requisitos FX y counts de bodega local;
- leer y actualizar el `investment_brief` con escritura atomica y control
  opcional de concurrencia mediante hash;
- leer y actualizar `portfolio_targets` completos mediante un mapping JSON
  validado, escritura atomica y control opcional de concurrencia mediante hash;
- simular una aportacion determinista con estado y targets locales validados,
  sin aceptar esos datos desde la interfaz ni ejecutar ordenes;
- guardar uploads DEGIRO con nombres normalizados antes de importarlos.
- consultar resumen analitico, TWR/MWR, riesgo, concentracion, correlaciones,
  benchmarks y definiciones de metricas mediante read models JSON.

## Analitica avanzada

`GetAnalyticsSummaryUseCase`, `GetPortfolioPerformanceUseCase`,
`GetPortfolioRiskUseCase` y `GetBenchmarkComparisonUseCase` comparten
`AnalyticsRequest` y devuelven `AnalyticsResult`. La consulta de definiciones
es `GetMetricDefinitionsUseCase`, sin configuracion ni acceso a datos.

La seleccion de periodo se delega en rendimiento: `last_month`, `last_quarter`,
`last_year` y `since_inception`. Todas las secciones usan su misma apertura
efectiva y fecha final; no se recortan independientemente. `benchmark_id`
sobrescribe la seleccion solo para esa consulta, sin escribir configuracion.
Las formulas siguen en dominio, no en esta capa.

Las consultas usan DuckDB en modo `read_only=True` y no persisten metricas,
crean carpetas ni inicializan schemas. Solo leen rutas de `Settings`: no
aceptan paths, DataFrames, providers ni estado de cartera en la request.
Sin bodega o historico normalizado devuelven `portfolio_data_unavailable`.
Errores de contrato/datos malformados se propagan; no se disfrazan como vacios.

Sin exportaciones de movimientos, TWR/MWR y retornos diarios quedan no
disponibles con `cash_flow_data_missing`. Sin tasa explicita, Sharpe/Sortino
quedan no disponibles. Datos ausentes nunca se convierten en ceros.

Las series por activo son proxies del precio de valoracion convertido a moneda
base, independientes de cantidades. Se identifican como `valuation_price_proxy`
y se marcan parciales: no incluyen dividendos ni ajustes adicionales de acciones
corporativas y pueden heredar precios arrastrados o cambios de ancla del motor
de valoracion. Solo cubren activos presentes al cierre y dias con historia
disponible; no se rellenan huecos. Si falta valoracion actual, no se renormalizan
los pesos restantes para atribuir riesgo. Buckets se resuelven por ID/ISIN
exactos; mappings contradictorios quedan sin clasificar.

La demo usa benchmarks sinteticos solo con `Settings.price_provider=synthetic`.
En local real, un backend puede inyectar `LoadedBenchmarkProvider` construido
con datos ya cargados; si no lo hace, se devuelve
`benchmark_provider_unavailable`, sin usar red ni sustituirlo por la demo.
El adaptador de fuente real y su persistencia siguen pendientes.

Ver los payloads y rutas HTTP previstas en [contratos API](../../docs/api_contracts.md#9-analitica-avanzada).
Tests: `tests/test_analytics_application.py` y `tests/test_interface_boundaries.py`.

Las migraciones deben ser progresivas: primero se anade el wrapper, despues se
adapta el script o la vista de Streamlit correspondiente.

## Quality checks

`RunAgentQualityChecksUseCase` centraliza las validaciones deterministas antes
de ejecutar agentes: cobertura de valoracion, precios/FX faltantes, existencia
de posiciones valoradas y alineacion de fechas entre metricas, informe mensual y
`portfolio_metrics_snapshot`. Devuelve `failed` con errores bloqueantes,
`partial` cuando solo hay warnings y `succeeded` cuando el preflight esta limpio.

## Agentes mensuales

`RunMonthlyAgentsUseCase` envuelve `src.agents.pipeline.run_monthly_agent_pipeline`
y devuelve un `ApplicationResult` con estado agregado, warnings y artefactos
principales (`run_id`, `as_of_date`, `output_dir`). Scripts, Streamlit y futuras
interfaces deben usar este caso de uso en vez de llamar directamente al pipeline.
El caso de uso ejecuta siempre el preflight antes de construir providers. Un
bloqueo devuelve `pipeline_result=None` y, con `persist=True`, guarda
`preflight.json` junto con `run_metadata.json`; un warning permite continuar y
fuerza estado agregado `partial`.

`ListAgentRunsUseCase` y `GetAgentRunAuditUseCase` exponen read models de la
auditoria persistida en disco. Streamlit los usa para visualizar plan interno,
acciones, fuentes, prompts, warnings, inputs y outputs sin acoplar la UI a la
estructura fisica de carpetas.

El read model de auditoria expone tambien la metadata reproducible del schema
v2: request y contexto efectivos, provider/modelo/opciones no secretas,
respuesta raw con estado y `reason_code`, y hashes SHA-256 de entrada/salida.
La lectura es compatible con runs legacy v1: los archivos o campos que todavia
no existian se devuelven como no disponibles y nunca se escriben durante una
consulta.

La capa de aplicacion no debe exponer credenciales al adaptar esta metadata para
Streamlit o una futura API. La configuracion de provider procede de una lista
permitida y los artefactos completos siguen siendo privados aunque contengan
solo hashes o providers offline.

`RunMonitorTematicoUseCase` ofrece el mismo limite para ejecuciones aisladas del
monitor. Sus defaults `static/null` no usan red.

Los casos de refresh respetan el proveedor configurado. La demo usa
`PRICE_PROVIDER=synthetic`, que mantiene FX y precios precargados sin intentar
descargas externas.

## Estado de cartera y adaptadores

`GetPortfolioStateUseCase` compone las metricas, posiciones, historico,
snapshot broker y aportaciones externas en tipos serializables. Es la frontera
prevista para `GET /api/v1/portfolio/state`.

`SaveDegiroUploadsUseCase` controla nombres y persistencia de archivos subidos;
`InferFxRequirementsUseCase` devuelve requisitos FX con fechas ISO mediante
`to_dict()`. Las interfaces no necesitan importar parsers ni repositorios.

`ReadPortfolioTargetsUseCase` expone el contrato completo, los pesos derivados,
la ruta como texto, el hash del contenido y cualquier error de validacion.
`UpdatePortfolioTargetsUseCase` recibe datos estructurados, valida con el mismo
contrato del dominio y solo reemplaza atomica y controladamente la ruta
configurada en `Settings.portfolio_targets_path`.

`SimulateContributionUseCase` carga ese contrato y
`GetPortfolioStateUseCase(persist=False)`, adapta solo posiciones actuales
valoradas y resuelve su bucket mediante `asset_bucket_mapping`. Delega el
calculo al planificador puro de cartera y devuelve una respuesta JSON estricta
con compras propuestas, pesos antes/despues, restricciones y caja residual. La
simulacion es `contributions_only`: nunca vende ni ejecuta ordenes. El efectivo
no invertido queda fuera del denominador de los pesos posteriores y se informa
por separado.

## Relacion con v2

La nota `docs/architecture_v2.md` define que una futura API FastAPI deberia
entrar por esta capa. El roadmap v2 contempla FastAPI local y React +
TypeScript + Vite; estos contratos preparan la migracion manteniendo Streamlit operativo.

Los contratos HTTP previstos antes de implementar FastAPI estan en
`docs/api_contracts.md`. Si un contrato necesita logica que todavia no existe
en esta capa, primero debe crearse el caso de uso correspondiente aqui y despues
adaptar la interfaz.

La v1 de Streamlit ya usa esta capa para acciones operativas y read models
principales. Los modulos `dashboard_*` pueden mantener transformaciones visuales
y composicion UI, pero no deberian llamar directamente a importadores, reports,
agentes o repositorios cuando exista un caso de uso equivalente aqui.
