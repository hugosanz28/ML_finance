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
- Las salidas incluyen `ApplicationResult` con estado, mensaje, avisos y
  artefactos relevantes.
- Los wrappers deben delegar en servicios existentes y evitar reescrituras
  grandes de dominio.

## Casos de uso objetivo

- importar exportaciones DEGIRO;
- refrescar FX;
- refrescar precios de mercado;
- generar informe mensual;
- ejecutar agentes mensuales;
- ejecutar quality checks antes de agentes o recomendaciones.

Las migraciones deben ser progresivas: primero se anade el wrapper, despues se
adapta el script o la vista de Streamlit correspondiente.

## Quality checks

`RunAgentQualityChecksUseCase` centraliza las validaciones deterministas antes
de ejecutar agentes: cobertura de valoracion, precios/FX faltantes, existencia
de posiciones valoradas y alineacion de fechas entre metricas, informe mensual y
`portfolio_metrics_snapshot`.

## Agentes mensuales

`RunMonthlyAgentsUseCase` envuelve `src.agents.pipeline.run_monthly_agent_pipeline`
y devuelve un `ApplicationResult` con estado agregado, warnings y artefactos
principales (`run_id`, `as_of_date`, `output_dir`). Scripts, Streamlit y futuras
interfaces deben usar este caso de uso en vez de llamar directamente al pipeline.

## Relacion con v2

La nota `docs/architecture_v2.md` define que una futura API FastAPI deberia
entrar por esta capa. La v2 no esta priorizada todavia, pero cada caso de uso
estable aqui reduce el coste de migrar desde Streamlit a una interfaz
cliente-servidor si mas adelante compensa.
