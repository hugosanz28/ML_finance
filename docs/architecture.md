# Arquitectura

## Objetivo

El proyecto debe ser útil para uso personal real y, al mismo tiempo, publicable como repositorio demostrable. La arquitectura está pensada para separar claramente:

- el código y la lógica reproducible,
- los datos de ejemplo compartibles,
- y los datos privados del usuario.

## Principios

1. La fuente de verdad del broker son las exportaciones oficiales.
2. El sistema debe funcionar en local sin depender de infraestructura externa.
3. Los datos personales no se suben al repositorio.
4. La interfaz inicial prioriza velocidad de iteración sobre sofisticación visual.
5. Cada capa debe poder probarse por separado.

## Componentes

### `src/application/`

Contiene casos de uso reutilizables por scripts, Streamlit y una futura API o
aplicacion de escritorio. Esta capa coordina servicios existentes y devuelve
resultados estructurados; no debe duplicar calculos financieros ni contener
logica de interfaz.

La v1 de Streamlit consume esta capa tanto para acciones operativas como para
read models principales del dashboard. Una futura API debe mantener la misma
regla: endpoints finos que llamen a `src/application/`, no a detalles internos
de `src/portfolio/`, `src/reports/` o `src/agents/`.

Fronteras ya disponibles:

- `GetPortfolioStateUseCase` para el read model JSON de cartera;
- `SaveDegiroUploadsUseCase` e `ImportDegiroUseCase` para uploads e importacion;
- `InferFxRequirementsUseCase`, `RefreshFxUseCase` y
  `RefreshMarketDataUseCase` para market data;
- `GenerateMonthlyReportUseCase` para informes;
- `RunMonthlyAgentsUseCase`, con preflight bloqueante obligatorio, y
  `RunMonitorTematicoUseCase` para agentes;
- `ReadInvestmentBriefUseCase` y `UpdateInvestmentBriefUseCase` para leer y
  guardar el mandato local sin escribir desde la interfaz;
- `ReadPortfolioTargetsUseCase` y `UpdatePortfolioTargetsUseCase` para leer,
  validar y guardar objetivos estructurados con control de concurrencia.

Las acciones operativas devuelven `ApplicationResult`. Los read models
destinados a adaptadores externos, como estado de cartera y requisitos FX,
usan resultados serializables y no exponen `DataFrame`, `Path` ni objetos
internos de dominio a una futura API.

### `src/degiro_exports/`

Contiene la entrada del sistema:

- `example/`: ejemplos saneados que sí pueden versionarse.
- `local/`: exportaciones reales del usuario, ignoradas por Git.

### `src/data/`

Contiene los artefactos derivados:

- `sample/`: datos sintéticos o anonimizados para demo pública.
- `local/`: base local, cachés, informes, snapshots y artefactos privados.

### `src/market_data/`

Responsable de descargar y normalizar precios, divisas y metadatos de mercado.
En la valoracion actual, los precios externos no sustituyen al precio del
broker: aportan variacion relativa desde el ultimo precio local observado en
snapshots DEGIRO.

El provider normal es `yfinance`. La demo configura `synthetic`, que no accede
a red ni genera cotizaciones: conserva los precios y FX sinteticos sembrados
por `scripts/bootstrap_demo.py`.

### `src/portfolio/`
Estado actual de esta capa:

- reconstruccion diaria de cantidades desde `transactions`,
- reconciliacion contra `portfolio_snapshots`,
- metricas agregadas con valor, pesos, drawdown y politica
  `broker_snapshot_anchored`,
- proyecciones compartidas de snapshot broker, coste y PnL en
  `state_projection.py`,
- consultas de aportaciones externas en `contributions.py`,
- y una base directa para reporting y Streamlit.

Responsable de reconstrucción histórica de posiciones, métricas agregadas e interfaz de Streamlit.

### `src/analytics/`

Responsable de análisis por activo, ETF, sector, correlaciones y otras métricas de apoyo.

### `src/agents/`

Responsable de encapsular flujos periódicos de análisis y recomendaciones.

## Flujo de datos

```text
DEGIRO exports
    -> ingestión raw
    -> normalización
    -> almacenamiento local
    -> refresco de FX/precios
    -> reconstruccion historica
    -> metricas ancladas a snapshots DEGIRO
    -> informes
    -> agentes
    -> Streamlit
```

## Modelo de almacenamiento actual

La implementacion actual usa:

- `DuckDB` como base local principal,
- `Parquet` para datasets intermedios o exportables,
- `Markdown` para informes generados.

Entidades mínimas previstas:

- `assets_master`
- `transactions`
- `cash_movements`
- `portfolio_snapshots`
- `prices_daily`
- `fx_rates`
- `reports_history`

## Streamlit

La primera interfaz se mantendrá simple y local. El objetivo no es hacer una aplicación final desde el primer día, sino una consola visual útil para:

- ver la asignación actual,
- revisar evolución histórica,
- refrescar FX y precios hasta hoy desde la vista general,
- consultar cambios recientes,
- y abrir los informes generados.

Punto de entrada previsto:

```text
src/portfolio/dashboard.py
```

`dashboard.py` actua como entrypoint y navegacion. Las pestanas viven en modulos
especificos (`dashboard_overview.py`, `dashboard_reports.py`,
`dashboard_data_update.py`, `dashboard_agents.py`) y los helpers compartidos en
`dashboard_common.py`. Esta separacion reduce acoplamiento antes de una posible
interfaz web futura.

## Direccion v2

La direccion tecnica para una futura v2 esta documentada en
`docs/architecture_v2.md`. La decision actual es no migrar todavia: Streamlit
sigue siendo la interfaz adecuada para consolidar la v1, mientras
`src/application/` prepara el camino para una posible API FastAPI y un frontend
Angular mas adelante.

Antes de implementar FastAPI, los contratos previstos de lectura/escritura viven
en `docs/api_contracts.md`. Sirven para estabilizar que datos necesita una UI
futura y que casos de uso deben existir en `src/application/`.

## Agentes

Los agentes no deben inventar el estado de la cartera. Deben consumir:

- datos estructurados ya validados,
- informes ya generados,
- y fuentes externas acotadas cuando sea necesario.

Secuencia recomendada:

1. consolidar datos,
2. generar informe base,
3. encapsular el informe en agentes especializados.

## Límite entre repo público y entorno privado

Por conveniencia local, los datos privados pueden vivir en rutas gitignoradas dentro del repo. Aun así, la arquitectura deja abierta una futura migración a rutas externas configuradas por variables de entorno si el volumen o la sensibilidad lo requieren.

La politica operativa de privacidad, rutas sensibles y revision de secretos esta
documentada en `docs/privacy.md`.
