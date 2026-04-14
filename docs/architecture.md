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

### `src/portfolio/`
Estado actual de esta capa:

- reconstruccion diaria de cantidades desde `transactions`,
- reconciliacion contra `portfolio_snapshots`,
- metricas agregadas con valor, pesos y drawdown,
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
    -> refresco de precios
    -> reconstrucción histórica
    -> métricas e informes
    -> agentes
    -> Streamlit
```

## Modelo de almacenamiento propuesto

La propuesta inicial es:

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
- consultar cambios recientes,
- y abrir los informes generados.

Punto de entrada previsto:

```text
src/portfolio/dashboard.py
```

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
