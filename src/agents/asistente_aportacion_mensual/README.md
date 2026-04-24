# asistente_aportacion_mensual

Objetivo:

Proponer la decision mensual de cartera con base en presupuesto, pesos objetivo, desviaciones observadas y objetivo de la cuenta.

Entradas previstas:

- mandato de la cuenta,
- presupuesto mensual,
- pesos objetivo,
- asignacion actual,
- restricciones configurables por el usuario,
- conclusiones de `monitor_tematico` y `analista_activos`.

Salidas previstas:

- propuesta de compra o reparto,
- propuesta opcional de venta, reduccion o rebalanceo,
- justificacion basada en desvios y reglas,
- advertencias cuando falten datos o existan limites no cubiertos.

## Encaje con la interfaz base

`asistente_aportacion_mensual` debera usar el contrato comun de `src/agents/`.

Request esperado:

- `scope`: cartera actual, presupuesto, universo invertible y movimientos potenciales.
- `parameters`: moneda, granularidad de propuesta, reglas de reparto y nivel de intervencion.
- `constraints`: pesos objetivo, limites por activo, umbrales de rebalanceo y restricciones del usuario.
- `input_refs`: `investment_brief`, informes de asignacion, metricas recientes y resultados de otros agentes.

Result esperado:

- `summary`: propuesta corta de aportacion y, si aplica, de rebalanceo.
- `findings`: recomendaciones estructuradas por activo o bloque.
- `artifacts`: tabla de reparto o recomendacion en markdown.
- `warnings`: carencias de datos, limites no cubiertos o imposibilidad de ejecutar parte de la propuesta.

## Papel en el flujo mensual

Este es el agente decisor del flujo. Debe sintetizar el mandato de la cuenta, el informe mensual, el contexto de mercado y el analisis de activos para responder:

- en que invertir `ingreso_mensual` este mes,
- si conviene no tocar ciertas posiciones,
- si hay que reducir o vender algun activo,
- y si la cartera se esta alejando del objetivo de forma suficiente como para justificar rebalanceo.
