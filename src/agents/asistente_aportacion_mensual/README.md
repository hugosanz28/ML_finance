# asistente_aportacion_mensual

Objetivo:

Proponer una aportacion mensual con base en presupuesto, pesos objetivo y desviaciones observadas.

Entradas previstas:

- presupuesto mensual,
- pesos objetivo,
- asignacion actual,
- restricciones configurables por el usuario.

Salidas previstas:

- propuesta de compra o reparto,
- justificacion basada en desvios y reglas,
- advertencias cuando falten datos o existan limites no cubiertos.

## Encaje con la interfaz base

`asistente_aportacion_mensual` debera usar el contrato comun de `src/agents/`.

Request esperado:

- `scope`: cartera actual, presupuesto y universo invertible.
- `parameters`: moneda, granularidad de propuesta o reglas de reparto.
- `constraints`: pesos objetivo, limites por activo y restricciones del usuario.
- `input_refs`: referencias a informes de asignacion y metricas recientes.

Result esperado:

- `summary`: propuesta corta de aportacion.
- `findings`: recomendaciones estructuradas por activo o bloque.
- `artifacts`: tabla de reparto o recomendacion en markdown.
- `warnings`: carencias de datos, limites no cubiertos o imposibilidad de ejecutar parte de la propuesta.
