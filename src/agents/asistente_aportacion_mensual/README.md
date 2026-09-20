# asistente_aportacion_mensual

Objetivo:

Proponer la decision mensual de cartera con base en presupuesto, pesos objetivo, desviaciones observadas y objetivo de la cuenta.

Entradas:

- mandato de la cuenta,
- presupuesto mensual,
- contrato completo de objetivos: pesos, limites, modo de rebalanceo y mapping
  exacto de activos a buckets,
- asignacion actual,
- restricciones configurables por el usuario,
- conclusiones de `monitor_tematico` y `analista_activos`.

Salidas:

- propuesta de compra o reparto,
- propuesta opcional de venta, reduccion o rebalanceo,
- escenarios `conservador`, `neutral` y `oportunista`,
- justificacion basada en desvios y reglas,
- advertencias cuando falten datos o existan limites no cubiertos.

## Encaje con la interfaz base

`asistente_aportacion_mensual` implementa `BaseAgent` y devuelve el contrato
comun `AgentResult` de `src/agents/`.

Inputs requeridos:

- `investment_brief`;
- `latest_monthly_report`.

Inputs opcionales que completan la decision:

- `portfolio_metrics_snapshot`;
- `target_weights`;
- resultados de `monitor_tematico` y `analista_activos`;
- `user_satellite_interest`;
- `monthly_budget` en la peticion o metadata del contexto.

Salida:

- `summary`: propuesta corta de aportacion y, si aplica, de rebalanceo.
- `findings`: recomendaciones estructuradas por activo o bloque.
- `artifacts`: tabla de reparto o recomendacion en markdown, incluyendo escenarios diferenciados.
- `warnings`: carencias de datos, limites no cubiertos o imposibilidad de ejecutar parte de la propuesta.

La decision estructurada (`MonthlyDecision`) mantiene `recommendations` como
salida principal compatible y añade `scenarios`. Cada escenario contiene:

- `name`: `conservador`, `neutral` u `oportunista`.
- `recommended_action` y `budget_to_invest`.
- recomendaciones internas por activo o bloque.
- condiciones de ejecucion y notas de riesgo.

Los escenarios deben usar pesos actuales frente a objetivos, desviaciones,
limites de concentracion y rol `core`/`satellite`/`cash` cuando esos datos
esten disponibles.

El presupuesto se resuelve desde la ejecucion y, si no se proporciona, desde
`portfolio_targets.yaml` o la configuracion general. La construccion directa usa
`StaticContributionLLMProvider`, determinista y offline;
`OpenAIContributionLLMProvider` debe seleccionarse explicitamente. Si falta el
snapshot o alguno de los dos resultados anteriores, la salida queda marcada
como `partial`.

Tras cualquier provider se validan de forma determinista el presupuesto, los
importes no negativos, el limite de gasto y la presencia unica de los tres
escenarios. Una salida incoherente se descarta como `partial`, sin findings ni
artefactos accionables. Un presupuesto explicito de cero se conserva y no puede
producir acciones de compra o rebalanceo.

## Papel en el flujo mensual

Este es el agente decisor del flujo. Debe sintetizar el mandato de la cuenta, el informe mensual, el contexto de mercado y el analisis de activos para responder:

- en que invertir `ingreso_mensual` este mes,
- si conviene no tocar ciertas posiciones,
- si hay que reducir o vender algun activo,
- y si la cartera se esta alejando del objetivo de forma suficiente como para justificar rebalanceo.

La propuesta siempre requiere revision manual. El agente no ejecuta operaciones
ni modifica datos de cartera.
