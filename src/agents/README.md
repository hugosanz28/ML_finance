# Agents

Los agentes se apoyan en datos e informes ya calculados. No son la fuente primaria de la cartera.

## Fin funcional del repo

El objetivo practico del proyecto no es solo describir la cartera, sino ayudar a decidir:

- en que invertir cada mes,
- cuando tiene sentido no invertir en un activo concreto,
- y cuando conviene vender, reducir o reequilibrar posiciones.

La decision debe estar alineada con el objetivo de la cuenta.

## Mandato de la cuenta

Los agentes deben trabajar a partir de un brief explicito de la cuenta, no solo desde la foto actual de posiciones.

Brief actual de referencia, editable con el tiempo:

> Es una cuenta de inversión en la que aporto 1.000 € al mes con el objetivo de acumular capital para la entrada de una vivienda en 3–4 años. Dado que el objetivo tiene una fecha relativamente cercana, priorizo la preservación del capital y una volatilidad moderada. El núcleo de la cartera debe estar en activos diversificados y relativamente estables para este horizonte, combinando exposición global de calidad con una parte defensiva/liquidez. Los satélites —temáticos, BTC, acciones individuales e ideas tácticas— deben ser minoritarios y no comprometer el objetivo principal.

Este `investment_brief` debe tratarse como un texto vivo que el usuario puede ir modificando con el tiempo. No hace falta modelarlo ahora como un formulario rigido ni como campos separados.

Este mandato debe condicionar el analisis de riesgo, horizonte, conveniencia de reequilibrio y encaje de nuevas ideas.

## Objetivos estructurados de cartera

Ademas del brief narrativo, la v1 soporta objetivos estructurados en:

```text
src/data/local/portfolio_targets.yaml
```

El contrato incluye:

- `base_currency`
- `monthly_contribution`
- `risk_profile`
- `target_allocation`
- `max_single_asset_weight`
- `max_sector_weight`
- `rebalance_mode`

`src.portfolio.targets.load_portfolio_targets()` valida el archivo y normaliza
pesos escritos como porcentaje (`70`) o decimal (`0.70`). El pipeline mensual
lo expone a los agentes como `target_weights` y usa `monthly_contribution` como
presupuesto mensual por defecto si la ejecucion no recibe uno explicito.

## Flujo objetivo

El flujo objetivo de agentes para una ejecucion mensual es:

1. Partir del `investment_brief` o mandato de la cuenta.
2. Consumir el estado de la cuenta desde el informe mensual con historial y metricas recientes.
3. Añadir, de forma opcional, una `user_satellite_interest` con alguna idea puntual del usuario.
4. Revisar noticias, eventos y cambios de contexto relevantes para posiciones actuales y candidatas.
5. Analizar posiciones actuales y activos candidatos a la luz del mandato de la cuenta.
6. Producir una recomendacion mensual accionable:
   compra, no compra, reduccion, venta o rebalanceo.

El resultado final no debe ser una lista de noticias, sino una propuesta fundada sobre como mover la cartera ese mes.
`asistente_aportacion_mensual` devuelve una recomendacion base y escenarios
`conservador`, `neutral` y `oportunista` para comparar ejecucion prudente,
normal y condicionada a mayor conviccion.

## Inputs comunes

El pipeline mensual usa estos `input_refs` comunes:

- `investment_brief`: texto editable con el objetivo de la cuenta, horizonte, filosofia `core + satellites` y restricciones o preferencias actuales.
- `latest_monthly_report`: informe mensual mas reciente con historial y asignacion.
- `portfolio_metrics_snapshot`: metricas agregadas y pesos actuales.
- `target_weights`: objetivos estructurados procedentes de `portfolio_targets.yaml` o de una entrada manual compatible.
- `watchlist_candidates`: candidatos observados o universo invertible.
- `user_satellite_interest`: idea opcional propuesta por el usuario para evaluar ese mes.

## Contrato base

El proyecto define una interfaz base comun para:

- contexto de entrada,
- peticion del agente,
- formato de salida,
- y trazabilidad minima de fechas y fuentes.

Esto permite que `monitor_tematico`, `analista_activos` y `asistente_aportacion_mensual` compartan contrato sin forzar la misma logica de negocio.

## Contrato comun

### `AgentContext`

Describe desde que foto del sistema se ejecuta un agente.

Campos principales:

- `agent_name`: identificador estable del agente.
- `run_id`: identificador de ejecucion.
- `as_of_date`: fecha de referencia del analisis.
- `generated_at`: timestamp real de ejecucion.
- `base_currency`: moneda base del proyecto.
- `settings`: configuracion resuelta del repo.
- `input_refs`: artefactos de entrada disponibles para el agente.
- `report_history`: informes historicos ya generados y utilizables como fuente.

### `AgentRequest`

Define que se le pide al agente en una ejecucion concreta.

Campos principales:

- `scope`: alcance funcional, por ejemplo activos, watchlist o presupuesto.
- `parameters`: configuracion especifica del agente.
- `constraints`: limites o reglas de usuario.
- `input_refs`: claves de entradas del contexto que la peticion solicita consumir.

### `AgentResult`

Todos los agentes devuelven un resultado estructurado y trazable.

Campos principales:

- `status`: `success`, `partial` o `failed`.
- `summary`: resumen ejecutivo corto.
- `findings`: hallazgos estructurados.
- `artifacts`: salidas materiales generadas.
- `sources`: fuentes usadas por el agente.
- `warnings`: incidencias no bloqueantes.
- `errors`: errores bloqueantes o parciales.
- `metadata`: metadatos libres para futuras integraciones.

## Submodelos de trazabilidad

- `AgentInputRef`: referencia a datasets, informes o artefactos disponibles en contexto.
- `AgentSource`: fuente citada por findings o resumen.
- `AgentArtifact`: salida generada por el agente.
- `AgentFinding`: hallazgo estructurado con categoria, severidad y fuentes.

## Interfaz comun

La interfaz base vive en `src/agents/base.py`:

- `BaseAgent.name`
- `BaseAgent.description`
- `BaseAgent.required_inputs()`
- `BaseAgent.validate_request(request, context)`
- `BaseAgent.execute(request, context)`
- `BaseAgent.run(request, context)`

Reglas:

- el agente valida que el `context.agent_name` coincida con su nombre,
- el contexto debe contener las entradas requeridas por el diseno del agente,
- la peticion solo puede pedir `input_refs` disponibles en el contexto,
- y toda salida debe poder incluir fecha y fuentes.

## Principios de uso

1. Los agentes consumen datos estructurados o informes ya generados.
2. La trazabilidad de fuentes y fechas es parte del contrato, no una nota opcional.
3. Los agentes pueden devolver `partial` cuando falta cobertura pero hay salida util.
4. El mandato de la cuenta pesa mas que una noticia aislada o una moda puntual.
5. La logica especifica de cada agente queda fuera de esta tarea.

## Validacion de fechas

La pipeline mensual no debe mezclar un informe antiguo con metricas actuales. Al
ejecutar `run_monthly_agent_pipeline`, el `as_of_date` del informe Markdown y el
`as_of_date` de `portfolio_metrics_snapshot` deben coincidir. Si el informe se
pasa por path, la fecha se extrae de `as_of_date:` en el frontmatter o del titulo
`Informe mensual ... YYYY-MM-DD`. Una combinacion como informe `2026-05-06` con
snapshot `2026-05-26` se rechaza antes de llamar a los agentes.

## Quality checks previos

La capa `src.portfolio.data_quality` define checks deterministas que pueden
ejecutarse antes de los agentes mediante
`RunAgentQualityChecksUseCase` en `src.application.quality_checks`.

Los checks bloquean cuando faltan metricas, posiciones valoradas, precios, FX o
cuando las fechas de informe, metricas calculadas y `portfolio_metrics_snapshot`
no coinciden. La cobertura de rentabilidad incompleta se trata como warning para
permitir una salida parcial con contexto explicito.

## Identidad de activos

Los agentes no deben depender solo del nombre que llega de DEGIRO, porque algunos
productos aparecen truncados con `...`. Antes de construir el contexto se aplica
`asset_overrides.csv` al `portfolio_metrics_snapshot`: `asset_name` pasa a ser el
nombre normalizado, el nombre original del broker se conserva como
`broker_asset_name`, y se añaden campos operativos como `ticker`,
`exchange_mic` y `trading_currency` cuando existen.

El `latest_monthly_report` que reciben los agentes añade una tabla
`Referencia de activos para agentes` con nombre normalizado, nombre broker, ISIN,
ticker, divisa y tipo. El monitor tematico recibe tambien las posiciones
estructuradas en metadata para generar queries con nombres completos.

## Persistencia compacta

`pipeline_result.json` guarda el resultado final de cada agente, pero no duplica
el contenido completo de informes, snapshots o resultados anteriores dentro de
cada fuente. En `source.metadata` se omiten claves voluminosas como `content`,
`text`, `positions`, `daily`, `agent_result`, `result` y `findings`; si se omiten, aparece
`omitted_metadata_keys`. Los agentes siguen recibiendo los inputs completos en
memoria durante la ejecucion.

## Audit trail por run

Cada ejecucion persistida de la pipeline mensual escribe un directorio privado
en `src/data/local/agents/monthly_pipeline/<run_id>/`. Ademas de
`pipeline_result.json`, se guardan:

- `run_metadata.json`: fecha, moneda base, estados por agente y versiones de prompts.
- `input_payload.json`: referencias de entrada completas usadas en el run.
- `agents/<agent_name>/context.json`: contexto efectivo del agente.
- `agents/<agent_name>/request.json`: request estructurada enviada al agente.
- `agents/<agent_name>/prompt_refs.json`: claves y versiones de prompts.
- `agents/<agent_name>/prompt_rendered.md`: prompt versionado usado en esa ejecucion.
- `agents/<agent_name>/raw_response.json`: placeholder de respuesta raw mientras el contrato LLM no la expone.
- `agents/<agent_name>/parsed_output.json`: salida estructurada parseada.

Estos artefactos viven bajo `src/data/local/`, por tanto son privados y estan
ignorados por Git. Para demos publicas deben usarse datos sinteticos.

## Prompts versionados

Los prompts de sistema de proveedores LLM reales viven en `src/agents/prompts/`
y se cargan mediante `src.agents.prompts.registry`. Cada prompt tiene una clave
estable y version, por ejemplo `monitor_tematico.query` -> `v1`.

Los proveedores `Static*LLMProvider` siguen siendo deterministas para tests y no
dependen de estos archivos. Si se cambia el comportamiento esperado de un agente,
el cambio debe quedar en el diff del prompt versionado o en una nueva version.

## Agentes implementados

- `monitor_tematico`
- `analista_activos`
- `asistente_aportacion_mensual`

## Autonomia acotada

El pipeline mantiene un orden fijo: `monitor_tematico`, `analista_activos` y
`asistente_aportacion_mensual`. La autonomia vive dentro de cada agente, no en
la orquestacion global.

Cada agente debe dejar en `AgentResult.metadata` una traza comun:

- `agent_plan`: pasos internos que siguio para resolver su tarea.
- `allowed_actions`: acciones que tenia permitido considerar.
- `selected_actions`: acciones que uso realmente.
- `skipped_actions`: acciones descartadas y motivo.
- `applied_constraints`: limites aplicados durante la ejecucion.
- `decision_basis`: inputs o senales que sustentan la salida.

Permisos por agente:

- `monitor_tematico` puede elegir temas, queries, busquedas y sintesis, pero no
  decide compras ni importes.
- `analista_activos` puede priorizar activos y omitir los de menor prioridad
  cuando hay limite de cobertura, pero no decide la aportacion mensual.
- `asistente_aportacion_mensual` puede elegir entre comprar, esperar, mantener
  liquidez, rebalancear con aportacion o pedir revision manual, pero no ejecuta
  operaciones.

La salida sigue siendo asesoramiento para revision manual. Ningun agente tiene
permiso para ejecutar ordenes, modificar datos privados o ignorar errores de
calidad bloqueantes.

## Encaje actual de los agentes

Con el objetivo actual del repo, los tres agentes implementados siguen siendo correctos:

- `monitor_tematico`: aporta contexto y disparadores, no decide por si solo la asignacion.
- `analista_activos`: juzga si posiciones actuales y candidatas encajan con el mandato de la cuenta.
- `asistente_aportacion_mensual`: sintetiza todo y emite la recomendacion final de compra, venta o rebalanceo.

No veo necesario crear un cuarto agente ahora mismo. La pieza importante es mantener el mandato de la cuenta y los objetivos estructurados como inputs comunes de todos.
