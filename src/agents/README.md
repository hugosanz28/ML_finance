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

## Inputs comunes recomendados

Ademas del contrato tecnico de P5-01, conviene estandarizar estos `input_refs`:

- `investment_brief`: texto editable con el objetivo de la cuenta, horizonte, filosofia `core + satellites` y restricciones o preferencias actuales.
- `latest_monthly_report`: informe mensual mas reciente con historial y asignacion.
- `portfolio_metrics_snapshot`: metricas agregadas y pesos actuales.
- `watchlist_candidates`: candidatos observados o universo invertible.
- `user_satellite_interest`: idea opcional propuesta por el usuario para evaluar ese mes.

## Objetivo de P5-01

Antes de implementar logica especifica por agente, el proyecto define una interfaz base comun para:

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

## Agentes previstos

- `monitor_tematico`
- `analista_activos`
- `asistente_aportacion_mensual`

## Encaje actual de los agentes

Con el objetivo actual del repo, los tres agentes previstos siguen siendo correctos:

- `monitor_tematico`: aporta contexto y disparadores, no decide por si solo la asignacion.
- `analista_activos`: juzga si posiciones actuales y candidatas encajan con el mandato de la cuenta.
- `asistente_aportacion_mensual`: sintetiza todo y emite la recomendacion final de compra, venta o rebalanceo.

No veo necesario crear un cuarto agente ahora mismo. La pieza que faltaba no era otro agente, sino documentar mejor el mandato de la cuenta y usarlo como input comun de todos.
