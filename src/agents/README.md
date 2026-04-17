# Agents

Los agentes se apoyan en datos e informes ya calculados. No son la fuente primaria de la cartera.

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
4. La logica especifica de cada agente queda fuera de esta tarea.

## Agentes previstos

- `monitor_tematico`
- `analista_activos`
- `asistente_aportacion_mensual`
