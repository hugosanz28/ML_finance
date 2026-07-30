# Prompts versionados

Este directorio contiene los prompts de sistema usados por proveedores LLM
reales. Los proveedores deterministas de tests y demo no dependen de estos
archivos.

Convenciones:

- cada prompt tiene un nombre estable terminado en version, por ejemplo
  `monitor_tematico_query_v1.md`;
- los cambios de comportamiento deben crear una nueva version o quedar
  justificados en el diff;
- el codigo carga prompts mediante `src.agents.prompts.registry`;
- los prompts consumen el `investment_brief` incluido en cada payload y no
  fijan objetivos, horizontes ni perfiles de riesgo personales.

Prompts actuales:

- `monitor_tematico_query_v1.md`: generacion de queries de busqueda.
- `monitor_tematico_synthesis_v1.md`: sintesis de resultados de busqueda.
- `analista_activos_analysis_v1.md`: analisis por activo.
- `asistente_aportacion_mensual_decision_v1.md`: decision mensual.

En los runs persistidos, `prompt_refs.json` registra claves/versiones y
`prompt_rendered.md` conserva el texto cargado. Ambos forman parte del
`input_hash` semantico del agente: cambiar una version o el texto renderizado
debe cambiar la huella aunque el resto de inputs sea igual.

`raw_response.json` es un artefacto distinto del prompt. En el schema de
auditoria v2 usa `captured`, `partial` o `not_captured` segun lo que exponga el
contrato del provider, y conserva un `reason_code` estable cuando la captura no
es completa. Los providers deterministas normalmente usan `not_captured`
porque no existe una respuesta bruta de SDK separada de su salida de dominio.

Los prompts renderizados y respuestas raw pueden contener contexto privado de
la cartera. Permanecen en el directorio local ignorado por Git y no deben
publicarse salvo que procedan de la demo sintetica y se hayan revisado.
