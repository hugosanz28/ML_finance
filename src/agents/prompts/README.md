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
`prompt_rendered.md` conserva el texto cargado. `raw_response.json` permanece
con estado `not_captured` porque los providers devuelven objetos de dominio ya
parseados.
