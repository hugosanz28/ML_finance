# Prompts versionados

Este directorio contiene los prompts de sistema usados por proveedores LLM
reales. Los proveedores deterministas de tests no dependen de estos archivos.

Convenciones:

- cada prompt tiene un nombre estable terminado en version, por ejemplo
  `monitor_tematico_query_v1.md`;
- los cambios de comportamiento deben crear una nueva version o quedar
  justificados en el diff;
- el codigo carga prompts mediante `src.agents.prompts.registry`.

Prompts actuales:

- `monitor_tematico_query_v1.md`: generacion de queries de busqueda.
- `monitor_tematico_synthesis_v1.md`: sintesis de resultados de busqueda.
- `analista_activos_analysis_v1.md`: analisis por activo.
- `asistente_aportacion_mensual_decision_v1.md`: decision mensual.
