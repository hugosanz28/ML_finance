# analista_activos

Objetivo:

Analizar las inversiones existentes y candidatas.

Cobertura prevista:

- para acciones: negocio, metricas, riesgos y cambios relevantes,
- para ETFs: proveedor, indice, holdings principales, sectores y sesgos geograficos.

Salidas previstas:

- ficha resumida por activo,
- cambios respecto a revisiones anteriores,
- observaciones utiles para seguimiento periodico.

## Encaje con la interfaz base

`analista_activos` debera usar el contrato comun de `src/agents/`.

Request esperado:

- `scope`: lista de activos a revisar.
- `parameters`: profundidad de analisis, metrica o plantilla deseada.
- `constraints`: exclusiones, limites de cobertura o prioridades.
- `input_refs`: referencias a informes previos y datasets internos disponibles.

Result esperado:

- `summary`: lectura ejecutiva de la revision.
- `findings`: fichas o cambios por activo en formato estructurado.
- `artifacts`: tablas o markdown derivado si aplica.
- `sources`: fuentes internas y externas utilizadas con fecha.
