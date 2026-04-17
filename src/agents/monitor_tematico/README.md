# monitor_tematico

Objetivo:

Seguir noticias y eventos sobre activos, sectores y geografias relevantes para la cartera.

Entradas previstas:

- posiciones actuales,
- watchlist,
- sectores y geografias expuestas,
- informe base reciente.

Salidas previstas:

- resumen de eventos relevantes,
- riesgos o catalizadores detectados,
- enlaces y fecha de las fuentes consultadas.

## Encaje con la interfaz base

`monitor_tematico` debera usar el contrato comun de `src/agents/`.

Request esperado:

- `scope`: activos, sectores, geografias y watchlist.
- `parameters`: ventanas temporales, tipos de evento, nivel de detalle.
- `constraints`: limites de fuentes o filtros.
- `input_refs`: referencias a informe base o datasets internos relevantes.

Result esperado:

- `summary`: panorama corto de eventos relevantes.
- `findings`: eventos, riesgos y catalizadores estructurados.
- `sources`: enlaces, informes o datasets consultados con fecha.
- `warnings`: cobertura incompleta o falta de fuentes para algun activo.
