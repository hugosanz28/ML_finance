# analista_activos

Objetivo:

Analizar las inversiones existentes y candidatas a la luz del mandato de la cuenta.

Cobertura prevista:

- para acciones: negocio, metricas fundamentales, valoracion, riesgos y cambios relevantes,
- para ETFs: proveedor, indice, holdings principales, sectores, sesgos geograficos y perfil agregado de valoracion o concentracion,
- para cualquier activo candidato: encaje con horizonte, liquidez, volatilidad esperada y rol `core` o `satellite`.

Salidas previstas:

- ficha resumida por activo,
- cambios respecto a revisiones anteriores,
- observaciones utiles para seguimiento periodico,
- juicio explicito sobre mantener, vigilar, reducir o incorporar,
- señales de posible sobrevaloracion, sobreextension o perdida de encaje con la cuenta.

## Encaje con la interfaz base

`analista_activos` debera usar el contrato comun de `src/agents/`.

Request esperado:

- `scope`: lista de posiciones actuales y candidatos a revisar.
- `parameters`: profundidad de analisis, metrica, plantilla deseada y tipo de encaje a evaluar.
- `constraints`: exclusiones, limites de cobertura, prioridades y criterio `core/satellite`.
- `input_refs`: `investment_brief`, informes previos y datasets internos disponibles.

Result esperado:

- `summary`: lectura ejecutiva de la revision.
- `findings`: fichas o cambios por activo en formato estructurado.
- `artifacts`: tablas o markdown derivado si aplica.
- `sources`: fuentes internas y externas utilizadas con fecha.

## Papel en el flujo mensual

`analista_activos` es el agente que convierte contexto en criterio de cartera. Debe responder:

- si una posicion actual sigue encajando con el objetivo de la cuenta,
- si una idea nueva del usuario encaja mejor como `satellite`, como `core` o no encaja,
- si algun activo deberia reducirse por riesgo, horizonte o exceso de peso,
- y si una venta o rebalanceo tiene mas sentido que una nueva compra.

Tambien debe ayudar a detectar situaciones como:

- una accion que ha subido mucho y cotiza con valoraciones exigentes,
- un ETF cuya composicion o concentracion lo hace menos adecuado para el `core`,
- o un activo que sigue subiendo pero ya no encaja bien con el objetivo temporal de la cuenta.

## Explicacion simple de su papel

`analista_activos` no decide el reparto final del mes. Su trabajo es hacer el juicio por activo que falta entre:

- las noticias o eventos detectados por `monitor_tematico`,
- y la decision final que luego toma `asistente_aportacion_mensual`.

En la practica, actua como una capa de evaluacion con preguntas como estas:

- este ETF o esta accion encajan con una cuenta cuyo objetivo es pagar la entrada de una vivienda en 3-4 anos,
- esta accion parece apoyada por fundamentales razonables o se ha inflado demasiado respecto a valoracion y riesgo,
- este activo es demasiado volatil para ser parte importante del `core`,
- esta idea nueva del usuario tiene sentido solo como `satellite` pequeno o no tiene sentido ahora,
- esta posicion actual sigue siendo razonable mantenerla,
- o hay motivos para reducirla aunque no haya una noticia concreta negativa.

## Tipos de analisis por activo

Para acciones individuales, el agente debe combinar:

- fundamentales: crecimiento, margenes, deuda, caja, beneficios y calidad del negocio,
- valoracion: multiplos relevantes y comparacion con historico o comparables,
- comportamiento de mercado: subida reciente, volatilidad, drawdown y peso en cartera.

Para ETFs, el analisis cambia a:

- indice y metodologia,
- concentracion en holdings o sectores,
- exposicion geografica,
- encaje como pieza `core` o `satellite`,
- y, cuando sea posible, señales agregadas de valoracion del subyacente.

Para otros activos como BTC o metales, no aplica el mismo marco empresarial, pero si:

- volatilidad,
- papel en cartera,
- tamaño razonable de exposicion,
- y encaje con el horizonte de la cuenta.

Ejemplos:

- si el usuario propone un ETF global grande, `analista_activos` evalua si encaja como pieza `core`.
- si el usuario propone BTC o una accion concreta, `analista_activos` evalua si solo deberia entrar como `satellite`, con mas cautela o incluso no entrar.
- si una posicion ya pesa demasiado para el objetivo de la cuenta, `analista_activos` puede concluir que tiene sentido reducir o no seguir aportando ahi.

Resumido en una frase: `analista_activos` no dice "compra 300 EUR de esto", sino "este activo encaja / no encaja / encaja solo como satelite / conviene reducirlo".
