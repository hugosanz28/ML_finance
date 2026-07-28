# analista_activos

Objetivo:

Analizar las inversiones existentes y candidatas a la luz del mandato de la cuenta.

Cobertura:

- para acciones: negocio, metricas fundamentales, valoracion, riesgos y cambios relevantes,
- para ETFs: proveedor, indice, holdings principales, sectores, sesgos geograficos y perfil agregado de valoracion o concentracion,
- para cualquier activo candidato: encaje con horizonte, liquidez, volatilidad esperada y rol `core` o `satellite`.

Salidas:

- ficha resumida por activo,
- juicio explicito sobre mantener, vigilar, reducir o incorporar,
- señales de posible sobrevaloracion, sobreextension o perdida de encaje con la
  cuenta,
- warnings cuando faltan metricas o contexto del monitor.

## Encaje con la interfaz base

`analista_activos` implementa `BaseAgent` y devuelve el contrato comun
`AgentResult` de `src/agents/`.

Inputs requeridos:

- `investment_brief`;
- `latest_monthly_report`.

Inputs opcionales:

- `portfolio_metrics_snapshot`;
- `watchlist_candidates`;
- `user_satellite_interest`;
- resultado de `monitor_tematico`;
- activos incluidos en `request.scope`.

`request.parameters.max_assets` limita la cobertura y vale `12` por defecto. El
agente construye un universo deduplicado y prioriza senales del monitor,
posiciones con mas peso y candidatos de mayor riesgo antes de aplicar ese
limite.

Salida:

- `summary`: lectura ejecutiva de la revision.
- `findings`: fichas o cambios por activo en formato estructurado.
- `artifacts`: tablas o markdown derivado si aplica.
- `sources`: fuentes internas y externas utilizadas con fecha.

La construccion directa usa `StaticAssetLLMProvider`, por lo que es
determinista y offline. `OpenAIAssetLLMProvider` debe seleccionarse
explicitamente. Este agente no busca fundamentales ni cotizaciones por su
cuenta: evalua solo los inputs y hallazgos trazables que recibe. Sin universo de
activos devuelve `failed`; sin metricas, contexto del monitor o salida
estructurada suficiente puede devolver `partial`.

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

- este ETF o esta accion encajan con el objetivo y horizonte descritos en `investment_brief`,
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

Ejemplos conceptuales sinteticos:

- si el usuario propone un ETF global grande, `analista_activos` evalua si encaja como pieza `core`.
- si el usuario propone BTC o una accion concreta, `analista_activos` evalua si solo deberia entrar como `satellite`, con mas cautela o incluso no entrar.
- si una posicion ya pesa demasiado para el objetivo de la cuenta, `analista_activos` puede concluir que tiene sentido reducir o no seguir aportando ahi.

Resumido en una frase: `analista_activos` no dice "compra 300 EUR de esto", sino "este activo encaja / no encaja / encaja solo como satelite / conviene reducirlo".
