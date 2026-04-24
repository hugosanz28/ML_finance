# monitor_tematico

## Objetivo

Seguir noticias y eventos sobre activos, sectores y geografias relevantes para la cartera y para ideas candidatas del usuario.

`monitor_tematico` no decide compras, ventas ni pesos. Su funcion es ampliar el informe mensual con contexto externo util y trazable para que `analista_activos` y `asistente_aportacion_mensual` puedan tomar mejores decisiones.

En una ejecucion mensual debe responder:

- que ha cambiado desde el ultimo informe,
- que puede afectar al `core`,
- que puede afectar a los `satellites`,
- que ideas nuevas del usuario merecen pasar a analisis,
- y que riesgos o catalizadores conviene tener presentes este mes.

## Papel en el flujo mensual

El flujo esperado es:

1. El sistema genera o localiza el informe mensual mas reciente.
2. `monitor_tematico` consume el mandato de la cuenta, la foto de cartera y, si existen, candidatos o intereses puntuales del usuario.
3. El agente detecta contexto externo relevante, priorizado por impacto potencial.
4. `analista_activos` usa esos hallazgos para juzgar encaje, riesgo y seguimiento por activo.
5. `asistente_aportacion_mensual` sintetiza informe, contexto y analisis para proponer la decision mensual.

Por tanto, la salida de este agente debe ser una capa intermedia de contexto, no una recomendacion final.

## Entradas previstas

Entradas requeridas:

- `investment_brief`: mandato vivo de la cuenta, horizonte, objetivo de vivienda, filosofia `core + satellites` y restricciones.
- `latest_monthly_report`: informe mensual mas reciente, con asignacion actual, cambios por periodo y notas de cobertura.

Entradas opcionales:

- `watchlist_candidates`: candidatos observados de forma recurrente o universo invertible basico que el usuario quiere mantener en radar.
- `user_satellite_interest`: idea puntual propuesta por el usuario para revisar como posible satelite.

Informacion que debe extraer de esas entradas:

- posiciones actuales y pesos aproximados,
- activos con mayor peso o cambio reciente,
- exposiciones relevantes por sector, geografia, divisa o tipo de activo,
- candidatos que no estan todavia en cartera, si existe `watchlist_candidates`,
- idea puntual del usuario, si existe `user_satellite_interest`,
- y cualquier restriccion explicita del mandato de la cuenta.

`watchlist_candidates` y `user_satellite_interest` no cumplen el mismo papel:

- `watchlist_candidates` es una lista estable de candidatos a vigilar de forma recurrente aunque el usuario no los mencione cada mes.
- `user_satellite_interest` es una idea puntual del mes que debe subir prioridad en esa ejecucion concreta.

Si no existe ninguno de los dos, la v1 debe funcionar igualmente usando solo cartera actual, exposiciones inferibles e `investment_brief`.

## Alcance funcional v1

La primera version debe ser acotada y mensual.

- Ventana temporal principal: desde el ultimo informe mensual hasta `context.as_of_date`.
- Ventana alternativa configurable: 30-45 dias si no hay fecha clara del informe anterior.
- Profundidad: contexto suficiente para priorizar hallazgos, no analisis fundamental completo.
- Volumen esperado: pocos hallazgos de alta senal, normalmente entre 5 y 10.
- Cobertura prioritaria: `core`, posiciones con mayor peso, posiciones con mayor cambio reciente, `satellites` relevantes, candidatos recurrentes si existen e intereses puntuales indicados por el usuario.

No debe intentar cubrir todo el mercado ni todo el universo invertible.

## Que informacion debe observar

El agente debe buscar cambios externos relacionados con:

- activos concretos en cartera,
- ETFs e indices asociados a posiciones `core`,
- sectores o geografias con peso material,
- metales, liquidez, tipos de interes e inflacion cuando afecten al mandato de la cuenta,
- BTC u otros satelites si estan en cartera o en candidatos,
- acciones individuales relevantes para cartera, watchlist o interes del usuario,
- cambios regulatorios, fiscales o de producto que alteren el riesgo o la disponibilidad del activo.

## Estrategia de busqueda y datos externos

La v1 debe priorizar una implementacion propia y controlada antes que depender de servicios de pago.

Orden recomendado de fuentes:

1. Inputs internos del repo: informe mensual, historico de informes, pesos, cambios y notas de cobertura.
2. Datos de mercado disponibles en el proyecto, especialmente precios y series descargadas con `yfinance`.
3. Busqueda web propia, acotada por queries generadas por el LLM desde los inputs y temas observados.
4. Tavily u otro proveedor externo solo como alternativa futura si el proveedor propio resulta insuficiente y el plan gratuito encaja.

La busqueda web propia debe ser simple y reemplazable:

- recibir queries concretas generadas por el LLM para activo, ETF, sector, geografia o tema,
- limitar ventana temporal y numero de resultados,
- evitar scraping masivo,
- guardar fuente, URL, fecha de recuperacion y fecha efectiva si se conoce,
- devolver resultados normalizados para que el agente pueda convertirlos en `AgentFinding`,
- y permitir cache local en `src/data/local/` para mejorar reproducibilidad.

`yfinance` puede usarse para contexto de mercado, por ejemplo:

- movimientos recientes de precio,
- volatilidad o drawdown basico,
- comparacion de comportamiento entre posicion y benchmark simple,
- confirmacion de si una noticia coincide con un movimiento material.

`yfinance` no debe ser la fuente principal de noticias. Si se usa informacion de noticias incluida por el proveedor, debe tratarse como una fuente mas y no como verdad unica.

Para mantener el agente desacoplado, la implementacion deberia exponer una interfaz de proveedor:

```text
SearchProvider:
  search(query, start_date, end_date, max_results) -> SearchResult[]
```

La primera implementacion puede incluir un proveedor propio sencillo y tests con proveedor fake. Tavily, Exa, SerpApi u otros proveedores podrian incorporarse despues sin cambiar la logica principal del agente.

## Papel del LLM

La implementacion debe tratar al LLM como el cerebro semantico del agente.

El LLM tiene dos responsabilidades principales:

- generar queries de busqueda a partir de `investment_brief`, `latest_monthly_report`, `watchlist_candidates` opcional, `user_satellite_interest` opcional y temas observados;
- sintetizar los resultados obtenidos y devolver hallazgos clasificados, priorizados y trazables.

El codigo no debe decidir semanticamente con reglas rigidas salvo como validacion, normalizacion o fallback controlado. La clasificacion `fact`, `risk`, `catalyst`, `macro`, `regulation`, `product_change` o `coverage`, la severidad y los hints para agentes posteriores deben venir del LLM en formato estructurado.

## Tipos de eventos relevantes

Si importan:

- decisiones o cambios de tono de bancos centrales que afecten a renta variable, bonos, liquidez o metales,
- datos macro con implicacion clara para el horizonte de 3-4 anos,
- eventos regulatorios que afecten a BTC, ETFs, sectores o geografias presentes en cartera,
- resultados, guidance, profit warnings o cambios de tesis en acciones individuales candidatas o en cartera,
- cambios relevantes en un ETF: indice, metodologia, comisiones, proveedor, replica, liquidacion o concentracion,
- shocks geopoliticos o de divisa con impacto plausible sobre exposiciones actuales,
- caidas, subidas o drawdowns relevantes solo si vienen acompanados de explicacion o cambio de riesgo,
- catalizadores proximos con posible impacto en el mes o trimestre siguiente.

No importan para v1:

- ruido intradia,
- noticias genericas de mercado sin conexion con cartera o candidatos,
- opiniones sin hecho nuevo,
- predicciones no respaldadas por datos o eventos,
- listas de "mejores acciones para comprar",
- movimientos pequenos de precio sin cambio de contexto,
- analisis profundo de valoracion que corresponde a `analista_activos`,
- recomendaciones directas de compra, venta o asignacion.

## Priorizacion de hallazgos

Los hallazgos deben ordenarse por impacto potencial sobre la cuenta, no por popularidad de la noticia.

Criterios de prioridad:

- impacto sobre el objetivo principal de la cuenta: entrada de vivienda en 3-4 anos,
- materialidad sobre posiciones actuales o exposiciones `core`,
- impacto potencial sobre `satellites` de alto riesgo o alta volatilidad,
- novedad frente al ultimo informe,
- cercania temporal del catalizador,
- posibilidad de afectar aportaciones, no compra, reduccion o rebalanceo,
- calidad y fecha de las fuentes,
- incertidumbre o falta de cobertura.

Regla practica:

- `high`: puede cambiar el juicio de encaje, riesgo o seguimiento este mes.
- `medium`: conviene vigilarlo, pero probablemente no cambia la decision mensual por si solo.
- `low`: contexto util, pero sin efecto claro sobre decisiones inmediatas.
- `info`: informacion de apoyo o cobertura.

## Tratamiento de core y satellites

El mandato de la cuenta da prioridad a preservacion de capital y volatilidad moderada. Por eso, los eventos que afecten al `core` deben evaluarse con especial cuidado.

Para `core`, el agente debe fijarse en:

- deterioro de diversificacion,
- concentracion excesiva,
- cambios macro que afecten a indices amplios,
- cambios de tipos, inflacion o divisa,
- eventos que aumenten volatilidad o drawdown esperado.

Para `satellites`, el agente debe fijarse en:

- catalizadores tacticos,
- riesgo regulatorio,
- cambios de narrativa o tesis,
- eventos binarios,
- volatilidad extrema,
- riesgo de que una posicion pequena deje de encajar con el objetivo de la cuenta.

Una idea nueva del usuario no debe elevarse automaticamente a recomendacion. Solo debe pasar a `analista_activos` con contexto suficiente para evaluarla.

## Encaje con la interfaz base

`monitor_tematico` debera usar el contrato comun de `src/agents/`.

Request esperado:

- `scope`: activos actuales, exposiciones agregadas, watchlist y posibles candidatos satelite.
- `parameters`: ventana temporal, tipos de evento, nivel de detalle, sensibilidad de alertas y limite de hallazgos.
- `constraints`: limites de fuentes, filtros de ruido, preferencia por contexto accionable y separacion `core`/`satellite`.
- `input_refs`: `investment_brief`, `latest_monthly_report` y opcionalmente `watchlist_candidates` y `user_satellite_interest`.

Inputs requeridos por diseno:

- `investment_brief`
- `latest_monthly_report`

Inputs opcionales:

- `watchlist_candidates`
- `user_satellite_interest`

Result esperado:

- `summary`: panorama corto de cambios relevantes desde el ultimo informe.
- `findings`: eventos, riesgos y catalizadores estructurados y priorizados.
- `sources`: enlaces, informes o datasets consultados con fecha.
- `warnings`: cobertura incompleta, fuentes insuficientes o candidatos ambiguos.
- `metadata`: parametros de ventana temporal, cobertura, proveedor de busqueda, universo observado y numero de hallazgos.

## Formato de findings

Cada `AgentFinding` deberia representar un unico hallazgo accionable para agentes posteriores.

Campos recomendados:

- `title`: frase corta con el cambio detectado.
- `detail`: explicacion de que ha pasado y por que importa para la cuenta.
- `category`: `fact`, `risk`, `catalyst`, `macro`, `regulation`, `product_change` o `coverage`.
- `severity`: `high`, `medium`, `low` o `info`.
- `asset_id`: activo afectado si aplica.
- `tags`: etiquetas como `core`, `satellite`, `candidate`, `macro`, `etf`, `btc`, `equity`, `rates`, `inflation`.
- `sources`: fuentes trazables usadas en el hallazgo.

Metadatos recomendados por hallazgo:

- `impact_scope`: `core`, `satellite`, `candidate`, `portfolio` o `mixed`.
- `change_type`: `fact`, `risk` o `catalyst`.
- `time_horizon`: `immediate`, `near_term` o `medium_term`.
- `novelty`: `new`, `ongoing` o `resolved`.
- `affected_exposure`: activo, sector, geografia, divisa o tipo de activo afectado.
- `potential_decision_relevance`: `buy`, `do_not_buy`, `reduce`, `sell`, `rebalance`, `watch` o `analysis_needed`.
- `downstream_hint`: `review_fit`, `watch_weight`, `consider_rebalance`, `candidate_needs_analysis` o `no_action_context`.

Ejemplo conceptual:

```text
Finding:
  title: "Sube el riesgo regulatorio para un satelite cripto"
  category: "regulation"
  severity: "medium"
  tags: ("satellite", "btc", "regulation")
  metadata:
    impact_scope: "satellite"
    change_type: "risk"
    time_horizon: "near_term"
    novelty: "new"
    potential_decision_relevance: "watch"
    downstream_hint: "review_fit"
```

## Formato de summary

El `summary` debe ser breve y orientado a decision mensual.

Debe incluir:

- lectura general del mes,
- si el contexto favorece prudencia, continuidad o revision,
- hallazgos mas importantes,
- diferencia clara entre impactos sobre `core` y `satellites`,
- y menciones a cobertura incompleta si aplica.

No debe incluir una propuesta de reparto ni una orden de compra o venta.

## Fuentes y trazabilidad

Cada hallazgo relevante debe tener al menos una fuente, salvo que derive de un input interno.

Fuentes aceptadas:

- informes internos del repo,
- datasets internos,
- paginas web o comunicados oficiales,
- noticias financieras,
- documentos de proveedores de ETF,
- fuentes manuales indicadas por el usuario.

Cada `AgentSource` debe incluir:

- `source_type`,
- `label`,
- `location`,
- `retrieved_at`,
- `effective_date` cuando se conozca,
- y metadatos utiles como proveedor, activo o tipo de fuente.

Si no hay cobertura suficiente para un activo o candidato, el resultado debe ser `partial` o incluir `warnings`.

## Salida minima para considerar P5-02 implementable

La primera implementacion puede considerarse suficiente si:

- valida los inputs requeridos,
- acepta la ausencia de `watchlist_candidates` sin fallar,
- acepta `user_satellite_interest` de forma opcional,
- construye una lista de temas a observar a partir de cartera, mandato, watchlist opcional e interes puntual opcional,
- usa una interfaz LLM desacoplada para generar queries y sintetizar resultados,
- usa una interfaz de busqueda desacoplada de proveedores concretos,
- genera un `AgentResult` con `summary`, `findings`, `sources`, `warnings` y `metadata`,
- diferencia `core`, `satellite` y `candidate` cuando aplique,
- distingue hechos, riesgos y catalizadores,
- prioriza hallazgos por impacto potencial,
- y no emite recomendaciones directas de asignacion.

## Fuera de alcance de v1

Queda fuera de la primera version:

- scoring cuantitativo avanzado,
- scraping masivo,
- monitorizacion diaria o intradia,
- backtesting de impacto de noticias,
- analisis fundamental completo por empresa,
- calculo de pesos objetivo,
- propuesta de operaciones,
- automatizacion completa de fuentes premium,
- y cobertura exhaustiva de todos los mercados.

## Notas de implementacion

La implementacion deberia apoyarse en `BaseAgent` y devolver siempre `AgentResult`.

Comportamiento esperado:

- `required_inputs()` devuelve `investment_brief` y `latest_monthly_report`.
- `supports()` puede restringir scopes no mensuales si hace falta.
- `run()` debe devolver `partial` si hay salida util pero falta cobertura externa.
- `run()` debe devolver `failed` solo cuando no pueda producir contexto minimo.

## Implementacion actual

La v1 implementada queda dividida en piezas pequenas para que sea facil cambiar fuentes o mejorar la priorizacion sin reescribir el agente entero.

Archivos principales:

- `agent.py`: contiene `MonitorTematicoAgent`, resuelve la ventana temporal, pide queries al LLM, ejecuta busquedas, pide sintesis al LLM y devuelve `AgentResult`.
- `topic_builder.py`: construye el universo observado a partir del informe mensual, watchlist opcional, interes puntual opcional, `request.scope` y temas macro basicos.
- `_types.py`: define tipos internos ligeros (`ObservedTopic` y `SearchResult`) que no sustituyen al contrato comun de agentes.
- `providers.py`: define la interfaz `SearchProvider` y proveedores concretos para no acoplar el agente a una API externa.
- `llm.py`: define la interfaz `ThemeLLMProvider`, el proveedor real `OpenAIThemeLLMProvider` y el proveedor fake `StaticThemeLLMProvider` para tests.
- `__init__.py`: exporta la API publica del modulo `monitor_tematico`.

Flujo actual de ejecucion:

1. `BaseAgent.execute()` valida que existan `investment_brief` y `latest_monthly_report`.
2. `MonitorTematicoAgent.run()` calcula la ventana de analisis.
3. `build_observed_topics()` crea una lista deduplicada de temas a vigilar.
4. El agente pide al `ThemeLLMProvider` que genere queries de busqueda.
5. El agente llama al `SearchProvider` configurado para cada query.
6. Los resultados web se convierten en fuentes (`AgentSource`).
7. El agente pide al `ThemeLLMProvider` que sintetice resultados y devuelva findings estructurados.
8. El resultado final incluye `summary`, `findings`, `sources`, `warnings` y `metadata` con ventana, proveedor LLM, proveedor de busqueda, queries y universo observado.

Proveedor LLM incluido:

- `OpenAIThemeLLMProvider`: usa la API de OpenAI con Structured Outputs para obtener JSON estructurado en la generacion de queries y en la sintesis.
- `StaticThemeLLMProvider`: proveedor determinista para tests y fixtures locales.

Configuracion OpenAI:

- `OPENAI_API_KEY`: clave de API, leida desde variables de entorno o `.env`.
- `OPENAI_MODEL`: modelo a usar; por defecto `gpt-4.1-mini`, configurable en `.env`.

Runner manual disponible:

- `scripts/run_monitor_tematico.py`: permite ejecutar el agente sin instanciarlo a mano desde Python.
- `--dry-run`: resuelve inputs y temas observados sin llamar al LLM ni a la web.
- `--llm-provider openai|static`: permite dejar montada la tuberia sin llamadas reales mientras `static` no genera queries.
- `--search-provider duckduckgo|null`: permite activar o desactivar la busqueda web.
- cache local por defecto en `src/data/local/agents/monitor_tematico/search_cache/`, salvo que se pase `--disable-cache`.

Proveedores incluidos:

- `NullSearchProvider`: no devuelve resultados; sirve para ejecutar el agente sin internet y comprobar comportamiento `partial`.
- `StaticSearchProvider`: devuelve resultados fijos; se usa en tests y puede servir para fixtures manuales.
- `DuckDuckGoHtmlSearchProvider`: proveedor propio basico basado en la pagina HTML de DuckDuckGo, sin servicios de pago ni librerias nuevas.
- `CachedSearchProvider`: wrapper opcional para persistir resultados web en disco y reutilizarlos en ejecuciones posteriores.

Limitaciones actuales:

- La calidad de queries y findings depende del modelo LLM configurado.
- La busqueda web propia es best-effort y puede fallar si cambia el HTML del buscador.
- El cache local evita repetir busquedas, pero todavia no implementa invalidacion por antiguedad ni limpieza automatica.
- Todavia no se usa `yfinance` dentro del agente; queda como siguiente mejora para complementar noticias con movimiento de precio, volatilidad o drawdown.
- Tavily queda como alternativa futura si el proveedor propio resulta insuficiente y su plan gratuito encaja.

Tests cubiertos:

- el agente solo exige `investment_brief` y `latest_monthly_report`,
- la ausencia de `watchlist_candidates` no falla,
- `watchlist_candidates` y `user_satellite_interest` se incorporan si existen,
- un proveedor LLM fake genera queries y findings priorizados con fuentes,
- y el contrato base de agentes sigue pasando.
