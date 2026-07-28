# Decisions

## ADR-001: Usar exportaciones oficiales de DEGIRO como fuente de verdad

Estado: aceptada

Contexto:

DEGIRO no ofrece una API pública oficial para automatizar el acceso a la cuenta. Basar el proyecto en wrappers no oficiales introduciría fragilidad operativa y riesgo de incompatibilidad con los términos del broker.

Decisión:

El proyecto usará exportaciones oficiales del broker como entrada principal.

Consecuencias:

- El flujo de importación será robusto y trazable.
- La actualización de transacciones no será totalmente automática.
- La valoración diaria sí podrá automatizarse con market data externa.

## ADR-002: Organizar el trabajo en GitHub

Estado: aceptada

Contexto:

El repositorio ya vive en GitHub y se quiere usar tanto como proyecto personal real como escaparate público.

Decisión:

La gestión se apoyará en `GitHub Issues`, `GitHub Projects` y documentación versionada en `docs/`.

Consecuencias:

- Una sola fuente de verdad para código, roadmap y tareas.
- Menor complejidad que introducir una herramienta extra de gestión desde el inicio.

## ADR-003: Mantener datos privados gitignorados y ejemplos públicos versionados

Estado: aceptada

Contexto:

El repositorio debe ser público o enseñable sin filtrar datos personales.

Decisión:

Los datos reales del usuario irán en rutas locales ignoradas por Git. Los datos públicos se representarán con ejemplos saneados o sintéticos.

Consecuencias:

- Se reduce mucho el riesgo de fuga accidental de datos.
- Hay que mantener un pequeño conjunto de ejemplos públicos para demo y tests.

## ADR-004: Empezar con Streamlit como interfaz

Estado: aceptada

Contexto:

Se quiere explorar la información de forma cómoda sin retrasar el núcleo del proyecto por una web más compleja.

Decisión:

La primera interfaz será una app local en Streamlit.

Consecuencias:

- Permite iterar rápido mientras maduran el modelo de datos y los informes.
- Si más adelante se necesita una interfaz más cuidada, se podrá migrar sin rehacer la lógica de dominio.

## ADR-005: Priorizar primero datos e informes, después agentes

Estado: aceptada

Contexto:

Los agentes solo aportan valor si consumen información consistente y trazable.

Decisión:

La secuencia será: datos -> histórico -> informes -> agentes.

Consecuencias:

- Menor riesgo de construir agentes vistosos sobre una base débil.
- La funcionalidad útil aparece antes, aunque la capa “inteligente” llegue después.

## ADR-006: Usar `src/application/` como frontera de interfaces

Estado: aceptada

Contexto:

Streamlit, los scripts y una futura API necesitan reutilizar los mismos flujos
sin acoplarse a parsers, repositorios, DataFrames o detalles de agentes.

Decision:

Toda interfaz debe entrar por casos de uso de `src/application/`. Las acciones
operativas coordinan servicios de dominio y devuelven `ApplicationResult`; los
read models, como `GetPortfolioStateUseCase`, devuelven contratos
serializables. Los calculos financieros, proyecciones broker y consultas
reutilizables permanecen en dominio/repositorios.

Consecuencias:

- Streamlit y los scripts comparten validaciones y defaults.
- FastAPI puede ser un adaptador fino si se implementa mas adelante.
- Cada nuevo flujo de interfaz requiere primero un caso de uso estable.

## ADR-007: Mantener providers offline explicitos para demo y tests

Estado: aceptada

Contexto:

La demo publica y los tests deben ser reproducibles, no depender de red y no
confundir datos sinteticos con hechos de mercado.

Decision:

Market data de demo usa `PRICE_PROVIDER=synthetic`; los agentes usan
`llm_provider=static` y `search_provider=null` por defecto. La combinacion
`static/static` se reserva para fixtures de busqueda sinteticos. `yfinance`,
OpenAI, Tavily y DuckDuckGo requieren seleccion explicita.

Consecuencias:

- Tests y demo funcionan offline.
- El provider `synthetic` conserva datos sembrados y no inventa series nuevas.
- Toda dependencia externa queda visible en configuracion o CLI.
