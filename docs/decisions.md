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
