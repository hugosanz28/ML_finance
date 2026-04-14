# Roadmap

## Objetivo del proyecto

Construir un sistema local y reproducible para seguir una cartera personal, partiendo de exportaciones oficiales de DEGIRO y enriqueciendo la información con datos de mercado, análisis y agentes de apoyo a la decisión.

## Cómo organizarlo en GitHub

La recomendación para GitHub es:

- usar `GitHub Issues` como unidad de trabajo,
- usar `GitHub Projects` con columnas `Backlog`, `Ready`, `Doing`, `Review`, `Done`,
- y etiquetar las tareas con un esquema simple y consistente.

Etiquetas recomendadas:

- `type:task`
- `type:feature`
- `type:bug`
- `type:docs`
- `area:data-model`
- `area:degiro`
- `area:market-data`
- `area:portfolio`
- `area:reports`
- `area:agents`
- `area:streamlit`
- `priority:p0`
- `priority:p1`
- `priority:p2`

## Fase 0: Base del proyecto

Estado: en marcha

- [x] Reestructurar el repositorio
- [x] Mover notebooks antiguos a `notebooks/old/`
- [x] Documentar roadmap, arquitectura y decisiones iniciales
- [x] Dejar lista la separación entre datos públicos y privados
- [x] Crear los primeros issues en GitHub a partir del backlog de este documento

## Fase 1: Modelo de datos local

Objetivo: definir la base sobre la que vivirá toda la lógica del proyecto.

- [ ] Elegir y documentar el formato principal de almacenamiento local
- [ ] Diseñar entidades mínimas: activos, transacciones, movimientos de efectivo, snapshots de cartera, precios y divisas
- [ ] Decidir la estrategia `raw -> normalized -> curated`
- [ ] Implementar un esquema inicial en DuckDB
- [ ] Añadir tests básicos de lectura y escritura

Issues sugeridos:

- `#1` `P1-01` Definir esquema inicial de DuckDB
- `#2` `P1-02` Crear utilidades de configuración y rutas
- `#12` `P1-03` Añadir fixtures sintéticos para tests

## Fase 2: Importador DEGIRO

Objetivo: convertir exportaciones oficiales en datos normalizados.

- [ ] Documentar formatos esperados de exportación
- [ ] Implementar parser de transacciones
- [ ] Implementar parser de estado de cuenta
- [ ] Implementar parser de cartera o snapshot exportado
- [ ] Guardar trazabilidad entre archivo origen y tablas normalizadas
- [ ] Validar con ejemplos saneados y con exportaciones reales locales

Issues sugeridos:

- `#3` `P2-01` Definir contrato de entrada para exportaciones DEGIRO
- `#4` `P2-02` Implementar parser de transacciones
- `#5` `P2-03` Implementar parser de movimientos de efectivo
- `#6` `P2-04` Implementar parser de snapshot de cartera

## Fase 3: Market data e histórico

Objetivo: poder valorar la cartera día a día y reconstruir su evolución.

- [ ] Definir proveedor inicial de precios
- [ ] Descargar precios diarios para activos en cartera
- [ ] Descargar tipos de cambio si son necesarios
- [x] Reconstruir posiciones por fecha a partir de transacciones
- [ ] Generar snapshots diarios o semanales reproducibles
- [ ] Calcular métricas básicas: valor, rentabilidad, asignación y drawdown

Issues sugeridos:

- `#13` `P3-01` Crear módulo de descarga de precios
- `#14` `P3-02` Crear histórico de posiciones por fecha
- `#15` `P3-03` Crear cálculo de métricas agregadas de cartera

## Fase 4: Informes automáticos

Objetivo: producir una actualización periódica útil para revisión personal.

- [ ] Diseñar plantilla de informe semanal
- [ ] Diseñar plantilla de informe mensual
- [ ] Generar salidas en Markdown
- [ ] Guardar histórico de informes generados
- [ ] Preparar ejecución manual y futura automatización

Issues sugeridos:

- `#7` `P4-01` Generar informe semanal en Markdown
- `#16` `P4-02` Generar informe mensual con asignación y cambios
- `#17` `P4-03` Guardar histórico de informes y metadatos

## Fase 5: Agentes

Objetivo: encapsular los informes y enriquecerlos con contexto útil.

- [ ] `monitor_tematico`: noticias y eventos sobre activos, sectores y geografías relevantes
- [ ] `analista_activos`: análisis de empresas y de ETFs por índice, holdings, sectores y proveedor
- [ ] `asistente_aportacion_mensual`: propuesta de aportación según presupuesto, pesos objetivo y desviaciones
- [ ] Definir contratos de entrada y salida para cada agente
- [ ] Añadir trazabilidad de las fuentes y fechas de actualización

Issues sugeridos:

- `#18` `P5-01` Diseñar interfaz base para agentes
- `#8` `P5-02` Implementar `monitor_tematico`
- `#9` `P5-03` Implementar `analista_activos`
- `#10` `P5-04` Implementar `asistente_aportacion_mensual`

## Fase 6: Demo pública y Streamlit

Objetivo: enseñar el proyecto sin exponer datos privados.

- [ ] Crear dataset sintético o anonimizado
- [ ] Montar dashboard de Streamlit
- [ ] Mostrar cartera ejemplo, evolución histórica y análisis agregados
- [ ] Documentar claramente la diferencia entre datos demo y datos reales locales
- [ ] Pulir README final para portfolio público y LinkedIn

Issues sugeridos:

- `#19` `P6-01` Crear demo pública reproducible
- `#20` `P6-02` Implementar dashboard inicial de Streamlit
- `#11` `P6-03` Mejorar README y material de showcase

## Orden de ejecución recomendado

1. Modelo de datos local
2. Importador DEGIRO
3. Market data e histórico
4. Informes automáticos
5. Agentes
6. Demo pública con Streamlit
