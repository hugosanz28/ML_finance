# Arquitectura v2: FastAPI + Angular

## Objetivo

Esta nota define una direccion tecnica para una futura v2 sin iniciar la
migracion de forma prematura. La v1 debe seguir priorizando una herramienta
local, reproducible y facil de iterar con Streamlit.

La propuesta v2 solo deberia activarse cuando la base actual sea suficientemente
estable y haya una necesidad clara de separar frontend, API y procesos en
segundo plano.

## Estado actual: v1 local con Streamlit

La v1 usa Streamlit como interfaz principal porque encaja con el momento del
proyecto:

- permite validar flujos de cartera, informes y agentes rapidamente;
- evita mantener una API y un frontend antes de que el dominio este cerrado;
- funciona bien en local con datos privados;
- reduce superficie de despliegue, autenticacion y operaciones;
- y reutiliza directamente la capa `src/application/`.

Streamlit no debe verse como deuda tecnica por si mismo. Es la interfaz adecuada
para consolidar el producto minimo local antes de convertirlo en una aplicacion
cliente-servidor.

## Capa de preparacion: `src/application/`

La preparacion real para v2 ya existe en `src/application/`. Esta capa debe ser
el contrato estable entre interfaces y dominio.

Responsabilidades:

- exponer casos de uso orientados a acciones de usuario;
- aceptar entradas estructuradas mediante dataclasses `*Request`;
- devolver resultados estructurados; las acciones operativas incluyen
  `ApplicationResult` y los read models destinados a API exponen primitivas
  serializables;
- coordinar `src/degiro_exports/`, `src/market_data/`, `src/portfolio/`,
  `src/reports/` y `src/agents/`;
- evitar logica de Streamlit, HTTP, Angular o CLI.

Regla para v2: FastAPI no deberia llamar directamente a modulos profundos del
dominio salvo que antes exista un caso de uso en `src/application/`.

Los contratos HTTP previstos estan documentados en `docs/api_contracts.md`.
Ese documento es deliberadamente previo a FastAPI: define que datos necesita la
UI y que casos de uso deben existir antes de crear endpoints reales.

La lectura principal ya dispone de `GetPortfolioStateUseCase`, que no devuelve
DataFrames ni paths. Uploads, requisitos FX, el monitor aislado y la escritura
del brief tambien tienen fronteras dedicadas: `SaveDegiroUploadsUseCase`,
`InferFxRequirementsUseCase`, `RunMonitorTematicoUseCase` y
`UpdateInvestmentBriefUseCase`. Los casos de uso completos de lectura/escritura
de targets siguen pendientes y no deben implementarse dentro de futuros
endpoints.

## Propuesta principal

```text
Angular desktop/web UI
        |
        v
FastAPI local API
        |
        v
src/application/
        |
        v
Dominio Python: DEGIRO, market data, portfolio, reports, agents
        |
        v
DuckDB / Parquet / Markdown / artefactos locales
```

### Angular

Angular seria la opcion para una interfaz mas completa cuando Streamlit se quede
corto en navegacion, estado de UI, formularios, tablas interactivas o empaquetado
como aplicacion de escritorio.

Responsabilidades previstas:

- vistas de cartera, evolucion, informes y agentes;
- formularios para configuracion local y objetivos de cartera;
- revision de artefactos generados;
- ejecucion manual de acciones;
- experiencia demo sin exponer datos reales.

Angular no deberia contener logica financiera ni reglas de recomendacion.

### FastAPI

FastAPI seria la API local que traduce HTTP a casos de uso Python.

Responsabilidades previstas:

- endpoints para importar exportaciones DEGIRO;
- endpoints para refrescar FX y precios;
- endpoints para generar informes;
- endpoints para ejecutar agentes;
- endpoints de lectura para cartera, metricas, artefactos y health checks;
- validacion de entrada/salida con esquemas tipados.

FastAPI debe ser una capa fina. Si un endpoint empieza a contener reglas de
negocio, esa logica debe bajar a `src/application/` o al dominio correspondiente.

## Workers y jobs

Algunos flujos no encajan bien en una peticion HTTP larga:

- refresco de market data;
- generacion de informes;
- ejecucion del pipeline de agentes;
- quality checks extensos;
- importaciones grandes.

En v2 esos flujos podrian ejecutarse como jobs locales con estado persistido:

```text
Angular -> FastAPI -> job queue local -> Python worker -> artefactos locales
```

Primera opcion simple:

- tabla local de jobs en DuckDB;
- worker Python lanzado por proceso local;
- polling desde Angular para ver estado;
- logs y artefactos guardados en rutas ya configuradas.

Solo tendria sentido introducir Redis, Celery u otra cola si el flujo local
simple se queda corto.

## Autenticacion local

La v2 no necesita autenticacion compleja si se ejecuta solo en localhost para uso
personal. Aun asi, debe contemplar controles basicos:

- bind por defecto a `127.0.0.1`;
- confirmaciones explicitas para acciones que escriben datos;
- separacion clara entre entorno demo y entorno real;
- proteccion frente a publicar accidentalmente rutas privadas.

El entorno demo usa market data `synthetic` ya sembrado y agentes offline
`static/null` (o `static/static` para busqueda sintetica). Una API no debe
activar `yfinance`, OpenAI, Tavily o DuckDuckGo de forma implicita.

Si en el futuro se expone fuera del equipo local, la autenticacion deja de ser
opcional y habria que replantear permisos, secretos, sesiones y cifrado.

## Almacenamiento futuro

La v1 puede seguir con DuckDB, Parquet y Markdown. Para v2 hay dos caminos:

- mantener DuckDB como base local principal;
- anadir Postgres si aparece necesidad real de concurrencia, usuarios, API
  multi-proceso o despliegue persistente.

Postgres no deberia introducirse solo por hacer la arquitectura mas "web". Para
uso local individual, DuckDB sigue siendo suficiente mientras no haya problemas
concretos de concurrencia o integridad.

## Alternativa: NestJS gateway + Python workers

Una alternativa seria usar Node/NestJS como gateway y mantener Python como capa
de calculo mediante workers o procesos.

```text
Angular -> NestJS gateway -> Python worker/API -> src/application/ -> dominio
```

Ventajas potenciales:

- ecosistema TypeScript extremo a extremo para frontend y gateway;
- estructura fuerte para auth, websockets, jobs y modulos;
- buen encaje si el proyecto se convierte en producto web amplio.

Costes:

- dos runtimes principales;
- frontera adicional entre Node y Python;
- mas complejidad de empaquetado local;
- mas superficie de mantenimiento para un proyecto personal.

Decision actual: no priorizar NestJS. FastAPI es el camino natural si el dominio
principal sigue viviendo en Python.

## Criterios para migrar

No migrar a v2 solo por preferencia tecnologica. Migrar cuando se cumplan varias
de estas condiciones:

- Streamlit limita claramente la UX o el empaquetado;
- la capa `src/application/` cubre los flujos principales;
- los contratos de datos y agentes estan estabilizados;
- la demo publica funciona sin datos reales;
- hay tests suficientes para evitar regresiones durante la migracion;
- se necesita una interfaz mas rica para mostrar el proyecto;
- los jobs largos requieren estado, progreso y reintentos mejor modelados.

## Prerrequisitos antes de empezar

Antes de crear carpetas `frontend/`, `api/` o similares:

1. Mantener v1 estable y demostrable.
2. Completar la separacion publico/privado.
3. Mantener `src/application/` como unica entrada de interfaces y completar
   los casos de escritura que siguen pendientes.
4. Mantener actualizados los contratos de lectura/escritura de
   `docs/api_contracts.md`.
5. Decidir si v2 sera solo local, app de escritorio o servicio web.

## Decision actual

La decision actual es mantener Streamlit en v1 y no implementar Angular, Node ni
FastAPI todavia.

La inversion correcta ahora es fortalecer dominio, tests, demo y documentacion.
La arquitectura v2 queda definida como direccion para evitar decisiones
improvisadas cuando el proyecto necesite una interfaz mas potente.
