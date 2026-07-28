# Roadmap

## Objetivo

Mantener una aplicacion local, reproducible y auditable para analizar una
cartera a partir de exportaciones oficiales de DEGIRO. El proyecto prioriza
calidad de datos, privacidad y revision humana antes que automatizacion o
ejecucion de operaciones.

El roadmap describe direccion, no fechas comprometidas. Los issues de GitHub
son la unidad de trabajo y deben enlazar los cambios que afecten a este orden.

## Ahora

La serie `v0.1.x` consolida la v1 local con Streamlit. Ya estan disponibles:

- importacion de transacciones, cuenta y snapshots DEGIRO;
- contratos normalizados y persistencia local DuckDB/Parquet;
- refresco de FX y precios con overrides;
- valoracion `broker_snapshot_anchored` y reconstruccion diaria;
- informes mensuales persistidos;
- dashboard Streamlit para cartera, datos, informes y agentes;
- pipeline mensual de tres agentes con prompts versionados y audit trail;
- demo publica reproducible con datos y proveedores sinteticos;
- frontera `src/application/` para operaciones, uploads, auditoria y read models,
  incluido un estado de cartera serializable;
- CI multiplataforma con lint, typecheck gradual, secret scanning, cobertura,
  bootstrap demo offline, auditoria de dependencias y build de wheel.

Las prioridades de mantenimiento son:

- preservar exactitud financiera y validacion de fechas/cobertura;
- mantener Python 3.11 o posterior verificado en la matriz de CI;
- sostener los gates de calidad, cobertura, seguridad y empaquetado;
- mantener una demo offline clara y separada de datos privados;
- documentar limites, proveedores y revision manual de recomendaciones.

## Siguiente

Una vez estable la serie `v0.1.x`, el trabajo previsto es:

- ampliar los read models de `src/application/` solo cuando una interfaz lo
  necesite y reducir transformaciones visuales duplicadas;
- convertir el build verificado de wheel en un proceso de release y documentar
  mejor la instalacion fuera del flujo PowerShell;
- ampliar analitica de riesgo, comparacion con benchmarks y explicacion de
  cobertura sin duplicar calculos financieros;
- reforzar evaluaciones de agentes, captura de respuestas raw cuando el
  contrato lo permita y trazabilidad de fuentes;
- mejorar el showcase. No hay capturas versionadas actualmente; cualquier
  captura futura debe obtenerse exclusivamente de la demo sintetica
  reproducible.

## Más adelante

La evolucion v2 se evaluara solo cuando las necesidades de UX, jobs o
distribucion lo justifiquen:

- API local sobre los casos de uso de `src/application/`;
- interfaz web separada, como FastAPI + Angular;
- ejecuciones programadas y notificaciones con permisos explicitos;
- soporte para varias cuentas o brokers manteniendo procedencia y aislamiento;
- opciones de despliegue que conserven un modo local-first.

La direccion tecnica ampliada vive en `docs/architecture_v2.md`. Streamlit sigue
siendo la interfaz principal hasta que exista una necesidad concreta y probada
de migracion.
