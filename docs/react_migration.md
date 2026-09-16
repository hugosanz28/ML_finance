# Migracion local a React/FastAPI (#58)

React es la interfaz mantenida. Streamlit, Altair, sus vistas, launchers y
tests exclusivos se retiran. No se borran CSV, DuckDB, Parquet, informes,
auditorias ni configuracion privada. El titular confirmo que solo usaba
Streamlit localmente y que no hay consumidores externos de su entrypoint.

## Arranque y recuperacion

Sigue el [quickstart](../README.md) para la demo y el
[frontend](../frontend/README.md) para datos reales. Ahora hay dos procesos:
FastAPI en 8000 y React en 5173, ambos solo loopback. Sin `--operations` la
API es de lectura. Deten cualquier proceso antiguo antes de escribir; no
mezcles CLI y worker sobre el mismo entorno.

La configuracion `.env` sigue en el servidor. No copies secretos a Vite.
No hay migracion del almacen financiero ni instalacion automatica de paquetes
en tu entorno existente: reinstalar el proyecto actualiza sus requisitos, pero
no desinstala paquetes antiguos. Una venv nueva permite comprobar la ausencia
de dependencias legacy sin tocar la venv del usuario.

La retirada va en una rama/PR aislado. Para deshacerla, revierte el commit de
la retirada o prueba el commit padre en otro checkout, con su propio entorno
virtual. No ejecutes ambas interfaces escribiendo la misma cartera.

## Paridad y diferencias intencionales

La [matriz operativa](react_operations.md) cubre los flujos principales.
La equivalencia es funcional, no una copia de cada widget:

| Antes | Ahora y motivo |
| --- | --- |
| Evolucion por activo, top N y checkbox de caja | Riesgo y activos → detalle avanzado → selector por ID (incluye caja y activos historicos). Precio de valoracion por unidad, no total return; no mezcla activos de igual nombre ni une huecos. Base en la primera valoracion valida desde la primera compra, aunque se filtre el periodo. |
| Valor, pesos, PnL, cobertura y drawdown | Resumen, posiciones y analitica. PnL no realizado se identifica como tal; no se presenta valor menos aportaciones menos PnL abierto como beneficio realizado contable. Drawdown de rentabilidad ajustada por flujos sustituye al drawdown de patrimonio. |
| Marcadores de compras sobre patrimonio | Valor historico y flujos externos se consultan por separado en Resumen/Rentabilidad; el informe mensual conserva el detalle operativo. No se representan compras como aportaciones externas. |
| Actualizar a hoy, FX y precios separados | Operaciones → Datos: fecha final vacia significa hoy; selector ambos/solo FX/solo precios y opcion de inferir FX para filas sin importe base. |
| Aviso de snapshot pendiente | El estado de cartera conserva `pending_portfolio_import`. Solo detecta snapshots mas recientes por nombre/fecha, no certifica que todos los CSV esten importados. Subir e importar siguen siendo pasos separados. |
| Flujo basico de un click | Importar → refresh → informe, con confirmacion y resultado de cada job. No se continua automaticamente tras un fallo. |
| Informe y controles de agentes | Selector de informe por ID contenido en el directorio autorizado; presupuesto, interes, pesos JSON decimales y brief solo para ese run. Sin overrides se usan configuraciones guardadas. Los cambios temporales no las sobrescriben. |
| Informe/snapshot financiero editable | Se selecciona un informe generado y las metricas se construyen en servidor. No se admite inventar cartera o editar el informe desde el formulario de agentes; generar otro informe si los datos cambian. Preflight mantiene el bloqueo por fechas y calidad. |
| Desactivar pesos | No se ofrece un interruptor ambiguo: el pipeline anterior podia recuperar los objetivos guardados cuando el override estaba vacio. Ahora `null` usa los guardados y un override debe ser explicito, no vacio y sumar 1. |
| Rutas privadas, counts de DuckDB, limpiar cache | No se exponen rutas ni diagnosticos de almacenamiento en la UI. Actualizar lecturas sustituye a limpiar cache; los casos de uso de diagnostico permanecen compartidos. |
| Monitor aislado | Sigue por CLI (`run_monitor_tematico.py`); la interfaz mensual ejecuta los tres agentes. No se crea un nuevo endpoint para una herramienta de desarrollo. |
| Auditoria y Markdown | Historial, prompts, fuentes, hashes y outputs desplegables; texto inerte, sin ejecutar HTML ni descargar recursos remotos. |

## Evidencias de regresion

- `tests/test_asset_history.py`: primera compra, cantidades, nulos/huecos,
  fechas futuras y activos con igual nombre.
- `tests/test_uploads.py`: nombres canonicos, fechas y guardado compartido.
- `tests/test_react_parity.py`: contratos y traspaso de controles a casos de uso.
- Tests API/jobs: contencion de rutas, lecturas sin escrituras, defaults
  offline, conflictos, errores, deduplicacion y preflight.
- Tests React y E2E mensual: confirmacion, errores, flujo completo sobre
  copia sintetica y red externa bloqueada; `npm run showcase` verifica la demo.
- Build y comprobacion del wheel: sin modulos ni requisitos Streamlit/Altair.

El showcase versionado se conserva: sus pantallas Resumen, Rentabilidad y
Aportaciones siguen representando la aplicacion con datos sinteticos. Las
nuevas opciones avanzadas no requieren publicar capturas adicionales.
