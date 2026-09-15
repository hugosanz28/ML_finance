# Operaciones React (#56)

La UI mantiene la analitica de #55 y anade una zona **Operaciones**, con pasos
manuales. La API ejecuta casos de uso existentes: no hay calculos financieros
nuevos en el navegador ni ejecucion de ordenes. Streamlit no se retira aqui.

## Activacion y entorno

Arranque: [frontend](../frontend/README.md). Por defecto la API es de lectura;
para acciones requiere `--operations demo|real`. `/health` devuelve el modo
operativo y `workspace_mode`; la UI muestra un banner independiente del proveedor
del benchmark. Solo se etiqueta demo cuando el servidor reconoce la configuracion
sintetica, nunca porque una curva sea sintetica. Fallar la verificacion bloquea
las escrituras. Antes de cada envio se vuelve a comprobar el entorno.
El banner describe el entorno, no certifica el contenido de archivos subidos.
No subir CSV reales a demo: ese modo no los anonimiza ni impide que contengan
datos privados. Para capturas usar exclusivamente fixtures sinteticas conocidas.

## Matriz Streamlit → React

| Flujo | React / API | Verificacion |
| --- | --- | --- |
| Subir CSV DEGIRO | Datos, paso 1; limites y nombres validados en servidor | E2E + tests API/uploads |
| Importar archivos | Datos, paso 2; accion separada de subir | E2E |
| FX y precios | Datos, paso 3; synthetic demo, seleccion explicita yfinance real | E2E offline + API |
| Benchmarks reales (#60) | Datos, paso 4; ventana completa y seleccion explicita Yahoo/BCE; no demo | Tests del adapter/cache/API |
| Informe mensual e historial | Informes; generacion, seleccion y lectura de texto Markdown | E2E + IDs actuales/legacy |
| Laboratorio de aportacion | Aportaciones; presupuesto, fracciones, minimo, maximo, fecha | E2E + tests del planificador |
| Investment brief | Configuracion; texto y hash obligatorio | Componentes/conflictos + E2E |
| Portfolio targets | Configuracion; editor avanzado JSON estructurado, no YAML | Componentes + API + E2E |
| Pipeline mensual de agentes | Agentes; preflight obligatorio, defaults static/null | E2E offline + API |
| Auditoria | Agentes; preflight, metadata, contexto/acciones/fuentes, prompts, providers, raw/output y hashes desplegables | E2E schema v2 + componente legacy |
| Estado de operaciones | Ejecuciones; progreso por etapas, avisos, errores y resultado | Componentes + jobs/API |

El monitor aislado y utilidades de diagnostico exclusivas de Streamlit no se
anaden como nuevos endpoints en #56. Antes de #58 se debe decidir su destino;
esta matriz cubre los flujos operativos acordados en #56, no autoriza aun borrar
Streamlit. El editor de targets es avanzado; no se inventan categorias ni mappings.

## Confirmacion, duplicados y conflictos

- Cada accion muestra destino y parametros antes de **Confirmar y ejecutar**.
  Los CSV se convierten a base64 solo en memoria; en la confirmacion se muestran
  nombres, no una copia de sus bytes. El servidor valida limites y parsers.
- Parametros y clave de idempotencia se congelan antes del envio. Doble click
  no crea dos solicitudes. No se guardan cartera, CSV ni claves en localStorage.
- Si se pierde la respuesta de un POST/PUT, la UI conserva cuerpo y clave. El
  usuario puede consultar reenviando **exactamente la misma solicitud**; no se
  repite automaticamente. No recargar la pagina hasta resolver ese envio. Tras
  cerrar la pagina, consultar el historial y revisar efectos antes de ejecutar de nuevo.
- Polling solo lee jobs; un fallo de lectura requiere **Actualizar ejecuciones**.
  `worker_interrupted`/`worker_stopped` no se reejecutan. Solo simulaciones
  fallidas ofrecen retry explicito, con una clave nueva y el estado actual.
- Brief/targets leen hash antes de guardar. Una vez enviados se bloquea otra
  edicion hasta recargar y revisar. `content_conflict` no sobrescribe; el borrador
  permanece visible. La comprobacion final del hash se hace dentro del worker.
- Una operacion activa bloquea nuevos formularios en esa UI. Las otras ventanas
  siguen protegidas por la cola, el bloqueo y la concurrencia del servidor.

## Resultados y privacidad

La simulacion separa compras, pesos calculados en servidor y caja residual. No
incluye comisiones, impuestos ni slippage. Los resultados JSON completos quedan
desplegables, incluidos avisos, restricciones y supuestos.

Informes Markdown se muestran como texto, y auditorias como JSON/texto: no se
ejecuta HTML ni se cargan imagenes o enlaces remotos del contenido. Schema v1
legacy muestra limitaciones; v2 conserva metadata reproducible. Todo resultado
real sigue siendo privado aunque este saneado; ver [privacidad](privacy.md).

## Pruebas

Componentes: confirmacion/cancelacion, solo lectura, cambio de entorno, envio
incierto con clave estable, conflicto de hash, retry limitado, limites CSV y
contenido legacy inerte. Backend: API/jobs y contratos de lectura.

`npm run test:e2e` usa Playwright con API real y workspace nuevo bajo `.test_tmp/`.
Recorre upload → import → refresh offline → informe/lectura → simulacion →
brief/targets → agentes/auditoria → historial tras recargar. Rechaza servidores
ya abiertos y conexiones externas; no usa `.env` privado. CI corre el mismo test.
Trazas/capturas en `frontend/test-results/` estan ignoradas. Un cierre forzado en
Windows puede dejar el directorio sintetico temporal, sin afectar al workspace real.
