# Política de seguridad

## Versiones soportadas

Mientras el proyecto siga en la serie `0.1.x`, las correcciones de seguridad se
aplican a la última versión publicada y a la rama principal.

## Comunicar una vulnerabilidad

No publiques credenciales, datos de cartera ni detalles explotables en un issue.
Usa la opción privada **Report a vulnerability** del repositorio en GitHub. Si
no está disponible, abre un issue sin detalles sensibles solicitando un canal
privado de contacto.

Incluye, cuando sea posible:

- componente y versión afectados;
- impacto y condiciones necesarias;
- pasos mínimos para reproducirlo sin datos reales;
- mitigación conocida.

La recepción se confirmará tan pronto como sea posible. La prioridad y el plazo
de corrección dependerán del impacto y de la reproducibilidad.

## Controles del repositorio

La CI escanea los archivos versionados con `detect-secrets`, audita las
dependencias de ejecución con `pip-audit --strict` y comprueba el artefacto
construido. Los comandos reproducibles están en `CONTRIBUTING.md`.

Estos controles reducen riesgo, pero no prueban que el repositorio esté libre de
secretos o vulnerabilidades. `.secrets.baseline` registra hallazgos revisados:
no es una lista donde se puedan autorizar credenciales reales.

## Credenciales expuestas

Si una clave o token llegó al historial de Git, revócalo inmediatamente en el
proveedor. Borrar el valor del último commit no lo invalida ni lo elimina del
historial. Indica en el reporte qué proveedor y versiones pueden estar
afectados, pero no copies el secreto ni datos financieros. Consulta
`docs/privacy.md` para el checklist de publicación.
