# Analitica para agentes

La issue #52 conecta los calculos existentes con los agentes mensuales.
**Python calcula; el LLM interpreta**. No hay nuevas operaciones con el broker
ni recomendaciones ejecutadas automaticamente.

## Construccion y reparto

`RunMonthlyAgentsUseCase` ejecuta los checks habituales. Si pasan, llama a
`BuildPortfolioAnalyticsSnapshotUseCase`, que consulta el resumen analitico
del ultimo ano (`last_year`) hasta el cierre validado del run. Usa la seleccion
local de benchmarks, no descarga datos y no inventa una tasa libre de riesgo.
Por ello Sharpe/Sortino pueden estar no disponibles. Los targets tambien se
leen localmente; las desviaciones se calculan en dominio, no en prompts.

`portfolio_analytics_snapshot` tiene `schema_version: 1`, fecha, moneda,
periodo solicitado/efectivo, estado, warnings y conteos de elementos omitidos.
Sus secciones son:

- `portfolio`: TWR/MWR, riesgo agregado, concentracion, comparaciones con
  benchmarks y desviacion firmada de buckets respecto a targets.
- `assets`: metricas por activo y contribucion aproximada al riesgo.
- `correlations`: pares de activos con metadatos de muestra y cobertura.

No incluye series diarias completas, curvas de crecimiento, definiciones
largas, rutas locales ni configuracion de credenciales. Cada metrica conserva
valor nullable, unidad, periodo cuando corresponda, observaciones, cobertura,
estado y `reason_code`. Las metricas comparativas heredan fechas del benchmark.

| Consumidor | Vista recibida |
| --- | --- |
| Monitor tematico | Ningun snapshot analitico; conserva contexto externo e inputs previos |
| Analista de activos | Activos, correlaciones, riesgo, concentracion y comparacion de cartera con benchmarks |
| Asistente mensual | TWR/MWR, riesgo agregado, concentracion, benchmark y desviaciones; sin matriz ni fichas por activo |
| Auditoria del run | Snapshot completo acotado y vistas efectivas de cada agente |

La comparacion con benchmark es de **cartera**, no una atribucion por activo.
Las series `valuation_price_proxy` son parciales: no equivalen a retornos
totales ni a atribucion historica exacta. Un bucket sin clasificacion completa
tiene desviacion `null`, no cero. La desviacion positiva significa sobrepeso.

## Limites y calidad

El snapshot selecciona hasta 12 activos por peso (desempate por ID), 24 pares
de correlacion de mayor magnitud entre ellos, 2 benchmarks (principal primero)
y 48 grupos de concentracion, priorizando dimensiones agregadas. Informa
omisiones y `analytics_snapshot_truncated`; no corta JSON a mitad de texto.
El preflight rechaza payloads de mas de 65 536 bytes serializados.

Bloquean **antes de construir cualquier provider de agentes**:

- schema, tipos JSON o estado de una metrica inconsistentes;
- valores no finitos o fechas/monedas incompatibles;
- coberturas fuera de rango o muestras negativas;
- valores estadisticos publicados con menos de 30 observaciones;
- tamano excesivo o fallo de contrato al construir el snapshot.

La muestra corta correctamente marcada con `value=null`, cobertura inferior
al 80 %, estado parcial o falta de analitica generan warnings y permiten un
run `partial`. La ausencia de analitica no elimina las barreras originales
de precios, FX y valoracion. El pipeline interno vuelve a comprobar el
snapshot, si lo recibe, como segunda barrera. Las llamadas internas legacy
sin snapshot siguen soportadas; las interfaces deben usar el caso de uso.

## Comportamiento y auditoria

Analista y asistente usan prompts v2 que prohiben recalcular o inventar
metricas. Los prompts v1 permanecen en el repo. No se cambian los del monitor.

En modo estatico, los tags incorporan reason codes reproducibles: por ejemplo
drawdown observado/no observado y sobrepeso frente a targets. Esto documenta
lo observado, no convierte una caida en senal automatica de compra/venta.
Si el snapshot esta no disponible o el TWR no es utilizable/cubre menos del
80 %, el asistente devuelve `hold` y escenarios con inversion cero, solicitando
revision manual. El modo estatico sigue siendo una demo, no asesoramiento.

El snapshot forma parte de `input_payload.json`; las vistas efectivas figuran
en `context.json` y en `request.input_refs` de sus dos consumidores. Los hashes
semanticos existentes incorporan estos inputs. El preflight añade
`inputs.analytics_snapshot_hash` sobre JSON canonico; un payload no
serializable no recibe hash. No se persiste el bruto invalido en un bloqueo.

Las fuentes resumidas omiten el snapshot voluminoso: el contenido completo
queda en los inputs de auditoria. Se mantiene schema v2 de auditoria, lectura
legacy sin reescrituras y redaccion de campos sensibles. Los snapshots y hashes
de carteras reales son privados; ver [privacidad](privacy.md).

## Validacion offline

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_agent_analytics.py tests/test_agent_prompts.py tests/test_agent_audit_trail.py
```

Los tests verifican demo en carpeta temporal, payloads LLM con transporte
simulado, preflight bloqueante, cobertura parcial, decisiones estaticas, limites
y hashes. Cambiar una metrica por activo cambia el hash del analista, mientras
el hash del monitor permanece igual. No se usan datos reales ni red.
