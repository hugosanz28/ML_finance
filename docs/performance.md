# Rendimiento de cartera: TWR y MWR/XIRR

## Objetivo

La base de analitica de rendimiento vive en `src/portfolio/performance.py` y
sus contratos en `src/portfolio/performance_models.py`. Separa el rendimiento
de mercado del efecto de aportar o retirar capital y devuelve resultados
auditables, sin sustituir datos ausentes por ceros.

Esta capa es dominio puro y offline. Todavia no se muestra en Streamlit ni se
expone mediante FastAPI o agentes; esas integraciones deben entrar por un caso
de uso de `src/application/`.

## Entradas y clasificacion de flujos

El calculo combina:

- valoraciones de `portfolio_daily_metrics`, en una unica moneda base;
- movimientos normalizados de `cash_movements`;
- una fecha `as_of_date` que debe tener una valoracion exacta.

Solo son capital externo:

| Tipo | Signo para la cartera |
| --- | --- |
| `DEPOSIT` | positivo |
| `WITHDRAWAL` | negativo |

Dividendos, retenciones, intereses, comisiones, impuestos, liquidaciones de
compraventa, conversiones FX, transferencias con la cuenta de efectivo y
acciones corporativas son movimientos internos. Afectan al resultado de la
cartera, pero no representan nuevo capital del inversor.

La fecha efectiva es `value_date` y, si falta, `movement_date`. Un tipo
desconocido, un duplicado o un deposito/retirada sin fecha, importe convertido
o moneda base coherente se excluye y genera un `reason_code`; no se presupone
que sea externo.

## Retorno diario y TWR

Para dos valoraciones consecutivas se calcula:

```text
retorno_t = (valor_cierre_t - flujo_externo_t) / valor_apertura_t - 1
TWR = producto(1 + retorno_t) - 1
```

Cada intervalo queda disponible como `DailyPerformanceObservation`, con valor
de apertura y cierre, flujo externo, cobertura, estado y motivo. El TWR enlaza
esos retornos, por lo que una aportacion grande no aparece como una ganancia.

La exportacion de DEGIRO no aporta una valoracion inmediatamente anterior a
cada movimiento ni su instante economico intradia. Por ello el calculo usa la
fecha efectiva y trata el flujo como ocurrido al cierre del intervalo. Es una
aproximacion determinista adecuada para la serie diaria disponible, pero no un
TWR intradia exacto.

## MWR/XIRR

El MWR mide la experiencia real del inversor y pondera el momento de sus
aportaciones. Se resuelve como XIRR sobre flujos fechados desde su perspectiva:

- valor inicial: salida negativa;
- deposito en cartera: salida negativa;
- retirada de cartera: entrada positiva;
- valor terminal: entrada positiva.

La tasa `mwr_xirr` es anualizada y satisface:

```text
0 = suma(flujo_i / (1 + tasa) ^ (dias_i / 365))
```

Si no hay cambio de signo, no existe solucion o aparecen varias soluciones, el
valor es `null`, el estado es `unavailable` y el motivo explica el caso.

## Periodos y contratos

`calculate_portfolio_performance` devuelve:

- ultimo mes (`last_month`);
- ultimo trimestre (`last_quarter`);
- ultimo ano (`last_year`);
- desde el inicio (`since_inception`).

Cada periodo conserva inicio solicitado, inicio realmente disponible, fecha
final, numero de valoraciones, flujos y aporte neto. Cada metrica incluye
`value`, `unit`, fechas, observaciones, `coverage_ratio`, `status` y
`reason_code`.

Estados:

- `available`: calculo completo;
- `partial`: hay resultado, pero la cobertura o clasificacion requiere cautela;
- `unavailable`: no se puede calcular; `value` es `null`, nunca un cero ficticio.

No se crea otro Parquet: estas metricas se derivan de valoraciones y flujos ya
persistidos. Asi se evita duplicar reglas o almacenar resultados obsoletos.

