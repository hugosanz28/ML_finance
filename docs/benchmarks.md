# Benchmarks y comparacion de rendimiento

## Objetivo

La capa de benchmarks permite comparar el TWR de la cartera con referencias
seleccionables y reproducibles. Un benchmark es una referencia de contexto, no
una recomendacion ni un objetivo universal para todas las carteras.

El catalogo y los providers viven en `src/market_data/benchmarks.py`. La
configuracion, el benchmark compuesto y las metricas comparativas viven en
`src/portfolio/benchmarks.py`. La API, UI y los agentes deben acceder a
esta funcionalidad mediante `src/application/`.

## Catalogo inicial

| ID estable | Referencia | Moneda nativa | Tipo de serie |
| --- | --- | --- | --- |
| `msci_world` | MSCI World Net Total Return EUR | EUR | retorno total |
| `sp500` | S&P 500 Total Return Index | USD | retorno total |
| `portfolio_60_40` | 60 % global equity / 40 % global aggregate bonds EUR hedged | EUR | retorno total compuesto |
| `estr_cash` | efectivo compuesto a €STR | EUR | retorno de efectivo |

El componente interno `global_aggregate_bonds_eur_hedged` no aparece como
opcion independiente. Se usa para construir el 60/40.

Las definiciones documentan de forma explicita si la serie representa precio,
retorno total, efectivo o una composicion. Cambiar de proveedor no debe cambiar
el significado del identificador. El catalogo describe la referencia conceptual;
la comparacion identifica por separado la fuente realmente utilizada y sus proxies.

## Seleccion y configuracion

La ruta se configura con `BENCHMARK_SELECTION_PATH`. El archivo JSON elige una
referencia principal, secundarias y, opcionalmente, pesos del compuesto:

```json
{
  "primary_benchmark_id": "msci_world",
  "secondary_benchmark_ids": ["sp500", "portfolio_60_40", "estr_cash"],
  "composite_weights": {
    "portfolio_60_40": {
      "msci_world": 0.6,
      "global_aggregate_bonds_eur_hedged": 0.4
    }
  }
}
```

Los IDs deben existir, no pueden repetirse y los pesos deben cubrir exactamente
los componentes y sumar `1.0`.

## Cartera 60/40

La baseline usa 60 % de MSCI World y 40 % de bonos global aggregate cubiertos a
EUR. Los pesos derivan con el rendimiento durante el mes y se restauran al
objetivo en la primera observacion comun de cada mes. La configuracion puede
cambiar los pesos, pero no inferir componentes por nombre.

## Calendario, divisa y cobertura

La comparacion usa los intervalos del calendario del benchmark. Si la cartera
tiene valoraciones durante un fin de semana, encadena sus retornos hasta cubrir
el intervalo viernes-lunes del benchmark. Un intervalo que no puede
reconstruirse se excluye y reduce la cobertura.

Las series se convierten antes de comparar. Los FX siguen la convencion del
repositorio: `EUR/USD` expresa unidades USD por una unidad EUR. Para convertir
un retorno USD a EUR:

```text
factor_EUR = (1 + retorno_USD) * FX_anterior / FX_actual
```

Huecos de calendario, FX ausente o cobertura parcial producen estado `partial`
o `unavailable` y un `reason_code`; no se rellenan con retornos cero.
Si quedan intervalos desconectados, se compara solo el ultimo tramo continuo,
con fechas y cobertura explicitas (`benchmark_common_window_truncated`). El
60/40 comienza ese tramo con los pesos objetivo, sin inventar su historia anterior.

## Metricas

Para cada benchmark se devuelven:

- crecimiento indexado desde 100 y drawdown de ambas series;
- retorno acumulado de cartera y benchmark;
- retorno relativo multiplicativo;
- maximo drawdown y su diferencia;
- correlacion;
- tracking error anualizado;
- beta y alpha diaria de regresion anualizada.

Correlacion, tracking error, beta y alpha requieren al menos 30 observaciones
alineadas. Beta y alpha tampoco se calculan si la varianza del benchmark es
cero. En esos casos el valor es `null`, nunca un cero ficticio.

## Demo y persistencia

`SyntheticBenchmarkProvider` genera series y FX deterministas sin red para las
cuatro opciones. La demo selecciona las cuatro en
`demo/synthetic_config/benchmark_selection.json`.

El dominio tambien admite `LoadedBenchmarkProvider` para series inyectadas.
En real, `CachedBenchmarkProvider` lee exclusivamente una cache validada: nunca
descarga en un GET ni usa datos sinteticos como respaldo.

## Fuentes reales locales

| Referencia | Fuente descargada | Limite |
| --- | --- | --- |
| MSCI World | EUNL.DE, IE00B4L5Y983 | ETF de acumulacion, aproximacion al indice |
| S&P 500 | SXR8.DE, IE00B5BMR087 | ETF de acumulacion, aproximacion al indice |
| Bonos del 60/40 | EUNA.DE, IE00BDBRDM35 | ETF global aggregate cubierto a EUR |
| Efectivo | BCE `EST.B.EU000A2QQF08.CI` | Indice compuesto €STR, no rentabilidad de un deposito |

Los ETF usan el cierre ajustado de Yahoo a traves de `yfinance` (dependencia ya
existente), con cotizacion Xetra EUR verificada. Son aproximaciones que incluyen
costes y desviaciones del fondo, **no historicos oficiales de los indices**.
Cotizar en EUR no implica que la renta variable este cubierta de divisa.
El 60/40 combina los proxies anteriores con rebalanceo mensual.

Fuentes de referencia: [MSCI World / iShares](https://www.ishares.com/uk/individual/en/products/251882/ishares-core-msci-world-ucits-etf?siteEntryPassthrough=true),
[S&P 500 / iShares](https://www.ishares.com/de/privatanleger/de/produkte/253743/ishares-sp-500-b-ucits-etf-acc-fund?siteEntryPassthrough=true&switchLocale=y),
[bonos EUR hedged / BlackRock](https://www.blackrock.com/ch/privatanleger/de/produkt/291770/ishares-core-global-aggregate-bond-ucits-etf-eur-hedged-acc-fund?switchLocale=Y)
y [serie €STR del BCE](https://data.ecb.europa.eu/data/datasets/EST/EST.B.EU000A2QQF08.CI).
No se reconstruye €STR antes de su disponibilidad en octubre de 2019.

Para carteras USD, GBP, CHF o JPY se descarga ademas el FX diario oficial BCE
`EXR.D.<MONEDA>.EUR.SP00.A`. EUR no necesita conversion. No se rellenan extremos
FX ausentes ni observaciones nulas. Otras monedas se rechazan antes de descargar.

### Actualizacion explicita

Con la configuracion real habitual y sin el worker operativo ni otro escritor
Streamlit/CLI activo, ejecuta (ajusta las fechas a todo el historial necesario):

```powershell
.\.venv\Scripts\python.exe scripts\refresh_benchmarks.py --start-date 2020-01-01 --end-date 2026-09-14 --provider yfinance_ecb --confirm
```

La fecha final debe ser anterior a hoy. Incluye una fecha inicial de referencia
para calcular el primer retorno. Cada actualizacion **reemplaza la ventana
completa**, no mezcla revisiones de cierres ajustados con descargas anteriores.
No se modifica el estado de cartera, el precio absoluto del broker ni su FX.

Alternativamente, con API `--operations real`, usa POST
`/api/v1/benchmarks/refresh` con `confirm: true`, `provider: "yfinance_ecb"`,
`start_date`, `end_date` e `Idempotency-Key`; devuelve un job. Ver
[jobs locales](local_jobs.md). La CLI comparte el bloqueo del worker. El refresh
general de precios no descarga benchmarks y no hay reintentos automaticos.
La UI muestra el resultado y ofrece el refresh en Operaciones → Datos (#56),
con seleccion explicita de fuente, fechas y confirmacion; ver [UI operativa](react_operations.md).

### Cache, auditoria y limites

`benchmark_cache.json` se guarda dentro de `DATA_DIR` real (privado e ignorado),
con niveles normalizados, proveedor, ticker/serie, ISIN en la referencia, moneda,
fechas y hashes SHA-256 de contenido. La sustitucion es atomica: si falla una
fuente, se conserva el ultimo snapshot completo. La cache auxiliar de yfinance
tambien queda dentro de `DATA_DIR`. No se guardan credenciales.

API y React exponen fuentes, cobertura, momento de descarga y hashes. Los hashes
detectan cambios de contenido; no son una firma del proveedor ni prueban que el
dato sea correcto. Los ajustes historicos pueden revisarse en futuras descargas;
esto no es una base point-in-time para backtesting. Una cache ausente, corrupta,
de otra moneda o antigua produce avisos explicitos, nunca una curva inventada.

Esta integracion se destina a uso personal/local, sin servicio de pago ni SLA.
La disponibilidad depende de Yahoo/BCE. Antes de publicar o redistribuir datos
debe revisarse la licencia de cada proveedor; descarga gratuita no equivale a
permiso de redistribucion. Demo y tests permanecen offline y sinteticos.
