# Analitica de riesgo y concentracion

`src/analytics/` contiene calculos offline de dominio y un catalogo explicativo
reutilizable. No carga datos privados, consulta proveedores ni implementa una UI.
La integracion en casos de uso esta en `src/application/analytics.py`; los
agentes consumen un [snapshot compacto](agent_analytics.md). La API de lectura
y la [UI React](../frontend/README.md) exponen estas metricas. React es la interfaz mantenida.

## Entradas y resultados

- `calculate_risk`: observaciones `DailyPerformanceObservation` de cartera o
  activo, fechas de inicio y fin, y tasa anual libre de riesgo opcional.
- `calculate_concentration`: posiciones `PositionExposure` valoradas en moneda
  base y clasificaciones explicitas. Solo soporta valores no negativos.
- `calculate_diversification`: retornos por activo en una misma moneda base y
  pesos actuales no negativos que suman uno. Deben ser retornos de precio o
  totales, nunca cambios del valor de una posicion causados por compras/ventas.
- `metric_catalog()`: identificador, nombre, descripcion, formula, unidad,
  interpretacion, limitaciones, datos requeridos y condiciones de validez.

Cada resultado numerico usa `PerformanceMetric`: incluye periodo, observaciones,
cobertura, `status` y `reason_code`. Un dato ausente o una division indefinida
produce `value=None`, no cero. Los resultados no finitos tampoco se publican.
Las explicaciones son educativas y neutrales, no recomendaciones de inversion.

## Calendario y muestra

Se usan intervalos de **un dia natural** y **365 periodos por ano**, coherentes
con la reconstruccion diaria de cartera. Se rechazan fechas duplicadas e
intervalos de varios dias: un retorno mensual no se anualiza como si fuera diario.
No se insertan ceros en huecos ni se rellenan precios en este modulo.

Volatilidad, downside volatility, Sharpe, Sortino, Calmar, correlaciones y
contribuciones requieren al menos **30 observaciones validas**. La volatilidad
usa desviacion tipica muestral. Downside volatility usa el promedio de los
cuadrados de `min(retorno, 0)`, incluyendo todos los dias en el denominador.

Sharpe y Sortino necesitan una tasa anual explicita; `None` no significa cero.
La referencia diaria es `(1 + tasa_anual) ** (1/365) - 1`. Sortino mide las
desviaciones negativas respecto a esa referencia. Denominadores nulos producen
motivos como `zero_volatility` o `zero_downside_deviation`.

La cobertura temporal es la suma de coberturas de observaciones validas dividida
por los dias esperados. Las estadisticas sobre muestras incompletas se marcan
`partial`. Drawdown y Calmar exigen una trayectoria sin dias ausentes: no unen
tramos separados por huecos. El drawdown parte del indice compuesto de retornos
ajustados por flujos, no de los cambios de patrimonio por aportaciones.
Su duracion incluye episodios sin recuperar hasta el final de la ventana.

## Exposiciones y diversificacion

Los pesos se calculan sobre el valor conocido por activo, bucket, moneda, tipo
y sector. Si falta valoracion se marcan parciales. La cobertura combina la
fraccion de posiciones valoradas con la fraccion de valor clasificado; no estima
el valor economico de las posiciones sin valoracion.

Las etiquetas ausentes quedan como `unclassified`. Un sector solo se utiliza
con `sector_source` declarado por el llamante, que debe aportar una fuente
fiable; el calculo no verifica proveedores ni infiere sectores por nombres.
No se realiza look-through de ETFs. HHI es la suma de pesos al cuadrado y se
omite en dimensiones con valor sin clasificar, para no tratar lo desconocido
como una concentracion real. Un unico activo conocido tiene HHI igual a uno.

Las correlaciones usan fechas comunes por pareja y requieren varianzas positivas.
Cada celda tiene su propia cobertura; con muestras distintas la matriz no tiene
garantizada la propiedad de ser semidefinida positiva.

La contribucion aproximada al riesgo usa fechas comunes a todos los activos
con peso positivo: `w_i * cov(r_i, retorno_cartera) / var(retorno_cartera)`.
Las contribuciones suman uno y pueden ser negativas por diversificacion.
Es una aproximacion con pesos actuales fijos, no atribucion historica exacta;
si falta la serie de un activo activo no se renormalizan los demas pesos.

## Relacion con rendimiento y benchmarks

Ver [rendimiento](performance.md) para retornos ajustados, TWR y MWR/XIRR, y
[benchmarks](benchmarks.md) para las metricas relativas existentes. El catalogo
incluye sus metricas comparativas sin duplicar el motor de benchmarks.
Ese motor usa su propia convencion de 252 sesiones; no deben intercambiarse
sus anualizaciones con las de las series de dias naturales de este modulo.

## Validacion

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_risk_analytics.py
```

Incluye resultados de referencia calculables, series constantes, poca muestra,
huecos, duracion de drawdowns, exposiciones sin clasificar y contribuciones con
correlacion perfecta. Todo se ejecuta sin red ni datos reales.

## Calidad del histórico reconstruido

El efectivo se reconstruye desde el Estado de cuenta: depósitos y retiradas
usan fecha valor, compras/ventas y otros movimientos usan fecha de movimiento.
Las transferencias internas entre subcuentas no se cuentan dos veces. Los
snapshots verifican el saldo con tolerancia de 0,02 unidades de la divisa; una
diferencia mayor invalida la valoración de caja desde ese punto. Los exports
solapados se deduplican para la caja y para los flujos de rentabilidad.

Un intervalo con alguna posición sin valorar no genera retorno. No se calcula
volatilidad usando cambios de un total parcial; los días válidos restantes
siguen señalados como muestra parcial. Precios y FX de más de siete días se
rechazan en la reconstrucción anclada. Una posición cerrada vale cero sin
necesitar precio, pero un saldo desconocido no se convierte en cero.

Un activo vendido sin snapshot puede usar el precio documentado de su primera
compra y la variación relativa del proveedor: `transaction_price_anchor`
identifica esta aproximación. Los derechos sin precio ni ancla válida conservan
su hueco. Para drawdown elige un periodo continuo o completa la fuente faltante.
La correlación del efectivo constante figura como no aplicable. La UI permite
introducir una tasa anual explícita para Sharpe/Sortino; dejarla vacía conserva
estas métricas como no disponibles.

## Derechos convertidos en dividendos pendientes

La reconstrucción distingue la cotización de los derechos de su conversión
en un dividendo pendiente. Solo reconoce esta última cuando el Estado de
cuenta contiene una conversión completa (venta de derechos y compra del
instrumento no negociable), su baja y un dividendo positivo con el mismo
identificador. Cantidades distintas, pagos ambiguos o liquidaciones aún no
registradas mantienen el hueco; no se vinculan productos por nombres parecidos.

Desde la fecha de registro de la conversión hasta el día anterior al abono,
se utiliza el importe bruto liquidado por DEGIRO como dividendo pendiente.
Se conserva incluso si la baja de los derechos tiene una fecha valor anterior
al abono. Al llegar el dinero se elimina el pendiente; la retención se registra
en efectivo ese día. Así no se cuenta dos veces el ingreso ni se crea una
pérdida seguida de una ganancia por el desfase de fechas.

`valued_dividend_receivable` y el aviso `dividend_receivable_reconstructed`
identifican esta **reconstrucción retrospectiva**, basada en el importe cobrado
después. No representa una cotización de mercado ni una serie que se conociera
en tiempo real. Una consulta con fecha final anterior al abono no usa ese pago
futuro. No se rellenan los precios de los días anteriores a la conversión.
