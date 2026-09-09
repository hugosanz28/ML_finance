# Analitica de riesgo y concentracion

`src/analytics/` contiene calculos offline de dominio y un catalogo explicativo
reutilizable. No carga datos privados, consulta proveedores ni implementa una UI.
La integracion en casos de uso, API, agentes y nueva UI pertenece a las tareas
posteriores del roadmap; estas metricas todavia no aparecen en el dashboard.

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
