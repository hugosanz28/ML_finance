# Planificación personal

## Alcance

La aplicación local permite repartir el sueldo previsto entre gastos, reservas
y metas definidas por el usuario. Planificación e Inversiones son zonas
independientes: ninguna requiere configurar la otra. Los datos manuales se
guardan en una base DuckDB privada del entorno; la demo usa datos sintéticos.
No se importan extractos bancarios ni PDF, no existe conexión bancaria y la
aplicación no mueve dinero ni ejecuta órdenes.

La primera versión admite una persona, una cuenta bancaria manual en EUR,
varias metas con reservas dentro de esa cuenta y, como máximo, una meta
vinculada a toda la cartera DEGIRO. El usuario elige el nombre y el destino de
cada meta; coche y vivienda son solo ejemplos. Los objetivos personales son
distintos de los pesos objetivo de la cartera de inversión.

## Datos y cálculo

- El usuario introduce sueldo mensual neto, día de cobro, nombre y saldo
  observado de la cuenta, gastos previstos por categoría, reserva para
  imprevistos y compromisos inmediatos. El saldo se introduce manualmente y
  no se reconstruye a partir de movimientos.
- Cada meta tiene nombre, importe objetivo, fecha orientativa opcional y
  aportación mensual calculada por plazo o introducida manualmente. Una meta
  bancaria puede tener una reserva inicial y movimientos de asignación o
  retirada; son apuntes virtuales dentro del saldo observado, no movimientos
  bancarios. Con 5.000 € de saldo, 4.000 € reservados para una meta y 500 €
  para otra, quedan 500 € sin asignar antes de otras reservas.
- La meta DEGIRO usa el valor actual de toda la cartera como progreso, con su
  fecha y avisos. Si no hay datos de cartera, el valor queda como no
  disponible. Sus movimientos manuales documentan aportaciones, pero no se
  suman de nuevo al valor de mercado ni se descuentan del saldo bancario.
- Para una meta con plazo, la cantidad mensual necesaria es lo que falta para
  el objetivo dividido entre los cobros pendientes hasta la fecha. El mes
  actual cuenta solo si aún no ha llegado el día de cobro. Las metas con
  aportación manual muestran el importe elegido y, si hay fecha y valoración,
  si es inferior al necesario.
- El margen mensual es `sueldo previsto - gastos mensuales`. Se comparan con
  él las aportaciones planificadas de todas las metas. Si faltan datos para
  calcular alguna aportación, el total queda indeterminado; si exceden el
  margen, se muestra el desfase. Las reservas bancarias se restan una sola vez
  del saldo observado. Un saldo inferior a lo reservado muestra el déficit sin
  modificar las reservas automáticamente.
- Cambiar sueldo, gastos, metas, importes o fechas recalcula la previsión.
  Los registros reales se conservan hasta que el usuario los edite o elimine.
  La UI distingue importes previstos, registrados y valorados por DEGIRO.

Las categorías iniciales cubren casa y comida, gasolina, ocio, imprevistos
del coche y otros gastos. El usuario puede añadir categorías; una compra
puntual, como un móvil, se registra como gasto real sin convertirla en gasto
mensual fijo.

## Calendario

El calendario mensual muestra el día previsto de cobro, gastos recurrentes,
aportaciones previstas por meta, fechas objetivo y movimientos reales.
Distingue previsiones y registros y ofrece una lista textual accesible.

## Tareas

El seguimiento se realiza en GitHub: [modelo y cálculos](https://github.com/hugosanz28/ML_finance/issues/62),
[persistencia y API](https://github.com/hugosanz28/ML_finance/issues/63),
[interfaz](https://github.com/hugosanz28/ML_finance/issues/64),
[calendario](https://github.com/hugosanz28/ML_finance/issues/65) y
[meta vinculada a DEGIRO](https://github.com/hugosanz28/ML_finance/issues/66).
