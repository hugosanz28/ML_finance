# Contrato de entrada DEGIRO

## Objetivo

Este documento define el contrato de entrada inicial para las exportaciones de
DEGIRO que alimentaran el importador del proyecto.

Su finalidad es fijar:

- que tipos de fichero se aceptan al inicio,
- como deben nombrarse y almacenarse,
- que metadatos mínimos se necesitan para importarlos,
- y que diferencias se permiten entre exportaciones reales y ejemplos saneados.

## Estado del contrato

Este contrato ya esta validado contra exportaciones reales locales y mezcla dos
niveles de certeza:

- reglas confirmadas en la ayuda oficial de DEGIRO,
- y observaciones confirmadas con CSV reales del usuario.

Referencias y muestras validadas el 2026-04-12:

- ayuda oficial:
  `https://www.degiro.es/helpdesk/impuestos/tratados-fiscales/que-tipo-de-informes-puedo-descargarme-y-donde-puedo-encontrarlos`
- `transactions_2025-11-01_2026-04-12.csv`
- `account_2025-11-01_2026-04-12.csv`
- `portfolio_2026-04-12.csv`

## Ficheros soportados inicialmente

La ayuda oficial de DEGIRO indica que se pueden descargar varios informes.
Para la primera version del importador, el alcance inicial queda limitado a:

1. `Transacciones`
2. `Estado de cuenta`
3. `Cartera` o snapshot historico de posiciones

### `Transacciones`

- Ubicacion en DEGIRO: `Buzon`
- Cobertura temporal: rango de fechas configurable
- Formatos ofrecidos por DEGIRO: Excel, PDF y CSV
- Formato soportado inicialmente por este proyecto: `CSV`

### `Estado de cuenta`

- Ubicacion en DEGIRO: `Buzon`
- Cobertura temporal: rango de fechas configurable
- Formatos ofrecidos por DEGIRO: Excel, PDF y CSV
- Formato soportado inicialmente por este proyecto: `CSV`

### `Cartera`

- Ubicacion en DEGIRO: seccion `Cartera`
- Cobertura temporal: una fecha concreta elegida por el usuario
- Formatos ofrecidos por DEGIRO: XLS, CSV y PDF
- Formato soportado inicialmente por este proyecto: `CSV`

## Fuera de alcance inicial

Aunque DEGIRO ofrece otros documentos, no forman parte del contrato inicial del
importador:

- `Informe anual`
- extractos de `Cuenta de efectivo`
- PDF de cualquier tipo
- Excel o XLS de cualquier tipo

Estos formatos podran documentarse o soportarse mas adelante, pero no bloquean
las tareas `P2-02`, `P2-03` y `P2-04`.

## Ubicacion en el repositorio

Separacion prevista:

- `src/degiro_exports/local/`: exportaciones reales, nunca versionadas.
- `src/degiro_exports/local/incoming/`: aterrizaje inicial de los ficheros tal y
  como salen de DEGIRO.
- `src/degiro_exports/example/`: ejemplos saneados y compartibles.

Regla operativa:

- los ficheros reales originales no deben editarse manualmente,
- y cualquier fixture de ejemplo debe derivarse de una exportacion real ya
  inspeccionada.

## Formato común confirmado en las muestras reales

Observado en los tres CSV inspeccionados:

- separador de campos: `,`
- una fila de cabecera seguida de filas de datos
- fechas en formato `DD-MM-YYYY`
- horas en formato `HH:MM`
- decimales con coma, por ejemplo `317,24`
- importes firmados desde la perspectiva del movimiento económico
- locale de cabeceras observado: español

Observaciones adicionales:

- los CSV inspeccionados no traen BOM UTF-8
- no se observaron separadores de miles en esta muestra concreta
- varias columnas monetarias usan una pareja `moneda + importe` donde la
  segunda cabecera aparece vacía

## Convención de nombres canónicos

El proyecto no depende del nombre original descargado por DEGIRO. Para trabajar
de forma reproducible, se define una convencion canonica al almacenar o
referenciar los CSV dentro del proyecto.

### Ficheros con rango temporal

Se usara:

- `transactions_YYYY-MM-DD_YYYY-MM-DD.csv`
- `account_YYYY-MM-DD_YYYY-MM-DD.csv`

Donde:

- el primer bloque es `date_from`,
- y el segundo bloque es `date_to`,
- ambos en formato ISO `YYYY-MM-DD`.

Ejemplos:

- `transactions_2024-01-01_2024-12-31.csv`
- `account_2024-01-01_2024-12-31.csv`

### Ficheros de snapshot

Se usara:

- `portfolio_YYYY-MM-DD.csv`

Donde la fecha representa el dia efectivo del snapshot exportado.

Ejemplo:

- `portfolio_2024-12-31.csv`

### Nombre original

Ademas del nombre canonico, cada importacion debe conservar el nombre original
descargado desde DEGIRO como metadato de trazabilidad.

## Metadatos minimos por fichero importado

Antes de parsear filas, cada fichero debe quedar identificado por metadatos
minimos.

Obligatorios:

- `broker = DEGIRO`
- `export_type = transactions | account | portfolio`
- `source_format = csv`
- `source_filename_original`
- `source_filename_canonical`
- `source_path`

Obligatorios para `transactions` y `account`:

- `date_from`
- `date_to`

Obligatorios para `portfolio`:

- `snapshot_date`

Opcionales pero recomendados:

- `exported_at`
- `account_label`
- `account_currency`
- `ui_locale`
- `csv_delimiter`
- `csv_encoding`

Valores por defecto observados en la muestra validada:

- `ui_locale = es`
- `csv_delimiter = ,`
- `csv_encoding = utf-8`

## Contrato de contenido por tipo de fichero

Las siguientes estructuras ya estan validadas con exportaciones reales
inspeccionadas localmente. Los apartados "Pendiente de confirmar" solo aplican
a variantes futuras que no aparecen en la muestra actual.

### `transactions`

Cabeceras observadas en la muestra validada:

1. `Fecha`
2. `Hora`
3. `Producto`
4. `ISIN`
5. `Bolsa de referencia`
6. `Centro de ejecución`
7. `Número`
8. `Precio`
9. `""` columna de divisa para `Precio`
10. `Valor local`
11. `""` columna de divisa para `Valor local`
12. `Valor EUR`
13. `Tipo de cambio`
14. `Comisión AutoFX`
15. `Costes de transacción y/o externos EUR`
16. `Total EUR`
17. `ID Orden`
18. `""` columna adicional sin nombre observada en parte de las filas

Reglas confirmadas:

- `Fecha` y `Hora` vienen separadas.
- `ISIN`, `Bolsa de referencia` y `Centro de ejecución` si aparecen en la
  muestra real.
- la direccion economica se puede leer en `Total EUR`:
  - `Total EUR < 0` implica salida de caja, tipicamente compra
  - `Total EUR > 0` implica entrada de caja, tipicamente venta
- `Número` suele venir positivo en compras y negativo en ventas con efectivo
  distinto de cero
- existen filas con `Número` vacio, por lo que no puede tratarse como campo
  siempre obligatorio
- existen filas de eventos sin efecto monetario directo, observadas en derechos
  o eventos corporativos, con `Precio = 0,0000` y `Total EUR = 0,00`
- `Tipo de cambio` y `Comisión AutoFX` solo se rellenan en operaciones con
  conversion de divisa

Supuestos operativos aceptados:

- el identificador practico de orden puede venir en `ID Orden` o en la columna
  final sin nombre, segun la fila; el parser debe tomar el ultimo valor no
  vacio entre ambas
- las columnas sin nombre junto a `Precio` y `Valor local` deben interpretarse
  como la divisa asociada al importe precedente

### `account`

Cabeceras observadas en la muestra validada:

1. `Fecha`
2. `Hora`
3. `Fecha valor`
4. `Producto`
5. `ISIN`
6. `Descripción`
7. `Tipo`
8. `Variación`
9. `""` columna de importe para `Variación`
10. `Saldo`
11. `""` columna de importe para `Saldo`
12. `ID Orden`

Reglas confirmadas:

- `Fecha`, `Hora` y `Fecha valor` vienen separadas.
- `Variación` y `Saldo` se representan como pareja `divisa + importe`.
- `Descripción` es el clasificador semantico principal del movimiento.
- el extracto mezcla varios tipos de evento:
  - compras y ventas
  - comisiones y costes
  - dividendos y retenciones
  - ingresos y retiradas
  - transferencias con la cuenta de efectivo
  - movimientos de cambio de divisa
- `ID Orden` es opcional y solo aparece en una parte de las filas.

Excepcion confirmada:

- la columna `Tipo` no es una categoria textual estable del movimiento
- en las filas de `Ingreso Cambio de Divisa` y `Retirada Cambio de Divisa`
  contiene un valor numerico que actua como tipo de cambio
- fuera de ese caso, suele venir vacia

Consecuencia para el parser:

- `Descripción` debe gobernar la clasificacion inicial del movimiento
- `Tipo` debe tratarse como campo contextual opcional, no como enumeracion de
  negocio

### `portfolio`

Cabeceras observadas en la muestra validada:

1. `Producto`
2. `Symbol/ISIN`
3. `Cantidad`
4. `Precio de`
5. `Valor local`
6. `""` columna de importe para `Valor local`
7. `Valor en EUR`

Reglas confirmadas:

- la fecha del snapshot no aparece dentro del CSV inspeccionado
- `snapshot_date` debe derivarse del contexto de exportacion o del nombre del
  fichero
- `Symbol/ISIN` contiene ISINs validos en todas las posiciones no monetarias
  observadas, aunque la cabecera sugiera un campo mixto
- `Precio de` contiene el precio unitario
- `Valor local` contiene la divisa de la posicion
- la columna sin nombre siguiente contiene el importe en divisa local
- `Valor en EUR` contiene la valoracion convertida a EUR

Excepciones confirmadas:

- existe al menos una fila de caja en EUR con `Symbol/ISIN`, `Cantidad` y
  `Precio de` vacios
- el parser del snapshot debe tolerar filas de efectivo sin ISIN

Pendiente de confirmar en futuras muestras:

- si otras cuentas o productos usan ticker en `Symbol/ISIN` en lugar de ISIN
- si el snapshot puede salir en una moneda base distinta de `EUR`

## Variaciones toleradas

El importador debe poder tolerar variaciones de presentacion que no cambien el
significado del fichero:

- cabeceras en espanol o ingles,
- pequenas diferencias de acentuacion o espacios,
- columnas auxiliares no utilizadas por el modelo normalizado,
- orden distinto de columnas,
- y separador o encoding del CSV, siempre que pueda detectarse de forma segura.
- tambien debe tolerar columnas sin nombre repetidas cuando representen divisas
  o extensiones del export original.

Si una exportacion cambia el significado de una columna, elimina una columna
critica o mezcla varios tipos de informe en un mismo fichero, debe rechazarse.

## Diferencias entre exportaciones reales y ejemplos saneados

Los ejemplos saneados pueden:

- anonimizar nombre de cuenta, referencias internas o identificadores sensibles,
- recortar periodos temporales,
- redondear o perturbar importes si hace falta proteger privacidad,
- y reducir el numero de filas.

Los ejemplos saneados no deben:

- cambiar nombres de columnas,
- alterar signos de importes,
- romper relaciones entre filas,
- ni modificar el tipo semantico del fichero.

## Flujo de validacion recomendado

1. Guardar el CSV original en `src/degiro_exports/local/incoming/`.
2. Inspeccionar manualmente cabecera, delimitador, encoding y rango temporal.
3. Asignar nombre canonico segun este contrato.
4. Registrar metadatos minimos.
5. Crear, si procede, un ejemplo saneado en `src/degiro_exports/example/`.
6. Ajustar este documento con las cabeceras reales observadas.

## Estado de cierre de `P2-01`

Con las tres exportaciones reales ya inspeccionadas, este contrato queda
suficientemente definido para arrancar:

- `P2-02` parser de transacciones
- `P2-03` parser de movimientos de efectivo
- `P2-04` parser de snapshot de cartera

Las futuras muestras reales solo deberian ampliar alias, excepciones o nuevos
casos de producto, no redefinir el contrato base ya observado.
