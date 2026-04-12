# DEGIRO Exports

Este directorio contiene la materia prima del proyecto.

- `example/`: ejemplos saneados que sirven para documentar formatos y probar parsers.
- `local/`: exportaciones reales del usuario. Esta carpeta está ignorada por Git.
- `local/incoming/`: aterrizaje inicial de CSV reales descargados desde DEGIRO.

Estado actual:

- ya existen parsers para transacciones, movimientos de efectivo y snapshot de cartera,
- y su salida normalizada se guarda bajo `src/data/local/normalized/degiro/`.

Tipos de exportación esperados:

- transacciones,
- estado de cuenta,
- cartera o snapshot de posiciones,
- y cualquier informe auxiliar que ayude a validar datos.

Convención canónica de nombres:

- `transactions_YYYY-MM-DD_YYYY-MM-DD.csv`
- `account_YYYY-MM-DD_YYYY-MM-DD.csv`
- `portfolio_YYYY-MM-DD.csv`

Contrato detallado:

- `docs/degiro_input_contract.md`
