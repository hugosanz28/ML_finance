# DEGIRO Exports

Este directorio contiene la materia prima del proyecto.

- `example/`: ejemplos saneados que sirven para documentar formatos y probar parsers.
- `local/`: exportaciones reales del usuario. Esta carpeta está ignorada por Git.

Tipos de exportación esperados:

- transacciones,
- estado de cuenta,
- cartera o snapshot de posiciones,
- y cualquier informe auxiliar que ayude a validar datos.

Convención recomendada de nombres:

- `transactions_YYYY-MM-DD.csv`
- `account_YYYY-MM-DD.csv`
- `portfolio_YYYY-MM-DD.csv`
