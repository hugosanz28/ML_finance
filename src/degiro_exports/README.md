# DEGIRO Exports

Este directorio contiene los parsers/importador de exportaciones DEGIRO y las
rutas de entrada asociadas.

- `example/`: ruta opcional para ejemplos saneados que documenten formatos.
- `local/`: exportaciones reales del usuario. Esta carpeta está ignorada por Git.
- `local/incoming/`: aterrizaje inicial de CSV reales descargados desde DEGIRO.
- `../../demo/synthetic_degiro_exports/`: exports ficticios usados por la demo publica.

Estado actual:

- ya existen parsers para transacciones, movimientos de efectivo y snapshot de cartera,
- su salida normalizada se guarda bajo `src/data/local/normalized/degiro/`,
- `scripts/import_degiro.py` ejecuta el import batch desde `local/incoming/`
  mediante `ImportDegiroUseCase`,
- y, salvo `--skip-duckdb-load`, carga despues los datasets normalizados en
  DuckDB.

Tipos de exportación esperados:

- transacciones,
- estado de cuenta,
- cartera o snapshot de posiciones,
- y cualquier informe auxiliar que ayude a validar datos.

Convención canónica de nombres:

- `transactions_YYYY-MM-DD_YYYY-MM-DD.csv`
- `account_YYYY-MM-DD_YYYY-MM-DD.csv`
- `portfolio_YYYY-MM-DD.csv`

Desde el dashboard Streamlit se pueden subir CSVs con nombres originales de
DEGIRO. La UI detecta el tipo por el nombre del archivo y los copia a
`local/incoming/` mediante `SaveDegiroUploadsUseCase`, usando esta convencion
canonica. Si no encuentra fechas en el nombre, usa el dia de subida.

Import manual:

```powershell
.\.venv\Scripts\python.exe scripts\import_degiro.py
```

Opciones utiles:

- `--dry-run`: lista los CSV detectados sin escribir parquets.
- `--ignore-unknown`: ignora CSV que no sigan la convencion canonica.
- `--incoming-dir RUTA`: cambia la carpeta de entrada.
- `--output-dir RUTA`: cambia la carpeta normalizada de salida.
- `--skip-duckdb-load`: conserva solo los Parquet normalizados y omite la carga
  posterior en DuckDB.

Contrato detallado:

- `docs/degiro_input_contract.md`
