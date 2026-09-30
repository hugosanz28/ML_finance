# Arranque y copias locales

## Un comando en Windows

Con Python, `.venv`, `requirements.txt`, Node.js 22.12 o posterior y las
dependencias del frontend (`npm ci`) instalados, ejecuta desde el repositorio:

```powershell
.\scripts\app.ps1
```

Inicia la API con `.env` y `--operations real` en `127.0.0.1:8000`, y Vite en
`127.0.0.1:5173`. Abre la web en el navegador predeterminado. Los procesos quedan
en segundo plano: puedes cerrar la terminal. No instala dependencias ni importa
CSV automáticamente. Programa las actualizaciones de [jobs locales](local_jobs.md);
`-NoRefresh` las desactiva en una API nueva. Al reutilizar una API conserva sus opciones.

```powershell
.\scripts\app.ps1 -NoBrowser -NoRefresh
.\scripts\app.ps1 -Action Status
.\scripts\app.ps1 -Action Stop
```

Reutiliza únicamente instancias identificadas de este checkout y listas para
uso real. Otro programa, checkout o proceso no identificable en esos puertos
bloquea el arranque: no se termina el proceso ni se elige otro puerto.
Una UI antigua iniciada con rutas relativas puede requerir detener su terminal
original una vez antes de usar este lanzador.

`Status` devuelve `ready`, `stopped`, `unready` o `foreign` por servicio.
`Stop` rechaza jobs pendientes/activos y termina solo procesos identificados
del proyecto. No envíes operaciones nuevas mientras lo detienes. Los fallos de
arranque conservan los logs en `src/data/local/app-*.log`; los servicios que
hayan arrancado permanecen disponibles para revisar o detener.

Requiere Windows PowerShell 5.1 o PowerShell 7. Busca un Node compatible en PATH
y, si está instalado, en el runtime local de Codex; no descarga ni instala nada.
También admite `-NodePath <node.exe>`. Para demo,
otros sistemas o diagnóstico, usa los [comandos manuales](../README.md#arranque-manual).

## Crear una copia

Detén la API (también en otro puerto) y cualquier importación, agente o refresh
CLI antes de copiar/restaurar. Se comprueba el puerto 8000 y se adquiere el
bloqueo del worker; los CLI legacy no comparten todos ese bloqueo.

```powershell
.\scripts\app.ps1 -Action Stop
.\scripts\app.ps1 -Action Backup
.\scripts\app.ps1
```

La salida indica el ZIP creado en `.local_backups/`, ignorado por Git. Incluye:

- `src/data/local/`: bases, planificación, precios, informes, configuración y auditoría;
- `src/degiro_exports/local/`: exportaciones originales;
- `.env`, si existe: configuración y posibles credenciales.

Omite logs (`*.log`), bloqueos (`*.lock`), temporales (`*.tmp`), `__pycache__`,
`.pytest_cache` y carpetas `test_tmp_*`. Las copias no se incluyen dentro de otras
copias. No incluye código, `.venv`, dependencias ni datos de demo. Requiere las
rutas privadas del modo operativo real y `.env` del repo; rechaza enlaces y junctions.

El manifiesto guarda tamaño y SHA-256 por archivo; se verifica después de copiar
y antes de restaurar. Detecta daños, no autentica al autor.
**El ZIP contiene datos financieros y puede contener secretos; no está cifrado.**
No lo subas a GitHub. Una copia en el mismo disco no protege frente a su pérdida:
guarda otra en un destino privado de tu elección. No hay envío a la nube ni
borrado automático de copias. Ver [privacidad](privacy.md).

## Restaurar

Sustituye el nombre del ejemplo por el ZIP concreto que creó Backup:

```powershell
.\scripts\app.ps1 -Action Stop
.\scripts\app.ps1 -Action Restore -Archive '.local_backups/mi-copia.zip' -ConfirmRestore
.\scripts\app.ps1
```

`-ConfirmRestore` es obligatorio. Sustituye el conjunto de archivos respaldados,
incluido `.env`; retira los archivos actuales ausentes de la copia, salvo los
temporales excluidos. No combina dos historiales financieros. Antes de modificar
nada crea otra copia verificada del estado actual y muestra su ruta al terminar.
Si falla la instalación, intenta revertir los archivos movidos. La copia previa
permanece incluso si falla la reversión o se interrumpe el sistema.

Rechaza rutas peligrosas, duplicados, enlaces, hashes incorrectos, más de
100.000 entradas o más de 10 GiB descomprimidos. Usa copias de confianza con una
versión compatible de la aplicación: no migra bases de versiones futuras.
Los jobs recuperados no se reejecutan automáticamente. Restaurar no arranca
servidores ni llama a proveedores.

CLI Python equivalente para otros sistemas, con la API detenida:

```bash
python scripts/local_backup.py backup
python scripts/local_backup.py restore --archive .local_backups/mi-copia.zip --confirm
```
