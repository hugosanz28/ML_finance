# Privacidad y secretos

## Objetivo

El repositorio debe poder publicarse o ensenarse sin exponer datos personales,
financieros ni credenciales. La regla base es simple: el codigo, la
documentacion y los ejemplos sinteticos pueden versionarse; los datos reales y
las salidas privadas se quedan en rutas locales ignoradas por Git.

La version actual es la **v1 local con Streamlit**. Esto significa que la
aplicacion se ejecuta en el ordenador del usuario y no necesita backend remoto
ni base de datos externa para funcionar. Aun asi, los agentes pueden enviar
contexto a proveedores externos si se configuran proveedores reales.

## Datos sensibles

Trata como privados, aunque no contengan contrasenas:

- exportaciones reales de DEGIRO: transacciones, cuenta, cartera y snapshots;
- bases DuckDB locales, parquets normalizados, caches de mercado y artefactos
  derivados de la cartera real;
- informes mensuales, historicos, outputs de agentes y audit trails;
- `investment_brief.md`, presupuestos, objetivos, pesos objetivo y notas
  personales de inversion;
- capturas del dashboard o logs de ejecucion generados con datos reales;
- `.env`, claves de OpenAI, Tavily u otros proveedores, y secretos de
  Streamlit.

Los datos de ejemplo que se publiquen deben ser sinteticos o estar saneados de
forma que no permitan reconstruir la cartera real, importes, fechas exactas o
decisiones personales.

## Auditoria de agentes

La auditoria reproducible conserva mas detalle que un log convencional. En
particular, `request.json`, `context.json`, `prompt_rendered.md`,
`raw_response.json` y `parsed_output.json` pueden contener el brief, posiciones,
presupuesto, objetivos, respuestas del provider y decisiones derivadas. Deben
tratarse siempre como datos financieros privados.

El schema v2 añade `provider.json` y hashes SHA-256. La configuracion del
provider se construye mediante una lista permitida de campos no secretos:
nombre, modelo y opciones operativas necesarias para interpretar el run. No
debe persistir API keys, tokens, passwords, cookies, cabeceras de autorizacion,
variables de entorno ni objetos cliente de un SDK.

Una respuesta raw puede quedar `captured`, `partial` o `not_captured`. Cuando se
captura, puede incluir texto completo de salida, identificadores del provider y
metricas de uso. Que no contenga una credencial no la convierte en publica:
puede revelar indirectamente los inputs financieros enviados.

Los `input_hash` y `output_hash` sirven para detectar cambios semanticos e
integridad entre ejecuciones. Un hash no anonimiza sus datos de origen, no
demuestra que dos decisiones sean correctas y no hace seguro publicar el
artefacto que lo contiene.

Los runs legacy v1 siguen siendo privados aunque no incluyan provider metadata,
hashes o respuesta raw capturable. La lectura compatible no reescribe esos
artefactos ni elimina posibles datos sensibles que ya contengan.

## Rutas privadas

Estas rutas estan disenadas para uso local y deben permanecer fuera de Git:

```text
src/degiro_exports/local/
src/data/local/
.env
.env.*
*.env
demo/local_data/
.streamlit/secrets.toml
*.duckdb
*.duckdb-wal
*.duckdb-shm
```

La configuracion actual de `.gitignore` cubre estas rutas. Antes de publicar,
crear una demo o abrir un pull request, revisa que no haya archivos privados ya
trackeados:

```powershell
git status --short
git ls-files src/degiro_exports/local src/data/local
git check-ignore -v src/degiro_exports/local/ src/data/local/ .env
```

`git ls-files` no debe devolver archivos bajo `src/degiro_exports/local` ni
`src/data/local`. Si devuelve algo, ese archivo ya esta versionado y hay que
retirarlo del indice antes de publicar.

## Demo sintetica

La demo publica vive en `demo/` y usa `demo/synthetic_config/.env.demo`. Ese
archivo no contiene secretos y apunta a `demo/local_data/`, que esta ignorado
por Git. Los CSV bajo `demo/synthetic_degiro_exports/` son ficticios y estan
disenados para ensenar el flujo sin exponer cartera real.

Para demos publicas, usa:

```powershell
.\scripts\run_demo.ps1
```

No uses capturas, informes ni audit trails generados desde `src/data/local/`.
El repositorio no incluye actualmente capturas del dashboard. Cualquier captura
publica futura debe salir de la demo sintetica recien generada y revisarse para
confirmar que no muestra rutas, terminales ni datos de una ejecucion real.

## Secret scanning

Antes de ensenar el repositorio, subir una rama o abrir un PR:

1. Revisa el diff completo:

   ```powershell
   git diff --stat
   git diff
   ```

2. Busca patrones obvios solo en archivos versionados, sin abrir rutas locales
   ignoradas:

   ```powershell
   $trackedFiles = git ls-files
   rg -n "OPENAI_API_KEY\s*=|TAVILY_API_KEY\s*=|api[_-]?key|password\s*=|token\s*=|secret\s*=|BEGIN .*PRIVATE KEY" -- $trackedFiles
   ```

3. Ejecuta el mismo escaner de secretos que usa CI:

   ```powershell
   $trackedFiles = git ls-files
   detect-secrets-hook --baseline .secrets.baseline $trackedFiles
   ```

4. Como defensa adicional puedes usar `gitleaks` o `git-secrets` y comprobar en
   GitHub que el repositorio tiene activado secret scanning cuando este
   disponible para la cuenta o el plan.

Si un secreto se ha subido alguna vez, no basta con borrarlo del archivo actual:
hay que rotarlo en el proveedor correspondiente y limpiar o invalidar el
historial afectado antes de considerar el repositorio seguro.

## Checklist antes de publicar o hacer demo

- `git status --short` no muestra archivos privados.
- `git ls-files src/degiro_exports/local src/data/local` no devuelve nada.
- `.env` existe solo en local y `.env.example` no contiene valores reales.
- No hay capturas versionadas o, si se anaden, proceden exclusivamente de una
  demo sintetica revisada.
- Los informes y outputs de agentes usados en demos proceden de datos sinteticos
  o saneados.
- Los `provider.json` revisados no contienen claves, tokens, cabeceras ni
  variables de entorno.
- Los `raw_response.json` y prompts usados en capturas proceden de la demo
  sintetica y se han revisado manualmente.
- No se considera un audit trail publicable solo porque incluya hashes.
- El diff no contiene rutas locales absolutas, claves API, tokens ni datos de
  cartera real.
- Si se han usado proveedores externos, los prompts y respuestas no incluyen
  informacion privada innecesaria.

## Uso con agentes

Los agentes pueden enviar contexto a proveedores externos cuando se configuran
proveedores reales. Antes de activar un proveedor no local, revisa el payload que
se va a enviar y evita incluir datos personales que no sean necesarios para el
analisis. Para pruebas publicas o demos, usa proveedores `static` o datos
sinteticos.

En la v1 local con Streamlit hay dos combinaciones offline:

- `LLM provider: static` y `Search provider: null`: baseline seguro sin
  resultados de busqueda;
- `LLM provider: static` y `Search provider: static`: demo completa con
  resultados locales etiquetados como sinteticos.

Los proveedores `openai`, `tavily` y `duckduckgo` deben tratarse como
ejecuciones reales o semi-reales, porque pueden sacar informacion fuera del
equipo local.

Si una credencial aparece por error en un audit trail, deja de compartirlo,
elimina la copia afectada y rota la credencial en el proveedor. Ocultarla en
Streamlit o sustituirla solo en una captura no invalida el secreto original.
