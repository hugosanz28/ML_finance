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
- capturas del dashboard, screenshots de demos y logs de ejecucion;
- `.env`, claves de OpenAI, Tavily u otros proveedores, y secretos de
  Streamlit.

Los datos de ejemplo que se publiquen deben ser sinteticos o estar saneados de
forma que no permitan reconstruir la cartera real, importes, fechas exactas o
decisiones personales.

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

## Secret scanning

Antes de ensenar el repositorio, subir una rama o abrir un PR:

1. Revisa el diff completo:

   ```powershell
   git diff --stat
   git diff
   ```

2. Busca patrones obvios de secretos o datos privados:

   ```powershell
   rg -n "OPENAI_API_KEY|TAVILY_API_KEY|api[_-]?key|secret|password|token|BEGIN .*PRIVATE KEY" .
   ```

3. Ejecuta un escaner de secretos si lo tienes instalado. Ejemplos:

   ```powershell
   gitleaks detect --source . --redact --no-banner
   git-secrets --scan
   ```

4. Comprueba en GitHub que el repositorio tiene activado secret scanning cuando
   este disponible para la cuenta o el plan.

Si un secreto se ha subido alguna vez, no basta con borrarlo del archivo actual:
hay que rotarlo en el proveedor correspondiente y limpiar o invalidar el
historial afectado antes de considerar el repositorio seguro.

## Checklist antes de publicar o hacer demo

- `git status --short` no muestra archivos privados.
- `git ls-files src/degiro_exports/local src/data/local` no devuelve nada.
- `.env` existe solo en local y `.env.example` no contiene valores reales.
- Las capturas del dashboard no muestran nombres, importes o fechas personales.
- Los informes y outputs de agentes usados en demos proceden de datos sinteticos
  o saneados.
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

En la v1 local con Streamlit, la opcion segura para demo es `LLM provider:
static` y `Search provider: null`. Los proveedores `openai`,
`tavily` y `duckduckgo` deben tratarse como ejecuciones reales o semi-reales,
porque pueden sacar informacion fuera del equipo local.
