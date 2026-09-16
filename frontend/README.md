# ML Finance UI

UI v2 (#55/#56): React + TypeScript estricto + Vite sobre FastAPI local.
Analitica y flujos operativos opt-in. React sustituye a Streamlit (#58);
ver [migracion](../docs/react_migration.md).

## Arranque

Requiere Node.js 22.12 o posterior y el entorno Python instalado. Desde la
raiz del repo, prepara la demo en una terminal PowerShell:

```powershell
$env:ML_FINANCE_ENV_FILE = "demo/synthetic_config/.env.demo"
.\.venv\Scripts\python.exe scripts/bootstrap_demo.py
.\.venv\Scripts\python.exe scripts/run_api.py --env-file demo/synthetic_config/.env.demo
```

En otra terminal:

```powershell
cd frontend
npm ci
npm run dev
```

Abre [la UI local](http://127.0.0.1:5173). API: `127.0.0.1:8000`.
Ambos procesos se detienen con Ctrl+C. No publiques estos puertos ni uses
tuneles. Vite y su preview usan el puerto fijo 5173 para respetar CORS.
Si el puerto esta ocupado, detiene tu proceso anterior: no se elige otro solo.

En POSIX, con la venv activa, sustituye los comandos Python por:

```bash
ML_FINANCE_ENV_FILE=demo/synthetic_config/.env.demo python scripts/bootstrap_demo.py
python scripts/run_api.py --env-file demo/synthetic_config/.env.demo
```

Los comandos npm son identicos. Para datos reales, usa `python scripts/run_api.py`
con la configuracion privada del servidor y sin la variable de demo en esa
terminal. El frontend no elige rutas ni archivos de entorno. Para habilitar
acciones, anade `--operations demo` al comando de demo o `--operations real` al
comando real. No arranques dos APIs ni escribas desde CLI a la vez.
La API fija el entorno; la UI no puede cambiarlo. Ver [jobs locales](../docs/local_jobs.md).

## Pantallas y limites

- **Resumen**: valor de posiciones (incluye caja si figura como posicion),
  aportaciones netas acumuladas, PnL no realizado, TWR y evolucion del valor.
  No confundir PnL de posiciones abiertas con resultado total, ni aportaciones
  con ganancias. El valor historico no es una curva de rentabilidad.
- **Rentabilidad**: TWR acumulado y MWR anualizado, referencias seleccionables,
  crecimiento del indice base, drawdown y metricas comparativas avanzadas.
- **Riesgo y activos**: volatilidad, caidas, concentracion por dimension,
  posiciones, correlaciones y detalle opcional de riesgo por activo.
- **Operaciones**: datos, aportaciones, informes, agentes, configuracion y
  ejecuciones. Ver [matriz de paridad y seguridad](../docs/react_operations.md).

Las explicaciones vienen del catalogo de la API. Los avisos permanecen visibles
y los datos ausentes nunca se convierten en cero. Los graficos SVG solo escalan
valores calculados por el servidor; tienen leyenda, fechas y tabla accesible.
En pantallas estrechas, los graficos y tablas se desplazan horizontalmente
dentro de su panel para mantener etiquetas legibles.
Las curvas no interpolan valores nulos. No hay fuentes, imagenes o scripts CDN,
telemetria, credenciales ni persistencia de cartera en el navegador.

El periodo filtra la analitica y la curva de valor; las tarjetas de fotografia
y posiciones corresponden a la fecha final, no a acumulados del periodo.
La cartera se solicita con la fecha final de la analitica. Las respuestas
obsoletas se descartan al cambiar filtros. Si falla una seccion, las otras
siguen disponibles; hay reintento manual y timeout de 60 segundos.

Los benchmarks reales necesitan una [descarga explicita a cache](../docs/benchmarks.md)
por CLI, API operativa o el panel Operaciones → Datos. La UI muestra fuentes, proxies ETF, fechas y hashes;
abrir una pantalla no descarga datos. Si falta la cache,
se muestra `benchmark_provider_unavailable`, nunca una curva sintetica de
respaldo. La etiqueta de benchmark sintetico describe **la referencia**, no
certifica que todos los datos de la cartera sean ficticios. Para capturas usa
exclusivamente la configuracion demo documentada arriba.

Sharpe/Sortino requieren tasa explicita; esta primera UI no la configura.
Las series de activos son `valuation_price_proxy`, no total return. Las
correlaciones pueden tener muestras distintas. La analitica no ejecuta agentes;
su ejecucion se confirma por separado en Operaciones. Nunca se
ofrece IA como autoridad de inversion. La facilidad de comprension en dos
minutos sigue siendo un objetivo pendiente de validar con usuarios.

## Desarrollo y validacion

```bash
npm run lint
npm run typecheck
npm test
npm run build
npm audit --audit-level=moderate
npx playwright install chromium
npm run test:e2e
```

`npm run preview` sirve el build solo en loopback y necesita la API arrancada.
El frontend se distribuye separado del wheel Python; no modifica su instalacion.
E2E exige puertos 8000/5173 libres, crea una copia sintetica temporal y no reutiliza
servidores existentes. En CI instala Chromium con `--with-deps`. No usa datos
reales ni proveedores externos. Ver [webServer de Playwright](https://playwright.dev/docs/test-webserver).

`npm run showcase` usa el mismo aislamiento para generar capturas, video y
portada en `test-results/showcase/`, sin modificar los assets versionados.
Revisalos antes de publicarlos: [procedimiento](../docs/showcase.md).

Para incluir la prueba de integracion con respuestas actuales, desde la raiz,
despues del bootstrap sintetico:

```powershell
.\.venv\Scripts\python.exe scripts/export_frontend_demo_contract.py
cd frontend
npm test
```

Sin `.test_tmp/frontend-api.json` se omite esa unica prueba de integracion;
CI siempre la genera. El JSON versionado en `src/test/synthetic-api.json` es
una proyeccion obtenida de FastAPI sobre la demo de abril de 2026, con historial
y crecimiento reducidos a sus extremos para los tests de componentes. No es
un fallback en runtime, ni un fichero de cartera real.

Arquitectura: `api.ts` transporte local; `contracts.ts` validacion Zod y tipos inferidos;
`App.tsx` navegacion/estados; `components.tsx` metricas, ayuda y graficos;
`format.ts` presentacion y mensajes; `styles.css` tema grafito/verde azulado,
fuentes del sistema y responsive. `Operations.tsx` coordina confirmacion/polling;
`operation-forms.tsx`, `operation-results.tsx` y `operations-api.ts` separan
formularios, resultados y contratos operativos. No duplicar formulas financieras aqui.

Referencias del stack: [React/TypeScript](https://react.dev/learn/typescript),
[Vite](https://vite.dev/guide/) y [API local](../docs/local_api.md).
