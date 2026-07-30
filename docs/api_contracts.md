# Contratos futuros de API local

## Estado

Este documento define contratos de API para una posible v2 local con FastAPI.
No implica que FastAPI exista ya ni que haya que implementarlo ahora.

La regla de diseno es simple: los endpoints deben ser capas finas sobre
`src/application/`. Si un endpoint necesita llamar directamente a
`src/portfolio/`, `src/reports/`, `src/agents/`, `src/market_data/` o
`src/degiro_exports/`, primero debe crearse o ampliarse un caso de uso en
`src/application/`.

## Convenciones

- Base path previsto: `/api/v1`.
- Todas las fechas viajan como `YYYY-MM-DD`.
- Los paths locales se devuelven como strings y deben tratarse como datos
  privados. La UI no debe exponerlos en una demo publica salvo que apunten al
  entorno demo.
- Las acciones que escriben en disco son `POST` o `PUT`.
- Las lecturas son `GET`.
- Los endpoints de larga duracion pueden empezar sin jobs y evolucionar despues
  a jobs locales si bloquean demasiado la UI.

Envelope comun para acciones:

```json
{
  "status": "succeeded",
  "message": "Human-readable summary.",
  "warnings": [],
  "artifacts": {}
}
```

`status` usa los mismos valores que `ApplicationResult`: `succeeded`,
`partial`, `failed` o `skipped`.

## 1. Cargar estado de cartera

### `GET /api/v1/portfolio/state`

Devuelve el read model principal para pintar cartera y resumen.

Caso de uso base:

- `GetPortfolioStateUseCase`

Query params:

| Nombre | Tipo | Default | Descripcion |
| --- | --- | --- | --- |
| `persist` | boolean | `true` | Si recalcular metricas debe persistir artefactos derivados. |
| `include_positions` | boolean | `true` | Incluye posiciones actuales. |
| `include_history` | boolean | `false` | Incluye serie diaria resumida. |
| `as_of_date` | date/null | `null` | Fecha de corte. Si se omite, ultima fecha disponible. |

Respuesta:

```json
{
  "as_of_date": "2026-05-31",
  "base_currency": "EUR",
  "summary": {
    "total_market_value_base": 12345.67,
    "total_unrealized_pnl_base": 456.78,
    "portfolio_return_pct": 0.0384,
    "drawdown_pct": -0.041,
    "valuation_coverage_ratio": 0.98,
    "net_external_contributions_base": 11000.0
  },
  "broker_snapshot": {
    "snapshot_date": "2026-05-30",
    "total_market_value_base": 12320.12
  },
  "positions": [
    {
      "asset_id": "IE00B44Z5B48",
      "asset_name": "SPDR MSCI ACWI UCITS ETF",
      "asset_type": "etf",
      "isin": "IE00B44Z5B48",
      "quantity": 10.0,
      "market_value_base": 1000.0,
      "weight": 0.08,
      "cost_basis_base": 900.0,
      "unrealized_pnl_base": 100.0,
      "unrealized_return_pct": 0.1111,
      "valuation_status": "valued_anchored"
    }
  ],
  "history": [
    {
      "valuation_date": "2026-05-31",
      "total_market_value_base": 12345.67,
      "drawdown_pct": -0.041,
      "valuation_coverage_ratio": 0.98
    }
  ],
  "data_quality": {
    "warnings": []
  }
}
```

Notas:

- La API no debe devolver DataFrames crudos.
- `GetPortfolioStateUseCase` ya transforma fechas, valores no finitos y tipos
  de pandas/numpy a un read model JSON estable.
- `data_quality.warnings` contiene codigos estables como
  `missing_price_positions:<count>` y `missing_fx_positions:<count>`.

## 2. Importar DEGIRO

### `POST /api/v1/degiro/import`

Importa CSVs canonicos de DEGIRO desde una carpeta local controlada.

Caso de uso base:

- `ImportDegiroUseCase`

Request:

```json
{
  "incoming_dir": "src/degiro_exports/local/incoming",
  "output_dir": null,
  "base_currency": "EUR",
  "account_id": null,
  "source_root": null,
  "ignore_unknown": false,
  "dry_run": false,
  "load_duckdb": true
}
```

Respuesta:

```json
{
  "status": "succeeded",
  "message": "Imported 4 DEGIRO file(s).",
  "warnings": [],
  "artifacts": {
    "incoming_dir": "src/degiro_exports/local/incoming",
    "output_dir": "src/data/local/normalized/degiro",
    "imported_count": 4,
    "failed_count": 0,
    "skipped_count": 0,
    "would_import_count": 0,
    "duckdb_rows": 1234
  }
}
```

Notas:

- En una UI publica/demo, este endpoint debe ejecutarse contra rutas demo.
- Los bytes subidos por una UI deben pasar primero por
  `SaveDegiroUploadsUseCase`, que detecta el tipo, genera un nombre canonico y
  solo escribe en `DEGIRO_EXPORTS_DIR/incoming`. La API no debe aceptar un path
  de destino arbitrario.
- `GetPendingDegiroImportStatusUseCase` permite avisar si hay snapshots de
  cartera recibidos que todavia no se han normalizado.

## 3. Refrescar datos

### `POST /api/v1/market-data/refresh`

Refresca FX y precios de mercado.

Casos de uso base:

- `InferFxRequirementsUseCase`
- `RefreshFxUseCase`
- `RefreshMarketDataUseCase`

Request:

```json
{
  "start_date": null,
  "end_date": "2026-06-23",
  "refresh_fx": true,
  "refresh_prices": true,
  "fx_provider": null,
  "price_provider": null,
  "fx_pairs": [],
  "infer_fx_from_normalized": true,
  "asset_ids": [],
  "include_inactive": false,
  "only_missing_base": false,
  "bootstrap_degiro_assets": true,
  "write_overrides_template": true
}
```

Respuesta:

```json
{
  "status": "partial",
  "message": "Market data refresh completed with warnings.",
  "warnings": [
    "Some assets require manual ticker overrides."
  ],
  "artifacts": {
    "fx": {
      "provider": "yfinance",
      "updated_pairs": 2,
      "skipped_pairs": 0,
      "rows_written": 120
    },
    "prices": {
      "provider": "yfinance",
      "synced_assets": 12,
      "updated_assets": 10,
      "skipped_assets": 2,
      "rows_written": 300,
      "override_template_path": "src/data/local/market_data/asset_overrides.csv"
    }
  }
}
```

Notas:

- Si el refresco tarda demasiado, este contrato puede evolucionar a:
  `POST /api/v1/jobs/market-data-refresh` + `GET /api/v1/jobs/{job_id}`.
- El frontend no debe asumir que `partial` es fallo total.
- Los providers de FX/precios soportados son `yfinance` y `synthetic`.
  `synthetic` es exclusivo de demo offline y conserva los datos sembrados; no
  inventa ni descarga nuevas series.

## 4. Generar informe

### `POST /api/v1/reports/monthly`

Genera el informe mensual Markdown.

Caso de uso base:

- `GenerateMonthlyReportUseCase`

Request:

```json
{
  "as_of_date": "2026-06-23",
  "output_dir": null,
  "normalized_degiro_dir": null,
  "persist": true
}
```

Respuesta:

```json
{
  "status": "succeeded",
  "message": "Monthly report generated for 2026-06-23.",
  "warnings": [],
  "artifacts": {
    "as_of_date": "2026-06-23",
    "output_path": "src/data/local/reports/monthly_report_2026-06-23.md",
    "base_currency": "EUR"
  }
}
```

### `GET /api/v1/reports/monthly/latest`

Lee metadatos del ultimo informe.

Caso de uso base:

- `GetLatestMonthlyReportUseCase`

Respuesta:

```json
{
  "status": "succeeded",
  "message": "Latest monthly report: monthly_2026-06-23.",
  "warnings": [],
  "artifacts": {
    "report_id": "monthly_2026-06-23",
    "as_of_date": "2026-06-23",
    "report_path": "src/data/local/reports/monthly_report_2026-06-23.md"
  }
}
```

## 5. Ejecutar agentes

### `POST /api/v1/agents/monthly-runs`

Ejecuta el pipeline mensual de agentes.

Caso de uso base:

- `RunMonthlyAgentsUseCase`
- `RunAgentQualityChecksUseCase` como preflight recomendado
- `BuildAgentDashboardSnapshotUseCase` si la UI envia snapshot preparado desde
  estado de cartera

Request:

```json
{
  "investment_brief_text": null,
  "investment_brief_path": null,
  "monthly_report_path": null,
  "user_satellite_interest": null,
  "llm_provider": "static",
  "search_provider": "null",
  "persist": true,
  "output_dir": null,
  "request_parameters": {
    "risk_budget": "bounded",
    "allow_suggestions": true
  },
  "portfolio_metrics_snapshot": null,
  "monthly_budget": 1500.0,
  "send_target_weights": true
}
```

Respuesta:

```json
{
  "status": "succeeded",
  "message": "Monthly agents completed successfully.",
  "warnings": [],
  "artifacts": {
    "run_id": "20260623T101500123456",
    "as_of_date": "2026-06-23",
    "output_dir": "src/data/local/agents/monthly_pipeline/20260623T101500123456",
    "monitor_tematico_status": "succeeded",
    "analista_activos_status": "succeeded",
    "asistente_aportacion_mensual_status": "succeeded"
  }
}
```

Notas:

- El contrato usa defaults offline seguros:
  `llm_provider=static` y `search_provider=null`. La demo publica puede optar
  por `static/static` para añadir contexto de busqueda sintetico.
- En ejecucion real, la API solo debe permitir providers configurados
  localmente. No debe exponer claves.
- Una futura API deberia tratar este endpoint como candidato claro a job local.
- Para ejecutar solo el monitor tematico existe
  `RunMonitorTematicoUseCase`, con los mismos nombres de providers y defaults
  `static/null`.

## 6. Leer auditoria de agentes

### `GET /api/v1/agents/monthly-runs`

Lista ejecuciones persistidas.

Caso de uso base:

- `ListAgentRunsUseCase`.

Query params:

| Nombre | Tipo | Default | Descripcion |
| --- | --- | --- | --- |
| `limit` | integer | `20` | Numero maximo de runs. |
| `environment` | string | `local` | `local` o `demo`, segun config activa. |

Respuesta:

```json
{
  "runs": [
    {
      "run_id": "20260623T101500123456",
      "as_of_date": "2026-06-23",
      "generated_at": "2026-06-23T10:15:00+02:00",
      "output_dir": "src/data/local/agents/monthly_pipeline/20260623T101500123456",
      "status": "succeeded",
      "agent_statuses": {
        "monitor_tematico": "succeeded",
        "analista_activos": "succeeded",
        "asistente_aportacion_mensual": "succeeded"
      }
    }
  ]
}
```

### `GET /api/v1/agents/monthly-runs/{run_id}`

Lee una ejecucion y sus artefactos principales.

Caso de uso base:

- `GetAgentRunAuditUseCase`.

Respuesta:

```json
{
  "run_id": "20260623T101500123456",
  "as_of_date": "2026-06-23",
  "status": "succeeded",
  "metadata": {
    "llm_provider": "static",
    "search_provider": "null"
  },
  "inputs": [
    {
      "key": "investment_brief",
      "label": "Investment brief",
      "location": "manual://investment-brief"
    }
  ],
  "agents": {
    "monitor_tematico": {
      "status": "succeeded",
      "summary": "Resumen corto del agente.",
      "warnings": []
    },
    "analista_activos": {
      "status": "succeeded",
      "summary": "Resumen corto del agente.",
      "warnings": []
    },
    "asistente_aportacion_mensual": {
      "status": "succeeded",
      "summary": "Resumen corto del agente.",
      "warnings": []
    }
  },
  "artifact_paths": {
    "pipeline_result": "src/data/local/agents/monthly_pipeline/20260623T101500123456/pipeline_result.json",
    "run_metadata": "src/data/local/agents/monthly_pipeline/20260623T101500123456/run_metadata.json",
    "input_payload": "src/data/local/agents/monthly_pipeline/20260623T101500123456/input_payload.json"
  }
}
```

Notas:

- El endpoint debe devolver resumen y metadatos, no necesariamente el JSON
  completo de auditoria si es muy grande.
- Para ver bruto: posible endpoint futuro
  `GET /api/v1/agents/monthly-runs/{run_id}/artifacts/{artifact_name}` con una
  allowlist estricta.

## 7. Cambiar brief y targets

### `GET /api/v1/settings/investment-brief`

Lee el brief activo.

Caso de uso base:

- `ReadInvestmentBriefUseCase`

Respuesta:

```json
{
  "content": "# Investment brief\n\nObjetivo: ...\n",
  "path": "src/data/local/investment_brief.md",
  "exists": true,
  "content_hash": "sha256:..."
}
```

### `PUT /api/v1/settings/investment-brief`

Actualiza el brief activo.

Caso de uso necesario:

- `UpdateInvestmentBriefUseCase`.

Request:

```json
{
  "content": "# Investment brief\n\nObjetivo: ...\n",
  "expected_previous_hash": "sha256:optional"
}
```

Respuesta:

```json
{
  "status": "succeeded",
  "message": "Investment brief updated.",
  "warnings": [],
  "artifacts": {
    "path": "src/data/local/investment_brief.md",
    "content_hash": "sha256:..."
  }
}
```

Notas:

- `expected_previous_hash` permite evitar sobrescribir cambios simultaneos si
  una UI deja el formulario abierto.
- La API debe escribir solo en la ruta configurada por `Settings`, no en paths
  arbitrarios enviados desde el cliente.

### `GET /api/v1/settings/portfolio-targets`

Lee targets estructurados activos.

Caso de uso base:

- `ReadTargetWeightsUseCase` para pesos simplificados.
- Conviene crear `ReadPortfolioTargetsUseCase` si la UI necesita todo el YAML
  validado, no solo `target_weights`.

Respuesta:

```json
{
  "target_weights": {
    "core": 0.8,
    "satellite": 0.2
  },
  "portfolio_targets": {
    "base_currency": "EUR",
    "monthly_contribution": 1500.0,
    "risk_profile": "balanced",
    "target_weights": {
      "core": 0.8,
      "satellite": 0.2
    }
  },
  "path": "src/data/local/portfolio_targets.yaml",
  "exists": true
}
```

### `PUT /api/v1/settings/portfolio-targets`

Actualiza targets estructurados.

Caso de uso necesario:

- Pendiente: `UpdatePortfolioTargetsUseCase`.

Request:

```json
{
  "portfolio_targets": {
    "base_currency": "EUR",
    "monthly_contribution": 1500.0,
    "risk_profile": "balanced",
    "target_weights": {
      "core": 0.8,
      "satellite": 0.2
    },
    "concentration_limits": {
      "single_asset_max": 0.2,
      "single_satellite_max": 0.1
    }
  },
  "expected_previous_hash": "sha256:optional"
}
```

Respuesta:

```json
{
  "status": "succeeded",
  "message": "Portfolio targets updated.",
  "warnings": [],
  "artifacts": {
    "path": "src/data/local/portfolio_targets.yaml",
    "content_hash": "sha256:..."
  }
}
```

Notas:

- Antes de persistir, el caso de uso debe validar con el mismo modelo que usa
  `load_portfolio_targets`.
- No conviene aceptar YAML libre desde la API. Mejor JSON estructurado validado
  y escritura controlada a YAML local.

## Casos de uso pendientes antes de FastAPI

Antes de implementar servidor, faltan estos casos de uso para que la API pueda
ser fina:

- `ReadPortfolioTargetsUseCase`: leer targets completos validados.
- `UpdatePortfolioTargetsUseCase`: validar y escribir targets estructurados.

## Orden recomendado

1. Implementar los casos de uso de lectura/escritura de targets pendientes sin
   FastAPI.
2. Adaptar Streamlit para usar esos casos de uso cuando aplique.
3. Anadir tests unitarios de contratos JSON sobre `src/application/`.
4. Solo entonces montar FastAPI como wrapper fino.
