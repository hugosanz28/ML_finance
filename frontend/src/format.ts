export function format(
  value: number | null | undefined,
  unit = "decimal",
  currency = "EUR",
): string {
  if (value == null || !Number.isFinite(value)) return "No disponible";
  if (unit === "money")
    return new Intl.NumberFormat("es-ES", {
      style: "currency",
      currency,
      maximumFractionDigits: 2,
    }).format(value);
  if (unit.startsWith("decimal"))
    return new Intl.NumberFormat("es-ES", {
      style: "percent",
      maximumFractionDigits: 2,
    }).format(value);
  return `${new Intl.NumberFormat("es-ES", { maximumFractionDigits: 2 }).format(value)}${unit === "days" ? " días" : ""}`;
}
export function dateLabel(date: string | null | undefined): string {
  return date
    ? new Intl.DateTimeFormat("es-ES", {
        dateStyle: "medium",
        timeZone: "UTC",
      }).format(new Date(`${date}T00:00:00Z`))
    : "Sin fecha";
}
const reasons: Record<string, string> = {
  benchmark_etf_proxy:
    "La referencia usa un ETF de acumulación como aproximación, no el índice oficial. Incluye costes del fondo, tracking difference y precios de mercado.",
  benchmark_cache_missing:
    "Falta descargar el histórico de benchmarks. Ejecuta una actualización explícita; abrir esta vista no descarga datos.",
  benchmark_cache_invalid:
    "La copia local de benchmarks no supera su validación. Vuelve a descargarla; no se usarán datos sin verificar.",
  benchmark_cache_stale:
    "El histórico de la referencia termina antes del periodo solicitado. Actualiza la copia local.",
  benchmark_cache_currency_mismatch:
    "La copia de benchmarks pertenece a otra moneda base. Descárgala de nuevo para la moneda de tu cartera.",
  benchmark_missing_observations:
    "El proveedor ha devuelto cotizaciones ausentes. Se conservan como huecos, no como retornos cero.",
  benchmark_common_window_truncated:
    "La comparación empieza después del último hueco: solo usa el tramo común continuo indicado, no todo el periodo solicitado.",
  portfolio_data_unavailable:
    "Todavía no hay una cartera importada. Prepara la demo o importa tus datos desde Streamlit.",
  connection_failed:
    "No se puede conectar con la API local. Arráncala en el puerto 8000 y vuelve a intentar.",
  workspace_busy:
    "Hay una operación en curso. Espera a que termine y vuelve a intentar.",
  invalid_response:
    "La API ha devuelto un formato inesperado. No se mostrarán datos sin validar.",
  benchmark_provider_unavailable:
    "Falta descargar el histórico real de benchmarks. Usa la actualización explícita; no se sustituye por datos ficticios.",
  cash_flow_data_missing:
    "Faltan movimientos de efectivo: no podemos separar aportaciones y rentabilidad.",
  valuation_price_proxy:
    "El riesgo por activo usa precios de valoración aproximados, no rentabilidad total con dividendos.",
  risk_free_rate_required:
    "Esta métrica necesita una tasa libre de riesgo explícita.",
  risk_free_rate_missing:
    "Sharpe y Sortino no están disponibles sin una tasa libre de riesgo explícita.",
  zero_drawdown:
    "No se observa una caída; no se puede calcular un ratio que divida por ella.",
  partial_exposure_coverage:
    "Parte de las posiciones no tiene valoración o clasificación completa.",
  classification_missing:
    "Faltan clasificaciones: la concentración de esas categorías no está disponible.",
  incomplete_return_path:
    "Hay huecos en la serie; no se unen para calcular caídas desde máximos.",
  partial_return_coverage:
    "Algunas estadísticas usan solo una parte del histórico.",
  request_timeout:
    "La lectura ha tardado demasiado. Comprueba la API y vuelve a intentar.",
  definitions_unavailable:
    "No se ha podido leer el catálogo de explicaciones. Puedes volver a intentar.",
  analytics_unavailable:
    "No hay analítica disponible con los datos y el periodo seleccionados.",
  request_failed:
    "La lectura ha fallado. Comprueba el servidor local y vuelve a intentar.",
  insufficient_observations:
    "No hay suficientes observaciones para calcular esta métrica.",
  incomplete_coverage:
    "La cobertura es incompleta; interpreta este resultado con cautela.",
};
export function reason(code: string): string {
  return (
    reasons[code] ??
    "El servidor informa de una limitación adicional. Revisa el código de diagnóstico."
  );
}
