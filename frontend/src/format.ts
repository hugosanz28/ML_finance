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
  portfolio_data_unavailable:
    "Todavía no hay una cartera importada. Prepara la demo o importa tus datos desde Streamlit.",
  connection_failed:
    "No se puede conectar con la API local. Arráncala en el puerto 8000 y vuelve a intentar.",
  workspace_busy:
    "Hay una operación en curso. Espera a que termine y vuelve a intentar.",
  invalid_response:
    "La API ha devuelto un formato inesperado. No se mostrarán datos sin validar.",
  benchmark_provider_unavailable:
    "La fuente del benchmark real aún no está conectada. No se sustituye por datos ficticios.",
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
