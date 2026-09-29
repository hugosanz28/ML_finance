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
  non_positive_opening_valuation:
    "La valoración inicial es cero o no es válida. Completa efectivo, precios y divisas, o elige un periodo posterior con datos completos.",
  cash_balance_mismatch:
    "El efectivo reconstruido no cuadra con el saldo de DEGIRO. Revisa el Estado de cuenta y su cobertura antes de calcular rentabilidad.",
  stale_price:
    "La última cotización tiene más de siete días. Actualiza precios en Operaciones → Datos; ese intervalo queda sin valorar.",
  constant_cash_not_applicable:
    "No aplicable: el precio del efectivo en su propia moneda es constante y su correlación no está definida.",
  transaction_price_anchor:
    "Un activo sin snapshot se valora usando su precio de compra DEGIRO y la variación del proveedor. Es una aproximación histórica identificada.",
  dividend_receivable_reconstructed:
    "Tras la conversión de derechos, el dividendo pendiente se reconstruye con el importe bruto que DEGIRO liquidó después. Es una reconstrucción contable retrospectiva, no una cotización histórica.",
  asset_classifications_partial:
    "No se han podido obtener todas las clasificaciones de activos. Los sectores ausentes se mantienen sin clasificar.",
  pending_portfolio_import: "Hay snapshots de cartera más recientes pendientes de importar. Revisa Operaciones → Datos.",
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
  partial_benchmark_coverage:
    "La referencia no cubre todas las fechas solicitadas. La comparación muestra su cobertura y usa únicamente observaciones disponibles.",
  incomplete_calendar_alignment:
    "La cartera y la referencia no tienen datos coincidentes en todos los días. Las métricas comparativas usan las fechas comunes indicadas.",
  partial_portfolio_coverage:
    "La comparación dispone de valoraciones de cartera incompletas en parte del periodo.",
  partial_portfolio_returns:
    "Algunos retornos de cartera de la comparación son parciales. Consulta la cobertura de cada métrica.",
  portfolio_data_unavailable:
    "Todavía no hay una cartera importada. Prepara la demo o importa tus datos desde Operaciones → Datos.",
  connection_failed:
    "No se puede conectar con la API local. Arráncala en el puerto 8000 y vuelve a intentar.",
  workspace_busy:
    "Hay una operación en curso. Espera a que termine y vuelve a intentar.",
  invalid_response:
    "La API ha devuelto un formato inesperado. No se mostrarán datos sin validar.",
  benchmark_provider_unavailable:
    "Falta descargar el histórico real de benchmarks. Usa la actualización explícita; no se sustituye por datos ficticios.",
  benchmark_not_selected:
    "No hay una referencia seleccionada para comparar la cartera.",
  cash_flow_data_missing:
    "Faltan movimientos de efectivo: no podemos separar aportaciones y rentabilidad.",
  valuation_price_proxy:
    "El riesgo por activo usa precios de valoración aproximados, no rentabilidad total con dividendos.",
  partial_valuation_coverage:
    "Faltan valoraciones completas en parte del histórico. Esos intervalos se excluyen de la rentabilidad; actualiza precios y divisas en Operaciones → Datos.",
  incomplete_position_valuation:
    "Falta valorar alguna posición para calcular la diversificación completa.",
  risk_free_rate_required:
    "Esta métrica necesita una tasa libre de riesgo explícita.",
  risk_free_rate_missing:
    "Sharpe y Sortino no están disponibles sin una tasa libre de riesgo explícita.",
  zero_drawdown:
    "No se observa una caída; no se puede calcular un ratio que divida por ella.",
  partial_exposure_coverage:
    "Parte de las posiciones no tiene valoración o clasificación completa.",
  classification_missing:
    "Faltan categorías o sectores. Las categorías se definen en Operaciones → Configuración; los fondos no tienen desglose automático por sector.",
  incomplete_return_path:
    "Hay intervalos sin valoración completa. El drawdown necesita una trayectoria continua; completa los datos o selecciona un periodo más corto.",
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
  zero_return_variance:
    "No se puede calcular esta correlación porque al menos una serie no varía.",
  incomplete_coverage:
    "La cobertura es incompleta; interpreta este resultado con cautela.",
};
export function reason(code: string): string {
  const missingPrices = /^missing_price_positions:(\d+)$/.exec(code);
  if (missingPrices) {
    const count = Number(missingPrices[1]);
    return `${count} ${count === 1 ? "posición" : "posiciones"} sin precio o ancla de valoración.`;
  }
  return (
    reasons[code] ??
    "El servidor informa de una limitación adicional. Revisa el código de diagnóstico."
  );
}
