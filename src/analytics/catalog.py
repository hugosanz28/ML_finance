"""Neutral metric explanations shared by future application consumers."""

from dataclasses import dataclass


@dataclass(frozen=True)
class MetricDefinition:
    metric_id: str
    name: str
    description: str
    formula: str
    unit: str
    interpretation: str
    limitations: str
    required_data: str
    validity_conditions: str


def metric_catalog() -> tuple[MetricDefinition, ...]:
    """Return stable IDs with educational explanations, not investment advice."""
    daily = "Retornos diarios ajustados por flujos, en la misma moneda base."
    minimum = "Al menos 30 observaciones; calendario diario, 365 periodos al ano."
    historical = "Describe el historico disponible; no predice rendimientos futuros."
    definitions = [
        ("volatility_annualized", "Volatilidad anualizada", "Dispersion de los retornos diarios.",
         "stdev_muestral(r) * sqrt(365)", "decimal_annualized", "Mayor valor indica mas variacion historica.",
         historical, daily, minimum),
        ("downside_volatility_annualized", "Volatilidad negativa", "Desviacion por debajo de retorno cero.",
         "sqrt(mean(min(r, 0)^2) * 365)", "decimal_annualized", "Resume oscilaciones negativas, incluyendo ceros en el denominador.",
         "Umbral cero; no es la desviacion tipica calculada solo sobre dias negativos.", daily, minimum),
        ("sharpe", "Sharpe", "Exceso de retorno por unidad de volatilidad.",
         "mean(r - rf_diario) * 365 / volatilidad_anual", "ratio", "Compara exceso medio y dispersion bajo los supuestos indicados.",
         "Sensible a muestra, referencia y anualizacion; no es una calificacion de compra.", daily + " Tasa libre de riesgo anual explicita.", minimum + " Volatilidad distinta de cero."),
        ("sortino", "Sortino", "Exceso de retorno por unidad de desviacion bajo la referencia.",
         "mean(r - rf_diario) * 365 / sqrt(mean(min(r - rf_diario, 0)^2) * 365)", "ratio",
         "Distingue la variabilidad por debajo de la referencia.", "La tasa explicita actua tambien como retorno minimo aceptable.",
         daily + " Tasa anual explicita.", minimum + " Desviacion negativa no nula."),
        ("max_drawdown", "Maximo drawdown", "Mayor caida desde un maximo del indice TWR.",
         "min(indice_TWR / maximo_acumulado - 1)", "decimal", "Un valor de -0.20 representa una caida del 20 %.",
         "Depende de las fechas elegidas; no une tramos con huecos.", daily, "Trayectoria completa; al menos un retorno."),
        ("drawdown_duration_days", "Duracion del drawdown", "Mayor tiempo desde un maximo hasta recuperar o terminar el periodo.",
         "max(dias desde maximo anterior hasta recuperacion o final)", "days", "Incluye episodios aun no recuperados.",
         "Mide dias naturales dentro de la ventana; no cuenta caidas anteriores al inicio.", daily, "Trayectoria diaria completa."),
        ("calmar", "Calmar", "Retorno compuesto anualizado relativo a la mayor caida.",
         "(producto(1+r)^(365/dias) - 1) / abs(max_drawdown)", "ratio", "Relaciona crecimiento historico con caida maxima.",
         "Anualizar ventanas cortas amplifica resultados.", daily, minimum + " Trayectoria completa y drawdown no nulo."),
        ("exposure_weight", "Peso de exposicion", "Fraccion del valor conocido por activo, bucket, moneda, tipo o sector.",
         "valor_grupo / suma_valores_conocidos", "decimal", "Muestra cuanto del valor observado corresponde al grupo.",
         "Si falta valoracion los pesos son parciales; no hay look-through de ETFs. Sector exige fuente declarada.",
         "Posiciones, valores en moneda base y clasificaciones explicitas.", "Cartera long-only; valor total positivo."),
        ("concentration_hhi", "Concentracion HHI", "Suma de los pesos de los grupos al cuadrado.",
         "suma(peso_grupo^2)", "coefficient", "1 corresponde a toda la exposicion en un unico grupo.",
         "Depende de como se agrupe; no mide por si solo el riesgo financiero.", "Pesos por dimension.", "Clasificacion completa; valoracion parcial se identifica."),
        ("asset_correlation", "Correlacion entre activos", "Asociacion lineal entre retornos simultaneos.",
         "cov(r_i,r_j) / (std(r_i)*std(r_j))", "coefficient", "Valores de -1 a 1 describen co-movimiento historico.",
         "Muestras por pares pueden diferir; la matriz no garantiza ser semidefinida positiva.", daily + " Retornos de precio o totales por activo, nunca variaciones de cantidad.", minimum + " Varianzas positivas."),
        ("risk_contribution_share", "Contribucion aproximada al riesgo", "Participacion del activo en la varianza de la cartera de pesos fijos.",
         "w_i * cov(r_i, suma(w_j*r_j)) / var(suma(w_j*r_j))", "decimal", "Las participaciones suman uno y pueden ser negativas por diversificacion.",
         "Usa pesos actuales fijos y fechas comunes; no reproduce pesos historicos ni atribucion exacta.", daily + " Pesos long-only sumando uno.", minimum + " Muestra comun a todos los activos con peso positivo y varianza positiva."),
    ]
    benchmark_specs = [
        ("portfolio_total_return", "Retorno de cartera comparado", "producto(1+r_cartera)-1", "decimal"),
        ("benchmark_total_return", "Retorno del benchmark", "producto(1+r_benchmark)-1", "decimal"),
        ("relative_return", "Retorno relativo", "(1+R_cartera)/(1+R_benchmark)-1", "decimal"),
        ("portfolio_max_drawdown", "Drawdown de cartera comparado", "min(indice_cartera/maximo-1)", "decimal"),
        ("benchmark_max_drawdown", "Drawdown del benchmark", "min(indice_benchmark/maximo-1)", "decimal"),
        ("drawdown_difference", "Diferencia de drawdowns", "drawdown_cartera-drawdown_benchmark", "decimal"),
        ("correlation", "Correlacion con benchmark", "cov(r_p,r_b)/(std(r_p)*std(r_b))", "coefficient"),
        ("tracking_error_annualized", "Tracking error", "std(r_p-r_b)*sqrt(252)", "decimal_annualized"),
        ("beta", "Beta", "cov(r_p,r_b)/var(r_b)", "coefficient"),
        ("alpha_annualized", "Alpha de regresion", "(mean(r_p)-beta*mean(r_b))*252", "decimal_annualized"),
    ]
    for metric_id, name, formula, unit in benchmark_specs:
        definitions.append((
            metric_id, name, "Comparacion historica contra la referencia seleccionada.", formula, unit,
            "Se interpreta junto al benchmark, periodo y cobertura de la comparacion.",
            "Provider y calendario afectan el resultado; alpha es intercepto de regresion, no Jensen ajustado por tasa libre de riesgo.",
            "Retornos alineados de cartera y benchmark en la misma moneda.",
            "Contrato de benchmarks: 252 sesiones/ano; estadisticas requieren 30 pares; denominadores no nulos.",
        ))
    return tuple(MetricDefinition(*values) for values in definitions)
