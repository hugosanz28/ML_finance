# Benchmarks y comparacion de rendimiento

## Objetivo

La capa de benchmarks permite comparar el TWR de la cartera con referencias
seleccionables y reproducibles. Un benchmark es una referencia de contexto, no
una recomendacion ni un objetivo universal para todas las carteras.

El catalogo y los providers viven en `src/market_data/benchmarks.py`. La
configuracion, el benchmark compuesto y las metricas comparativas viven en
`src/portfolio/benchmarks.py`. La futura API, UI y los agentes deben acceder a
esta funcionalidad mediante `src/application/`.

## Catalogo inicial

| ID estable | Referencia | Moneda nativa | Tipo de serie |
| --- | --- | --- | --- |
| `msci_world` | MSCI World Net Total Return EUR | EUR | retorno total |
| `sp500` | S&P 500 Total Return Index | USD | retorno total |
| `portfolio_60_40` | 60 % global equity / 40 % global aggregate bonds EUR hedged | EUR | retorno total compuesto |
| `estr_cash` | efectivo compuesto a €STR | EUR | retorno de efectivo |

El componente interno `global_aggregate_bonds_eur_hedged` no aparece como
opcion independiente. Se usa para construir el 60/40.

Las definiciones documentan de forma explicita si la serie representa precio,
retorno total, efectivo o una composicion. Cambiar de proveedor no debe cambiar
el significado del identificador.

## Seleccion y configuracion

La ruta se configura con `BENCHMARK_SELECTION_PATH`. El archivo JSON elige una
referencia principal, secundarias y, opcionalmente, pesos del compuesto:

```json
{
  "primary_benchmark_id": "msci_world",
  "secondary_benchmark_ids": ["sp500", "portfolio_60_40", "estr_cash"],
  "composite_weights": {
    "portfolio_60_40": {
      "msci_world": 0.6,
      "global_aggregate_bonds_eur_hedged": 0.4
    }
  }
}
```

Los IDs deben existir, no pueden repetirse y los pesos deben cubrir exactamente
los componentes y sumar `1.0`.

## Cartera 60/40

La baseline usa 60 % de MSCI World y 40 % de bonos global aggregate cubiertos a
EUR. Los pesos derivan con el rendimiento durante el mes y se restauran al
objetivo en la primera observacion comun de cada mes. La configuracion puede
cambiar los pesos, pero no inferir componentes por nombre.

## Calendario, divisa y cobertura

La comparacion usa los intervalos del calendario del benchmark. Si la cartera
tiene valoraciones durante un fin de semana, encadena sus retornos hasta cubrir
el intervalo viernes-lunes del benchmark. Un intervalo que no puede
reconstruirse se excluye y reduce la cobertura.

Las series se convierten antes de comparar. Los FX siguen la convencion del
repositorio: `EUR/USD` expresa unidades USD por una unidad EUR. Para convertir
un retorno USD a EUR:

```text
factor_EUR = (1 + retorno_USD) * FX_anterior / FX_actual
```

Huecos de calendario, FX ausente o cobertura parcial producen estado `partial`
o `unavailable` y un `reason_code`; no se rellenan con retornos cero.

## Metricas

Para cada benchmark se devuelven:

- crecimiento indexado desde 100 y drawdown de ambas series;
- retorno acumulado de cartera y benchmark;
- retorno relativo multiplicativo;
- maximo drawdown y su diferencia;
- correlacion;
- tracking error anualizado;
- beta y alpha diaria de regresion anualizada.

Correlacion, tracking error, beta y alpha requieren al menos 30 observaciones
alineadas. Beta y alpha tampoco se calculan si la varianza del benchmark es
cero. En esos casos el valor es `null`, nunca un cero ficticio.

## Demo y persistencia

`SyntheticBenchmarkProvider` genera series y FX deterministas sin red para las
cuatro opciones. La demo selecciona las cuatro en
`demo/synthetic_config/benchmark_selection.json`.

No se crea una tabla nueva: el dominio puede consumir retornos cargados mediante
`LoadedBenchmarkProvider` o el provider sintetico. La persistencia de una fuente
real se decidira junto con su adapter para evitar duplicar las tablas de precios
y FX existentes.

