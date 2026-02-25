# ML_finance

Proyecto de análisis financiero en Jupyter centrado en un único cuaderno reproducible.

## Archivo principal (se sube al repo)

- `analisis_financiero_cartera.ipynb`

## Que contiene el cuaderno

El notebook recorre un flujo completo de análisis y construccion de cartera:

1. Selección de empresas por sector y descarga de precios con `yfinance`.
2. Cálculo de retornos y exportacion inicial de datos (`financial_data.xlsx`).
3. Limpieza de datos:
   - filtrado de valores NaN por día y por empresa,
   - revisión de duplicados,
   - limpieza de días con retorno 0,
   - generacion del dataset limpio (`financial_data_clean.xlsx`).
4. Análisis por sector:
   - matrices de correlación por sector,
   - correlación entre sectores (retornos medios),
   - PCA sobre matriz de distancias.
5. Análisis global entre empresas:
   - matriz de correlacion global,
   - K-means,
   - selección del número de clusters con silhouette score.
6. Grafo de correlaciones y detección de comunidades (NetworkX + Louvain).
7. Algoritmo genético para optimización de cartera (in-sample).
8. Evaluación train/test de la cartera construida.

## Archivos generados por el cuaderno (no se suben)

- `financial_data.xlsx`
- `financial_data_clean.xlsx`

Tambien se excluyen notebooks locales:

- `alg_gen.ipynb`
- `mi_inv.ipynb`

## Requisitos

Python 3.10+ recomendado.

Instalación rapida:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Para abrir y ejecutar el cuaderno:

```powershell
jupyter lab
```

## Contenido del repo publico

```text
ML_finance/
|- analisis_financiero_cartera.ipynb
|- requirements.txt
|- .gitignore
|- .gitattributes
|- README.md
```
