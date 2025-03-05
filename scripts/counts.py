import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# Cargar datos
df_pivot = pd.read_csv("chunk_pivoteado_1.csv")

# Convertir "No Data" o strings vacíos en valores NaN reales
df_pivot.replace(["No Data", "", "nan"], np.nan, inplace=True)

# Conteo de valores por variable
conteo_por_variable = df_pivot.count()

# Calcular percentil 90 para seleccionar variables
percentil_90 = np.percentile(conteo_por_variable, 90)
variables_usables_90 = conteo_por_variable[conteo_por_variable > percentil_90].index.tolist()

# Filtrar variables que contienen "Temperature"
variables_temperatura = [var for var in variables_usables_90 if "Temperature" in var]
print("Variables relevantes para temperatura:", variables_temperatura)

# Configurar subgráficos
num_vars = len(variables_temperatura)
cols = 3  # Número de columnas en la figura
rows = (num_vars // cols) + (num_vars % cols > 0)  # Calcular filas necesarias

fig, axes = plt.subplots(rows, cols, figsize=(15, 5 * rows))  # Crear subgráficos

# Convertir `axes` en una lista para fácil iteración
axes = axes.flatten()

# Graficar cada variable en un subplot
for i, var in enumerate(variables_temperatura):
    data = df_pivot[var].dropna()
    
    if len(data) > 1:  # Verificar que haya datos suficientes
        axes[i].hist(data, bins=50, edgecolor="black", alpha=0.7)
        axes[i].set_xlabel(var)
        axes[i].set_ylabel("Frecuencia")
        axes[i].set_title(f"Distribución: {var}")
        axes[i].grid(True)
    else:
        axes[i].set_visible(False)  # Ocultar subgráfico si no hay suficientes datos

# Ajustar el diseño para evitar superposiciones
plt.tight_layout()
plt.show()

