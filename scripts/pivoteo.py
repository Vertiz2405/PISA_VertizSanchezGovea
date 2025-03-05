import pandas as pd
import json

# Lista de variables de interés
variables_interes = [
    "CONTIFORM_MMA.CONTIFORM_MMA1.ActualTemperatureCoolingCircuit2.0",
    "CONTIFORM_MMA.CONTIFORM_MMA1.BeltDriveSpeedSetPoint.0",
    "CONTIFORM_MMA.CONTIFORM_MMA1.CoolingAirTemperatureActualValue.0",
    "CONTIFORM_MMA.CONTIFORM_MMA1.CurrentPreformNeckFinishTemperature.0",
    "CONTIFORM_MMA.CONTIFORM_MMA1.CurrentPreformTemperatureOvenInfeed.0",
    "CONTIFORM_MMA.CONTIFORM_MMA1.CurrentProcessType_ConfigValue.0",
    "CONTIFORM_MMA.CONTIFORM_MMA1.CurrentTemperatureBrake.1",
    "CONTIFORM_MMA.CONTIFORM_MMA1.CurrentTemperatureBrake.2",
    "CONTIFORM_MMA.CONTIFORM_MMA1.CurrentTemperaturePressureDewPoint.0",
    "CONTIFORM_MMA.CONTIFORM_MMA1.CurrentTemperatureRotaryJoint.0",
    "CONTIFORM_MMA.CONTIFORM_MMA1.EnergyDataHeatingControlLayer.1",
    "CONTIFORM_MMA.CONTIFORM_MMA1.EnergyDataHeatingControlLayer.2",
    "CONTIFORM_MMA.CONTIFORM_MMA1.EnergyDataHeatingControlLayer.3",
    "CONTIFORM_MMA.CONTIFORM_MMA1.EnergyDataHeatingControlLayer.4",
    "CONTIFORM_MMA.CONTIFORM_MMA1.EnergyDataHeatingControlLayer.5",
    "CONTIFORM_MMA.CONTIFORM_MMA1.EnergyDataHeatingControlLayer.6",
    "CONTIFORM_MMA.CONTIFORM_MMA1.EnergyDataHeatingControlLayer.7",
    "CONTIFORM_MMA.CONTIFORM_MMA1.EnergyDataHeatingControlLayer.8",
    "CONTIFORM_MMA.CONTIFORM_MMA1.EnergyDataHeatingControlLayer.9",
    "CONTIFORM_MMA.CONTIFORM_MMA1.EnergyDataHeatingControlLayer.10",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.1",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.2",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.3",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.4",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.5",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.6",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.7",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.8",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.9",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.10",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.11",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.12",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.13",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.14",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.15",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.16",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.17",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.18",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.19",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.20",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.21",
    "CONTIFORM_MMA.CONTIFORM_MMA1.Heater.22"
]

# Cargar CSV
df = pd.read_csv("data_2025-01-15.csv", dtype={"variable": str, "message": str}, low_memory=False)

# Filtrar variables de interés
df["variable"] = df["variable"].str.strip()
df_filtrado = df[df["variable"].isin(variables_interes)].copy()

# Convertir `message` en JSON o diccionario vacío
df_filtrado["message"] = df_filtrado["message"].apply(lambda x: json.loads(x) if isinstance(x, str) else {})

# Expandir JSON
df_expandido = df_filtrado.join(pd.json_normalize(df_filtrado["message"])).drop(columns=["message"])

# Asegurar que todas las variables estén en el dataframe
for var in variables_interes:
    if var not in df_expandido["variable"].values:
        df_expandido = pd.concat([df_expandido, pd.DataFrame([{"variable": var, "user_ts": None}])], ignore_index=True)

# Pivotear datos
df_pivot = df_expandido.pivot_table(index="user_ts", columns="variable", aggfunc="mean").reset_index()

# Aplanar nombres de columnas
df_pivot.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col for col in df_pivot.columns]

# Llenar valores faltantes
df_pivot.fillna("No Data", inplace=True)

# Guardar CSV
df_pivot.to_csv("chunk_pivoteado_1.csv", index=False)

print(f"Se pivotearon {len(df_pivot.columns)} columnas correctamente.")