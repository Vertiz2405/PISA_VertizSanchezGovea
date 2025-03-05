import pyarrow.parquet as pq
import os
import pandas as pd

# Ruta de la carpeta con los archivos
parquet_folder = "data"
parquet_files = ["DataEnero.parquet", "DataNoviembre.parquet"]

# Selección del archivo específico
file_path = os.path.join(parquet_folder, parquet_files[0])
parquet_table = pq.ParquetFile(file_path)

# Tamaño del chunk
chunk_size = 100000

# Fecha objetivo en formato YYYY-MM-DD
target_date = "2025-01-15"  #Cambia esto a la fecha que quieres filtrar

# Definir la ventana de tiempo ajustada (6 horas antes)
target_datetime = pd.to_datetime(target_date)

# Asignar una zona horaria a target_datetime (suponiendo UTC)
target_datetime = target_datetime.tz_localize("UTC")

# Ajustar el rango de tiempo
start_time = target_datetime - pd.Timedelta(hours=6)
end_time = target_datetime + pd.Timedelta(hours=18)  # 24 horas en total, pero empezando antes

# Nombre del archivo CSV de salida
output_csv = f"data_{target_date}.csv"

it = 0
df_result = []  # Lista para almacenar los datos filtrados

# Iterar sobre los chunks
for batch in parquet_table.iter_batches(batch_size=chunk_size):
    it += 1
    df_chunk = batch.to_pandas()

    # Asegurar que la columna 'user_ts' está en formato datetime y con zona horaria
    df_chunk["user_ts"] = pd.to_datetime(df_chunk["user_ts"], utc=True)  # Convierte a tz-aware en UTC

    # Filtrar por la nueva ventana de tiempo
    df_filtered = df_chunk[(df_chunk["user_ts"] >= start_time) & (df_chunk["user_ts"] < end_time)]

    # Si hay datos en la fecha deseada, los guardamos
    if not df_filtered.empty:
        df_result.append(df_filtered)

# Unir todos los chunks filtrados en un solo DataFrame
if df_result:
    df_final = pd.concat(df_result, ignore_index=True)
    
    print(f'Datos filtrados desde {start_time} hasta {end_time}: {len(df_final)} filas')

    # Mostrar las primeras 5 y las últimas 5 filas
    print("🔹 Primeras 5 filas:")
    print(df_final.head(5))

    print("\n🔹 Últimas 5 filas:")
    print(df_final.tail(5))

    # Guardar como CSV
    df_final.to_csv(output_csv, index=False)
    print(f'Archivo guardado como: {output_csv}')

else:
    print(f'⚠️ No se encontraron datos en la ventana de tiempo desde {start_time} hasta {end_time}')