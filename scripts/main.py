import pyarrow.parquet as pq
import os
import pandas as pd

parquet_folder = "/Users/diegovertiz/Documents/AACuarto Semestre/Análisis de Ciencia de Datos/PISA/PISA_VertizSanchezGovea/data"
parquet_files = ["DataEnero.parquet", "DataNoviembre.parquet"]

file_path = os.path.join(parquet_folder, parquet_files[0])
parquet_table = pq.ParquetFile(file_path)

chunk_size = 100000
it = 0

for batch in parquet_table.iter_batches(batch_size = chunk_size):
    it += 1
    df_chunk = batch.to_pandas()
    print(df_chunk.head())
    print(f'Chunk con: {len(df_chunk)} filas procesadas')
    print(it)


    

