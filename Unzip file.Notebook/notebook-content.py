# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f239751c-e8f2-49be-87fd-5907f2cd88bc",
# META       "default_lakehouse_name": "DE_LH_BRONZE_ModMedEMA",
# META       "default_lakehouse_workspace_id": "effca747-308e-4d79-aeab-8d1360a96a36"
# META     }
# META   }
# META }

# CELL ********************

import zipfile
import os
import shutil
from pyspark.sql import SparkSession

# Utworzenie sesji Spark
spark = SparkSession.builder.getOrCreate()

# Ścieżka do pliku ZIP w Lakehouse (przykład)
zip_file_path = '/lakehouse/default/Files/forefrontdermatology_2024-10-16_enc.zip'

# Ścieżka, gdzie tymczasowo rozpakujesz plik ZIP
extract_path = '/mnt/data/extracted_files/'

# Ścieżka docelowa w Lakehouse, gdzie chcesz skopiować rozpakowane pliki
lakehouse_target_path = '/lakehouse/default/Files/unzipped/'

# Krok 1: Odczyt pliku ZIP bezpośrednio z Lakehouse
with open(zip_file_path, 'rb') as zip_file:
    # Krok 2: Rozpakowywanie pliku ZIP w lokalnym systemie notebooka
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

print("Plik ZIP został rozpakowany w lokalnym systemie notebooka.")

# Krok 3: Tworzenie folderu docelowego w Lakehouse (jeśli nie istnieje)
os.makedirs(lakehouse_target_path, exist_ok=True)

# Krok 4: Kopiowanie rozpakowanych plików do folderu docelowego na Lakehouse
for root, dirs, files in os.walk(extract_path):
    for file in files:
        full_file_path = os.path.join(root, file)
        lakehouse_file_path = os.path.join(lakehouse_target_path, file)
        
        # Kopiowanie pliku do Lakehouse
        shutil.copyfile(full_file_path, lakehouse_file_path)
        print(f"Plik {file} został skopiowany do {lakehouse_file_path}")

print("Wszystkie pliki zostały pomyślnie skopiowane do Lakehouse.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
