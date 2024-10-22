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

import shutil
import os

# Ścieżka do pliku w Lakehouse
lakehouse_file_path = '/lakehouse/default/Files/decrypted/forefrontdermatology.zip'

# Ścieżka lokalna, gdzie chcesz zapisać plik w notebooku
local_file_path = '/mnt/data/forefrontdermatology.zip'

# Upewnienie się, że katalog lokalny istnieje
os.makedirs('/mnt/data', exist_ok=True)

# Skopiowanie pliku z Lakehouse do lokalnego systemu notebooka
shutil.copyfile(lakehouse_file_path, local_file_path)

print(f"Plik jest zapisany w: {local_file_path}. Możesz go teraz pobrać.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from IPython.display import FileLink

# Ścieżka lokalna do pliku w systemie notebooka
local_file_path = '/mnt/data/forefrontdermatology.zip'

# Wyświetlenie linku do pobrania pliku
FileLink(local_file_path)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import shutil
import os

# Ścieżka lokalna, gdzie znajduje się plik ZIP
local_file_path = '/mnt/data/forefrontdermatology.zip'

# Ścieżka docelowa w Lakehouse, gdzie zostanie zapisany plik
lakehouse_destination_path = '/lakehouse/default/Files/decrypted/forefrontdermatology_download.zip'

# Skopiowanie pliku z lokalnego systemu notebooka do Lakehouse
shutil.copyfile(local_file_path, lakehouse_destination_path)

print(f"Plik został przeniesiony do Lakehouse pod: {lakehouse_destination_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import shutil
from IPython.display import FileLink

# Ścieżka lokalna, gdzie zapisaliśmy plik ZIP w systemie notebooka
local_file_path = '/mnt/data/forefrontdermatology.zip'

# Generowanie klikalnego linku do pobrania pliku
FileLink(local_file_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import shutil

# Ścieżka lokalna, gdzie znajduje się plik ZIP
local_file_path = '/mnt/data/forefrontdermatology.zip'

# Ścieżka docelowa w Lakehouse, gdzie zostanie zapisany plik do pobrania
lakehouse_destination_path = '/lakehouse/default/Files/decrypted/forefrontdermatology_download.zip'

# Skopiowanie pliku z lokalnego systemu notebooka do Lakehouse
shutil.copyfile(local_file_path, lakehouse_destination_path)

print(f"Plik został przeniesiony do Lakehouse pod: {lakehouse_destination_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import subprocess
import zipfile

# Funkcja do sprawdzenia, czy plik ZIP ma prawidłową sygnaturę "PK"
def is_valid_zip(file_path):
    with open(file_path, 'rb') as file:
        signature = file.read(2)
        return signature == b'PK'

# Ścieżki plików
encrypted_zip_path = '/lakehouse/default/Files/unzipped/forefrontdermatology.zip.enc'
encrypted_key_path = '/lakehouse/default/Files/unzipped/key.bin.enc'
private_key_path = '/lakehouse/default/Files/keys/private_key.pem'
local_encrypted_zip_path = '/mnt/data/forefrontdermatology.zip.enc'
local_decrypted_key_path = '/mnt/data/key.bin'
decrypted_zip_path = '/mnt/data/forefrontdermatology_decrypted.zip'

# Krok 1: Odszyfrowanie klucza symetrycznego
subprocess.run([
    'openssl', 'rsautl', '-decrypt',
    '-inkey', private_key_path,
    '-in', encrypted_key_path,
    '-out', local_decrypted_key_path
])

# Krok 2: Odszyfrowanie pliku ZIP
subprocess.run([
    'openssl', 'enc', '-d', '-aes-256-cbc',
    '-in', local_encrypted_zip_path,
    '-out', decrypted_zip_path,
    '-pass', f'file:{local_decrypted_key_path}'
])

# Krok 3: Weryfikacja, czy plik ZIP ma prawidłową sygnaturę
if os.path.exists(decrypted_zip_path):
    if is_valid_zip(decrypted_zip_path):
        print("Plik ZIP jest poprawny i gotowy do pobrania.")
    else:
        print("Plik ZIP jest uszkodzony lub nie ma poprawnej sygnatury.")
else:
    print("Błąd: Plik nie został poprawnie odszyfrowany.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import shutil
import os

# Ścieżki plików w Lakehouse
lakehouse_zip_enc = '/lakehouse/default/Files/unzipped/forefrontdermatology.zip.enc'
lakehouse_key_enc = '/lakehouse/default/Files/unzipped/key.bin.enc'

# Lokalne ścieżki do zapisania plików w notebooku
local_zip_enc = '/mnt/data/forefrontdermatology.zip.enc'
local_key_enc = '/mnt/data/key.bin.enc'

# Upewnienie się, że lokalny katalog istnieje
os.makedirs('/mnt/data', exist_ok=True)

# Kopiowanie zaszyfrowanych plików z Lakehouse do systemu notebooka
shutil.copyfile(lakehouse_zip_enc, local_zip_enc)
shutil.copyfile(lakehouse_key_enc, local_key_enc)

print("Pliki zostały skopiowane do lokalnego systemu notebooka.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import subprocess
import zipfile

# Funkcja do sprawdzenia, czy plik ZIP ma prawidłową sygnaturę "PK"
def is_valid_zip(file_path):
    with open(file_path, 'rb') as file:
        signature = file.read(2)
        return signature == b'PK'

# Lokalne ścieżki plików
local_decrypted_key_path = '/mnt/data/key.bin'
decrypted_zip_path = '/mnt/data/forefrontdermatology_decrypted.zip'

# Odszyfrowanie klucza symetrycznego
subprocess.run([
    'openssl', 'rsautl', '-decrypt',
    '-inkey', '/mnt/data/private_key.pem',
    '-in', '/mnt/data/key.bin.enc',
    '-out', local_decrypted_key_path
])

# Odszyfrowanie pliku ZIP
subprocess.run([
    'openssl', 'enc', '-d', '-aes-256-cbc',
    '-in', '/mnt/data/forefrontdermatology.zip.enc',
    '-out', decrypted_zip_path,
    '-pass', f'file:{local_decrypted_key_path}'
])

# Sprawdzenie, czy plik ZIP jest poprawny
if os.path.exists(decrypted_zip_path):
    if is_valid_zip(decrypted_zip_path):
        print("Plik ZIP jest poprawny i gotowy do pobrania.")
    else:
        print("Plik ZIP jest uszkodzony lub nie ma poprawnej sygnatury.")
else:
    print("Błąd: Plik nie został poprawnie odszyfrowany.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import shutil
import os

# Ścieżka do pliku private_key.pem w Lakehouse
lakehouse_private_key = '/lakehouse/default/Files/keys/private_key.pem'

# Ścieżka lokalna, gdzie zostanie zapisany plik private_key.pem
local_private_key = '/mnt/data/private_key.pem'

# Upewnienie się, że lokalny katalog istnieje
os.makedirs('/mnt/data', exist_ok=True)

# Skopiowanie klucza prywatnego z Lakehouse do systemu notebooka
shutil.copyfile(lakehouse_private_key, local_private_key)

print("Klucz prywatny został skopiowany do lokalnego systemu notebooka.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
