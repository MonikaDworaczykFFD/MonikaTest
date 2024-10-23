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
import subprocess

# Ścieżki plików
zip_file_path = '/lakehouse/default/Files/forefrontdermatology_2024-10-16_enc.zip'
local_zip_path = '/mnt/data/forefrontdermatology_2024-10-16_enc.zip'
extract_path = '/mnt/data/extracted_files/'
encrypted_zip_path = '/mnt/data/forefrontdermatology.zip.enc'
encrypted_key_path = '/mnt/data/key.bin.enc'
private_key_path = '/lakehouse/default/Files/keys/private_key.pem'
local_decrypted_key_path = '/mnt/data/key.bin'
decrypted_zip_path = '/mnt/data/forefrontdermatology.zip'
lakehouse_target_path = '/lakehouse/default/Files/decrypted/'

# Krok 1: Skopiowanie pliku ZIP z Lakehouse do lokalnej ścieżki notebooka
print("Pobieranie pliku ZIP z Lakehouse.")
shutil.copyfile(zip_file_path, local_zip_path)
print(f"Plik ZIP został pobrany i zapisany lokalnie: {local_zip_path}")

# Krok 2: Rozpakowanie zaszyfrowanego pliku ZIP
print("Rozpakowywanie zaszyfrowanego pliku ZIP.")
with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

# Krok 3: Wydrukowanie zawartości rozpakowanego folderu
print("Zawartość rozpakowanego folderu:")
print(os.listdir(extract_path))

# Krok 4: Zaktualizowanie ścieżki do plików wewnątrz podfolderu "forefrontdermatology"
forefront_path = os.path.join(extract_path, 'forefrontdermatology')

# Krok 5: Sprawdzenie, czy pliki .enc istnieją po rozpakowaniu w podfolderze
if 'forefrontdermatology.zip.enc' in os.listdir(forefront_path) and 'key.bin.enc' in os.listdir(forefront_path):
    print("Zaszyfrowane pliki zostały znalezione. Rozpoczynamy odszyfrowanie.")
else:
    raise FileNotFoundError("Nie znaleziono zaszyfrowanych plików w rozpakowanym archiwum.")

# Krok 6: Skopiowanie klucza prywatnego do systemu lokalnego
shutil.copyfile(private_key_path, '/mnt/data/private_key.pem')

# Krok 7: Odszyfrowanie klucza symetrycznego (key.bin.enc) za pomocą klucza prywatnego
subprocess.run([
    'openssl', 'rsautl', '-decrypt',
    '-inkey', '/mnt/data/private_key.pem',
    '-in', os.path.join(forefront_path, 'key.bin.enc'),
    '-out', local_decrypted_key_path
])

# Sprawdzenie, czy klucz został odszyfrowany
if os.path.exists(local_decrypted_key_path):
    print("Klucz symetryczny został odszyfrowany.")
else:
    print("Błąd: Nie udało się odszyfrować klucza.")
    raise FileNotFoundError("Klucz nie został odszyfrowany.")

# Krok 8: Odszyfrowanie pliku ZIP za pomocą odszyfrowanego klucza symetrycznego
subprocess.run([
    'openssl', 'enc', '-d', '-aes-256-cbc', 
    '-in', os.path.join(forefront_path, 'forefrontdermatology.zip.enc'),
    '-out', decrypted_zip_path,
    '-pass', f'file:{local_decrypted_key_path}',
    '-md', 'md5'
])

# Sprawdzenie, czy plik ZIP został odszyfrowany
if os.path.exists(decrypted_zip_path):
    print("Plik ZIP został odszyfrowany.")
else:
    print("Błąd: Nie udało się odszyfrować pliku ZIP.")
    raise FileNotFoundError("Plik ZIP nie został odszyfrowany.")

# Krok 9: Kopiowanie odszyfrowanego pliku ZIP do Lakehouse w folderze decrypted
os.makedirs(lakehouse_target_path, exist_ok=True)
lakehouse_file_path = os.path.join(lakehouse_target_path, 'forefrontdermatology.zip')
shutil.copyfile(decrypted_zip_path, lakehouse_file_path)
print(f"Odszyfrowany plik ZIP został skopiowany do {lakehouse_file_path}")

# Krok 10: Czyszczenie plików tymczasowych
os.remove(local_decrypted_key_path)
os.remove(local_zip_path)
shutil.rmtree(extract_path)
os.remove(decrypted_zip_path)

print("Pliki tymczasowe zostały usunięte.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
