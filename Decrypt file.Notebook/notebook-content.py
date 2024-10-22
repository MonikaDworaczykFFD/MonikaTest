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

import os
import subprocess
import shutil
import zipfile

# Ścieżki do plików
encrypted_zip_path = '/lakehouse/default/Files/unzipped/forefrontdermatology.zip.enc'
encrypted_key_path = '/lakehouse/default/Files/unzipped/key.bin.enc'
private_key_path = '/lakehouse/default/Files/keys/private_key.pem'  # Klucz prywatny z Lakehouse
local_encrypted_zip_path = '/mnt/data/forefrontdermatology.zip.enc'
local_encrypted_key_path = '/mnt/data/key.bin.enc'
decrypted_zip_path = '/mnt/data/forefrontdermatology.zip'
lakehouse_target_path = '/lakehouse/default/Files/decrypted/'
local_decrypted_key_path = '/mnt/data/key.bin'

# Krok 1: Utworzenie lokalnych katalogów, jeśli nie istnieją
os.makedirs('/mnt/data', exist_ok=True)

# Krok 2: Skopiowanie zaszyfrowanych plików i klucza prywatnego do systemu notebooka
shutil.copyfile(encrypted_zip_path, local_encrypted_zip_path)
shutil.copyfile(encrypted_key_path, local_encrypted_key_path)
shutil.copyfile(private_key_path, '/mnt/data/private_key.pem')  # Kopiowanie klucza do lokalnego systemu

# Krok 3: Odszyfrowanie pliku key.bin.enc przy użyciu klucza prywatnego
subprocess.run([
    'openssl', 'rsautl', '-decrypt',
    '-inkey', '/mnt/data/private_key.pem',
    '-in', local_encrypted_key_path,
    '-out', local_decrypted_key_path
])

# Sprawdzenie, czy klucz został odszyfrowany
if os.path.exists(local_decrypted_key_path):
    print("Klucz symetryczny został odszyfrowany.")
else:
    print("Błąd: Nie udało się odszyfrować klucza.")
    raise FileNotFoundError("Klucz nie został odszyfrowany.")

# Krok 4: Odszyfrowanie pliku ZIP za pomocą odszyfrowanego klucza symetrycznego
subprocess.run([
    'openssl', 'enc', '-d', '-aes-256-cbc',
    '-in', local_encrypted_zip_path,
    '-out', decrypted_zip_path,
    '-pass', f'file:{local_decrypted_key_path}'
])

# Sprawdzenie, czy plik ZIP został odszyfrowany
if os.path.exists(decrypted_zip_path):
    print("Plik ZIP został odszyfrowany.")
else:
    print("Błąd: Nie udało się odszyfrować pliku ZIP.")
    raise FileNotFoundError("Plik ZIP nie został odszyfrowany.")

# Krok 5: Rozpakowywanie pliku ZIP
extract_path = '/mnt/data/extracted_files'
os.makedirs(extract_path, exist_ok=True)

with zipfile.ZipFile(decrypted_zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

print("Plik ZIP został rozpakowany.")

# Krok 6: Skopiowanie rozpakowanych plików do folderu docelowego w Lakehouse
os.makedirs(lakehouse_target_path, exist_ok=True)

for root, dirs, files in os.walk(extract_path):
    for file in files:
        full_file_path = os.path.join(root, file)
        lakehouse_file_path = os.path.join(lakehouse_target_path, file)
        shutil.copyfile(full_file_path, lakehouse_file_path)
        print(f"Plik {file} został skopiowany do {lakehouse_file_path}")

# Krok 7: Czyszczenie plików tymczasowych
os.remove(local_decrypted_key_path)
os.remove(local_encrypted_zip_path)
os.remove(local_encrypted_key_path)
shutil.rmtree(extract_path)
os.remove(decrypted_zip_path)

print("Pliki tymczasowe zostały usunięte.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import subprocess
import shutil
import zipfile

# Ścieżki do plików
encrypted_zip_path = '/lakehouse/default/Files/unzipped/forefrontdermatology.zip.enc'
encrypted_key_path = '/lakehouse/default/Files/unzipped/key.bin.enc'
private_key_path = '/lakehouse/default/Files/keys/private_key.pem'  # Klucz prywatny z Lakehouse
local_encrypted_zip_path = '/mnt/data/forefrontdermatology.zip.enc'
local_encrypted_key_path = '/mnt/data/key.bin.enc'
decrypted_zip_path = '/mnt/data/forefrontdermatology.zip'
lakehouse_target_path = '/lakehouse/default/Files/decrypted/'
local_decrypted_key_path = '/mnt/data/key.bin'

# Krok 1: Utworzenie lokalnych katalogów, jeśli nie istnieją
os.makedirs('/mnt/data', exist_ok=True)

# Krok 2: Skopiowanie zaszyfrowanych plików i klucza prywatnego do systemu notebooka
shutil.copyfile(encrypted_zip_path, local_encrypted_zip_path)
shutil.copyfile(encrypted_key_path, local_encrypted_key_path)
shutil.copyfile(private_key_path, '/mnt/data/private_key.pem')  # Kopiowanie klucza do lokalnego systemu

# Krok 3: Odszyfrowanie pliku key.bin.enc przy użyciu klucza prywatnego
subprocess.run([
    'openssl', 'rsautl', '-decrypt',
    '-inkey', '/mnt/data/private_key.pem',
    '-in', local_encrypted_key_path,
    '-out', local_decrypted_key_path
])

# Sprawdzenie, czy klucz został odszyfrowany
if os.path.exists(local_decrypted_key_path):
    print("Klucz symetryczny został odszyfrowany.")
else:
    print("Błąd: Nie udało się odszyfrować klucza.")
    raise FileNotFoundError("Klucz nie został odszyfrowany.")

# Krok 4: Odszyfrowanie pliku ZIP za pomocą odszyfrowanego klucza symetrycznego z flagą -pbkdf2
subprocess.run([
    'openssl', 'enc', '-d', '-aes-256-cbc', '-pbkdf2',  # <- Dodanie flagi -pbkdf2
    '-in', local_encrypted_zip_path,
    '-out', decrypted_zip_path,
    '-pass', f'file:{local_decrypted_key_path}'
])

# Sprawdzenie, czy plik ZIP został odszyfrowany
if os.path.exists(decrypted_zip_path):
    print("Plik ZIP został odszyfrowany.")
else:
    print("Błąd: Nie udało się odszyfrować pliku ZIP.")
    raise FileNotFoundError("Plik ZIP nie został odszyfrowany.")

# Krok 5: Rozpakowywanie pliku ZIP
extract_path = '/mnt/data/extracted_files'
os.makedirs(extract_path, exist_ok=True)

with zipfile.ZipFile(decrypted_zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

print("Plik ZIP został rozpakowany.")

# Krok 6: Skopiowanie rozpakowanych plików do folderu docelowego w Lakehouse
os.makedirs(lakehouse_target_path, exist_ok=True)

for root, dirs, files in os.walk(extract_path):
    for file in files:
        full_file_path = os.path.join(root, file)
        lakehouse_file_path = os.path.join(lakehouse_target_path, file)
        shutil.copyfile(full_file_path, lakehouse_file_path)
        print(f"Plik {file} został skopiowany do {lakehouse_file_path}")

# Krok 7: Czyszczenie plików tymczasowych
os.remove(local_decrypted_key_path)
os.remove(local_encrypted_zip_path)
os.remove(local_encrypted_key_path)
shutil.rmtree(extract_path)
os.remove(decrypted_zip_path)

print("Pliki tymczasowe zostały usunięte.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import subprocess
import shutil
import zipfile

# Ścieżki do plików
encrypted_zip_path = '/lakehouse/default/Files/unzipped/forefrontdermatology.zip.enc'
encrypted_key_path = '/lakehouse/default/Files/unzipped/key.bin.enc'
private_key_path = '/lakehouse/default/Files/keys/private_key.pem'  # Klucz prywatny z Lakehouse
local_encrypted_zip_path = '/mnt/data/forefrontdermatology.zip.enc'
local_encrypted_key_path = '/mnt/data/key.bin.enc'
decrypted_zip_path = '/mnt/data/forefrontdermatology.zip'
lakehouse_target_path = '/lakehouse/default/Files/decrypted/'
local_decrypted_key_path = '/mnt/data/key.bin'

# Krok 1: Utworzenie lokalnych katalogów, jeśli nie istnieją
os.makedirs('/mnt/data', exist_ok=True)

# Krok 2: Skopiowanie zaszyfrowanych plików i klucza prywatnego do systemu notebooka
shutil.copyfile(encrypted_zip_path, local_encrypted_zip_path)
shutil.copyfile(encrypted_key_path, local_encrypted_key_path)
shutil.copyfile(private_key_path, '/mnt/data/private_key.pem')  # Kopiowanie klucza do lokalnego systemu

# Krok 3: Odszyfrowanie klucza symetrycznego (key.bin.enc) za pomocą klucza prywatnego (private_key.pem)
subprocess.run([
    'openssl', 'rsautl', '-decrypt',
    '-inkey', '/mnt/data/private_key.pem',
    '-in', local_encrypted_key_path,
    '-out', local_decrypted_key_path
])

# Sprawdzenie, czy klucz został odszyfrowany
if os.path.exists(local_decrypted_key_path):
    print("Klucz symetryczny został odszyfrowany.")
else:
    print("Błąd: Nie udało się odszyfrować klucza.")
    raise FileNotFoundError("Klucz nie został odszyfrowany.")

# Krok 4: Odszyfrowanie pliku ZIP za pomocą odszyfrowanego klucza symetrycznego (bez -pbkdf2, bo w BAT tego nie było)
subprocess.run([
    'openssl', 'enc', '-d', '-aes-256-cbc',  # <- To odwzorowuje dokładnie stary skrypt
    '-in', local_encrypted_zip_path,
    '-out', decrypted_zip_path,
    '-pass', f'file:{local_decrypted_key_path}'
])

# Sprawdzenie, czy plik ZIP został odszyfrowany
if os.path.exists(decrypted_zip_path):
    print("Plik ZIP został odszyfrowany.")
else:
    print("Błąd: Nie udało się odszyfrować pliku ZIP.")
    raise FileNotFoundError("Plik ZIP nie został odszyfrowany.")

# Krok 5: Rozpakowanie pliku ZIP
extract_path = '/mnt/data/extracted_files'
os.makedirs(extract_path, exist_ok=True)

# Użycie 7zip (tak jak w oryginalnym skrypcie BAT) zamiast wbudowanej biblioteki zipfile
subprocess.run([
    '7z', 'x', decrypted_zip_path, f'-o{extract_path}'
])

print("Plik ZIP został rozpakowany.")

# Krok 6: Skopiowanie rozpakowanych plików do folderu docelowego w Lakehouse
os.makedirs(lakehouse_target_path, exist_ok=True)

for root, dirs, files in os.walk(extract_path):
    for file in files:
        full_file_path = os.path.join(root, file)
        lakehouse_file_path = os.path.join(lakehouse_target_path, file)
        shutil.copyfile(full_file_path, lakehouse_file_path)
        print(f"Plik {file} został skopiowany do {lakehouse_file_path}")

# Krok 7: Czyszczenie plików tymczasowych
os.remove(local_decrypted_key_path)
os.remove(local_encrypted_zip_path)
os.remove(local_encrypted_key_path)
shutil.rmtree(extract_path)
os.remove(decrypted_zip_path)

print("Pliki tymczasowe zostały usunięte.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import subprocess
import shutil
import zipfile

# Ścieżki do plików
encrypted_zip_path = '/lakehouse/default/Files/unzipped/forefrontdermatology.zip.enc'
encrypted_key_path = '/lakehouse/default/Files/unzipped/key.bin.enc'
private_key_path = '/lakehouse/default/Files/keys/private_key.pem'  # Klucz prywatny z Lakehouse
local_encrypted_zip_path = '/mnt/data/forefrontdermatology.zip.enc'
local_encrypted_key_path = '/mnt/data/key.bin.enc'
decrypted_zip_path = '/mnt/data/forefrontdermatology.zip'
lakehouse_target_path = '/lakehouse/default/Files/decrypted/'
local_decrypted_key_path = '/mnt/data/key.bin'

# Krok 1: Utworzenie lokalnych katalogów, jeśli nie istnieją
os.makedirs('/mnt/data', exist_ok=True)

# Krok 2: Skopiowanie zaszyfrowanych plików i klucza prywatnego do systemu notebooka
shutil.copyfile(encrypted_zip_path, local_encrypted_zip_path)
shutil.copyfile(encrypted_key_path, local_encrypted_key_path)
shutil.copyfile(private_key_path, '/mnt/data/private_key.pem')  # Kopiowanie klucza do lokalnego systemu

# Krok 3: Odszyfrowanie klucza symetrycznego (key.bin.enc) za pomocą klucza prywatnego (private_key.pem)
subprocess.run([
    'openssl', 'rsautl', '-decrypt',
    '-inkey', '/mnt/data/private_key.pem',
    '-in', local_encrypted_key_path,
    '-out', local_decrypted_key_path
])

# Sprawdzenie, czy klucz został odszyfrowany
if os.path.exists(local_decrypted_key_path):
    print("Klucz symetryczny został odszyfrowany.")
else:
    print("Błąd: Nie udało się odszyfrować klucza.")
    raise FileNotFoundError("Klucz nie został odszyfrowany.")

# Krok 4: Odszyfrowanie pliku ZIP za pomocą odszyfrowanego klucza symetrycznego
subprocess.run([
    'openssl', 'enc', '-d', '-aes-256-cbc',
    '-in', local_encrypted_zip_path,
    '-out', decrypted_zip_path,
    '-pass', f'file:{local_decrypted_key_path}'
])

# Sprawdzenie, czy plik ZIP został odszyfrowany
if os.path.exists(decrypted_zip_path):
    print("Plik ZIP został odszyfrowany.")
else:
    print("Błąd: Nie udało się odszyfrować pliku ZIP.")
    raise FileNotFoundError("Plik ZIP nie został odszyfrowany.")

# Krok 5: Rozpakowanie pliku ZIP za pomocą wbudowanej biblioteki zipfile
extract_path = '/mnt/data/extracted_files'
os.makedirs(extract_path, exist_ok=True)

try:
    with zipfile.ZipFile(decrypted_zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    print("Plik ZIP został rozpakowany.")
except zipfile.BadZipFile:
    print("Błąd: Plik ZIP jest uszkodzony lub nieprawidłowy.")

# Krok 6: Skopiowanie rozpakowanych plików do folderu docelowego w Lakehouse
os.makedirs(lakehouse_target_path, exist_ok=True)

for root, dirs, files in os.walk(extract_path):
    for file in files:
        full_file_path = os.path.join(root, file)
        lakehouse_file_path = os.path.join(lakehouse_target_path, file)
        shutil.copyfile(full_file_path, lakehouse_file_path)
        print(f"Plik {file} został skopiowany do {lakehouse_file_path}")

# Krok 7: Czyszczenie plików tymczasowych
os.remove(local_decrypted_key_path)
os.remove(local_encrypted_zip_path)
os.remove(local_encrypted_key_path)
shutil.rmtree(extract_path)
os.remove(decrypted_zip_path)

print("Pliki tymczasowe zostały usunięte.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import subprocess
import shutil
import zipfile

# Ścieżki do plików
encrypted_zip_path = '/lakehouse/default/Files/unzipped/forefrontdermatology.zip.enc'
encrypted_key_path = '/lakehouse/default/Files/unzipped/key.bin.enc'
private_key_path = '/lakehouse/default/Files/keys/private_key.pem'  # Klucz prywatny z Lakehouse
local_encrypted_zip_path = '/mnt/data/forefrontdermatology.zip.enc'
local_encrypted_key_path = '/mnt/data/key.bin.enc'
decrypted_zip_path = '/mnt/data/forefrontdermatology.zip'
lakehouse_target_path = '/lakehouse/default/Files/decrypted/'
local_decrypted_key_path = '/mnt/data/key.bin'

# Krok 1: Utworzenie lokalnych katalogów, jeśli nie istnieją
os.makedirs('/mnt/data', exist_ok=True)

# Krok 2: Skopiowanie zaszyfrowanych plików i klucza prywatnego do systemu notebooka
shutil.copyfile(encrypted_zip_path, local_encrypted_zip_path)
shutil.copyfile(encrypted_key_path, local_encrypted_key_path)
shutil.copyfile(private_key_path, '/mnt/data/private_key.pem')  # Kopiowanie klucza do lokalnego systemu

# Krok 3: Odszyfrowanie klucza symetrycznego (key.bin.enc) za pomocą klucza prywatnego (private_key.pem)
subprocess.run([
    'openssl', 'rsautl', '-decrypt',
    '-inkey', '/mnt/data/private_key.pem',
    '-in', local_encrypted_key_path,
    '-out', local_decrypted_key_path
])

# Sprawdzenie, czy klucz został odszyfrowany
if os.path.exists(local_decrypted_key_path):
    print("Klucz symetryczny został odszyfrowany.")
else:
    print("Błąd: Nie udało się odszyfrować klucza.")
    raise FileNotFoundError("Klucz nie został odszyfrowany.")

# Krok 4: Odszyfrowanie pliku ZIP za pomocą odszyfrowanego klucza symetrycznego z flagą -pbkdf2
subprocess.run([
    'openssl', 'enc', '-d', '-aes-256-cbc', '-pbkdf2',  # <- Używamy flagi -pbkdf2
    '-in', local_encrypted_zip_path,
    '-out', decrypted_zip_path,
    '-pass', f'file:{local_decrypted_key_path}'
])

# Sprawdzenie, czy plik ZIP został odszyfrowany
if os.path.exists(decrypted_zip_path):
    print("Plik ZIP został odszyfrowany.")
else:
    print("Błąd: Nie udało się odszyfrować pliku ZIP.")
    raise FileNotFoundError("Plik ZIP nie został odszyfrowany.")

# Krok 5: Rozpakowanie pliku ZIP za pomocą wbudowanej biblioteki zipfile
extract_path = '/mnt/data/extracted_files'
os.makedirs(extract_path, exist_ok=True)

try:
    with zipfile.ZipFile(decrypted_zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    print("Plik ZIP został rozpakowany.")
except zipfile.BadZipFile:
    print("Błąd: Plik ZIP jest uszkodzony lub nieprawidłowy.")

# Krok 6: Skopiowanie rozpakowanych plików do folderu docelowego w Lakehouse
os.makedirs(lakehouse_target_path, exist_ok=True)

for root, dirs, files in os.walk(extract_path):
    for file in files:
        full_file_path = os.path.join(root, file)
        lakehouse_file_path = os.path.join(lakehouse_target_path, file)
        shutil.copyfile(full_file_path, lakehouse_file_path)
        print(f"Plik {file} został skopiowany do {lakehouse_file_path}")

# Krok 7: Czyszczenie plików tymczasowych
os.remove(local_decrypted_key_path)
os.remove(local_encrypted_zip_path)
os.remove(local_encrypted_key_path)
shutil.rmtree(extract_path)
os.remove(decrypted_zip_path)

print("Pliki tymczasowe zostały usunięte.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import subprocess
import shutil
import zipfile

# Ścieżki do plików
encrypted_zip_path = '/lakehouse/default/Files/unzipped/forefrontdermatology.zip.enc'
encrypted_key_path = '/lakehouse/default/Files/unzipped/key.bin.enc'
private_key_path = '/lakehouse/default/Files/keys/private_key.pem'  # Klucz prywatny z Lakehouse
local_encrypted_zip_path = '/mnt/data/forefrontdermatology.zip.enc'
local_encrypted_key_path = '/mnt/data/key.bin.enc'
decrypted_zip_path = '/mnt/data/forefrontdermatology.zip'
lakehouse_target_path = '/lakehouse/default/Files/decrypted/'
local_decrypted_key_path = '/mnt/data/key.bin'

# Krok 1: Utworzenie lokalnych katalogów, jeśli nie istnieją
os.makedirs('/mnt/data', exist_ok=True)

# Krok 2: Skopiowanie zaszyfrowanych plików i klucza prywatnego do systemu notebooka
shutil.copyfile(encrypted_zip_path, local_encrypted_zip_path)
shutil.copyfile(encrypted_key_path, local_encrypted_key_path)
shutil.copyfile(private_key_path, '/mnt/data/private_key.pem')  # Kopiowanie klucza do lokalnego systemu

# Krok 3: Odszyfrowanie klucza symetrycznego (key.bin.enc) za pomocą klucza prywatnego (private_key.pem)
subprocess.run([
    'openssl', 'rsautl', '-decrypt',
    '-inkey', '/mnt/data/private_key.pem',
    '-in', local_encrypted_key_path,
    '-out', local_decrypted_key_path
])

# Sprawdzenie, czy klucz został odszyfrowany
if os.path.exists(local_decrypted_key_path):
    print("Klucz symetryczny został odszyfrowany.")
else:
    print("Błąd: Nie udało się odszyfrować klucza.")
    raise FileNotFoundError("Klucz nie został odszyfrowany.")

# Krok 4: Odszyfrowanie pliku ZIP za pomocą odszyfrowanego klucza symetrycznego (bez -pbkdf2)
subprocess.run([
    'openssl', 'enc', '-d', '-aes-256-cbc',  # <- bez używania -pbkdf2
    '-in', local_encrypted_zip_path,
    '-out', decrypted_zip_path,
    '-pass', f'file:{local_decrypted_key_path}'
])

# Sprawdzenie, czy plik ZIP został odszyfrowany
if os.path.exists(decrypted_zip_path):
    print("Plik ZIP został odszyfrowany.")
else:
    print("Błąd: Nie udało się odszyfrować pliku ZIP.")
    raise FileNotFoundError("Plik ZIP nie został odszyfrowany.")

# Krok 5: Rozpakowanie pliku ZIP za pomocą wbudowanej biblioteki zipfile
extract_path = '/mnt/data/extracted_files'
os.makedirs(extract_path, exist_ok=True)

try:
    with zipfile.ZipFile(decrypted_zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    print("Plik ZIP został rozpakowany.")
except zipfile.BadZipFile:
    print("Błąd: Plik ZIP jest uszkodzony lub nieprawidłowy.")

# Krok 6: Skopiowanie rozpakowanych plików do folderu docelowego w Lakehouse
os.makedirs(lakehouse_target_path, exist_ok=True)

for root, dirs, files in os.walk(extract_path):
    for file in files:
        full_file_path = os.path.join(root, file)
        lakehouse_file_path = os.path.join(lakehouse_target_path, file)
        shutil.copyfile(full_file_path, lakehouse_file_path)
        print(f"Plik {file} został skopiowany do {lakehouse_file_path}")

# Krok 7: Czyszczenie plików tymczasowych
os.remove(local_decrypted_key_path)
os.remove(local_encrypted_zip_path)
os.remove(local_encrypted_key_path)
shutil.rmtree(extract_path)
os.remove(decrypted_zip_path)

print("Pliki tymczasowe zostały usunięte.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import subprocess
import shutil
import hashlib
import zipfile

# Funkcja do obliczenia sumy kontrolnej pliku
def calculate_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

# Ścieżki do plików
encrypted_zip_path = '/lakehouse/default/Files/unzipped/forefrontdermatology.zip.enc'
encrypted_key_path = '/lakehouse/default/Files/unzipped/key.bin.enc'
private_key_path = '/lakehouse/default/Files/keys/private_key.pem'  # Klucz prywatny z Lakehouse
local_encrypted_zip_path = '/mnt/data/forefrontdermatology.zip.enc'
local_encrypted_key_path = '/mnt/data/key.bin.enc'
decrypted_zip_path = '/mnt/data/forefrontdermatology.zip'
lakehouse_target_path = '/lakehouse/default/Files/decrypted/'
local_decrypted_key_path = '/mnt/data/key.bin'

# Krok 1: Utworzenie lokalnych katalogów, jeśli nie istnieją
os.makedirs('/mnt/data', exist_ok=True)

# Krok 2: Skopiowanie zaszyfrowanych plików i klucza prywatnego do systemu notebooka
shutil.copyfile(encrypted_zip_path, local_encrypted_zip_path)
shutil.copyfile(encrypted_key_path, local_encrypted_key_path)
shutil.copyfile(private_key_path, '/mnt/data/private_key.pem')  # Kopiowanie klucza do lokalnego systemu

# Obliczanie sumy kontrolnej dla zaszyfrowanego pliku ZIP przed odszyfrowaniem
print(f"Suma kontrolna zaszyfrowanego pliku ZIP: {calculate_checksum(local_encrypted_zip_path)}")

# Krok 3: Odszyfrowanie klucza symetrycznego (key.bin.enc) za pomocą klucza prywatnego (private_key.pem)
subprocess.run([
    'openssl', 'rsautl', '-decrypt',
    '-inkey', '/mnt/data/private_key.pem',
    '-in', local_encrypted_key_path,
    '-out', local_decrypted_key_path
])

# Sprawdzenie, czy klucz został odszyfrowany
if os.path.exists(local_decrypted_key_path):
    print(f"Klucz symetryczny został odszyfrowany. Suma kontrolna klucza: {calculate_checksum(local_decrypted_key_path)}")
else:
    print("Błąd: Nie udało się odszyfrować klucza.")
    raise FileNotFoundError("Klucz nie został odszyfrowany.")

# Krok 4: Odszyfrowanie pliku ZIP za pomocą odszyfrowanego klucza symetrycznego (bez -pbkdf2)
subprocess.run([
    'openssl', 'enc', '-d', '-aes-256-cbc',
    '-in', local_encrypted_zip_path,
    '-out', decrypted_zip_path,
    '-pass', f'file:{local_decrypted_key_path}'
])

# Sprawdzenie, czy plik ZIP został odszyfrowany
if os.path.exists(decrypted_zip_path):
    print(f"Plik ZIP został odszyfrowany. Suma kontrolna odszyfrowanego pliku ZIP: {calculate_checksum(decrypted_zip_path)}")
else:
    print("Błąd: Nie udało się odszyfrować pliku ZIP.")
    raise FileNotFoundError("Plik ZIP nie został odszyfrowany.")

# Krok 5: Rozpakowanie pliku ZIP za pomocą wbudowanej biblioteki zipfile
extract_path = '/mnt/data/extracted_files'
os.makedirs(extract_path, exist_ok=True)

try:
    with zipfile.ZipFile(decrypted_zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    print("Plik ZIP został rozpakowany.")
except zipfile.BadZipFile:
    print("Błąd: Plik ZIP jest uszkodzony lub nieprawidłowy.")

# Krok 6: Skopiowanie rozpakowanych plików do folderu docelowego w Lakehouse
os.makedirs(lakehouse_target_path, exist_ok=True)

for root, dirs, files in os.walk(extract_path):
    for file in files:
        full_file_path = os.path.join(root, file)
        lakehouse_file_path = os.path.join(lakehouse_target_path, file)
        shutil.copyfile(full_file_path, lakehouse_file_path)
        print(f"Plik {file} został skopiowany do {lakehouse_file_path}")

# Krok 7: Czyszczenie plików tymczasowych
os.remove(local_decrypted_key_path)
os.remove(local_encrypted_zip_path)
os.remove(local_encrypted_key_path)
shutil.rmtree(extract_path)
os.remove(decrypted_zip_path)

print("Pliki tymczasowe zostały usunięte.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import subprocess
import shutil
import hashlib
import zipfile

# Funkcja do obliczenia sumy kontrolnej pliku
def calculate_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

# Ścieżki do plików
encrypted_zip_path = '/lakehouse/default/Files/unzipped/forefrontdermatology.zip.enc'
encrypted_key_path = '/lakehouse/default/Files/unzipped/key.bin.enc'
private_key_path = '/lakehouse/default/Files/keys/private_key.pem'  # Klucz prywatny z Lakehouse
local_encrypted_zip_path = '/mnt/data/forefrontdermatology.zip.enc'
local_encrypted_key_path = '/mnt/data/key.bin.enc'
decrypted_zip_path = '/mnt/data/forefrontdermatology.zip'
lakehouse_target_path = '/lakehouse/default/Files/decrypted/'
local_decrypted_key_path = '/mnt/data/key.bin'

# Krok 1: Utworzenie lokalnych katalogów, jeśli nie istnieją
os.makedirs('/mnt/data', exist_ok=True)

# Krok 2: Skopiowanie zaszyfrowanych plików i klucza prywatnego do systemu notebooka
shutil.copyfile(encrypted_zip_path, local_encrypted_zip_path)
shutil.copyfile(encrypted_key_path, local_encrypted_key_path)
shutil.copyfile(private_key_path, '/mnt/data/private_key.pem')  # Kopiowanie klucza do lokalnego systemu

# Obliczanie sumy kontrolnej dla zaszyfrowanego pliku ZIP przed odszyfrowaniem
print(f"Suma kontrolna zaszyfrowanego pliku ZIP: {calculate_checksum(local_encrypted_zip_path)}")

# Krok 3: Odszyfrowanie klucza symetrycznego (key.bin.enc) za pomocą klucza prywatnego (private_key.pem)
subprocess.run([
    'openssl', 'rsautl', '-decrypt',
    '-inkey', '/mnt/data/private_key.pem',
    '-in', local_encrypted_key_path,
    '-out', local_decrypted_key_path
])

# Sprawdzenie, czy klucz został odszyfrowany
if os.path.exists(local_decrypted_key_path):
    print(f"Klucz symetryczny został odszyfrowany. Suma kontrolna klucza: {calculate_checksum(local_decrypted_key_path)}")
else:
    print("Błąd: Nie udało się odszyfrować klucza.")
    raise FileNotFoundError("Klucz nie został odszyfrowany.")

# Krok 4: Odszyfrowanie pliku ZIP za pomocą odszyfrowanego klucza symetrycznego (bez -pbkdf2)
subprocess.run([
    'openssl', 'enc', '-d', '-aes-256-cbc',
    '-in', local_encrypted_zip_path,
    '-out', decrypted_zip_path,
    '-pass', f'file:{local_decrypted_key_path}'
])

# Sprawdzenie, czy plik ZIP został odszyfrowany
if os.path.exists(decrypted_zip_path):
    print(f"Plik ZIP został odszyfrowany. Suma kontrolna odszyfrowanego pliku ZIP: {calculate_checksum(decrypted_zip_path)}")
else:
    print("Błąd: Nie udało się odszyfrować pliku ZIP.")
    raise FileNotFoundError("Plik ZIP nie został odszyfrowany.")

# Krok 5: Rozpakowanie pliku ZIP za pomocą wbudowanej biblioteki zipfile
extract_path = '/mnt/data/extracted_files'
os.makedirs(extract_path, exist_ok=True)

try:
    with zipfile.ZipFile(decrypted_zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    print("Plik ZIP został rozpakowany.")
except zipfile.BadZipFile:
    print("Błąd: Plik ZIP jest uszkodzony lub nieprawidłowy.")

# Krok 6: Skopiowanie rozpakowanych plików do folderu docelowego w Lakehouse
os.makedirs(lakehouse_target_path, exist_ok=True)

for root, dirs, files in os.walk(extract_path):
    for file in files:
        full_file_path = os.path.join(root, file)
        lakehouse_file_path = os.path.join(lakehouse_target_path, file)
        shutil.copyfile(full_file_path, lakehouse_file_path)
        print(f"Plik {file} został skopiowany do {lakehouse_file_path}")

# Krok 7: Czyszczenie plików tymczasowych
os.remove(local_decrypted_key_path)
os.remove(local_encrypted_zip_path)
os.remove(local_encrypted_key_path)
shutil.rmtree(extract_path)
os.remove(decrypted_zip_path)

print("Pliki tymczasowe zostały usunięte.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import subprocess
import shutil
import hashlib
import zipfile

# Funkcja do obliczenia sumy kontrolnej pliku
def calculate_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

# Funkcja do weryfikacji, czy plik ma sygnaturę ZIP
def is_zip_file(file_path):
    with open(file_path, "rb") as f:
        signature = f.read(2)
        return signature == b'PK'  # Sygnatura ZIP zaczyna się od "PK"

# Ścieżki do plików
encrypted_zip_path = '/lakehouse/default/Files/unzipped/forefrontdermatology.zip.enc'
encrypted_key_path = '/lakehouse/default/Files/unzipped/key.bin.enc'
private_key_path = '/lakehouse/default/Files/keys/private_key.pem'  # Klucz prywatny z Lakehouse
local_encrypted_zip_path = '/mnt/data/forefrontdermatology.zip.enc'
local_encrypted_key_path = '/mnt/data/key.bin.enc'
decrypted_zip_path = '/mnt/data/forefrontdermatology.zip'
lakehouse_target_path = '/lakehouse/default/Files/decrypted/'
local_decrypted_key_path = '/mnt/data/key.bin'

# Krok 1: Utworzenie lokalnych katalogów, jeśli nie istnieją
os.makedirs('/mnt/data', exist_ok=True)

# Krok 2: Skopiowanie zaszyfrowanych plików i klucza prywatnego do systemu notebooka
shutil.copyfile(encrypted_zip_path, local_encrypted_zip_path)
shutil.copyfile(encrypted_key_path, local_encrypted_key_path)
shutil.copyfile(private_key_path, '/mnt/data/private_key.pem')  # Kopiowanie klucza do lokalnego systemu

# Obliczanie sumy kontrolnej dla zaszyfrowanego pliku ZIP przed odszyfrowaniem
print(f"Suma kontrolna zaszyfrowanego pliku ZIP: {calculate_checksum(local_encrypted_zip_path)}")

# Krok 3: Odszyfrowanie klucza symetrycznego (key.bin.enc) za pomocą klucza prywatnego (private_key.pem)
subprocess.run([
    'openssl', 'rsautl', '-decrypt',
    '-inkey', '/mnt/data/private_key.pem',
    '-in', local_encrypted_key_path,
    '-out', local_decrypted_key_path
])

# Sprawdzenie, czy klucz został odszyfrowany
if os.path.exists(local_decrypted_key_path):
    print(f"Klucz symetryczny został odszyfrowany. Suma kontrolna klucza: {calculate_checksum(local_decrypted_key_path)}")
else:
    print("Błąd: Nie udało się odszyfrować klucza.")
    raise FileNotFoundError("Klucz nie został odszyfrowany.")

# Krok 4: Odszyfrowanie pliku ZIP za pomocą odszyfrowanego klucza symetrycznego (bez -pbkdf2)
subprocess.run([
    'openssl', 'enc', '-d', '-aes-256-cbc',
    '-in', local_encrypted_zip_path,
    '-out', decrypted_zip_path,
    '-pass', f'file:{local_decrypted_key_path}'
])

# Sprawdzenie, czy plik ZIP został odszyfrowany
if os.path.exists(decrypted_zip_path):
    print(f"Plik ZIP został odszyfrowany. Suma kontrolna odszyfrowanego pliku ZIP: {calculate_checksum(decrypted_zip_path)}")
    
    # Sprawdzenie, czy plik ma sygnaturę ZIP (czy zaczyna się od "PK")
    if is_zip_file(decrypted_zip_path):
        print("Plik wygląda na poprawny plik ZIP (sygnatura ZIP jest obecna).")
    else:
        print("Błąd: Plik nie wygląda na poprawny plik ZIP (brak sygnatury ZIP).")

else:
    print("Błąd: Nie udało się odszyfrować pliku ZIP.")
    raise FileNotFoundError("Plik ZIP nie został odszyfrowany.")

# Krok 5: Rozpakowanie pliku ZIP za pomocą wbudowanej biblioteki zipfile
extract_path = '/mnt/data/extracted_files'
os.makedirs(extract_path, exist_ok=True)

try:
    with zipfile.ZipFile(decrypted_zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    print("Plik ZIP został rozpakowany.")
except zipfile.BadZipFile:
    print("Błąd: Plik ZIP jest uszkodzony lub nieprawidłowy.")

# Krok 6: Skopiowanie rozpakowanych plików do folderu docelowego w Lakehouse
os.makedirs(lakehouse_target_path, exist_ok=True)

for root, dirs, files in os.walk(extract_path):
    for file in files:
        full_file_path = os.path.join(root, file)
        lakehouse_file_path = os.path.join(lakehouse_target_path, file)
        shutil.copyfile(full_file_path, lakehouse_file_path)
        print(f"Plik {file} został skopiowany do {lakehouse_file_path}")

# Krok 7: Czyszczenie plików tymczasowych
os.remove(local_decrypted_key_path)
os.remove(local_encrypted_zip_path)
os.remove(local_encrypted_key_path)
shutil.rmtree(extract_path)
os.remove(decrypted_zip_path)

print("Pliki tymczasowe zostały usunięte.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import subprocess
import shutil
import hashlib

# Funkcja do obliczenia sumy kontrolnej pliku
def calculate_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

# Ścieżki do plików
encrypted_zip_path = '/lakehouse/default/Files/unzipped/forefrontdermatology.zip.enc'
encrypted_key_path = '/lakehouse/default/Files/unzipped/key.bin.enc'
private_key_path = '/lakehouse/default/Files/keys/private_key.pem'  # Klucz prywatny z Lakehouse
local_encrypted_zip_path = '/mnt/data/forefrontdermatology.zip.enc'
local_encrypted_key_path = '/mnt/data/key.bin.enc'
decrypted_zip_path = '/mnt/data/forefrontdermatology.zip'
lakehouse_target_path = '/lakehouse/default/Files/decrypted/'
local_decrypted_key_path = '/mnt/data/key.bin'

# Krok 1: Utworzenie lokalnych katalogów, jeśli nie istnieją
os.makedirs('/mnt/data', exist_ok=True)

# Krok 2: Skopiowanie zaszyfrowanych plików i klucza prywatnego do systemu notebooka
shutil.copyfile(encrypted_zip_path, local_encrypted_zip_path)
shutil.copyfile(encrypted_key_path, local_encrypted_key_path)
shutil.copyfile(private_key_path, '/mnt/data/private_key.pem')  # Kopiowanie klucza do lokalnego systemu

# Obliczanie sumy kontrolnej dla zaszyfrowanego pliku ZIP przed odszyfrowaniem
print(f"Suma kontrolna zaszyfrowanego pliku ZIP: {calculate_checksum(local_encrypted_zip_path)}")

# Krok 3: Odszyfrowanie klucza symetrycznego (key.bin.enc) za pomocą klucza prywatnego (private_key.pem)
subprocess.run([
    'openssl', 'rsautl', '-decrypt',
    '-inkey', '/mnt/data/private_key.pem',
    '-in', local_encrypted_key_path,
    '-out', local_decrypted_key_path
])

# Sprawdzenie, czy klucz został odszyfrowany
if os.path.exists(local_decrypted_key_path):
    print(f"Klucz symetryczny został odszyfrowany. Suma kontrolna klucza: {calculate_checksum(local_decrypted_key_path)}")
else:
    print("Błąd: Nie udało się odszyfrować klucza.")
    raise FileNotFoundError("Klucz nie został odszyfrowany.")

# Krok 4: Odszyfrowanie pliku ZIP za pomocą odszyfrowanego klucza symetrycznego (bez -pbkdf2)
subprocess.run([
    'openssl', 'enc', '-d', '-aes-256-cbc',
    '-in', local_encrypted_zip_path,
    '-out', decrypted_zip_path,
    '-pass', f'file:{local_decrypted_key_path}'
])

# Sprawdzenie, czy plik ZIP został odszyfrowany
if os.path.exists(decrypted_zip_path):
    print(f"Plik ZIP został odszyfrowany. Suma kontrolna odszyfrowanego pliku ZIP: {calculate_checksum(decrypted_zip_path)}")
    
    # Kopiowanie odszyfrowanego pliku do Lakehouse bez próby rozpakowania
    shutil.copyfile(decrypted_zip_path, os.path.join(lakehouse_target_path, 'forefrontdermatology.zip'))
    print(f"Odszyfrowany plik ZIP został zapisany w {lakehouse_target_path}")
else:
    print("Błąd: Nie udało się odszyfrować pliku ZIP.")
    raise FileNotFoundError("Plik ZIP nie został odszyfrowany.")

# Krok 5: Czyszczenie plików tymczasowych
os.remove(local_decrypted_key_path)
os.remove(local_encrypted_zip_path)
os.remove(local_encrypted_key_path)
os.remove(decrypted_zip_path)

print("Pliki tymczasowe zostały usunięte.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import shutil
import os
import subprocess

# Funkcja do sprawdzenia, czy plik ZIP ma prawidłową sygnaturę "PK"
def is_valid_zip(file_path):
    with open(file_path, 'rb') as file:
        signature = file.read(2)
        return signature == b'PK'

# Ścieżki do plików w Lakehouse
lakehouse_zip_enc = '/lakehouse/default/Files/unzipped/forefrontdermatology.zip.enc'
lakehouse_key_enc = '/lakehouse/default/Files/unzipped/key.bin.enc'
lakehouse_private_key = '/lakehouse/default/Files/keys/private_key.pem'

# Lokalne ścieżki do zapisania plików w notebooku
local_zip_enc = '/mnt/data/forefrontdermatology.zip.enc'
local_key_enc = '/mnt/data/key.bin.enc'
local_private_key = '/mnt/data/private_key.pem'
local_decrypted_key_path = '/mnt/data/key.bin'
decrypted_zip_path = '/mnt/data/forefrontdermatology_decrypted.zip'

# Upewnienie się, że lokalny katalog istnieje
os.makedirs('/mnt/data', exist_ok=True)

# Krok 1: Kopiowanie zaszyfrowanych plików i klucza prywatnego z Lakehouse do lokalnego systemu notebooka
print("Kopiowanie zaszyfrowanych plików i klucza prywatnego...")
shutil.copyfile(lakehouse_zip_enc, local_zip_enc)
shutil.copyfile(lakehouse_key_enc, local_key_enc)
shutil.copyfile(lakehouse_private_key, local_private_key)
print("Pliki zostały skopiowane.")

# Krok 2: Odszyfrowanie klucza symetrycznego (key.bin.enc) za pomocą klucza prywatnego (private_key.pem)
print("Odszyfrowywanie klucza symetrycznego...")
subprocess.run([
    'openssl', 'rsautl', '-decrypt',
    '-inkey', local_private_key,
    '-in', local_key_enc,
    '-out', local_decrypted_key_path
])

# Krok 3: Odszyfrowanie pliku ZIP z flagą -pbkdf2 (jeśli wymagana)
print("Odszyfrowywanie pliku ZIP...")
subprocess.run([
    'openssl', 'enc', '-d', '-a', '-aes-256-cbc', '-pbkdf2',
    '-in', local_zip_enc,
    '-out', decrypted_zip_path,
    '-pass', f'file:{local_decrypted_key_path}'
])

# Krok 4: Sprawdzenie, czy plik ZIP został poprawnie odszyfrowany
if os.path.exists(decrypted_zip_path):
    if is_valid_zip(decrypted_zip_path):
        print("Plik ZIP jest poprawny i gotowy do pobrania.")
    else:
        print("Błąd: Plik ZIP jest uszkodzony lub nie ma poprawnej sygnatury.")
else:
    print("Błąd: Plik nie został poprawnie odszyfrowany.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import subprocess
import shutil

# Ścieżki plików w Lakehouse
lakehouse_private_key = '/lakehouse/default/Files/keys/private_key.pem'
lakehouse_zip_enc = '/lakehouse/default/Files/unzipped/forefrontdermatology.zip.enc'
lakehouse_key_enc = '/lakehouse/default/Files/unzipped/key.bin.enc'

# Lokalne ścieżki do zapisania plików w systemie notebooka
local_private_key = '/mnt/data/private_key.pem'
local_zip_enc = '/mnt/data/forefrontdermatology.zip.enc'
local_key_enc = '/mnt/data/key.bin.enc'
local_decrypted_key = '/mnt/data/key.bin'
decrypted_zip_path = '/mnt/data/forefrontdermatology.zip'
extract_path = '/mnt/data/extracted_files'

# Upewnienie się, że katalog lokalny istnieje
os.makedirs('/mnt/data', exist_ok=True)

# Krok 1: Skopiowanie zaszyfrowanych plików i klucza prywatnego z Lakehouse do systemu notebooka
shutil.copyfile(lakehouse_private_key, local_private_key)
shutil.copyfile(lakehouse_zip_enc, local_zip_enc)
shutil.copyfile(lakehouse_key_enc, local_key_enc)

print("Pliki zostały skopiowane do lokalnego systemu.")

# Krok 2: Odszyfrowanie klucza symetrycznego (key.bin.enc) za pomocą klucza prywatnego (private_key.pem)
subprocess.run([
    'openssl', 'rsautl', '-decrypt',
    '-inkey', local_private_key,
    '-in', local_key_enc,
    '-out', local_decrypted_key
])

print("Klucz symetryczny został odszyfrowany.")

# Krok 3: Odszyfrowanie pliku ZIP za pomocą odszyfrowanego klucza symetrycznego
subprocess.run([
    'openssl', 'enc', '-d', '-aes-256-cbc',
    '-in', local_zip_enc,
    '-out', decrypted_zip_path,
    '-pass', f'file:{local_decrypted_key}'
])

# Sprawdzenie, czy plik ZIP został odszyfrowany
if os.path.exists(decrypted_zip_path):
    print(f"Plik ZIP został odszyfrowany: {decrypted_zip_path}")
else:
    print("Błąd: Plik ZIP nie został poprawnie odszyfrowany.")

# Krok 4: Rozpakowanie pliku ZIP za pomocą 7-Zip (jeśli masz dostęp do 7-Zip w swoim środowisku)
os.makedirs(extract_path, exist_ok=True)

try:
    subprocess.run(['7z', 'x', decrypted_zip_path, f'-o{extract_path}'])
    print(f"Plik ZIP został rozpakowany do folderu: {extract_path}")
except Exception as e:
    print(f"Błąd podczas rozpakowywania pliku ZIP: {e}")

# Krok 5: Kopiowanie rozpakowanych plików do folderu docelowego w Lakehouse (jeśli wymagane)
lakehouse_target_path = '/lakehouse/default/Files/decrypted/'
os.makedirs(lakehouse_target_path, exist_ok=True)

for root, dirs, files in os.walk(extract_path):
    for file in files:
        full_file_path = os.path.join(root, file)
        lakehouse_file_path = os.path.join(lakehouse_target_path, file)
        shutil.copyfile(full_file_path, lakehouse_file_path)
        print(f"Plik {file} został skopiowany do {lakehouse_file_path}")

# Czyszczenie plików tymczasowych
os.remove(local_decrypted_key)
os.remove(local_zip_enc)
os.remove(local_key_enc)
shutil.rmtree(extract_path)
os.remove(decrypted_zip_path)

print("Proces zakończony, pliki tymczasowe zostały usunięte.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import shutil
import subprocess
import os

# Ścieżki plików w Lakehouse
lakehouse_private_key = '/lakehouse/default/Files/keys/private_key.pem'
lakehouse_key_enc = '/lakehouse/default/Files/unzipped/key.bin.enc'

# Lokalne ścieżki do zapisania plików w notebooku
local_private_key = '/mnt/data/private_key.pem'
local_key_enc = '/mnt/data/key.bin.enc'
local_decrypted_key = '/mnt/data/key.bin'

# Ścieżka docelowa do zapisania odszyfrowanego pliku w Lakehouse
lakehouse_decrypted_key_path = '/lakehouse/default/Files/decrypted/key.bin'

# Upewnienie się, że lokalny katalog istnieje
os.makedirs('/mnt/data', exist_ok=True)

# Krok 1: Skopiowanie zaszyfrowanego klucza i klucza prywatnego z Lakehouse do lokalnego systemu notebooka
shutil.copyfile(lakehouse_private_key, local_private_key)
shutil.copyfile(lakehouse_key_enc, local_key_enc)

print("Pliki zostały skopiowane do lokalnego systemu.")

# Krok 2: Odszyfrowanie klucza symetrycznego (key.bin.enc) za pomocą klucza prywatnego (private_key.pem)
subprocess.run([
    'openssl', 'rsautl', '-decrypt',
    '-inkey', local_private_key,
    '-in', local_key_enc,
    '-out', local_decrypted_key
])

# Sprawdzenie, czy plik został odszyfrowany
if os.path.exists(local_decrypted_key):
    print(f"Klucz symetryczny został odszyfrowany: {local_decrypted_key}")
else:
    print("Błąd: Klucz symetryczny nie został poprawnie odszyfrowany.")

# Krok 3: Skopiowanie odszyfrowanego pliku do Lakehouse
shutil.copyfile(local_decrypted_key, lakehouse_decrypted_key_path)
print(f"Odszyfrowany klucz został zapisany w: {lakehouse_decrypted_key_path}")

# Czyszczenie plików tymczasowych
os.remove(local_decrypted_key)
os.remove(local_key_enc)
os.remove(local_private_key)

print("Proces zakończony, pliki tymczasowe zostały usunięte.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import shutil
import subprocess
import os

# Ścieżki plików w Lakehouse
lakehouse_zip_enc = '/lakehouse/default/Files/unzipped/forefrontdermatology.zip.enc'
lakehouse_decrypted_key = '/lakehouse/default/Files/decrypted/key.bin'

# Lokalne ścieżki do zapisania plików w notebooku
local_zip_enc = '/mnt/data/forefrontdermatology.zip.enc'
local_decrypted_key = '/mnt/data/key.bin'
decrypted_zip_path = '/mnt/data/forefrontdermatology.zip'

# Ścieżka do zapisania odszyfrowanego pliku ZIP w Lakehouse
lakehouse_decrypted_zip = '/lakehouse/default/Files/decrypted/forefrontdermatology.zip'

# Upewnienie się, że lokalny katalog istnieje
os.makedirs('/mnt/data', exist_ok=True)

# Krok 1: Skopiowanie zaszyfrowanego pliku ZIP i odszyfrowanego klucza z Lakehouse do lokalnego systemu notebooka
shutil.copyfile(lakehouse_zip_enc, local_zip_enc)
shutil.copyfile(lakehouse_decrypted_key, local_decrypted_key)

print("Pliki zostały skopiowane do lokalnego systemu.")

# Krok 2: Odszyfrowanie pliku ZIP przy użyciu odszyfrowanego klucza symetrycznego (bez -pbkdf2)
subprocess.run([
    'openssl', 'enc', '-d', '-aes-256-cbc',
    '-in', local_zip_enc,
    '-out', decrypted_zip_path,
    '-pass', '0ufodivBvLIQ3dbwtriRx0qKdUh/tl79g2ElnIJZhGS2GRCnzn4C2jlIN0GgekPWzRXbxx/wBNZf2K8+P1jWbBMtoSOodf2dy46DUEp8vKPr/atb/nvMjwWo/FYBQ49hAWBQIhHLh3+tKRB5kHD0gPmNa/rKl0vQ41XOf/ozrIU='
])

# Sprawdzenie, czy plik został odszyfrowany
if os.path.exists(decrypted_zip_path):
    print(f"Plik ZIP został odszyfrowany: {decrypted_zip_path}")
else:
    print("Błąd: Plik ZIP nie został poprawnie odszyfrowany.")

# Krok 3: Skopiowanie odszyfrowanego pliku ZIP do Lakehouse
shutil.copyfile(decrypted_zip_path, lakehouse_decrypted_zip)
print(f"Odszyfrowany plik ZIP został zapisany w: {lakehouse_decrypted_zip}")

# Czyszczenie plików tymczasowych
os.remove(decrypted_zip_path)
os.remove(local_zip_enc)
os.remove(local_decrypted_key)

print("Proces zakończony, pliki tymczasowe zostały usunięte.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import shutil
import subprocess
import os

# Ścieżki plików w Lakehouse
lakehouse_zip_enc = '/lakehouse/default/Files/unzipped/forefrontdermatology.zip.enc'

# Lokalne ścieżki do zapisania plików w notebooku
local_zip_enc = '/mnt/data/forefrontdermatology.zip.enc'
decrypted_zip_path = '/mnt/data/forefrontdermatology.zip'

# Ścieżka do zapisania odszyfrowanego pliku ZIP w Lakehouse
lakehouse_decrypted_zip = '/lakehouse/default/Files/decrypted/forefrontdermatology.zip'

# Twardy klucz symetryczny (przykład na podstawie zawartości key.bin)
hardcoded_key = """
0ufodivBvLIQ3dbwtriRx0qKdUh/tl79g2ElnIJZhGS2GRCnzn4C2jlIN0GgekPW
zRXbxx/wBNZf2K8+P1jWbBMtoSOodf2dy46DUEp8vKPr/atb/nvMjwWo/FYBQ49h
AWBQIhHLh3+tKRB5kHD0gPmNa/rKl0vQ41XOf/ozrIU=
"""

# Zapisywanie twardego klucza symetrycznego do pliku tymczasowego
local_key_path = '/mnt/data/hardcoded_key.bin'
with open(local_key_path, 'w') as key_file:
    key_file.write(hardcoded_key)

# Upewnienie się, że lokalny katalog istnieje
os.makedirs('/mnt/data', exist_ok=True)

# Krok 1: Skopiowanie zaszyfrowanego pliku ZIP z Lakehouse do lokalnego systemu notebooka
shutil.copyfile(lakehouse_zip_enc, local_zip_enc)

print("Pliki zostały skopiowane do lokalnego systemu.")

# Krok 2: Odszyfrowanie pliku ZIP przy użyciu twardego klucza symetrycznego
subprocess.run([
    'openssl', 'enc', '-d', '-aes-256-cbc',
    '-in', local_zip_enc,
    '-out', decrypted_zip_path,
    '-pass', f'file:{local_key_path}'
])

# Sprawdzenie, czy plik został odszyfrowany
if os.path.exists(decrypted_zip_path):
    print(f"Plik ZIP został odszyfrowany: {decrypted_zip_path}")
else:
    print("Błąd: Plik ZIP nie został poprawnie odszyfrowany.")

# Krok 3: Skopiowanie odszyfrowanego pliku ZIP do Lakehouse
shutil.copyfile(decrypted_zip_path, lakehouse_decrypted_zip)
print(f"Odszyfrowany plik ZIP został zapisany w: {lakehouse_decrypted_zip}")

# Czyszczenie plików tymczasowych
os.remove(decrypted_zip_path)
os.remove(local_zip_enc)
os.remove(local_key_path)

print("Proces zakończony, pliki tymczasowe zostały usunięte.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import shutil
import subprocess
import os

# Ścieżki plików w Lakehouse
lakehouse_zip_enc = '/lakehouse/default/Files/unzipped/forefrontdermatology.zip.enc'

# Lokalne ścieżki do zapisania plików w notebooku
local_zip_enc = '/mnt/data/forefrontdermatology.zip.enc'
decrypted_zip_path = '/mnt/data/forefrontdermatology.zip'

# Ścieżka do zapisania odszyfrowanego pliku ZIP w Lakehouse
lakehouse_decrypted_zip = '/lakehouse/default/Files/decrypted/forefrontdermatology.zip'

# Twardy klucz symetryczny (na podstawie zawartości podanego key.bin)
hardcoded_key = """
0ufodivBvLIQ3dbwtriRx0qKdUh/tl79g2ElnIJZhGS2GRCnzn4C2jlIN0GgekPW
zRXbxx/wBNZf2K8+P1jWbBMtoSOodf2dy46DUEp8vKPr/atb/nvMjwWo/FYBQ49h
AWBQIhHLh3+tKRB5kHD0gPmNa/rKl0vQ41XOf/ozrIU=
"""

# Zapisywanie twardego klucza symetrycznego do pliku tymczasowego
local_key_path = '/mnt/data/hardcoded_key.bin'
with open(local_key_path, 'w') as key_file:
    key_file.write(hardcoded_key)

# Upewnienie się, że lokalny katalog istnieje
os.makedirs('/mnt/data', exist_ok=True)

# Krok 1: Skopiowanie zaszyfrowanego pliku ZIP z Lakehouse do lokalnego systemu notebooka
shutil.copyfile(lakehouse_zip_enc, local_zip_enc)

print("Pliki zostały skopiowane do lokalnego systemu.")

# Krok 2: Odszyfrowanie pliku ZIP przy użyciu twardego klucza symetrycznego (bez pbkdf2)
subprocess.run([
    'openssl', 'enc', '-d', '-aes-256-cbc',
    '-in', local_zip_enc,
    '-out', decrypted_zip_path,
    '-pass', f'file:{local_key_path}'
])

# Sprawdzenie, czy plik został odszyfrowany
if os.path.exists(decrypted_zip_path):
    print(f"Plik ZIP został odszyfrowany: {decrypted_zip_path}")
else:
    print("Błąd: Plik ZIP nie został poprawnie odszyfrowany.")

# Krok 3: Skopiowanie odszyfrowanego pliku ZIP do Lakehouse
shutil.copyfile(decrypted_zip_path, lakehouse_decrypted_zip)
print(f"Odszyfrowany plik ZIP został zapisany w: {lakehouse_decrypted_zip}")

# Czyszczenie plików tymczasowych
os.remove(decrypted_zip_path)
os.remove(local_zip_enc)
os.remove(local_key_path)

print("Proces zakończony, pliki tymczasowe zostały usunięte.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
