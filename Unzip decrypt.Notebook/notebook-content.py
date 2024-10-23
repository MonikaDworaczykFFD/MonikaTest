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

# CELL ********************

import os
import zipfile

# Ścieżki do plików i folderów
zip_file_path = '/lakehouse/default/Files/decrypted/forefrontdermatology.zip'
extract_target_path = '/lakehouse/default/Files/fltr/'

# Lista plików, które chcesz rozpakować z pełną ścieżką
files_to_extract = [
    'forefrontdermatology/appointment.txt',
    'forefrontdermatology/appointment_attachment.txt',
    'forefrontdermatology/appointment_attachment_snapshot.txt',
    'forefrontdermatology/appointment_insurance_policy.txt',
    'forefrontdermatology/appointment_insurance_policy_snapshot.txt',
    'forefrontdermatology/appointment_type.txt',
    'forefrontdermatology/batch.txt',
    'forefrontdermatology/bill.txt',
    'forefrontdermatology/bill_insurance.txt',
    'forefrontdermatology/bill_insurance_timely_filing.txt',
    'forefrontdermatology/bill_item.txt',
    'forefrontdermatology/bill_item_diagnosis.txt',
    'forefrontdermatology/charges.txt',
    'forefrontdermatology/claim.txt',
    'forefrontdermatology/claim_bill_item.txt',
    'forefrontdermatology/diagnosis.txt',
    'forefrontdermatology/diagnosis_measurement.txt',
    'forefrontdermatology/document_category.txt',
    'forefrontdermatology/exam_element.txt',
    'forefrontdermatology/exam_element_metadata.txt',
    'forefrontdermatology/exam_element_metadata_s.txt',
    'forefrontdermatology/facility.txt',
    'forefrontdermatology/file_attachment.txt',
    'forefrontdermatology/final_bill_diagnosis.txt',
    'forefrontdermatology/final_bill_procedure.txt',
    'forefrontdermatology/firm.txt',
    'forefrontdermatology/guarantor.txt',
    'forefrontdermatology/hpi_response.txt',
    'forefrontdermatology/icd10.txt',
    'forefrontdermatology/insurance_policy.txt',
    'forefrontdermatology/insurance_policy_authorization_attachment.txt',
    'forefrontdermatology/mips_ia_selection_ep.txt',
    'forefrontdermatology/mips_ia_selection_gp.txt',
    'forefrontdermatology/mips_pi_objective_score.txt',
    'forefrontdermatology/mips_pi_selection_ep.txt',
    'forefrontdermatology/mips_pi_selection_gp.txt',
    'forefrontdermatology/mips_quality_measure_score.txt',
    'forefrontdermatology/mips_quality_selection_ep.txt',
    'forefrontdermatology/mips_quality_selection_gp.txt',
    'forefrontdermatology/mips_score.txt',
    'forefrontdermatology/medication.txt',
    'forefrontdermatology/original_bill_diagnosis.txt',
    'forefrontdermatology/original_bill_procedure.txt',
    'forefrontdermatology/pathology_log.txt',
    'forefrontdermatology/pathology_log_action.txt',
    'forefrontdermatology/pathology_log_notification.txt',
    'forefrontdermatology/pathology_log_plan.txt',
    'forefrontdermatology/patient.txt',
    'forefrontdermatology/patient_adjustments.txt',
    'forefrontdermatology/patient_case_attachment.txt',
    'forefrontdermatology/payer.txt',
    'forefrontdermatology/payer_address.txt',
    'forefrontdermatology/payer_adjustments.txt',
    'forefrontdermatology/payments_posted.txt',
    'forefrontdermatology/payments_received.txt',
    'forefrontdermatology/pi_objective.txt',
    'forefrontdermatology/pm_note.txt',
    'forefrontdermatology/procedure.txt',
    'forefrontdermatology/procedure_body_location.txt',
    'forefrontdermatology/procedure_mips_quality.txt',
    'forefrontdermatology/production_summary.txt',
    'forefrontdermatology/provider_level_adjustment.txt',
    'forefrontdermatology/quality_measure.txt',
    'forefrontdermatology/referral_contact.txt',
    'forefrontdermatology/rx.txt',
    'forefrontdermatology/staff.txt',
    'forefrontdermatology/staff_mips_settings.txt',
    'forefrontdermatology/task.txt',
    'forefrontdermatology/task_staff_assignee.txt',
    'forefrontdermatology/unposted_charges.txt',
    'forefrontdermatology/visit.txt',
    'forefrontdermatology/visit_attendee.txt',
    'forefrontdermatology/visit_quality_measure.txt'
]

# Krok 1: Sprawdzenie, czy plik ZIP istnieje
if os.path.exists(zip_file_path):
    print("Plik ZIP istnieje, zaczynamy rozpakowywanie.")
else:
    raise FileNotFoundError(f"Plik ZIP {zip_file_path} nie istnieje.")

# Krok 2: Rozpakowywanie tylko wybranych plików
os.makedirs(extract_target_path, exist_ok=True)

with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    all_files_in_zip = zip_ref.namelist()  # Pobranie listy wszystkich plików w archiwum
    
    # Iterowanie po plikach, które chcesz wyodrębnić
    for file in files_to_extract:
        if file in all_files_in_zip:
            print(f"Rozpakowywanie pliku: {file}")
            zip_ref.extract(file, extract_target_path)  # Rozpakowanie do docelowego folderu
        else:
            print(f"Plik {file} nie istnieje w archiwum.")

print("Wybrane pliki zostały rozpakowane.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
