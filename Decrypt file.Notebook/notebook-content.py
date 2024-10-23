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

# Ścieżki do plików
encrypted_zip_path = '/lakehouse/default/Files/unzipped/forefrontdermatology.zip.enc'
encrypted_key_path = '/lakehouse/default/Files/unzipped/key.bin.enc'
private_key_path = '/lakehouse/default/Files/keys/private_key.pem'  # Klucz prywatny z Lakehouse
local_encrypted_zip_path = '/mnt/data/forefrontdermatology.zip.enc'
local_encrypted_key_path = '/mnt/data/key.bin.enc'
decrypted_zip_path = '/mnt/data/forefrontdermatology.zip'
lakehouse_target_path = '/lakehouse/default/Files/dec_zip/'
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
    'openssl', 'enc', '-d', '-aes-256-cbc', 
    '-in', local_encrypted_zip_path,
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

# Krok 5: Skopiowanie odszyfrowanego pliku ZIP do folderu docelowego w Lakehouse
os.makedirs(lakehouse_target_path, exist_ok=True)

lakehouse_file_path = os.path.join(lakehouse_target_path, 'forefrontdermatology.zip')
shutil.copyfile(decrypted_zip_path, lakehouse_file_path)
print(f"Odszyfrowany plik ZIP został skopiowany do {lakehouse_file_path}")

# Krok 6: Czyszczenie plików tymczasowych
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

import os
import zipfile

# Ścieżki do plików i folderów
zip_file_path = '/lakehouse/default/Files/dec_zip/forefrontdermatology.zip'
extract_target_path = '/lakehouse/default/Files/filtered_2/'

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
