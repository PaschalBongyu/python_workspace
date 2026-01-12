import zipfile
import os

# Paden
zip_dbfs_path = "dbfs:/mnt/rawdata/test.zip"                    # ZIP bestand in DBFS
zip_local_path = "/tmp/test.zip"                                # Lokale tijdelijke locatie voor ZIP
extract_path = "/tmp/test_unzipped"                             # Lokale map voor uitgepakte bestanden
dbfs_extract_path = "dbfs:/mnt/processeddata/unzipped_files"   # Permanente DBFS map voor uitgepakte bestanden

# Stap 1: Kopieer ZIP bestand van DBFS naar lokale driver node
dbutils.fs.cp(zip_dbfs_path, f"file:{zip_local_path}")

# Stap 2: Maak lokale map aan om te unzippen (als deze nog niet bestaat)
os.makedirs(extract_path, exist_ok=True)

# Stap 3: Pak het ZIP bestand uit naar de lokale map
with zipfile.ZipFile(zip_local_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

print(f"Bestanden uitgepakt naar: {extract_path}")

# Stap 4: Upload elk bestand uit de uitgepakte map terug naar DBFS
for file_name in os.listdir(extract_path):
    local_file = os.path.join(extract_path, file_name)
    dbfs_file = f"{dbfs_extract_path}/{file_name}"
    
    dbutils.fs.cp(f"file:{local_file}", dbfs_file)
    print(f"Geüpload: {file_name} → {dbfs_file}")

print("Alle bestanden veilig opgeslagen in DBFS.")
