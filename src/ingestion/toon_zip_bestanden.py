
# Code om de bestanden in een ZIP-bestand te tonen!
# 1. Kopieer test.zip van DBFS naar de lokale driver node,
# 2. Open het ZIP-bestand,
# 3. Geef alle bestandsnamen weer,
# 4. En print ze, inclusief de laatste wijzigingsdatum.

import zipfile
import datetime

# Stap 1: Definieer de paden
zip_dbfs_path = "dbfs:/mnt/rawdata/test.zip"    # Pad in DBFS
zip_local_path = "/tmp/test.zip"                # Lokaal pad

# Stap 2: Kopieer het ZIP-bestand van DBFS naar het lokale bestandssysteem
dbutils.fs.cp(zip_dbfs_path, f"file:{zip_local_path}")

# Stap 3: Open het ZIP-bestand en toon de inhoud met wijzigingsdatum
with zipfile.ZipFile(zip_local_path, 'r') as zip_ref:
    print("Bestanden in test.zip met laatste wijzigingsdatum:\n")
    for file_info in zip_ref.infolist():
        file_name = file_info.filename
        mod_time = datetime.datetime(*file_info.date_time)
        print(f"{file_name} - Laatst gewijzigd: {mod_time}")
