import requests        # Voor het maken van HTTP-aanvragen
import zipfile         # Werken met ZIP-bestanden (inpakken/uitpakken)
import io              # Gegevens tijdelijk in het geheugen bewaren
import os              # Bestanden en mappen beheren (aanmaken, verwijderen)

# URL van het PDOK ZIP-bestand
pdok_zip_url = "https://api.pdok.nl/kadaster/kadastralekaart/download/v5_0/full/predefined/kadastralekaart-gml-nl-nohist.zip"

# Download het ZIP-bestand via streaming (zonder eerst lokaal op te slaan)
response = requests.get(pdok_zip_url, stream=True)
response.raise_for_status()  # Controleer of de download succesvol was (statuscode 200)

# Zet de gedownloade data om naar een in-memory bestand met BytesIO
zip_bytes = io.BytesIO(response.content)

# Open het ZIP-bestand vanuit het geheugen
with zipfile.ZipFile(zip_bytes) as zip_file:
    # Loop door alle bestanden in het ZIP-archief
    for filename in zip_file.namelist():
        print(f"Bezig met uploaden: {filename}")  # Laat zien welk bestand verwerkt wordt

        # Maak het volledige pad aan voor opslag in gemounte Azure Storage (DBFS pad)
        dbfs_path = f"/dbfs/mnt/rawdata/{filename}"

        # Zorg dat de benodigde mappen bestaan, maak ze aan indien nodig
        os.makedirs(os.path.dirname(dbfs_path), exist_ok=True)

        # Open het huidige bestand uit de ZIP
        with zip_file.open(filename) as file, open(dbfs_path, "wb") as f:
            # Lees het bestand in blokken (chunks) van 1 MB en schrijf deze direct weg
            while True:
                chunk = file.read(1024*1024)  # 1 MB per keer lezen
                if not chunk:
                    break  # Stop als er niets meer te lezen is
                f.write(chunk)  # Schrijf chunk naar bestand

# Bevestig dat alle bestanden succesvol zijn geüpload
print("Alle GML-bestanden uit de ZIP zijn geüpload naar Azure Storage via /mnt/rawdata")
