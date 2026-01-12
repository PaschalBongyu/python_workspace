import requests, zipfile, io, os

# Download ZIP-bestand
pdok_zip_url = "https://api.pdok.nl/kadaster/kadastralekaart/download/v5_0/full/predefined/kadastralekaart-gml-nl-nohist.zip"
response = requests.get(pdok_zip_url, stream=True)
response.raise_for_status()

# PROBLEEM: ZIP volledig in geheugen laden (inefficiënt bij grote bestanden)
zip_bytes = io.BytesIO()
for chunk in response.iter_content(chunk_size=1024*256):
    zip_bytes.write(chunk)
zip_bytes.seek(0)

# Uitpakken en opslaan
with zipfile.ZipFile(zip_bytes) as zip_file:
    for filename in zip_file.namelist():
        dbfs_path = f"/dbfs/mnt/rawdata/{filename}"

        # PROBLEEM: handmatig padbeheer en lokale filesystem calls
        os.makedirs(os.path.dirname(dbfs_path), exist_ok=True)

        # PROBLEEM: handmatig in chunks schrijven
        with zip_file.open(filename) as file, open(dbfs_path, "wb") as f:
            while True:
                chunk = file.read(256*1024)
                if not chunk:
                    break
                f.write(chunk)

print("Bestanden uitgepakt en opgeslagen in /mnt/rawdata")
