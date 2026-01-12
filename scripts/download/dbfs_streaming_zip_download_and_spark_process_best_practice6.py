import requests
from pathlib import Path
import subprocess

# 1️ Configuratie: URL's en paden

url = "https://api.pdok.nl/kadaster/kadastralekaart/download/v5_0/full/predefined/kadastralekaart-gml-nl-nohist.zip"

# DBFS-paden (Python gebruikt /dbfs prefix)
mnt_zip_path = "/dbfs/mnt/rawdata/kadastralekaart-gml-nl-nohist.zip"
mnt_extract_path = "/dbfs/mnt/rawdata/gml_data/"

# Zorg dat de extractiemap bestaat
Path(mnt_extract_path).mkdir(parents=True, exist_ok=True)

# 2️ Download ZIP rechtstreeks naar DBFS mount

print("ZIP aan het downloaden...")
with requests.get(url, stream=True) as r:
    r.raise_for_status()
    with open(mnt_zip_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)
print("Download voltooid!")

# 3️ Unzip rechtstreeks in DBFS mount met bash

print("Bestanden aan het uitpakken...")
subprocess.run([
    "unzip", "-o", mnt_zip_path, "-d", mnt_extract_path
], check=True)
print("Uitpakken voltooid!")

# 4️ Toon uitgepakte bestanden

files = dbutils.fs.ls("/mnt/rawdata/gml_data/")
display(files)

# 5️ Lees GML-bestanden in Spark DataFrame (als tekst)

df_gml = spark.read.text("/mnt/rawdata/gml_data/*.gml")
display(df_gml)


