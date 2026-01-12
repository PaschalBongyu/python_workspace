import requests
import zipfile
from pathlib import Path

# Configuratie: URL en lokale paden
url = "https://api.pdok.nl/kadaster/kadastralekaart/download/v5_0/full/predefined/kadastralekaart-gml-nl-nohist.zip"
local_zip_path = "/tmp/kadastralekaart-gml-nl-nohist.zip"
local_extract_path = "/tmp/gml_data/"
mnt_zip_path = "/mnt/rawdata/kadastralekaart-gml-nl-nohist.zip"
mnt_extract_path = "/mnt/rawdata/gml_data/"

# 1. Download ZIP naar lokale driver storage
with requests.get(url, stream=True) as r:
    r.raise_for_status()
    with open(local_zip_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024*256):
            if chunk:
                f.write(chunk)

# 2. Kopieer ZIP naar mount point
dbutils.fs.cp(f"file:{local_zip_path}", mnt_zip_path)

# 3. Pak ZIP uit op lokale driver storage
Path(local_extract_path).mkdir(parents=True, exist_ok=True)
with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
    zip_ref.extractall(local_extract_path)

# 4. Kopieer uitgepakte bestanden naar mount point
dbutils.fs.mkdirs(mnt_extract_path)
for file in Path(local_extract_path).glob("*.gml"):
    dbutils.fs.cp(f"file:{file}", mnt_extract_path + file.name)

# 5. Toon uitgepakte bestanden
files = dbutils.fs.ls(mnt_extract_path)
displayHTML(
    "<ul>" + "".join([f"<li>{f.name}</li>" for f in files[:5]]) + "</ul>"
)

# 6. Lees GML-bestanden in Spark DataFrame
df_gml = spark.read.text(mnt_extract_path + "*.gml")
display(df_gml)