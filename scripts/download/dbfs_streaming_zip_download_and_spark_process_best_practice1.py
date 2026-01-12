# Databricks-native GML ETL (direct naar /mnt/rawdata)

from pyspark.sql import SparkSession
import requests
import zipfile
from pathlib import Path

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Configuratie: URL en mountpoint paden
url = "https://api.pdok.nl/kadaster/kadastralekaart/download/v5_0/full/predefined/kadastralekaart-gml-nl-nohist.zip"
mnt_zip_path = "/mnt/rawdata/kadastralekaart-gml-nl-nohist.zip"
mnt_extract_path = "/mnt/rawdata/gml_data/"

# 1. Download ZIP rechtstreeks naar /mnt/rawdata
with requests.get(url, stream=True) as r:
    r.raise_for_status()
    with open(mnt_zip_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024*256):  # 256 KB chunks
            if chunk:
                f.write(chunk)

print("ZIP succesvol gedownload naar /mnt/rawdata")

# 2. Pak ZIP uit direct op /mnt/rawdata
Path(mnt_extract_path).mkdir(parents=True, exist_ok=True)
with zipfile.ZipFile(mnt_zip_path, 'r') as zip_ref:
    zip_ref.extractall(mnt_extract_path)

print("ZIP succesvol uitgepakt op /mnt/rawdata")

# 3. Toon uitgepakte bestanden
files = dbutils.fs.ls("mnt/rawdata/gml_data/")
print("Uitgepakte bestanden:", [f.name for f in files[:5]])  # eerste 5 tonen

# 4. Lees GML-bestanden in Spark DataFrame
df_gml = spark.read.text("/mnt/rawdata/gml_data/*.gml")
df_gml.show(10, truncate=False)
print(f"Totaal aantal regels geladen: {df_gml.count()}")

print("GML ETL proces voltooid, volledig mount-point-native en Spark-ready")
