# Databricks-native GML ETL

from pyspark.sql import SparkSession
import requests
import zipfile
from pathlib import Path

# Start Spark session
spark = SparkSession.builder.getOrCreate()


# Configuratie: URL en DBFS-paden

url = "https://api.pdok.nl/kadaster/kadastralekaart/download/v5_0/full/predefined/kadastralekaart-gml-nl-nohist.zip"
dbfs_zip_path = "/dbfs/mnt/rawdata/kadastralekaart-gml-nl-nohist.zip"
dbfs_extract_path = "/dbfs/mnt/rawdata/gml_data/"


# 1 Download ZIP rechtstreeks naar DBFS (streaming)

with requests.get(url, stream=True) as r:
    r.raise_for_status()
    with open(dbfs_zip_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024*256):  # 256 KB chunks
            if chunk:
                f.write(chunk)

print(" ZIP succesvol gedownload naar DBFS")


# 2 Pak ZIP uit direct op DBFS

Path(dbfs_extract_path).mkdir(parents=True, exist_ok=True)  # map aanmaken indien nodig
with zipfile.ZipFile(dbfs_zip_path, 'r') as zip_ref:
    zip_ref.extractall(dbfs_extract_path)

print(" ZIP succesvol uitgepakt op DBFS")


# 3 Toon uitgepakte bestanden

dbfs_uri = dbfs_extract_path.replace("/dbfs", "dbfs:")  # dbutils vereist dbfs: URI
files = dbutils.fs.ls(dbfs_uri)
print("Uitgepakte bestanden:", [f.name for f in files[:5]])  # eerste 5 tonen


# 4 Lees GML-bestanden in Spark DataFrame

df_gml = spark.read.text(dbfs_uri + "*.gml")  # laad alle GML-bestanden
df_gml.show(10, truncate=False)  # eerste 10 regels tonen
print(f"Totaal aantal regels geladen: {df_gml.count()}")

print(" GML ETL proces voltooid, volledig DBFS-native en Spark-ready")
