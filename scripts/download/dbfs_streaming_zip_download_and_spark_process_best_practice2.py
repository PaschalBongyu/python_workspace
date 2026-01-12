# Databricks-native GML ETL


from pyspark.sql import SparkSession
import requests


# Spark session

spark = SparkSession.builder.getOrCreate()


# Configuration

zip_url = "https://api.pdok.nl/kadaster/kadastralekaart/download/v5_0/full/predefined/kadastralekaart-gml-nl-nohist.zip"
zip_dbfs_path = "/dbfs/mnt/rawdata/kadastralekaart-gml-nl-nohist.zip"
extract_dbfs_path = "/dbfs/mnt/rawdata/gml_data/"

# Ensure extraction directory exists in DBFS
dbutils.fs.mkdirs("dbfs:/mnt/rawdata/gml_data/")


# 1. Stream download ZIP directly to DBFS (one-pass)

print("⬇ Downloading ZIP directly to DBFS...")
with requests.get(zip_url, stream=True) as r:
    r.raise_for_status()
    with open(zip_dbfs_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024*256):  # 256 KB chunks
            if chunk:
                f.write(chunk)

print(f" ZIP downloaded to {zip_dbfs_path}")


# 2. Extract ZIP directly on DBFS using system unzip

print(" Extracting ZIP directly on DBFS...")
# Use /dbfs/ path for mount point access
unzip -o /dbfs/mnt/rawdata/kadastralekaart-gml-nl-nohist.zip -d /dbfs/mnt/rawdata/gml_data/

print(f" ZIP extracted to {extract_dbfs_path}")


# 3. List first few extracted files

dbfs_uri = "dbfs:/mnt/rawdata/gml_data/"
files = dbutils.fs.ls(dbfs_uri)
print("First 5 extracted files:", [f.name for f in files[:5]])


# 4. Read GML files into Spark DataFrame

print(" Loading GML files into Spark DataFrame...")
df_gml = spark.read.text(dbfs_uri + "*.gml")

# Preview
df_gml.show(10, truncate=False)
print(f"Total lines loaded: {df_gml.count()}")

print(" GML ETL completed, fully DBFS-native and Spark-ready")
