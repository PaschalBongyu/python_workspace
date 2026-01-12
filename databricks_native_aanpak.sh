#  Gecorrigeerde Databricks-native aanpak

# Stap 1: Download ZIP-bestand direct naar DBFS
# VERANDERING: geen io.BytesIO meer, bestand gaat direct naar DBFS
# VERANDERING: geen lokale filesystem calls (os/open)
# MAGIC %sh
wget -O /dbfs/mnt/rawdata/kadastralekaart.zip \
"https://api.pdok.nl/kadaster/kadastralekaart/download/v5_0/full/predefined/kadastralekaart-gml-nl-nohist.zip"

# Stap 2: Pak ZIP-bestand direct uit op DBFS
# VERANDERING: geen os.makedirs nodig, unzip doet dit automatisch
# VERANDERING: geen chunked writes of loops
# MAGIC %sh
unzip -o /dbfs/mnt/rawdata/kadastralekaart.zip -d /dbfs/mnt/rawdata/gml_data

# Stap 3: Inspecteer bestanden via DBFS
# VERANDERING: geen handmatig padbeheer, gebruik dbutils.fs
files = dbutils.fs.ls("dbfs:/mnt/rawdata/gml_data")
print("Uitgepakte bestanden:", [f.name for f in files])

# Stap 4: Lees GML-bestanden met Spark
# VERANDERING: Spark-read gebruiken i.p.v. lokaal open(), schaalbaar en Databricks-native
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.read.text("dbfs:/mnt/rawdata/gml_data/*.gml")
df.show(10, truncate=False)
