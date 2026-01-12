# Bekijk welke bestanden er staan in de gemounte Azure Blob Storage map
dbutils.fs.ls("/mnt/rawdata/")

# Lees het GML-bestand in met Spark XML-reader
df = spark.read \
    .format("xml") \                          # Gebruik de XML reader van Spark
    .option("rowTag", "gml:featureMember") \  # Definieer de XML-tag die één record bevat
    .load("/mnt/rawdata/kadastralekaart.gml")  # Pad naar het GML-bestand in Azure Blob Storage

# Toon het schema van de ingelezen data zodat je de structuur ziet
df.printSchema()

# Laat een tabelweergave van de data zien in de notebook (Databricks-specifiek)
df.display()
