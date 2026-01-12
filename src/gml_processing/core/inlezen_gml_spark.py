from pyspark.sql import SparkSession  # Importeer SparkSession om met Spark te werken

# Start een SparkSession (in Databricks is die vaak al actief)
spark = SparkSession.builder.appName("Inlezen GML").getOrCreate()
# - Maak een Spark-sessie of gebruik een bestaande
# - Geef de sessie de naam "Inlezen GML"

# Locatie van de GML-bestanden in DBFS (DataBricks File System)
gml_path = "/mnt/rawdata/*.gml"
# - Hier staan alle GML-bestanden die we willen inlezen
# - *.gml betekent: alle bestanden die eindigen op .gml

# Lees de GML-bestanden in met de Spark XML-reader
df = spark.read.format("xml") \
    .option("rowTag", "gml:featureMember") \  # Pas aan als nodig na inspectie
    .load(gml_path)
# - Gebruik de XML-lezer van Spark om de bestanden te openen
# - rowTag geeft aan welke tag één record is
# - Laad alle GML-bestanden uit de map in een DataFrame

# Bekijk de structuur van de DataFrame
df.printSchema()
# - Toont de kolommen en datatypes
# - Zo begrijp je hoe de data is opgebouwd

# Toon de eerste 5 regels van de data, zonder afkappen
df.show(5, truncate=False)
# - Laat een voorbeeld zien van de data
# - 5 rijen om een indruk te krijgen
# - truncate=False zorgt dat tekst niet wordt ingekort
