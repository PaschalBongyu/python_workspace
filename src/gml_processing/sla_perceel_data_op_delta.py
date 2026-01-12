from pyspark.sql import SparkSession

# Start Spark-sessie
spark = SparkSession.builder.appName("GML Perceel Inlezen en Opslaan").getOrCreate()

# Basis pad naar de uitgepakte GML-bestanden
base_path = "/mnt/processeddata/unzipped_files/"

# Lees het perceel GML-bestand in als DataFrame (geflatteerd)
df_perceel = spark.read.format("xml") \
    .option("rowTag", "kk:perceelMember") \
    .load(base_path + "kadastralekaart_perceel.gml")

# Sla de DataFrame op als Delta-tabel
df_perceel.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dpt_test_bronze.bron.kadastraal_data_delta")

print("Data succesvol opgeslagen als Delta-tabel.")

# Stop de Spark-sessie
spark.stop()
