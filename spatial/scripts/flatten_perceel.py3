from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split

# Start Spark session
spark = SparkSession.builder.appName("GML Perceel Inlezen en Opslaan").getOrCreate()

# Base path
base_path = "/mnt/processeddata/unzipped_files/"

# Read raw GML
df_perceel = spark.read.format("xml") \
    .option("rowTag", "kk:perceelMember") \
    .load(base_path + "kadastralekaart_perceel.gml")

# Flatten the fields
perceel_df_flat = (
    df_perceel
    .withColumn("perceel_id", col("oz:Perceel._gml:id"))
    .withColumn("identificatie_waarde", col("oz:Perceel.ko:identificatie._VALUE"))
    .withColumn("identificatie_domein", col("oz:Perceel.ko:identificatie._domein"))
    .withColumn("begin_geldigheid", col("oz:Perceel.oz:historie.h:beginGeldigheid").cast("timestamp"))
    .withColumn("tijdstip_registratie", col("oz:Perceel.oz:historie.h:tijdstipRegistratie").cast("timestamp"))
    .withColumn("status_historie", col("oz:Perceel.oz:historie.h:statusHistorie.h:StatusHistorie.t:waarde"))
    .withColumn("gemeente_code", col("oz:Perceel.oz:kadastraleAanduiding.oz:TypeKadastraleAanduiding.oz:aKRKadastraleGemeenteCode.oz:AKRKadastraleGemeenteCode.t:waarde"))
    .withColumn("sectie", col("oz:Perceel.oz:kadastraleAanduiding.oz:TypeKadastraleAanduiding.oz:sectie"))
    .withColumn("perceelnummer", col("oz:Perceel.oz:kadastraleAanduiding.oz:TypeKadastraleAanduiding.oz:perceelnummer").cast("int"))
    .withColumn("oppervlakte", col("oz:Perceel.oz:kadastraleGrootte.oz:TypeOppervlak.oz:waarde").cast("double"))
    .withColumn("rotatie", col("oz:Perceel.oz:perceelnummerRotatie"))
    .withColumn("deltaX", col("oz:Perceel.oz:perceelnummerVerschuiving.oz:TypePerceelnummerVerschuiving.oz:deltaX").cast("double"))
    .withColumn("deltaY", col("oz:Perceel.oz:perceelnummerVerschuiving.oz:TypePerceelnummerVerschuiving.oz:deltaY").cast("double"))
    .withColumn("coords_punt", split(col("oz:Perceel.oz:plaatscoordinaten.gml:Point.gml:pos"), " "))
    .withColumn("exterior_coords", split(col("oz:Perceel.oz:begrenzingPerceel.gml:Polygon.gml:exterior.gml:LinearRing.gml:posList"), " "))
)

# Optional: ensure DB exists
spark.sql("CREATE DATABASE IF NOT EXISTS dpt_test_bronze.bron")

# Save the flattened DataFrame as a Delta table
perceel_df_flat.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dpt_test_bronze.bron.kadastraal_data_delta")

print(" Flattened data successfully saved as Delta table.")
