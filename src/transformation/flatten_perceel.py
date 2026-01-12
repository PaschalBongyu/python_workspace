from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode

# Start Spark session
spark = SparkSession.builder.appName("GML Perceel Inlezen").getOrCreate()

# Base path
base_path = "/mnt/processeddata/unzipped_files/"

# Read the perceel GML file
df_perceel = spark.read.format("xml") \
    .option("rowTag", "kk:perceelMember") \
    .load(base_path + "kadastralekaart_perceel.gml")

# Flatten main fields
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

# Handle interior rings (holes), if present
perceel_df_with_holes = (
    perceel_df_flat
    .withColumn("interior_ring", explode(col("oz:Perceel.oz:begrenzingPerceel.gml:Polygon.gml:interior")))
    .withColumn("interior_coords", split(col("interior_ring.gml:LinearRing.gml:posList"), " "))
)

