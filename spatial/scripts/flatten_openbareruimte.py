# Start Spark sessie (indien nog niet gestart)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split

spark = SparkSession.builder.appName("GML Inlezen").getOrCreate()

# Basis pad naar de GML-bestanden (pas aan naar jouw locatie)
base_path = "/mnt/processeddata/unzipped_files/"

# Stap 0: Lees kadastralekaart_openbareruimtelabel.gml in met rowTag "cityObjectMember"
df = spark.read.format("xml") \
    .option("rowTag", "cityObjectMember") \
    .load(f"{base_path}kadastralekaart_openbareruimtelabel.gml")

# Stap 1: Maak een plat DataFrame met alle belangrijke velden van het label
label_df_flat = (
    df
    .withColumn("label_id", col("label._gml:id"))
    .withColumn("creation_date", col("label.creationDate._VALUE").cast("timestamp"))
    .withColumn("publicatiedatum", col("label.imgeo:LV-publicatiedatum").cast("timestamp"))
    .withColumn("bgt_status", col("label.imgeo:bgt-status._VALUE"))
    .withColumn("bronhouder", col("label.imgeo:bronhouder"))
    .withColumn("lokaalID", col("label.imgeo:identificatie.imgeo:NEN3610ID.imgeo:lokaalID"))
    .withColumn("namespace", col("label.imgeo:identificatie.imgeo:NEN3610ID.imgeo:namespace"))
    .withColumn("bag_opr", col("label.imgeo:identificatieBAGOPR"))
    .withColumn("naam", col("label.imgeo:openbareRuimteNaam.imgeo:Label.imgeo:tekst"))
    .withColumn("type", col("label.imgeo:openbareRuimteType._VALUE"))
    .withColumn("plus_status", col("label.imgeo:plus-status._VALUE"))
    .withColumn("hoogteligging", col("label.imgeo:relatieveHoogteligging").cast("double"))
    .withColumn("tijdstip_registratie", col("label.imgeo:tijdstipRegistratie").cast("timestamp"))
)

# Stap 2: Explode de posities-array zodat elke positie apart komt te staan
label_with_posities = (
    label_df_flat
    .withColumn("positie", explode(col("label.imgeo:openbareRuimteNaam.imgeo:Label.imgeo:positie")))
    .withColumn("hoek", col("positie.imgeo:Labelpositie.imgeo:hoek"))
    .withColumn("coord_str", col("positie.imgeo:Labelpositie.imgeo:plaatsingspunt.gml:Point.gml:pos"))
    .withColumn("coords", split(col("coord_str"), " "))
)



