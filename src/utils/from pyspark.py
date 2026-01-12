from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import StringType

# Start Spark session
spark = SparkSession.builder.appName("GML Inlezen").getOrCreate()

# PROBLEEM: hardcoded pad en bestandsnaam
base_path = "/mnt/processeddata/unzipped_files/"
df_pand = spark.read.format("xml") \
    .option("rowTag", "cityObjectMember") \
    .load(base_path + "kadastralekaart_pand.gml")

# PROBLEEM: hardcoded explode op één specifieke array
df_exploded = df_pand.select(
    col("BuildingPart._gml:id").alias("pand_id"),
    explode("BuildingPart.imgeo:geometrie2dGrondvlak.gml:MultiSurface.gml:surfaceMember").alias("surfaceMember")
)

# PROBLEEM: hardcoded selectie van één veld
geom_df = df_exploded.select(
    "pand_id",
    col("surfaceMember.gml:Polygon.gml:exterior.gml:LinearRing.gml:posList._VALUE").alias("posList")
)

# WKT-conversiefunctie
def naar_wkt(poslist_string):
    try:
        coords = list(map(float, poslist_string.strip().split()))
        punten = [(coords[i], coords[i+1]) for i in range(0, len(coords), 2)]
        if punten[0] != punten[-1]:
            punten.append(punten[0])  # sluit polygon
        tekst = ", ".join([f"{x} {y}" for x, y in punten])
        return f"POLYGON(({tekst}))"
    except Exception:
        return None

wkt_udf = udf(naar_wkt, StringType())

# PROBLEEM: alleen toepasbaar op één kolom
geom_df = geom_df.withColumn("wkt_polygon", wkt_udf(col("posList")))

geom_df.select("pand_id", "wkt_polygon").show(5, truncate=False)
