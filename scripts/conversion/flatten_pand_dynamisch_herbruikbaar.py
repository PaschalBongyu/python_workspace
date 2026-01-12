from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer, udf, lit, current_date
from pyspark.sql.types import StringType
import logging

# -------------------------------
# 1. Start Spark sessie & logging
# -------------------------------
spark = SparkSession.builder.appName("GML Flattening Pipeline").getOrCreate()

# Logger instellen
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GML_Flattening")

# -------------------------------
# 2. Paden
# -------------------------------
input_path = "/mnt/processeddata/unzipped_files/"     # GML-bestanden map
output_path = "/mnt/processeddata/flattened_output/"  # map voor resultaten
dbutils.fs.mkdirs(output_path)                        # maak output map aan

# -------------------------------
# 3. Functie om DataFrame te flatten
# -------------------------------
def flatten_df(df):
    """
    Flatten structs naar kolommen en explode arrays.
    Werkt voor onbekend schema.
    """
    while True:
        struct_cols = [c[0] for c in df.dtypes if c[1].startswith("struct")]
        array_cols = [c[0] for c in df.dtypes if c[1].startswith("array")]

        if not struct_cols and not array_cols:
            break

        # Struct kolommen omzetten naar losse kolommen
        for c in struct_cols:
            expanded = [col(f"{c}.{f.name}").alias(f"{c}_{f.name}") 
                        for f in df.schema[c].dataType.fields]
            df = df.select("*", *expanded).drop(c)

        # Array kolommen "exploden" naar meerdere rijen
        for c in array_cols:
            df = df.withColumn(c, explode_outer(col(c)))

    return df

# -------------------------------
# 4. Functie om posList naar WKT te converteren
# -------------------------------
def poslist_to_wkt(poslist_string):
    """
    Zet een ruimte-gescheiden lijst van coordinaten om naar WKT POLYGON.
    """
    try:
        coords = list(map(float, poslist_string.strip().split()))
        points = [(coords[i], coords[i+1]) for i in range(0, len(coords), 2)]
        if points[0] != points[-1]:
            points.append(points[0])  # polygon sluiten
        text = ", ".join([f"{x} {y}" for x, y in points])
        return f"POLYGON(({text}))"
    except Exception:
        return None

wkt_udf = udf(poslist_to_wkt, StringType())

# -------------------------------
# 5. Verwerk alle GML-bestanden
# -------------------------------
# lijst van bestanden in DBFS
gml_files = [f.name for f in dbutils.fs.ls(input_path) if f.name.endswith(".gml")]

for gml_file in gml_files:
    logger.info(f"Verwerken van bestand: {gml_file}")
    try:
        # Lees GML bestand
        df = spark.read.format("xml") \
            .option("rowTag", "cityObjectMember") \
            .load(input_path + gml_file)

        logger.info("Origineel schema:")
        df.printSchema()

        # Flatten DataFrame
        df_flat = flatten_df(df)
        logger.info("Flattened schema:")
        df_flat.printSchema()

        # Zoek automatisch alle posList kolommen
        poslist_cols = [c for c in df_flat.columns if "posList" in c]

        # Zet posList om naar WKT
        for col_name in poslist_cols:
            df_flat = df_flat.withColumn(f"{col_name}_wkt", wkt_udf(col(col_name)))

        # Voeg metadata toe
        df_flat = df_flat.withColumn("source_file", lit(gml_file))
        df_flat = df_flat.withColumn("processing_date", current_date())

        # Sla op als Parquet
        output_file = output_path + gml_file.replace(".gml", ".parquet")
        df_flat.write.mode("overwrite").parquet(output_file)
        logger.info(f"Opgeslagen naar: {output_file}")

    except Exception as e:
        logger.error(f"Fout bij verwerken van {gml_file}: {e}")
        continue

logger.info("Alle bestanden zijn succesvol verwerkt.")
