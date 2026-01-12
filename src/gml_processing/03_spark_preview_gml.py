from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Step 3: List extracted files
files = dbutils.fs.ls("dbfs:/mnt/rawdata/gml_data")
print("Uitgepakte bestanden:", [f.name for f in files])

# Read a few lines of GML files
df_sample = spark.read.text("dbfs:/mnt/rawdata/gml_data/*.gml")
df_sample.show(10, truncate=False)

# Placeholder for XML parsing when rowTag is known (commented)
# xml_df = spark.read.format("xml")\
#     .option("rowTag", "<JOUW_ROW_TAG>")\
#     .load("dbfs:/mnt/rawdata/gml_data/*.gml")

print(
    "Download, extractie en inspectie voltooid. Alles DBFS-native, "
    "geen lokaal geheugen, geen os."
)
