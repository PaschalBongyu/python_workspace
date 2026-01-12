df = spark.read.format("xml") \
    .option("rowTag", "cityObjectMember") \
    .load("dbfs:/mnt/processeddata/unzipped_files/kadastralekaart_openbareruimtelabel.gml")

df.printSchema()
df.show(5, truncate=False)
