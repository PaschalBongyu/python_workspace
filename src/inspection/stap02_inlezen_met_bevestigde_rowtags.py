# Inlezen van bestanden met bevestigde rowTags

# Lees kadastralekaart_pand.gml in met rowTag "cityObjectMember"
df_pand = spark.read.format("xml") \
    .option("rowTag", "cityObjectMember") \
    .load(base_path + "kadastralekaart_pand.gml")

# Lees kadastralekaart_perceel.gml in met rowTag "kk:perceelMember"
df_perceel = spark.read.format("xml") \
    .option("rowTag", "kk:perceelMember") \
    .load(base_path + "kadastralekaart_perceel.gml")
